#!/usr/bin/env python3
"""Latency + availability benchmark for the cryptostack RPC router.

For each selected chain it runs each selected JSON-RPC method `--samples` times
through BASE/rpc/{chainId}, records latency and success/failure (with the error
text and its classified cause), then aggregates per (chain, method) and per chain.

Method groups: simple (metadata), complex (latest block / state / logs) and
history (eth_getBlockByNumber at three depths below the live head: recent,
mid = beyond a 1024-block window, early = first quarter of the chain). Each
history sample uses a different random block so it always reaches an upstream.

Examples
--------
  # quick smoke test on the chains that actually get traffic
  python3 rpc_bench.py --chains traffic --methods all --samples 3

  # full sweep, simple methods only, 5 samples, 24 workers
  python3 rpc_bench.py --chains all --methods simple --samples 5 --concurrency 24

  # only the known major chains, complex methods
  python3 rpc_bench.py --chains majors --methods complex --samples 4

  # historical-block availability only
  python3 rpc_bench.py --chains all --methods history --samples 3
"""
import argparse
import concurrent.futures as cf
import csv
import json
import random
import sys
import time
from collections import Counter
from datetime import datetime, timezone

from rpcutil import (
    SIMPLE_METHODS, COMPLEX_METHODS, HISTORY_METHODS, ALL_METHODS, METHOD_GROUP,
    rpc_call, pct, load_chains, history_params, classify_error, ERROR_KINDS,
)

MAJORS = {1, 56, 137, 8453, 42161, 10, 43114, 250, 100, 324, 59144, 534352, 5000, 81457}


def select_methods(name):
    return {"simple": SIMPLE_METHODS, "complex": COMPLEX_METHODS,
            "history": HISTORY_METHODS, "all": ALL_METHODS}[name]


def select_chains(items, spec, limit):
    if spec == "all":
        chosen = items
    elif spec == "traffic":
        chosen = [x for x in items if (x.get("ingressTotal", 0) or 0) > 0]
    elif spec == "majors":
        chosen = [x for x in items if x["chainId"] in MAJORS]
    else:
        ids = {int(s) for s in spec.split(",") if s.strip()}
        chosen = [x for x in items if x["chainId"] in ids]
    if limit:
        chosen = chosen[:limit]
    return chosen


def live_head(chain, timeout):
    """Current head via eth_blockNumber; falls back to the catalog snapshot."""
    r = rpc_call(chain["chainId"], {"method": "eth_blockNumber", "params": []}, timeout=timeout)
    if r["ok"]:
        try:
            return int(r["result"], 16), "live"
        except (ValueError, TypeError):
            pass
    return (chain.get("head") or 0), "snapshot"


def bench_chain(chain, methods, samples, timeout, seed):
    """Run all methods*samples for one chain. Returns per-method aggregates."""
    cid = chain["chainId"]
    per_method = {}
    head = head_src = None
    rng = random.Random(seed + cid)
    if any(METHOD_GROUP[m["id"]] == "history" for m in methods):
        head, head_src = live_head(chain, timeout)
    for spec in methods:
        lats, errors, blocks = [], [], []
        ok = total = 0
        correct = None  # eth_chainId correctness, if applicable
        skipped = None
        for i in range(samples):
            call = spec
            if METHOD_GROUP[spec["id"]] == "history":
                params = history_params(spec, head or 0, rng)
                if params is None:
                    skipped = "chain too short (head=%s)" % head
                    break
                call = dict(spec, params=params)
                blocks.append(int(params[0], 16))
            r = rpc_call(cid, call, timeout=timeout, req_id=i + 1)
            total += 1
            if r["ok"] and spec["id"] == "eth_chainId":
                try:
                    correct = (int(r["result"], 16) == cid)
                except (ValueError, TypeError):
                    correct = False
                if not correct:
                    r = dict(r, ok=False, error="wrong-chain: got %s" % r["result"])
            if r["ok"]:
                ok += 1
                lats.append(r["latency"])
            else:
                errors.append(r["error"])
        kinds = Counter(classify_error(e) for e in errors)
        top_kind = kinds.most_common(1)[0][0] if kinds else None
        per_method[spec["id"]] = {
            "group": METHOD_GROUP[spec["id"]],
            "ok": ok,
            "total": total,
            "success_rate": ok / total if total else None,
            "lat_min": min(lats) if lats else None,
            "lat_avg": (sum(lats) / len(lats)) if lats else None,
            "lat_p50": pct(lats, 50),
            "lat_p95": pct(lats, 95),
            "lat_max": max(lats) if lats else None,
            "chainId_correct": correct,
            # sample_error is an example of the most common cause
            "sample_error": next((e for e in errors if classify_error(e) == top_kind), None),
            "error_kind": top_kind,
            "errors": [{"error": e, "kind": classify_error(e), "n": n}
                       for e, n in Counter(errors).most_common()],
            "blocks": blocks or None,
            "skipped": skipped,
        }
    return cid, {"head": head, "head_source": head_src, "methods": per_method}


def fmt_ms(x):
    return "   -  " if x is None else "%6.0f" % (x * 1000)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--chains", default="traffic",
                    help="'all' | 'traffic' | 'majors' | comma-list of chainIds (default: traffic)")
    ap.add_argument("--methods", default="all", choices=["simple", "complex", "history", "all"],
                    help="simple | complex | history (3 historical-block depths) | all (default)")
    ap.add_argument("--samples", type=int, default=3, help="requests per (chain,method) (default 3)")
    ap.add_argument("--concurrency", type=int, default=16, help="chains tested in parallel (default 16)")
    ap.add_argument("--timeout", type=float, default=15.0, help="per-request timeout seconds (default 15)")
    ap.add_argument("--limit", type=int, default=0, help="cap number of chains (0 = no cap)")
    ap.add_argument("--seed", type=int, default=0, help="RNG seed for historical block picks (0=time)")
    ap.add_argument("--out", default="results", help="output dir/prefix (default: results)")
    args = ap.parse_args()
    seed = args.seed or int(time.time())

    items = load_chains()
    methods = select_methods(args.methods)
    chains = select_chains(items, args.chains, args.limit)
    name_by_id = {x["chainId"]: x["name"] for x in items}
    tier_by_id = {x["chainId"]: (x.get("active", 0) or 0) for x in items}

    if not chains:
        print("no chains matched selection:", args.chains, file=sys.stderr)
        return 1

    total_calls = len(chains) * len(methods) * args.samples
    print("=== RPC bench ===")
    print("chains=%d  methods=%d (%s)  samples=%d  -> %d calls  | concurrency=%d timeout=%ss" % (
        len(chains), len(methods), args.methods, args.samples, total_calls, args.concurrency, args.timeout))
    print()

    t0 = time.perf_counter()
    results = {}
    done = 0
    with cf.ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        futs = {ex.submit(bench_chain, c, methods, args.samples, args.timeout, seed): c for c in chains}
        for fut in cf.as_completed(futs):
            cid, res = fut.result()
            results[cid] = res
            done += 1
            # live progress on one line
            pm = res["methods"]
            avail = sum(1 for m in pm.values() if m["success_rate"] == 1.0)
            ran = sum(1 for m in pm.values() if m["total"])
            print("  [%3d/%3d] chain %-10s  methods_full_ok=%d/%d  %s" % (
                done, len(chains), cid, avail, ran, name_by_id.get(cid, "")[:28]))
    elapsed = time.perf_counter() - t0
    print("\nwall time: %.1fs" % elapsed)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    # ---- write raw json ----
    raw_path = "%s_raw_%s.json" % (args.out, ts)
    with open(raw_path, "w") as f:
        json.dump({
            "meta": {"ts": ts, "chains": args.chains, "methods": args.methods,
                     "samples": args.samples, "timeout": args.timeout, "seed": seed,
                     "total_calls": sum(m["total"] for r in results.values() for m in r["methods"].values()),
                     "wall_seconds": round(elapsed, 2)},
            "results": results,
        }, f, indent=2, default=str)

    # ---- write per (chain,method) csv ----
    csv_path = "%s_summary_%s.csv" % (args.out, ts)
    with open(csv_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["chainId", "name", "active_endpoints", "group", "method", "ok", "total",
                    "success_rate", "lat_min_ms", "lat_avg_ms", "lat_p50_ms",
                    "lat_p95_ms", "lat_max_ms", "chainId_correct", "error_kind", "sample_error"])
        for cid, res in results.items():
            for mid, m in res["methods"].items():
                w.writerow([
                    cid, name_by_id.get(cid, ""), tier_by_id.get(cid, ""), m["group"], mid,
                    m["ok"], m["total"], "" if m["success_rate"] is None else "%.3f" % m["success_rate"],
                    "" if m["lat_min"] is None else "%.0f" % (m["lat_min"] * 1000),
                    "" if m["lat_avg"] is None else "%.0f" % (m["lat_avg"] * 1000),
                    "" if m["lat_p50"] is None else "%.0f" % (m["lat_p50"] * 1000),
                    "" if m["lat_p95"] is None else "%.0f" % (m["lat_p95"] * 1000),
                    "" if m["lat_max"] is None else "%.0f" % (m["lat_max"] * 1000),
                    m["chainId_correct"], m["error_kind"] or "", m["sample_error"] or m["skipped"] or "",
                ])

    # ---- console report ----
    print_report(results, methods, name_by_id, tier_by_id)
    print("\nwrote %s" % raw_path)
    print("wrote %s" % csv_path)
    return 0


def print_report(results, methods, name_by_id, tier_by_id):
    mids = [m["id"] for m in methods]
    pms = {cid: r["methods"] for cid, r in results.items()}

    # per-method rollup across chains
    print("\n=== per-method availability + latency (across %d chains) ===" % len(results))
    print("  %-8s %-24s %7s %11s  %8s %8s %8s" % ("group", "method", "avail%", "ok/total", "avg_ms", "p50_ms", "p95_ms"))
    for mid in mids:
        oks = tot = 0
        lat_avgs = []
        for pm in pms.values():
            m = pm[mid]
            oks += m["ok"]; tot += m["total"]
            if m["lat_avg"] is not None:
                lat_avgs.append(m["lat_avg"])
        avail = (oks / tot * 100) if tot else 0
        print("  %-8s %-24s %6.1f%% %11s  %s %s %s" % (
            METHOD_GROUP[mid], mid, avail, "%d/%d" % (oks, tot),
            fmt_ms(sum(lat_avgs) / len(lat_avgs) if lat_avgs else None),
            fmt_ms(pct(lat_avgs, 50)), fmt_ms(pct(lat_avgs, 95))))

    # every failing (chain, method), grouped by error kind
    fails = []
    for cid, pm in pms.items():
        for mid, m in pm.items():
            if m["total"] and m["ok"] < m["total"]:
                fails.append((m["error_kind"], cid, mid, m))
    print("\n=== failures by cause (%d chain×method) ===" % len(fails))
    if not fails:
        print("  none — every selected chain answered every call")
    for kind in sorted({f[0] for f in fails}, key=lambda k: list(ERROR_KINDS).index(k)):
        label, desc = ERROR_KINDS[kind]
        rows = sorted([f for f in fails if f[0] == kind], key=lambda f: (f[2], f[1]))
        print("  -- %s [%s] · %d  (%s)" % (label, kind, len(rows), desc))
        for _, cid, mid, m in rows:
            print("     %-10s %-22s %d/%d  %-26s %s" % (
                cid, mid, m["ok"], m["total"], name_by_id.get(cid, "")[:26], (m["sample_error"] or "")[:90]))

    # per-chain overall availability on the regular (latest-block) methods
    rows = []
    for cid, pm in pms.items():
        reg = [m for m in pm.values() if m["group"] != "history"]
        oks = sum(m["ok"] for m in reg)
        tot = sum(m["total"] for m in reg)
        avg_lats = [m["lat_avg"] for m in reg if m["lat_avg"] is not None]
        if tot:
            rows.append((cid, oks / tot, sum(avg_lats) / len(avg_lats) if avg_lats else None))

    # slowest fully-available chains
    good = sorted([r for r in rows if r[1] == 1.0 and r[2] is not None],
                  key=lambda r: -r[2])[:10]
    print("\n=== slowest fully-available chains (avg latency, regular methods, top 10) ===")
    print("  %-10s %9s %4s  %s" % ("chainId", "avg_ms", "act", "name"))
    for cid, _, avg in good:
        print("  %-10s %s %4d  %s" % (cid, fmt_ms(avg), tier_by_id.get(cid, 0), name_by_id.get(cid, "")[:30]))


if __name__ == "__main__":
    sys.exit(main())
