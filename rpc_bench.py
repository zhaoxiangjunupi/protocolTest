#!/usr/bin/env python3
"""Latency + availability benchmark for the cryptostack RPC router.

For each selected chain it runs each selected JSON-RPC method `--samples` times
through BASE/rpc/{chainId}, records latency and success/failure, then aggregates
per (chain, method) and per chain.

Examples
--------
  # quick smoke test on the chains that actually get traffic
  python3 rpc_bench.py --chains traffic --methods all --samples 3

  # full sweep, simple methods only, 5 samples, 24 workers
  python3 rpc_bench.py --chains all --methods simple --samples 5 --concurrency 24

  # only the known major chains, complex methods
  python3 rpc_bench.py --chains majors --methods complex --samples 4
"""
import argparse
import concurrent.futures as cf
import csv
import json
import sys
import time
from datetime import datetime, timezone

from rpcutil import (
    SIMPLE_METHODS, COMPLEX_METHODS, ALL_METHODS,
    rpc_call, pct, load_chains,
)

MAJORS = {1, 56, 137, 8453, 42161, 10, 43114, 250, 100, 324, 59144, 534352, 5000, 81457}


def select_methods(name):
    return {"simple": SIMPLE_METHODS, "complex": COMPLEX_METHODS, "all": ALL_METHODS}[name]


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


def bench_chain(chain, methods, samples, timeout):
    """Run all methods*samples for one chain. Returns per-method aggregates."""
    cid = chain["chainId"]
    per_method = {}
    for spec in methods:
        lats, errors = [], []
        ok = 0
        correct = None  # eth_chainId correctness, if applicable
        for i in range(samples):
            r = rpc_call(cid, spec, timeout=timeout, req_id=i + 1)
            if r["ok"]:
                ok += 1
                lats.append(r["latency"])
                if spec["id"] == "eth_chainId":
                    try:
                        correct = (int(r["result"], 16) == cid)
                    except (ValueError, TypeError):
                        correct = False
            else:
                errors.append(r["error"])
        per_method[spec["id"]] = {
            "ok": ok,
            "total": samples,
            "success_rate": ok / samples if samples else 0.0,
            "lat_min": min(lats) if lats else None,
            "lat_avg": (sum(lats) / len(lats)) if lats else None,
            "lat_p50": pct(lats, 50),
            "lat_p95": pct(lats, 95),
            "lat_max": max(lats) if lats else None,
            "chainId_correct": correct,
            "sample_error": errors[0] if errors else None,
        }
    return cid, per_method


def fmt_ms(x):
    return "   -  " if x is None else "%6.0f" % (x * 1000)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--chains", default="traffic",
                    help="'all' | 'traffic' | 'majors' | comma-list of chainIds (default: traffic)")
    ap.add_argument("--methods", default="all", choices=["simple", "complex", "all"])
    ap.add_argument("--samples", type=int, default=3, help="requests per (chain,method) (default 3)")
    ap.add_argument("--concurrency", type=int, default=16, help="chains tested in parallel (default 16)")
    ap.add_argument("--timeout", type=float, default=15.0, help="per-request timeout seconds (default 15)")
    ap.add_argument("--limit", type=int, default=0, help="cap number of chains (0 = no cap)")
    ap.add_argument("--out", default="results", help="output dir/prefix (default: results)")
    args = ap.parse_args()

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
        futs = {ex.submit(bench_chain, c, methods, args.samples, args.timeout): c for c in chains}
        for fut in cf.as_completed(futs):
            cid, per_method = fut.result()
            results[cid] = per_method
            done += 1
            # live progress on one line
            avail = sum(1 for m in per_method.values() if m["success_rate"] == 1.0)
            print("  [%3d/%3d] chain %-10s  methods_full_ok=%d/%d  %s" % (
                done, len(chains), cid, avail, len(methods), name_by_id.get(cid, "")[:28]))
    elapsed = time.perf_counter() - t0
    print("\nwall time: %.1fs" % elapsed)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    # ---- write raw json ----
    raw_path = "%s_raw_%s.json" % (args.out, ts)
    with open(raw_path, "w") as f:
        json.dump({
            "meta": {"ts": ts, "chains": args.chains, "methods": args.methods,
                     "samples": args.samples, "timeout": args.timeout,
                     "wall_seconds": round(elapsed, 2)},
            "results": results,
        }, f, indent=2, default=str)

    # ---- write per (chain,method) csv ----
    csv_path = "%s_summary_%s.csv" % (args.out, ts)
    with open(csv_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["chainId", "name", "active_endpoints", "method", "ok", "total",
                    "success_rate", "lat_min_ms", "lat_avg_ms", "lat_p50_ms",
                    "lat_p95_ms", "lat_max_ms", "chainId_correct", "sample_error"])
        for cid, pm in results.items():
            for mid, m in pm.items():
                w.writerow([
                    cid, name_by_id.get(cid, ""), tier_by_id.get(cid, ""), mid,
                    m["ok"], m["total"], "%.3f" % m["success_rate"],
                    "" if m["lat_min"] is None else "%.0f" % (m["lat_min"] * 1000),
                    "" if m["lat_avg"] is None else "%.0f" % (m["lat_avg"] * 1000),
                    "" if m["lat_p50"] is None else "%.0f" % (m["lat_p50"] * 1000),
                    "" if m["lat_p95"] is None else "%.0f" % (m["lat_p95"] * 1000),
                    "" if m["lat_max"] is None else "%.0f" % (m["lat_max"] * 1000),
                    m["chainId_correct"], m["sample_error"] or "",
                ])

    # ---- console report ----
    print_report(results, methods, name_by_id, tier_by_id)
    print("\nwrote %s" % raw_path)
    print("wrote %s" % csv_path)
    return 0


def print_report(results, methods, name_by_id, tier_by_id):
    mids = [m["id"] for m in methods]

    # per-method rollup across chains
    print("\n=== per-method availability + latency (across %d chains) ===" % len(results))
    print("  %-26s %7s  %8s %8s %8s" % ("method", "avail%", "avg_ms", "p50_ms", "p95_ms"))
    for mid in mids:
        oks = tot = 0
        lat_avgs = []
        for pm in results.values():
            m = pm[mid]
            oks += m["ok"]; tot += m["total"]
            if m["lat_avg"] is not None:
                lat_avgs.append(m["lat_avg"])
        avail = (oks / tot * 100) if tot else 0
        print("  %-26s %6.1f%%  %s %s %s" % (
            mid, avail,
            fmt_ms(sum(lat_avgs) / len(lat_avgs) if lat_avgs else None),
            fmt_ms(pct(lat_avgs, 50)), fmt_ms(pct(lat_avgs, 95))))

    # per-chain overall availability (fraction of method-calls that succeeded)
    rows = []
    for cid, pm in results.items():
        oks = sum(m["ok"] for m in pm.values())
        tot = sum(m["total"] for m in pm.values())
        avg_lats = [m["lat_avg"] for m in pm.values() if m["lat_avg"] is not None]
        rows.append((cid, oks / tot if tot else 0,
                     sum(avg_lats) / len(avg_lats) if avg_lats else None))

    # worst availability first, then slowest
    print("\n=== chains needing attention (availability < 100%, worst first) ===")
    bad = sorted([r for r in rows if r[1] < 1.0], key=lambda r: (r[1], -(r[2] or 0)))
    if not bad:
        print("  none — every selected chain answered every call")
    else:
        print("  %-10s %7s %9s  %s" % ("chainId", "avail%", "avg_ms", "name"))
        for cid, avail, avg in bad:
            print("  %-10s %6.1f%% %s  %s" % (cid, avail * 100, fmt_ms(avg), name_by_id.get(cid, "")[:30]))

    # slowest fully-available chains
    good = sorted([r for r in rows if r[1] == 1.0 and r[2] is not None],
                  key=lambda r: -r[2])[:10]
    print("\n=== slowest fully-available chains (avg latency, top 10) ===")
    print("  %-10s %9s %4s  %s" % ("chainId", "avg_ms", "act", "name"))
    for cid, _, avg in good:
        print("  %-10s %s %4d  %s" % (cid, fmt_ms(avg), tier_by_id.get(cid, 0), name_by_id.get(cid, "")[:30]))


if __name__ == "__main__":
    sys.exit(main())
