#!/usr/bin/env python3
"""Stable latency (multi-sample, multi-round) + cache-hit benefit for the router.

Two things the first-pass bench did not do:

1. STABLE LATENCY
   Within a chain, samples run strictly sequentially (no self-contention), with
   many samples and optional repeated rounds spaced by --gap seconds, so p50/p95
   /p99 and jitter are trustworthy. Chains still run in parallel.

2. CACHE BENEFIT
   The router has a response cache but exposes no per-call hit header, so we
   measure it by latency: for each (chain) we pick a RANDOM historical block
   (very unlikely to be warm) and call eth_getBlockByNumber(block,false)
   K+1 times back-to-back. Call #1 = cold (cache miss / upstream), calls #2..K =
   warm (should be served from cache). We report cold vs warm and the speedup.
   For contrast we do the same with the moving "latest" tag.

Examples
--------
  python3 latency_cache.py --chains traffic --samples 12
  python3 latency_cache.py --chains majors --samples 20 --rounds 3 --gap 20
  python3 latency_cache.py --chains 1,56,8453 --cache-reps 6
"""
import argparse
import concurrent.futures as cf
import csv
import json
import random
import statistics
import sys
import time
from datetime import datetime, timezone

from rpcutil import rpc_call, pct, load_chains

MAJORS = {1, 56, 137, 8453, 42161, 10, 43114, 250, 100, 324, 59144, 534352, 5000, 81457}

# methods used for the stable-latency sweep (mix of simple + complex, all generic)
LAT_METHODS = [
    {"id": "eth_chainId",          "method": "eth_chainId",          "params": []},
    {"id": "eth_blockNumber",      "method": "eth_blockNumber",      "params": []},
    {"id": "eth_gasPrice",         "method": "eth_gasPrice",         "params": []},
    {"id": "eth_getBalance",       "method": "eth_getBalance",
     "params": ["0x0000000000000000000000000000000000000000", "latest"]},
    {"id": "eth_getBlockByNumber", "method": "eth_getBlockByNumber", "params": ["latest", True]},
]


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
    return chosen[:limit] if limit else chosen


def stable_latency(chain, samples, rounds, gap, timeout):
    """Sequential samples per method, optionally repeated over `rounds`."""
    cid = chain["chainId"]
    acc = {m["id"]: {"lat": [], "ok": 0, "total": 0, "err": None} for m in LAT_METHODS}
    for rnd in range(rounds):
        for spec in LAT_METHODS:
            a = acc[spec["id"]]
            for i in range(samples):
                r = rpc_call(cid, spec, timeout=timeout, req_id=i + 1)
                a["total"] += 1
                if r["ok"]:
                    a["ok"] += 1
                    a["lat"].append(r["latency"])
                elif a["err"] is None:
                    a["err"] = r["error"]
        if rnd + 1 < rounds and gap:
            time.sleep(gap)
    out = {}
    for mid, a in acc.items():
        lat = a["lat"]
        out[mid] = {
            "ok": a["ok"], "total": a["total"],
            "min": min(lat) if lat else None,
            "p50": pct(lat, 50), "p90": pct(lat, 90),
            "p95": pct(lat, 95), "p99": pct(lat, 99),
            "max": max(lat) if lat else None,
            "stdev": statistics.pstdev(lat) if len(lat) > 1 else 0.0,
            "err": a["err"],
        }
    return cid, out


def cache_probe(chain, reps, timeout, rng):
    """cold vs warm latency for a fixed (cacheable) request and for 'latest'."""
    cid = chain["chainId"]
    head = chain.get("head") or 0
    # a fixed, immutable historical block, randomized so it is very likely cold
    hi = max(1000, min(head - 10, 5_000_000)) if head > 2000 else max(1, head - 1)
    blk = rng.randint(1000, hi) if hi > 1000 else 1
    fixed_spec = {"id": "fixed_block", "method": "eth_getBlockByNumber",
                  "params": [hex(blk), False]}
    latest_spec = {"id": "latest_block", "method": "eth_getBlockByNumber",
                   "params": ["latest", False]}

    def run(spec):
        lats = []
        for i in range(reps + 1):      # 1 cold + reps warm
            r = rpc_call(cid, spec, timeout=timeout, req_id=i + 1)
            lats.append(r["latency"] if r["ok"] else None)
        oks = [x for x in lats if x is not None]
        cold = lats[0]
        warm = oks[1:] if len(oks) > 1 else []
        return {
            "cold": cold,
            "warm_med": statistics.median(warm) if warm else None,
            "warm_min": min(warm) if warm else None,
            "ok": len(oks), "reps": reps + 1,
        }

    return cid, {"fixed": run(fixed_spec), "latest": run(latest_spec), "block": blk}


def fmt(x):
    return "   -  " if x is None else "%6.0f" % (x * 1000)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--chains", default="traffic")
    ap.add_argument("--samples", type=int, default=12, help="samples per method per round (default 12)")
    ap.add_argument("--rounds", type=int, default=1, help="repeat the whole sweep N times (default 1)")
    ap.add_argument("--gap", type=float, default=15.0, help="seconds between rounds (default 15)")
    ap.add_argument("--cache-reps", type=int, default=5, help="warm repeats in cache probe (default 5)")
    ap.add_argument("--concurrency", type=int, default=12)
    ap.add_argument("--timeout", type=float, default=15.0)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--seed", type=int, default=0, help="RNG seed for cache block picks (0=time)")
    ap.add_argument("--out", default="results")
    args = ap.parse_args()

    items = load_chains()
    chains = select_chains(items, args.chains, args.limit)
    name_by_id = {x["chainId"]: x["name"] for x in items}
    if not chains:
        print("no chains matched:", args.chains, file=sys.stderr)
        return 1

    print("=== stable latency + cache probe ===")
    print("chains=%d  samples=%d x rounds=%d  cache_reps=%d  concurrency=%d" % (
        len(chains), args.samples, args.rounds, args.cache_reps, args.concurrency))

    # ---- stable latency sweep ----
    t0 = time.perf_counter()
    lat_res = {}
    with cf.ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        futs = [ex.submit(stable_latency, c, args.samples, args.rounds, args.gap, args.timeout) for c in chains]
        for fut in cf.as_completed(futs):
            cid, out = fut.result()
            lat_res[cid] = out
    print("latency sweep done in %.1fs" % (time.perf_counter() - t0))

    # ---- cache probe ----
    rng = random.Random(args.seed or None)
    t1 = time.perf_counter()
    cache_res = {}
    with cf.ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        # distinct Random per chain for reproducibility-independence
        futs = [ex.submit(cache_probe, c, args.cache_reps, args.timeout,
                          random.Random((args.seed or int(time.time())) + c["chainId"])) for c in chains]
        for fut in cf.as_completed(futs):
            cid, out = fut.result()
            cache_res[cid] = out
    print("cache probe done in %.1fs\n" % (time.perf_counter() - t1))

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    _write_outputs(args, ts, lat_res, cache_res, name_by_id)
    _report(lat_res, cache_res, name_by_id)
    return 0


def _write_outputs(args, ts, lat_res, cache_res, name_by_id):
    raw = "%s_latcache_raw_%s.json" % (args.out, ts)
    with open(raw, "w") as f:
        json.dump({"ts": ts, "latency": lat_res, "cache": cache_res}, f, indent=2, default=str)
    lat_csv = "%s_latency_%s.csv" % (args.out, ts)
    with open(lat_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["chainId", "name", "method", "ok", "total",
                    "min_ms", "p50_ms", "p90_ms", "p95_ms", "p99_ms", "max_ms", "stdev_ms", "err"])
        for cid, pm in lat_res.items():
            for mid, m in pm.items():
                w.writerow([cid, name_by_id.get(cid, ""), mid, m["ok"], m["total"]]
                           + ["" if m[k] is None else "%.0f" % (m[k] * 1000)
                              for k in ("min", "p50", "p90", "p95", "p99", "max", "stdev")]
                           + [m["err"] or ""])
    cache_csv = "%s_cache_%s.csv" % (args.out, ts)
    with open(cache_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["chainId", "name", "variant", "block", "cold_ms", "warm_med_ms",
                    "warm_min_ms", "speedup_x", "saved_ms", "ok", "reps"])
        for cid, c in cache_res.items():
            for variant in ("fixed", "latest"):
                v = c[variant]
                cold, warm = v["cold"], v["warm_med"]
                speed = (cold / warm) if (cold and warm) else ""
                saved = ((cold - warm) * 1000) if (cold and warm) else ""
                w.writerow([cid, name_by_id.get(cid, ""), variant,
                            c["block"] if variant == "fixed" else "",
                            "" if cold is None else "%.0f" % (cold * 1000),
                            "" if warm is None else "%.0f" % (warm * 1000),
                            "" if v["warm_min"] is None else "%.0f" % (v["warm_min"] * 1000),
                            "" if speed == "" else "%.2f" % speed,
                            "" if saved == "" else "%.0f" % saved,
                            v["ok"], v["reps"]])
    print("wrote %s" % raw)
    print("wrote %s" % lat_csv)
    print("wrote %s" % cache_csv)


def _report(lat_res, cache_res, name_by_id):
    mids = [m["id"] for m in LAT_METHODS]
    print("\n=== stable latency per method (aggregated across %d chains) ===" % len(lat_res))
    print("  %-24s %8s %8s %8s %8s %9s" % ("method", "p50_ms", "p90_ms", "p95_ms", "p99_ms", "jitter"))
    for mid in mids:
        p50s = [pm[mid]["p50"] for pm in lat_res.values() if pm[mid]["p50"] is not None]
        p90s = [pm[mid]["p90"] for pm in lat_res.values() if pm[mid]["p90"] is not None]
        p95s = [pm[mid]["p95"] for pm in lat_res.values() if pm[mid]["p95"] is not None]
        p99s = [pm[mid]["p99"] for pm in lat_res.values() if pm[mid]["p99"] is not None]
        jit = [pm[mid]["stdev"] for pm in lat_res.values() if pm[mid]["stdev"] is not None]
        print("  %-24s %s %s %s %s %s" % (
            mid, fmt(pct(p50s, 50)), fmt(pct(p90s, 50)), fmt(pct(p95s, 50)),
            fmt(pct(p99s, 50)), fmt(statistics.mean(jit) if jit else None)))

    # cache benefit
    print("\n=== cache benefit: cold (miss) vs warm (hit), eth_getBlockByNumber ===")
    print("  variant        cold_p50  warm_p50  median_speedup   chains_with_>1.3x")
    for variant, label in (("fixed", "fixed-block"), ("latest", "latest-tag ")):
        colds, warms, speeds = [], [], []
        nbenef = 0
        for c in cache_res.values():
            v = c[variant]
            if v["cold"] is not None:
                colds.append(v["cold"])
            if v["warm_med"] is not None:
                warms.append(v["warm_med"])
            if v["cold"] and v["warm_med"]:
                s = v["cold"] / v["warm_med"]
                speeds.append(s)
                if s >= 1.3:
                    nbenef += 1
        print("  %-13s %s  %s     %5.2fx          %d/%d" % (
            label, fmt(pct(colds, 50)), fmt(pct(warms, 50)),
            (statistics.median(speeds) if speeds else 0), nbenef, len(cache_res)))

    # biggest cache winners (fixed-block)
    wins = []
    for cid, c in cache_res.items():
        v = c["fixed"]
        if v["cold"] and v["warm_med"]:
            wins.append((cid, v["cold"], v["warm_med"], v["cold"] / v["warm_med"]))
    wins.sort(key=lambda r: -r[3])
    print("\n=== biggest cache speedups (fixed-block, top 10) ===")
    print("  %-10s %8s %8s %8s  %s" % ("chainId", "cold_ms", "warm_ms", "speedup", "name"))
    for cid, cold, warm, sp in wins[:10]:
        print("  %-10s %s %s %7.1fx  %s" % (cid, fmt(cold), fmt(warm), sp, name_by_id.get(cid, "")[:28]))

    # high-jitter chains (unstable latency) on eth_blockNumber
    print("\n=== least stable chains (highest jitter on eth_blockNumber, top 10) ===")
    j = [(cid, pm["eth_blockNumber"]["stdev"], pm["eth_blockNumber"]["p50"])
         for cid, pm in lat_res.items() if pm["eth_blockNumber"]["stdev"] is not None]
    j.sort(key=lambda r: -(r[1] or 0))
    print("  %-10s %9s %8s  %s" % ("chainId", "jitter_ms", "p50_ms", "name"))
    for cid, st, p50 in j[:10]:
        print("  %-10s %s %s  %s" % (cid, fmt(st), fmt(p50), name_by_id.get(cid, "")[:28]))


if __name__ == "__main__":
    sys.exit(main())
