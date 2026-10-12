#!/usr/bin/env python3
"""Stable latency (multi-sample, multi-round) + cache-hit benefit for the router.

Two things the first-pass bench did not do:

1. STABLE LATENCY
   Within a chain, samples run strictly sequentially (no self-contention), with
   many samples and optional repeated rounds spaced by --gap seconds, so p50/p95
   /p99 and jitter are trustworthy. Chains still run in parallel.

2. CACHE BENEFIT
   The router has a response cache but exposes no per-call hit header, so we
   measure it by latency: for each chain we pick --cache-blocks RANDOM recent
   blocks (200-800 below the live head: very unlikely to be warm, but still
   inside the window pruned upstreams serve) and call eth_getBlockByNumber(block,
   false) K+1 times back-to-back per block. Call #1 = cold (cache miss /
   upstream), calls #2..K+1 = warm (should be served from cache). Cold = median
   of the blocks' first calls, warm = median of all later calls; we report cold
   vs warm and the speedup. The cold calls are paced by --interval.
   For contrast we do the same with the moving "latest" tag.
   Every call's error text and classified cause are kept; a null block counts
   as a failure. When call #1 fails we record why, how long it took, and which
   later call first succeeded (i.e. the router did not fail over by itself).

3. WHICH METHODS ARE CACHED
   Each method of CACHE_METHODS is called 1+--method-reps times back-to-back.
   A repeat is a hit when it takes at most the chain's cache-hit latency (the
   fixed-block warm median) + max(2.5 ms, half of it); the method is cached on
   the chain when most repeats are hits. Uncached methods reach an upstream on
   every call (some upstreams answer in 5-10 ms, so the margin is kept tight).

4. LATEST CACHE LIFETIME
   eth_blockNumber is polled every --ttl-poll seconds for up to --ttl-window
   seconds. A call slower than the geometric mean of the chain's hit and cold
   latencies (at least the hit limit above), or returning another block number
   than the call before, reached an upstream (the cached answer had expired);
   back-to-back upstream calls count as one refresh. Lifetime = median
   time from one refresh's end to the next one's start, minus half a poll interval
   (the answer expired somewhere in the last one). No second refresh in the
   window -> a lower bound; no hit at all -> not cached.

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
import math
import random
import statistics
import sys
import time
from datetime import datetime, timezone

from rpcutil import (rpc_call, pct, load_chains, live_head, classify_error, ERROR_KINDS, conn_stats, Pacer,
                     ADVANCED_METHODS, ZERO_ADDR)

MAJORS = {1, 56, 137, 8453, 42161, 10, 43114, 250, 100, 324, 59144, 534352, 5000, 81457}
BENEFIT_X = 1.3      # fixed-block speedup below this: the chain's cache does not work, skip tests 3 and 4
# a call is a cache hit when it takes <= the chain's hit latency + max(this, half of it) (s):
# 99.8% of the fixed-block hits fall within, only 0.8% of the cold calls do (2026-10-12 run)
HIT_SLACK = 0.0025

_ADV = {m["id"]: m for m in ADVANCED_METHODS}
# methods checked for "does the router cache it"; all answers are small, so a hit
# is not slowed down by the payload. block_fixed gets a fresh random block per chain.
CACHE_METHODS = [
    {"id": "eth_chainId",             "method": "eth_chainId",             "params": []},
    {"id": "net_version",             "method": "net_version",             "params": []},
    {"id": "web3_clientVersion",      "method": "web3_clientVersion",      "params": []},
    {"id": "eth_blockNumber",         "method": "eth_blockNumber",         "params": []},
    {"id": "eth_gasPrice",            "method": "eth_gasPrice",            "params": []},
    {"id": "eth_getBalance",          "method": "eth_getBalance",          "params": [ZERO_ADDR, "latest"]},
    {"id": "eth_getTransactionCount", "method": "eth_getTransactionCount", "params": [ZERO_ADDR, "latest"]},
    {"id": "eth_getLogs", "method": "eth_getLogs", "label": "eth_getLogs (latest, 过滤地址)",
     "params": [{"fromBlock": "latest", "toBlock": "latest", "address": ZERO_ADDR}]},
    {"id": "block_latest", "method": "eth_getBlockByNumber", "label": "eth_getBlockByNumber (latest)",
     "params": ["latest", False], "nonnull": True},
    {"id": "block_fixed", "method": "eth_getBlockByNumber", "label": "eth_getBlockByNumber (固定区块)",
     "nonnull": True},
    dict(_ADV["eth_call"], label="eth_call (预编译合约)"),
    dict(_ADV["eth_estimateGas"], label="eth_estimateGas (转账)"),
]
TTL_SPEC = {"id": "eth_blockNumber", "method": "eth_blockNumber", "params": []}


def hit_limit(ref):
    return ref + max(HIT_SLACK, ref / 2)


def miss_limit(ref, cold):
    """Above this a lifetime poll reached an upstream. Looser than hit_limit: under load a hit
    sometimes takes a few ms more, and one such call taken for a refresh would split a
    lifetime in two, while a fast upstream call taken for a hit only merges two."""
    return max(hit_limit(ref), math.sqrt(ref * cold))

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
            "err_kind": classify_error(a["err"]),
        }
    return cid, out


def cache_probe(chain, blocks, reps, timeout, interval, rng, method_reps, ttl_window, ttl_poll):
    """cold vs warm latency on `blocks` fixed (cacheable) blocks and for 'latest', then
    which methods are cached and how long a latest answer stays cached."""
    cid = chain["chainId"]
    head, head_src = live_head(chain, timeout)
    # fixed, immutable blocks, randomized so they are very likely cold. Kept recent
    # (head-200..800): pruned upstreams only serve the last ~1024 blocks, so an
    # old block would measure archive availability (rpc_bench --methods history)
    # instead of the cache.
    picks = sorted(rng.sample(range(head - 800, head - 199), blocks), reverse=True) \
        if head > 1000 else [max(1, head // 2)]
    latest_spec = {"id": "latest_block", "method": "eth_getBlockByNumber",
                   "params": ["latest", False], "nonnull": True}
    pace = Pacer(interval)

    def run(spec):
        # only the first (cold) call reaches an upstream: pace it, keep the warm ones back-to-back
        pace.wait()
        calls = []
        for i in range(reps + 1):      # 1 cold + reps warm
            r = rpc_call(cid, spec, timeout=timeout, req_id=i + 1)
            calls.append({"ok": r["ok"], "latency": r["latency"], "error": r["error"],
                          "kind": classify_error(r["error"])})
        lats = [c["latency"] if c["ok"] else None for c in calls]
        oks = [x for x in lats if x is not None]
        cold = lats[0]
        # warm = successful calls after the first one; if the cold call failed,
        # the first success is effectively the upstream fetch, so skip it too
        warm = [x for x in lats[1:] if x is not None]
        first_ok = next((i for i, c in enumerate(calls) if c["ok"]), None)
        if cold is None and warm:
            warm = warm[1:]
        return {
            "cold": cold,
            # why call #1 failed (None when it succeeded) and how long it took to fail
            "cold_error": calls[0]["error"],
            "cold_kind": calls[0]["kind"],
            "cold_fail_latency": None if calls[0]["ok"] else calls[0]["latency"],
            # when call #1 failed: which call first succeeded (1-based) and its latency
            "retry_ok_call": None if cold is not None or first_ok is None else first_ok + 1,
            "retry_ok_latency": None if cold is not None or first_ok is None else lats[first_ok],
            "warm": warm,
            "warm_med": statistics.median(warm) if warm else None,
            "warm_min": min(warm) if warm else None,
            "ok": len(oks), "total": reps + 1, "reps": reps + 1,
            "calls": calls,
        }

    probes = [dict(run({"id": "fixed_block", "method": "eth_getBlockByNumber",
                        "params": [hex(b), False], "nonnull": True}), block=b) for b in picks]
    fixed = merge_probes(probes)
    out = {"fixed": fixed, "latest": run(latest_spec),
           "block": fixed["cold_fail_block"] or picks[0], "blocks": picks,
           "head": head, "head_source": head_src}

    # tests 3 and 4 tell hits from upstream calls by the fixed blocks' hit latency
    ref = fixed["warm_med"]
    if ref is None or fixed["cold"] is None:
        out["skip"] = "固定区块缓存测试没有拿到冷回源 / 缓存命中耗时"
    elif fixed["cold"] / ref < BENEFIT_X:
        out["skip"] = "这条链的缓存没有生效(固定区块加速 %.1f×)" % (fixed["cold"] / ref)
    else:
        rest = sorted(set(range(head - 800, head - 199)) - set(picks)) if head > 1000 else []
        fresh = hex(rng.choice(rest)) if rest else None
        specs = [dict(s, params=[fresh, False]) if s["id"] == "block_fixed" else s
                 for s in CACHE_METHODS if s["id"] != "block_fixed" or fresh]
        out["ref"] = ref
        out["methods"] = method_cache(cid, specs, method_reps, hit_limit(ref), timeout, pace)
        out["ttl"] = latest_ttl(cid, miss_limit(ref, fixed["cold"]), ttl_window, ttl_poll, timeout)
    return cid, out


def method_cache(cid, specs, reps, limit, timeout, pace):
    """Each spec 1+reps times back-to-back: cached when most repeats take <= limit (s)."""
    out = {}
    for spec in specs:
        pace.wait()     # the first call may reach an upstream
        calls = [rpc_call(cid, spec, timeout=timeout, req_id=1)]
        # no repeats after a failure: a few failed calls in a row (e.g. eth_estimateGas
        # on ZK Stack chains) make the router take every upstream of the chain out for
        # minutes ("all upstream endpoints exhausted" for all methods)
        if calls[0]["ok"]:
            calls += [rpc_call(cid, spec, timeout=timeout, req_id=i + 2) for i in range(reps)]
        lats = [c["latency"] if c["ok"] else None for c in calls]
        rep = [x for x in lats[1:] if x is not None]
        hits = sum(1 for x in rep if x <= limit)
        err = next((c["error"] for c in calls if c["error"]), None)
        out[spec["id"]] = {
            "label": spec.get("label", spec["id"]),
            "cached": (hits * 2 > len(rep)) if len(rep) >= 2 else None,
            "first": lats[0], "rep": rep, "rep_med": statistics.median(rep) if rep else None,
            "hits": hits, "n": len(rep), "ok": sum(1 for c in calls if c["ok"]), "total": len(calls),
            "error": err, "kind": classify_error(err), "limit": limit,
        }
    return out


def latest_ttl(cid, limit, window, poll, timeout, want=5, give_up=10):
    """Poll eth_blockNumber; lifetime = median gap from the end of one refresh to the next one's start.

    A call reached an upstream when it took more than `limit` (s) or returned another
    block number than the call before (a cached answer cannot change; this catches
    upstreams that answer within a few ms). A refresh is a run of upstream calls with
    no hit in between (the router sometimes goes upstream several times in a row
    right after expiry), so a gap is only measured when an upstream call follows a
    hit. Stops after `want` gaps, or when the first `give_up` answers all came from an
    upstream (not cached) or the first `give_up` calls all failed."""
    t0 = time.perf_counter()
    pts, gaps, last_end, prev_hit, n_err, err, prev_val = [], [], None, False, 0, None, None
    while time.perf_counter() - t0 < window and len(gaps) < want:
        start = time.perf_counter()
        r = rpc_call(cid, TTL_SPEC, timeout=timeout)
        end = time.perf_counter()
        try:
            val = int(r["result"], 16) if r["ok"] else None
        except (TypeError, ValueError):
            val = None
        hit = r["ok"] and r["latency"] <= limit and (val is None or prev_val is None or val == prev_val)
        prev_val = val if val is not None else prev_val
        pts.append([round(start - t0, 3), round(r["latency"] * 1000, 1) if r["ok"] else None, hit, val])
        if not r["ok"]:
            n_err += 1
            err = err or r["error"]
            last_end = None   # a failed call breaks the chain of refreshes
        elif not hit:
            if prev_hit and last_end is not None:
                gaps.append(start - last_end)
            last_end = end
        prev_hit = hit
        if len(pts) - n_err >= give_up and not any(p[2] for p in pts):
            break
        if n_err >= give_up and n_err == len(pts):
            break
        time.sleep(poll)
    now = time.perf_counter()
    n_miss = sum(1 for _, ms_, hit, _ in pts if ms_ is not None and not hit)
    # no two upstream calls in a row: the answer stayed cached at least since the
    # last upstream call (or the start); unknown when a failed call came after it
    ttl_min = None
    if not gaps and n_miss + n_err < len(pts) and (last_end is not None or n_miss == 0):
        ttl_min = round(now - (last_end or t0), 2)
    return {
        # the answer expired between the last hit and the upstream call (one poll
        # interval): take the middle of that interval
        "ttl": max(0.0, statistics.median(gaps) - poll / 2) if gaps else None, "ttl_min": ttl_min,
        # every call reached an upstream: not cached for even one poll interval
        "all_miss": n_miss >= 2 and n_miss == len(pts) - n_err,
        "gaps": [round(g, 3) for g in gaps], "n_miss": n_miss, "n": len(pts), "n_err": n_err,
        "error": err, "kind": classify_error(err), "limit": limit,
        "poll": poll, "window": window, "points": pts,
    }


def merge_probes(probes):
    """Summary over the per-block probes: cold = median of the blocks' first calls,
    warm = median of all their later calls. The cold_* failure fields describe the
    first block whose cold call failed (None if every cold call succeeded)."""
    colds = [p["cold"] for p in probes if p["cold"] is not None]
    warm = [x for p in probes for x in p["warm"]]
    failed = next((p for p in probes if p["cold"] is None), {})
    return {
        "cold": statistics.median(colds) if colds else None,
        "cold_all": [p["cold"] for p in probes],
        "n_blocks": len(probes), "cold_ok": len(colds),
        "warm_med": statistics.median(warm) if warm else None,
        "warm_min": min(warm) if warm else None,
        "ok": sum(p["ok"] for p in probes), "total": sum(p["total"] for p in probes),
        "reps": probes[0]["reps"],   # calls per block
        "cold_fail_block": failed.get("block"),
        "cold_error": failed.get("cold_error"), "cold_kind": failed.get("cold_kind"),
        "cold_fail_latency": failed.get("cold_fail_latency"),
        "retry_ok_call": failed.get("retry_ok_call"), "retry_ok_latency": failed.get("retry_ok_latency"),
        "probes": [{"block": p["block"], "cold": p["cold"], "calls": p["calls"]} for p in probes],
    }


def fmt(x):
    return "   -  " if x is None else "%6.0f" % (x * 1000)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--chains", default="traffic")
    ap.add_argument("--samples", type=int, default=12, help="samples per method per round (default 12)")
    ap.add_argument("--rounds", type=int, default=1, help="repeat the whole sweep N times (default 1)")
    ap.add_argument("--gap", type=float, default=15.0, help="seconds between rounds (default 15)")
    ap.add_argument("--cache-blocks", type=int, default=10,
                    help="random blocks per chain in cache probe = cold samples (default 10)")
    ap.add_argument("--cache-reps", type=int, default=10, help="warm repeats per block in cache probe (default 10)")
    ap.add_argument("--interval", type=float, default=0.5,
                    help="min seconds between two cold (upstream) calls to the same chain (default 0.5)")
    ap.add_argument("--method-reps", type=int, default=3,
                    help="repeats after the first call when checking whether a method is cached (default 3)")
    ap.add_argument("--ttl-window", type=float, default=15.0,
                    help="max seconds of eth_blockNumber polling per chain for the latest cache lifetime (default 15)")
    ap.add_argument("--ttl-poll", type=float, default=0.1,
                    help="pause between two polls = resolution of the lifetime (default 0.1)")
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
    print("chains=%d  samples=%d x rounds=%d  cache_blocks=%d x (1+%d)  interval=%ss  concurrency=%d" % (
        len(chains), args.samples, args.rounds, args.cache_blocks, args.cache_reps, args.interval, args.concurrency))
    print("cached? %d methods x (1+%d)  latest lifetime: poll %ss for <= %ss" % (
        len(CACHE_METHODS), args.method_reps, args.ttl_poll, args.ttl_window))

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
        futs = [ex.submit(cache_probe, c, args.cache_blocks, args.cache_reps, args.timeout, args.interval,
                          random.Random((args.seed or int(time.time())) + c["chainId"]),
                          args.method_reps, args.ttl_window, args.ttl_poll) for c in chains]
        for fut in cf.as_completed(futs):
            cid, out = fut.result()
            cache_res[cid] = out
    conns = conn_stats()
    print("cache probe done in %.1fs  (%d requests over %d connections)\n" % (
        time.perf_counter() - t1, conns["requests"], conns["connects"]))

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
        w.writerow(["chainId", "name", "variant", "blocks", "cold_ok", "cold_ms", "warm_med_ms",
                    "warm_min_ms", "speedup_x", "saved_ms", "ok", "total", "cold_error_kind", "cold_error"])
        for cid, c in cache_res.items():
            for variant in ("fixed", "latest"):
                v = c[variant]
                cold, warm = v["cold"], v["warm_med"]
                speed = (cold / warm) if (cold and warm) else ""
                saved = ((cold - warm) * 1000) if (cold and warm) else ""
                w.writerow([cid, name_by_id.get(cid, ""), variant,
                            " ".join(map(str, c["blocks"])) if variant == "fixed" else "",
                            "%d/%d" % (v["cold_ok"], v["n_blocks"]) if variant == "fixed" else "",
                            "" if cold is None else "%.0f" % (cold * 1000),
                            "" if warm is None else "%.0f" % (warm * 1000),
                            "" if v["warm_min"] is None else "%.0f" % (v["warm_min"] * 1000),
                            "" if speed == "" else "%.2f" % speed,
                            "" if saved == "" else "%.0f" % saved,
                            v["ok"], v["total"], v["cold_kind"] or "", v["cold_error"] or ""])
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

    # chains where some block's cold call failed (all blocks failed -> no cold latency / speedup)
    failed = [(cid, c["fixed"]) for cid, c in cache_res.items() if c["fixed"]["cold_ok"] < c["fixed"]["n_blocks"]]
    print("\n=== cold call failed (fixed-block): %d chains (failed blocks left out of the cold median) ===" % len(
        failed))
    for cid, v in sorted(failed, key=lambda x: x[0]):
        after = ("call #%d ok %s ms" % (v["retry_ok_call"], fmt(v["retry_ok_latency"]).strip())
                 if v["retry_ok_call"] else "all %d calls failed" % v["reps"])
        print("  %-10s %2d/%-2d blocks  %-14s %-22s %s | %s" % (
            cid, v["n_blocks"] - v["cold_ok"], v["n_blocks"], ERROR_KINDS[v["cold_kind"]][0], after,
            name_by_id.get(cid, "")[:22], (v["cold_error"] or "")[:80]))

    # which methods are cached
    tested = [c for c in cache_res.values() if "methods" in c]
    print("\n=== cached methods (%d chains tested, %d skipped) ===" % (len(tested), len(cache_res) - len(tested)))
    print("  %-34s %7s %8s %8s %12s" % ("method", "cached", "uncached", "unknown", "repeat_p50"))
    for spec in CACHE_METHODS:
        vs = [c["methods"][spec["id"]] for c in tested if spec["id"] in c["methods"]]
        print("  %-34s %7d %8d %8d %s" % (
            spec.get("label", spec["id"])[:34], sum(1 for v in vs if v["cached"]),
            sum(1 for v in vs if v["cached"] is False), sum(1 for v in vs if v["cached"] is None),
            fmt(pct([v["rep_med"] for v in vs if v["rep_med"] is not None], 50)).rjust(12)))

    # latest cache lifetime
    ttls = [(cid, c["ttl"]) for cid, c in cache_res.items() if "ttl" in c]
    vals = [t["ttl"] for _, t in ttls if t["ttl"] is not None]
    print("\n=== latest cache lifetime (eth_blockNumber): median %s s over %d chains, "
          "%d not cached, %d longer than the window, %d unknown ===" % (
              "-" if not vals else "%.2f" % pct(vals, 50), len(vals), sum(1 for _, t in ttls if t["all_miss"]),
              sum(1 for _, t in ttls if t["ttl_min"] is not None),
              sum(1 for _, t in ttls if t["ttl"] is None and t["ttl_min"] is None)))
    for cid, t in sorted(ttls, key=lambda x: (x[1]["ttl"] is None, x[1]["ttl"] or 0))[:10]:
        print("  %-10s %6s s  gaps %s  %s" % (cid, "-" if t["ttl"] is None else "%.2f" % t["ttl"],
                                             t["gaps"], name_by_id.get(cid, "")[:28]))

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
