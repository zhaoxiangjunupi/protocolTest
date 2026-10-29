#!/usr/bin/env python3
"""Check whether the router covers the workload in bsc_use.md on BSC (chain 56) or Ethereum (chain 1).

--chain picks the chain profile (token, holder, deep heights); default 56. Polygon has its own
script (polygon_polymarket_check.py) because its workload is different.

Scenarios (all against POST /rpc/{chain}):
  basic     : the plain methods listed in bsc_use.md
  call      : eth_call latest - ERC20 balanceOf, Multicall3 aggregate3, Safe getThreshold/getOwners
  logs_rt   : eth_getLogs with address+topic filter over the latest 130 / 500 blocks, repeated
  archive   : eth_call / eth_getBalance / eth_getBlockByNumber / eth_getLogs at historical heights
              (incl. ~2026-04-01), each height repeated so upstream mixing shows up
  logs_big  : eth_getLogs over 2k / 10k / 50k blocks (small result) and 1k/2k/5k blocks (large result)
  batch     : JSON-RPC batches of 20 / 50 / 100 calls
  burst     : the 10:00 daily-report peak - 3000 balanceOf in batches of 100, several threads
  sustain   : ~1.5 req/s mixed traffic for N seconds

Stdlib only; uses rpcutil for the keep-alive connection and error classification.
"""
import argparse
import json
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import rpcutil
from rpcutil import rpc_call, pct

CHAIN = 56
USDT = "0x55d398326f99059fF775485246999027B3197955"
MULTICALL3 = "0xcA11bde05977b3631167028862bE2a173976CA11"
SAFE_FACTORY_130 = "0xa6B71E26C5e0845f74c812102Ca7114b6a896AB2"
SAFE_FACTORY_141 = "0x4e1DCf7AD4e460CfD30791CCC4F9c8a4f820ec67"
SAFE_SINGLETON_L2_130 = "0x3E5c63644E683549055b9Be8653de26E0B4CD36E"
BINANCE_HOT = "0x8894E0a0c962CB723c1976a4421c95949bE2D4E3"
TOPIC_TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
TOPIC_PROXY_CREATION = "0x4f51faf6c4561ff95f067657e43439f0f856d97c04d9ec9070a6199ad418e235"
SEL_BALANCE_OF = "0x70a08231"
SEL_GET_THRESHOLD = "0xe75235b8"
SEL_GET_OWNERS = "0xa0e67e2b"
SEL_AGGREGATE3 = "0x82ad56cb"
NAME = "BSC"

# Per-chain constants. USDT / BINANCE_HOT keep their BSC-era names: they mean "the token" and
# "a big holder of it" on whichever chain is selected. Multicall3 and the Safe factories have the
# same address on every chain.
CHAIN_PROFILES = {
    56: {"name": "BSC", "slug": "bsc", "chain_hex": "0x38",
         "token": "0x55d398326f99059fF775485246999027B3197955",          # BSC-USD (USDT)
         "holder": "0x8894E0a0c962CB723c1976a4421c95949bE2D4E3",         # Binance hot wallet
         "apr_lo": lambda h: 40_000_000,                                 # binary search lower bound
         "deep_heights": lambda h: [85_000_000, 90_000_000, 95_000_000, 96_500_000, 100_000_000, 105_000_000,
                                    110_000_000, 115_000_000, 120_000_000, 125_000_000, 128_000_000, 130_000_000]},
    1: {"name": "Ethereum", "slug": "eth", "chain_hex": "0x1",
        "token": "0xdAC17F958D2ee523a2206206994597C13D831ec7",           # USDT
        "holder": "0x28C6c06298d514Db089934071355E5743bf21d60",          # Binance 14
        "apr_lo": lambda h: h - 3_000_000,                               # ~14 months at 12 s
        "deep_heights": lambda h: [h - k * 216_000 for k in (12, 9, 8, 7, 6, 5, 4, 3, 2, 1)]},   # ~monthly back
}


def use_chain(chain_id):
    """Point the module-level constants at another chain profile."""
    global CHAIN, USDT, BINANCE_HOT, NAME, PROFILE
    PROFILE = CHAIN_PROFILES[chain_id]
    CHAIN, USDT, BINANCE_HOT, NAME = chain_id, PROFILE["token"], PROFILE["holder"], PROFILE["name"]


PROFILE = CHAIN_PROFILES[56]

LOG = []
RESULTS = {}


def log(msg):
    line = "%s %s" % (datetime.now(timezone.utc).strftime("%H:%M:%S"), msg)
    print(line, flush=True)
    LOG.append(line)


def hx(n):
    return hex(n)


def pad_addr(a):
    return "0" * 24 + a[2:].lower()


def call_spec(to, data, block="latest"):
    return {"method": "eth_call", "params": [{"to": to, "data": data}, block]}


def balance_of_data(holder):
    return SEL_BALANCE_OF + pad_addr(holder)


def aggregate3_data(token, holders):
    """aggregate3((address,bool,bytes)[]) encoding of balanceOf(holder) for every holder."""
    n = len(holders)
    head = "%064x" % 0x20 + "%064x" % n
    offsets, tails = "", ""
    base = 0x20 * n
    for h in holders:
        offsets += "%064x" % (base + len(tails) // 2)
        cd = balance_of_data(h)[2:]                      # 36 bytes
        # tuple(address target, bool allowFailure, bytes callData): head = target, allowFailure, offset(0x60)
        tails += (pad_addr(token) + "%064x" % 1 + "%064x" % 0x60
                  + "%064x" % (len(cd) // 2) + cd.ljust(128, "0"))
    return SEL_AGGREGATE3 + head + offsets + tails


def rec(name, r, extra=None):
    """Store one sample under RESULTS[name]."""
    RESULTS.setdefault(name, []).append({
        "ok": r["ok"], "ms": round(r["latency"] * 1000), "error": r["error"],
        "kind": rpcutil.classify_error(r["error"]) if r["error"] else None, **(extra or {})})


def summarize(name):
    xs = RESULTS.get(name, [])
    ok = [x for x in xs if x["ok"]]
    lat = sorted(x["ms"] for x in ok)
    errs = {}
    for x in xs:
        if not x["ok"]:
            errs[x["error"]] = errs.get(x["error"], 0) + 1
    return {"n": len(xs), "ok": len(ok), "p50": pct(lat, 50), "p95": pct(lat, 95),
            "max": lat[-1] if lat else None, "errors": errs}


def retry(spec, tries=3, timeout=20, sleep=0.3):
    last = None
    for _ in range(tries):
        last = rpc_call(CHAIN, spec, timeout=timeout)
        if last["ok"]:
            return last
        time.sleep(sleep)
    return last


# ---------------------------------------------------------------- helpers --

def head():
    r = retry({"method": "eth_blockNumber", "params": []})
    if not r["ok"]:
        raise SystemExit("eth_blockNumber failed: %s" % r["error"])
    return int(r["result"], 16)


def block(n, full=False, tries=3):
    return retry({"method": "eth_getBlockByNumber", "params": [hx(n), full], "nonnull": True}, tries=tries)


def find_block_at(ts_target, lo, hi):
    """Binary search block height whose timestamp is closest to ts_target. Records every probe."""
    probes = 0
    while hi - lo > 2:
        mid = (lo + hi) // 2
        r = block(mid, tries=5)
        probes += 1
        rec("archive_probe_getBlockByNumber", r, {"block": mid})
        if not r["ok"]:
            log("  probe block %d failed after retries: %s" % (mid, r["error"]))
            return None, probes
        ts = int(r["result"]["timestamp"], 16)
        if ts < ts_target:
            lo = mid
        else:
            hi = mid
    return hi, probes


# -------------------------------------------------------------- scenarios --

def scen_basic(h):
    log("== basic methods ==")
    b = block(h, True)
    txs = b["result"]["transactions"] if b["ok"] else []
    tx_hash = txs[0]["hash"] if txs else None
    specs = [
        ("eth_chainId", {"method": "eth_chainId", "params": [], "expect": PROFILE["chain_hex"]}),
        ("eth_blockNumber", {"method": "eth_blockNumber", "params": []}),
        ("eth_getBlockByNumber_latest", {"method": "eth_getBlockByNumber", "params": ["latest", False], "nonnull": True}),
        ("eth_getCode_USDT", {"method": "eth_getCode", "params": [USDT, "latest"]}),
        ("eth_getTransactionCount", {"method": "eth_getTransactionCount", "params": [BINANCE_HOT, "latest"]}),
        ("eth_getBalance", {"method": "eth_getBalance", "params": [BINANCE_HOT, "latest"]}),
    ]
    if tx_hash:
        specs.append(("eth_getTransactionReceipt", {"method": "eth_getTransactionReceipt", "params": [tx_hash], "nonnull": True}))
    for name, spec in specs:
        for _ in range(5):
            r = rpc_call(CHAIN, spec)
            if name == "eth_getCode_USDT" and r["ok"] and len(r["result"]) < 100:
                r = {**r, "ok": False, "error": "wrong-result: empty code"}
            rec("basic/" + name, r)
            time.sleep(0.2)
        log("  %-32s %s" % (name, summarize("basic/" + name)))
    return [t["from"] for t in txs] + [t["to"] for t in txs if t.get("to")]


def scen_call(addrs):
    log("== eth_call latest ==")
    holders = list(dict.fromkeys(a for a in addrs if a))[:100] or [BINANCE_HOT]
    for _ in range(5):
        r = rpc_call(CHAIN, call_spec(USDT, balance_of_data(BINANCE_HOT)))
        if r["ok"] and (not isinstance(r["result"], str) or len(r["result"]) != 66):
            r = {**r, "ok": False, "error": "wrong-result: %s" % str(r["result"])[:40]}
        rec("call/balanceOf", r)
        time.sleep(0.2)
    log("  balanceOf(USDT)             %s" % summarize("call/balanceOf"))
    for n in (20, 50, 100):
        hs = (holders * n)[:n]
        for _ in range(3):
            r = rpc_call(CHAIN, call_spec(MULTICALL3, aggregate3_data(USDT, hs)), timeout=30)
            if r["ok"] and len(r["result"]) < 2 + 2 * (64 + n * 32 + n * 128):
                r = {**r, "ok": False, "error": "wrong-result: short return %d" % len(r["result"])}
            rec("call/multicall3_%d" % n, r)
            time.sleep(0.3)
        log("  Multicall3 aggregate3 x%-4d %s" % (n, summarize("call/multicall3_%d" % n)))

    # a real Safe: take a ProxyCreation event from the last 20k blocks (also a getLogs test)
    h = head()
    safe = None
    for factory in (SAFE_FACTORY_141, SAFE_FACTORY_130):
        r = retry({"method": "eth_getLogs", "params": [{"fromBlock": hx(h - 20000), "toBlock": hx(h),
                                                        "address": factory, "topics": [TOPIC_PROXY_CREATION]}]}, timeout=30)
        rec("call/find_safe_getLogs_20k", r, {"factory": factory, "n_logs": len(r["result"]) if r["ok"] else None})
        if r["ok"] and r["result"]:
            lg = r["result"][-1]
            # 1.4.1 indexes proxy in topics[1]; 1.3.0 puts it in data
            if len(lg["topics"]) > 1:
                safe = "0x" + lg["topics"][1][-40:]
            else:
                safe = "0x" + lg["data"][2:][24:64]
            break
    log("  Safe found via ProxyCreation logs: %s" % safe)
    target = safe or SAFE_SINGLETON_L2_130
    for name, sel in (("getThreshold", SEL_GET_THRESHOLD), ("getOwners", SEL_GET_OWNERS)):
        for _ in range(5):
            r = rpc_call(CHAIN, call_spec(target, sel))
            if r["ok"] and (not isinstance(r["result"], str) or len(r["result"]) < 66):
                r = {**r, "ok": False, "error": "wrong-result: %s" % str(r["result"])[:40]}
            rec("call/safe_" + name, r)
            time.sleep(0.2)
        log("  Safe %-22s %s" % (name, summarize("call/safe_" + name)))
    RESULTS["_safe"] = target
    return holders


def scen_logs_rt(reps):
    log("== eth_getLogs realtime (USDT Transfer, latest window) ==")
    for window in (130, 500):
        for i in range(reps):
            h = head()
            r = rpc_call(CHAIN, {"method": "eth_getLogs", "params": [{"fromBlock": hx(h - window + 1), "toBlock": hx(h),
                                                                     "address": USDT, "topics": [TOPIC_TRANSFER]}]}, timeout=30)
            rec("logs_rt/%d" % window, r, {"n_logs": len(r["result"]) if r["ok"] else None, "head": h})
            time.sleep(1.0)
        s = summarize("logs_rt/%d" % window)
        ns = [x["n_logs"] for x in RESULTS["logs_rt/%d" % window] if x["ok"]]
        log("  window %-4d %s logs/req avg=%s" % (window, s, round(sum(ns) / len(ns)) if ns else None))


def scen_archive(h, reps):
    log("== archive (historical heights) ==")
    # locate 2026-04-01 00:00 UTC by binary search
    ts_apr = int(datetime(2026, 4, 1, tzinfo=timezone.utc).timestamp())
    b_apr, probes = find_block_at(ts_apr, PROFILE["apr_lo"](h), h)
    log("  block at 2026-04-01: %s (%d probes, %s)" % (b_apr, probes, summarize("archive_probe_getBlockByNumber")))
    heights = [("head-500", h - 500), ("head-1000", h - 1000), ("head-1100", h - 1100), ("head-2000", h - 2000),
               ("head-10k", h - 10_000), ("head-100k", h - 100_000), ("head-1M", h - 1_000_000),
               ("head-5M", h - 5_000_000)]
    if b_apr:
        heights += [("2026-04-01", b_apr), ("2026-04-01+1M", b_apr + 1_000_000),
                    ("2026-04-01-1M (before)", b_apr - 1_000_000)]
    RESULTS["_heights"] = heights
    for label, n in heights:
        for i in range(reps):
            # state at height: eth_call balanceOf + eth_getBalance ; block body ; logs in 500 blocks ending there
            r1 = rpc_call(CHAIN, call_spec(USDT, balance_of_data(BINANCE_HOT), hx(n)))
            if r1["ok"] and (not isinstance(r1["result"], str) or len(r1["result"]) != 66):
                r1 = {**r1, "ok": False, "error": "wrong-result: %s" % str(r1["result"])[:40]}
            rec("archive/eth_call@%s" % label, r1, {"block": n})
            r2 = rpc_call(CHAIN, {"method": "eth_getBalance", "params": [BINANCE_HOT, hx(n)]})
            rec("archive/eth_getBalance@%s" % label, r2, {"block": n})
            r3 = rpc_call(CHAIN, {"method": "eth_getBlockByNumber", "params": [hx(n), False], "nonnull": True})
            rec("archive/eth_getBlockByNumber@%s" % label, r3, {"block": n})
            r4 = rpc_call(CHAIN, {"method": "eth_getLogs", "params": [{"fromBlock": hx(n - 499), "toBlock": hx(n),
                                                                      "address": USDT, "topics": [TOPIC_TRANSFER]}]}, timeout=30)
            rec("archive/eth_getLogs500@%s" % label, r4, {"block": n, "n_logs": len(r4["result"]) if r4["ok"] else None})
            if r4["ok"] and r4["result"] == []:
                RESULTS["archive/eth_getLogs500@%s" % label][-1]["note"] = "empty"
            time.sleep(0.5)
        for m in ("eth_call", "eth_getBalance", "eth_getBlockByNumber", "eth_getLogs500"):
            log("  %-26s %-22s %s" % (label, m, summarize("archive/%s@%s" % (m, label))))


def scen_logs_big(h):
    log("== eth_getLogs big ranges ==")
    # small result set: Safe ProxyCreation over large windows
    for span in (2_000, 10_000, 50_000):
        for _ in range(3):
            r = rpc_call(CHAIN, {"method": "eth_getLogs", "params": [{"fromBlock": hx(h - span), "toBlock": hx(h - 1),
                                                                     "address": SAFE_FACTORY_141, "topics": [TOPIC_PROXY_CREATION]}]}, timeout=60)
            rec("logs_big/small_result_%dk" % (span // 1000), r, {"n_logs": len(r["result"]) if r["ok"] else None})
            time.sleep(0.5)
        log("  ProxyCreation span=%-6d %s" % (span, summarize("logs_big/small_result_%dk" % (span // 1000))))
    # large result set: USDT Transfer over growing windows
    for span in (1_000, 2_000, 5_000):
        for _ in range(2):
            r = rpc_call(CHAIN, {"method": "eth_getLogs", "params": [{"fromBlock": hx(h - span), "toBlock": hx(h - 1),
                                                                     "address": USDT, "topics": [TOPIC_TRANSFER]}]}, timeout=90)
            rec("logs_big/usdt_%dk" % (span // 1000), r, {"n_logs": len(r["result"]) if r["ok"] else None})
            time.sleep(0.5)
        xs = RESULTS["logs_big/usdt_%dk" % (span // 1000)]
        log("  USDT Transfer span=%-6d %s n_logs=%s" % (span, summarize("logs_big/usdt_%dk" % (span // 1000)),
                                                       [x["n_logs"] for x in xs]))
    # historical big range (2026-04-01 area) if found
    hs = dict(RESULTS.get("_heights", []))
    if "2026-04-01" in hs:
        n = hs["2026-04-01"]
        for span in (10_000, 50_000):
            for _ in range(3):
                r = rpc_call(CHAIN, {"method": "eth_getLogs", "params": [{"fromBlock": hx(n), "toBlock": hx(n + span - 1),
                                                                         "address": SAFE_FACTORY_141, "topics": [TOPIC_PROXY_CREATION]}]}, timeout=60)
                rec("logs_big/apr_small_result_%dk" % (span // 1000), r, {"n_logs": len(r["result"]) if r["ok"] else None})
                time.sleep(0.5)
            log("  ProxyCreation @2026-04-01 span=%-6d %s" % (span, summarize("logs_big/apr_small_result_%dk" % (span // 1000))))


def scen_batch(holders):
    log("== JSON-RPC batch ==")
    for n in (20, 50, 100):
        hs = (holders * 20)[:n]
        for _ in range(3):
            spec = {"batch": [("eth_call", [{"to": USDT, "data": balance_of_data(a)}, "latest"]) for a in hs]}
            r = rpc_call(CHAIN, spec, timeout=60)
            rec("batch/eth_call_x%d" % n, r)
            time.sleep(0.5)
        log("  eth_call x%-4d %s" % (n, summarize("batch/eth_call_x%d" % n)))
    for n in (50, 100):
        hs = (holders * 20)[:n]
        for _ in range(3):
            spec = {"batch": [("eth_getBalance", [a, "latest"]) for a in hs]}
            r = rpc_call(CHAIN, spec, timeout=60)
            rec("batch/eth_getBalance_x%d" % n, r)
            time.sleep(0.5)
        log("  eth_getBalance x%-4d %s" % (n, summarize("batch/eth_getBalance_x%d" % n)))
    # mixed batch like a monitor round
    h = head()
    spec = {"batch": [("eth_blockNumber", []), ("eth_getBlockByNumber", [hx(h), False]),
                      ("eth_getLogs", [{"fromBlock": hx(h - 129), "toBlock": hx(h), "address": USDT, "topics": [TOPIC_TRANSFER]}]),
                      ("eth_call", [{"to": USDT, "data": balance_of_data(BINANCE_HOT)}, "latest"]),
                      ("eth_getBalance", [BINANCE_HOT, "latest"]), ("eth_getTransactionCount", [BINANCE_HOT, "latest"])]}
    for _ in range(3):
        rec("batch/mixed_x6", rpc_call(CHAIN, spec, timeout=60))
        time.sleep(0.5)
    log("  mixed x6       %s" % summarize("batch/mixed_x6"))


def scen_burst(holders, total, threads):
    log("== burst: %d balanceOf in batches of 100, %d threads (daily report peak) ==" % (total, threads))
    hs = (holders * 1000)[:total]
    chunks = [hs[i:i + 100] for i in range(0, len(hs), 100)]

    def one(chunk):
        spec = {"batch": [("eth_call", [{"to": USDT, "data": balance_of_data(a)}, "latest"]) for a in chunk]}
        return rpc_call(CHAIN, spec, timeout=60)

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=threads) as ex:
        for r in ex.map(one, chunks):
            rec("burst/batch100", r)
    dt = time.perf_counter() - t0
    s = summarize("burst/batch100")
    log("  %d batches in %.1fs = %.1f calls/s ; %s" % (len(chunks), dt, total / dt, s))
    RESULTS["_burst_wall_s"] = round(dt, 1)
    # same volume as single calls, threads x 1 call (what a naive client does)
    t0 = time.perf_counter()
    single = hs[:300]
    with ThreadPoolExecutor(max_workers=threads) as ex:
        for r in ex.map(lambda a: rpc_call(CHAIN, call_spec(USDT, balance_of_data(a))), single):
            rec("burst/single", r)
    dt = time.perf_counter() - t0
    log("  %d single eth_call in %.1fs = %.1f req/s ; %s" % (len(single), dt, len(single) / dt, summarize("burst/single")))


def scen_sustain(seconds, rps):
    log("== sustained %.1f req/s mixed for %ds ==" % (rps, seconds))
    t_end = time.time() + seconds
    i = 0
    while time.time() < t_end:
        h = head()
        rec("sustain/eth_blockNumber", {"ok": True, "latency": 0, "error": None})
        kind = i % 4
        if kind == 0:
            r = rpc_call(CHAIN, {"method": "eth_getLogs", "params": [{"fromBlock": hx(h - 129), "toBlock": hx(h), "address": USDT, "topics": [TOPIC_TRANSFER]}]}, timeout=30)
            rec("sustain/getLogs130", r)
        elif kind == 1:
            r = rpc_call(CHAIN, call_spec(USDT, balance_of_data(BINANCE_HOT)))
            rec("sustain/eth_call", r)
        elif kind == 2:
            r = rpc_call(CHAIN, {"method": "eth_getBlockByNumber", "params": [hx(h), False], "nonnull": True})
            rec("sustain/getBlockByNumber", r)
        else:
            r = rpc_call(CHAIN, {"method": "eth_getLogs", "params": [{"fromBlock": hx(h - 499), "toBlock": hx(h), "address": USDT, "topics": [TOPIC_TRANSFER]}]}, timeout=30)
            rec("sustain/getLogs500", r)
        i += 1
        time.sleep(max(0, 2.0 / rps - r["latency"]))
    for k in ("getLogs130", "getLogs500", "eth_call", "getBlockByNumber"):
        log("  %-18s %s" % (k, summarize("sustain/" + k)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenarios", default="basic,call,logs_rt,archive,logs_big,batch,sustain,burst")
    ap.add_argument("--reps", type=int, default=6, help="repeats per archive height / realtime logs window")
    ap.add_argument("--burst-total", type=int, default=3000)
    ap.add_argument("--burst-threads", type=int, default=6)
    ap.add_argument("--sustain-seconds", type=int, default=120)
    ap.add_argument("--chain", type=int, default=56, choices=sorted(CHAIN_PROFILES))
    ap.add_argument("--out", help="default results_<chain>_use_<time>.json")
    a = ap.parse_args()
    use_chain(a.chain)
    a.out = a.out or "results_%s_use_%s.json" % (PROFILE["slug"], datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    want = set(a.scenarios.split(","))

    h = head()
    log("%s (chain %d) head = %d (%s)" % (NAME, CHAIN, h, rpcutil.BASE))
    addrs = scen_basic(h) if "basic" in want else [BINANCE_HOT]
    holders = scen_call(addrs) if "call" in want else [BINANCE_HOT]
    if "logs_rt" in want:
        scen_logs_rt(a.reps)
    if "archive" in want:
        scen_archive(h, a.reps)
    if "logs_big" in want:
        scen_logs_big(h)
    if "batch" in want:
        scen_batch(holders)
    if "sustain" in want:
        scen_sustain(a.sustain_seconds, 1.5)
    if "burst" in want:
        scen_burst(holders, a.burst_total, a.burst_threads)

    summary = {k: summarize(k) for k in RESULTS if not k.startswith("_")}
    json.dump({"chain": CHAIN, "head": h, "when": datetime.now(timezone.utc).isoformat(), "summary": summary,
               "samples": {k: v for k, v in RESULTS.items()}, "log": LOG, "conn": rpcutil.conn_stats()},
              open(a.out, "w"), indent=1, ensure_ascii=False)
    log("saved %s ; %s" % (a.out, rpcutil.conn_stats()))




# ------------------------------------------------------ follow-up scenarios --

def rand_addr(rng):
    return "0x%040x" % rng.getrandbits(160)


def scen_archive_deep(reps):
    """Fixed deep heights: block body / state / logs, every try recorded (no retry)."""
    log("== archive deep (fixed heights, %d tries each) ==" % reps)
    heights = PROFILE["deep_heights"](head())
    for n in heights:
        ts = None
        for _ in range(reps):
            r = rpc_call(CHAIN, {"method": "eth_getBlockByNumber", "params": [hx(n), False], "nonnull": True})
            rec("deep/getBlockByNumber@%d" % n, r, {"block": n})
            if r["ok"] and ts is None:
                ts = datetime.fromtimestamp(int(r["result"]["timestamp"], 16), timezone.utc).strftime("%Y-%m-%d")
            r = rpc_call(CHAIN, call_spec(USDT, balance_of_data(BINANCE_HOT), hx(n)))
            rec("deep/eth_call@%d" % n, r, {"block": n})
            r = rpc_call(CHAIN, {"method": "eth_getBalance", "params": [BINANCE_HOT, hx(n)]})
            rec("deep/eth_getBalance@%d" % n, r, {"block": n})
            r = rpc_call(CHAIN, {"method": "eth_getLogs", "params": [{"fromBlock": hx(n - 499), "toBlock": hx(n),
                                                                     "address": SAFE_FACTORY_141, "topics": [TOPIC_PROXY_CREATION]}]}, timeout=30)
            rec("deep/getLogs500@%d" % n, r, {"block": n, "n_logs": len(r["result"]) if r["ok"] else None})
            time.sleep(0.4)
        for m in ("getBlockByNumber", "eth_call", "eth_getBalance", "getLogs500"):
            s = summarize("deep/%s@%d" % (m, n))
            log("  %-10d %-11s %-17s ok %d/%d  %s" % (n, ts or "?", m, s["ok"], s["n"], s["errors"] or ""))


def scen_batch_limit(reps):
    """Batch size threshold with unique addresses (no cache hits)."""
    log("== batch size limit (unique addresses) ==")
    rng = random.Random(1)
    for n in (50, 60, 70, 80, 90, 100):
        for _ in range(reps):
            spec = {"batch": [("eth_getBalance", [rand_addr(rng), "latest"]) for _ in range(n)]}
            rec("batchlim/eth_getBalance_x%d" % n, rpc_call(CHAIN, spec, timeout=60))
            time.sleep(0.5)
        s = summarize("batchlim/eth_getBalance_x%d" % n)
        log("  eth_getBalance x%-4d ok %d/%d p50=%s p95=%s %s" % (n, s["ok"], s["n"], s["p50"], s["p95"], s["errors"] or ""))
    for n in (50, 100):
        for _ in range(reps):
            spec = {"batch": [("eth_call", [{"to": USDT, "data": balance_of_data(rand_addr(rng))}, "latest"]) for _ in range(n)]}
            rec("batchlim/eth_call_x%d" % n, rpc_call(CHAIN, spec, timeout=60))
            time.sleep(0.5)
        s = summarize("batchlim/eth_call_x%d" % n)
        log("  eth_call x%-4d ok %d/%d p50=%s p95=%s %s" % (n, s["ok"], s["n"], s["p50"], s["p95"], s["errors"] or ""))


def scen_logs_filtered(reps):
    """Realtime getLogs with a filter that stays far below 20k results, 130 / 500 / 2000 blocks."""
    log("== getLogs realtime with narrow filter (token Transfer to the big holder) ==")
    topics = [TOPIC_TRANSFER, None, "0x" + pad_addr(BINANCE_HOT)]
    for window in (130, 500, 2000):
        for _ in range(reps):
            h = head()
            r = rpc_call(CHAIN, {"method": "eth_getLogs", "params": [{"fromBlock": hx(h - window + 1), "toBlock": hx(h),
                                                                     "address": USDT, "topics": topics}]}, timeout=30)
            rec("logsf/%d" % window, r, {"n_logs": len(r["result"]) if r["ok"] else None})
            time.sleep(0.8)
        s = summarize("logsf/%d" % window)
        ns = [x["n_logs"] for x in RESULTS["logsf/%d" % window] if x["ok"]]
        log("  window %-5d ok %d/%d p50=%s p95=%s logs=%s %s" % (window, s["ok"], s["n"], s["p50"], s["p95"], ns, s["errors"] or ""))
    log("== getLogs block-range limit (ProxyCreation, small result) ==")
    h = head()
    for span in (3_000, 5_000, 10_000, 20_000, 50_000):
        for _ in range(reps):
            r = rpc_call(CHAIN, {"method": "eth_getLogs", "params": [{"fromBlock": hx(h - span), "toBlock": hx(h - 1),
                                                                     "address": SAFE_FACTORY_141, "topics": [TOPIC_PROXY_CREATION]}]}, timeout=60)
            rec("span/%d" % span, r, {"n_logs": len(r["result"]) if r["ok"] else None})
            time.sleep(0.5)
        s = summarize("span/%d" % span)
        log("  span %-6d ok %d/%d p50=%s p95=%s %s" % (span, s["ok"], s["n"], s["p50"], s["p95"], s["errors"] or ""))


def scen_burst_unique(total, threads):
    log("== burst with unique addresses: %d balanceOf, %d threads ==" % (total, threads))
    rng = random.Random(2)
    for bs in (50, 100):
        addrs = [rand_addr(rng) for _ in range(total)]
        chunks = [addrs[i:i + bs] for i in range(0, total, bs)]

        def one(chunk):
            return rpc_call(CHAIN, {"batch": [("eth_call", [{"to": USDT, "data": balance_of_data(a)}, "latest"]) for a in chunk]}, timeout=60)

        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=threads) as ex:
            for r in ex.map(one, chunks):
                rec("burstu/batch%d" % bs, r)
        dt = time.perf_counter() - t0
        s = summarize("burstu/batch%d" % bs)
        log("  batch %-3d: %d batches in %.1fs = %.0f calls/s ; ok %d/%d p50=%s p95=%s %s" % (
            bs, len(chunks), dt, total / dt, s["ok"], s["n"], s["p50"], s["p95"], s["errors"] or ""))
        time.sleep(3)
    single = [rand_addr(rng) for _ in range(300)]
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=threads) as ex:
        for r in ex.map(lambda a: rpc_call(CHAIN, call_spec(USDT, balance_of_data(a))), single):
            rec("burstu/single", r)
    dt = time.perf_counter() - t0
    s = summarize("burstu/single")
    log("  300 single eth_call in %.1fs = %.0f req/s ; ok %d/%d p50=%s p95=%s %s" % (
        dt, len(single) / dt, s["ok"], s["n"], s["p50"], s["p95"], s["errors"] or ""))


def main2():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reps", type=int, default=8)
    ap.add_argument("--scenarios", default="archive_deep,batch_limit,logs_filtered,burst_unique")
    ap.add_argument("--chain", type=int, default=56, choices=sorted(CHAIN_PROFILES))
    ap.add_argument("--out", help="default results_<chain>_use2_<time>.json")
    a = ap.parse_args()
    use_chain(a.chain)
    a.out = a.out or "results_%s_use2_%s.json" % (PROFILE["slug"], datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    want = set(a.scenarios.split(","))
    log("%s (chain %d) head = %d" % (NAME, CHAIN, head()))
    if "archive_deep" in want:
        scen_archive_deep(a.reps)
    if "batch_limit" in want:
        scen_batch_limit(max(4, a.reps // 2))
    if "logs_filtered" in want:
        scen_logs_filtered(max(4, a.reps // 2))
    if "burst_unique" in want:
        scen_burst_unique(3000, 6)
    summary = {k: summarize(k) for k in RESULTS if not k.startswith("_")}
    json.dump({"when": datetime.now(timezone.utc).isoformat(), "summary": summary, "samples": RESULTS, "log": LOG},
              open(a.out, "w"), indent=1, ensure_ascii=False)
    log("saved %s ; %s" % (a.out, rpcutil.conn_stats()))


if __name__ == "__main__" and "--followup" in sys.argv:
    sys.argv.remove("--followup")
    main2()
    sys.exit(0)

if __name__ == "__main__":
    main()
