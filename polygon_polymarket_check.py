#!/usr/bin/env python3
"""Check the router's Polygon endpoint (POST /rpc/137) for a Polymarket-style workload.

Workload (mirrors bsc_use.md, with Polymarket's contracts):
  - eth_getLogs with address + topic filter over the latest blocks:
      OrderFilled on the two live exchanges, ERC-1155 TransferSingle/TransferBatch on
      ConditionalTokens, Transfer on the collateral token and on USDC.e
  - eth_call: collateral / USDC.e balanceOf, ConditionalTokens balanceOf(address,id) and
      balanceOfBatch, Multicall3 aggregate3 of 100 balanceOf
  - eth_blockNumber / eth_getBlockByNumber / eth_getCode / eth_getTransactionCount /
      eth_getTransactionReceipt / eth_getBalance / eth_chainId
  - JSON-RPC batches, historical heights (state, blocks, receipts, logs), big log ranges,
      ~1.5 req/s sustained and a burst

Contract addresses and topics were taken from live receipts on 2026-10-29 (Polymarket
moved to new exchange contracts; the old 0x4bFb… / 0xC5d5… exchanges emit nothing now).
"""
import argparse
import json
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import rpcutil
from rpcutil import rpc_call
from bsc_use_check import log, rec, summarize, hx, pad_addr, call_spec, rand_addr, RESULTS, LOG, MULTICALL3, aggregate3_data

CHAIN = 137
EXCHANGE = "0xe111180000d2663c0091e4f400237545b87b996b"          # live CTF exchange (tx.to of most trades)
NEG_RISK_EXCHANGE = "0xe2222d279d744050d28e00520010520000310f59"
CONDITIONAL_TOKENS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
COLLATERAL = "0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb"       # collateral token used by the live exchange
USDCE = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
TOPIC_ORDER_FILLED = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
TOPIC_TRANSFER_SINGLE = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
TOPIC_TRANSFER_BATCH = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"
TOPIC_TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
SEL_BALANCE_OF = "0x70a08231"
SEL_BALANCE_OF_1155 = "0x00fdd58e"      # balanceOf(address,uint256)
SEL_BALANCE_OF_BATCH = "0x4e1273f4"     # balanceOfBatch(address[],uint256[])
CTX = {}


def head():
    for _ in range(3):
        r = rpc_call(CHAIN, {"method": "eth_blockNumber", "params": []})
        if r["ok"]:
            return int(r["result"], 16)
        time.sleep(0.3)
    raise SystemExit("eth_blockNumber failed: %s" % r["error"])


def logs_spec(frm, to, address, topics, timeout=30):
    return {"method": "eth_getLogs", "params": [{"fromBlock": hx(frm), "toBlock": hx(to), "address": address, "topics": topics}]}, timeout


def get_logs(frm, to, address, topics, timeout=30):
    spec, t = logs_spec(frm, to, address, topics, timeout)
    return rpc_call(CHAIN, spec, timeout=t)


def n_logs(r):
    return len(r["result"]) if r["ok"] else None


def show(name, extra=""):
    s = summarize(name)
    log("  %-44s ok %d/%d p50=%s p95=%s %s%s" % (name.split("/", 1)[1], s["ok"], s["n"], s["p50"], s["p95"], extra, (" " + str(s["errors"])) if s["errors"] else ""))


def ts_of(block_result):
    return datetime.fromtimestamp(int(block_result["timestamp"], 16), timezone.utc).strftime("%Y-%m-%d")


# -------------------------------------------------------------- scenarios --

def scen_basic(h):
    log("== basic methods ==")
    r = get_logs(h - 30, h, CONDITIONAL_TOKENS, [TOPIC_TRANSFER_SINGLE])
    if r["ok"] and r["result"]:
        lg = r["result"][-1]
        CTX["tx"] = lg["transactionHash"]
        zero = "0x" + "0" * 40
        CTX["holders"] = [a for a in dict.fromkeys("0x" + l["topics"][3][-40:] for l in r["result"]) if a != zero][:100]
        lg = next(l for l in reversed(r["result"]) if "0x" + l["topics"][3][-40:] != zero)
        CTX["tx"] = lg["transactionHash"]
        CTX["holder"] = "0x" + lg["topics"][3][-40:]            # to (burns go to the zero address, skip those)
        CTX["token_id"] = "0x" + lg["data"][2:66]               # id (first word of data)
    else:
        log("  could not fetch a recent TransferSingle: %s" % r["error"])
        CTX.update({"tx": None, "holder": EXCHANGE, "token_id": "0x" + "0" * 64, "holders": [EXCHANGE]})
    specs = [
        ("eth_chainId", {"method": "eth_chainId", "params": [], "expect": "0x89"}),
        ("eth_blockNumber", {"method": "eth_blockNumber", "params": []}),
        ("eth_getBlockByNumber_latest", {"method": "eth_getBlockByNumber", "params": ["latest", False], "nonnull": True}),
        ("eth_getBlockByNumber_latest_full", {"method": "eth_getBlockByNumber", "params": ["latest", True], "nonnull": True}),
        ("eth_getCode_exchange", {"method": "eth_getCode", "params": [EXCHANGE, "latest"]}),
        ("eth_getTransactionCount", {"method": "eth_getTransactionCount", "params": [CTX["holder"], "latest"]}),
        ("eth_getBalance", {"method": "eth_getBalance", "params": [CTX["holder"], "latest"]}),
        ("eth_gasPrice", {"method": "eth_gasPrice", "params": []}),
    ]
    if CTX["tx"]:
        specs.append(("eth_getTransactionReceipt", {"method": "eth_getTransactionReceipt", "params": [CTX["tx"]], "nonnull": True}))
    for name, spec in specs:
        for _ in range(5):
            r = rpc_call(CHAIN, spec)
            if name == "eth_getCode_exchange" and r["ok"] and len(r["result"]) < 100:
                r = {**r, "ok": False, "error": "wrong-result: empty code"}
            rec("basic/" + name, r)
            time.sleep(0.2)
        show("basic/" + name)


def scen_call():
    log("== eth_call latest ==")
    holder, tid, holders = CTX["holder"], CTX["token_id"], CTX["holders"]
    checks = [
        ("collateral_balanceOf", call_spec(COLLATERAL, SEL_BALANCE_OF + pad_addr(holder)), 66),
        ("usdce_balanceOf", call_spec(USDCE, SEL_BALANCE_OF + pad_addr(holder)), 66),
        ("ctf_balanceOf(addr,id)", call_spec(CONDITIONAL_TOKENS, SEL_BALANCE_OF_1155 + pad_addr(holder) + tid[2:]), 66),
    ]
    # balanceOfBatch for 50 (holder, id) pairs
    n = 50
    hs = (holders * n)[:n]
    data = (SEL_BALANCE_OF_BATCH + "%064x" % 0x40 + "%064x" % (0x60 + 0x20 * n) + "%064x" % n
            + "".join(pad_addr(a) for a in hs) + "%064x" % n + tid[2:] * n)
    checks.append(("ctf_balanceOfBatch_x50", call_spec(CONDITIONAL_TOKENS, data), 2 + 2 * (64 + 32 * n)))
    checks.append(("multicall3_usdce_balanceOf_x100", call_spec(MULTICALL3, aggregate3_data(USDCE, (holders * 100)[:100])), 2 + 2 * (64 + 100 * 32 + 100 * 128)))
    for name, spec, min_len in checks:
        for _ in range(5):
            r = rpc_call(CHAIN, spec, timeout=30)
            if r["ok"] and (not isinstance(r["result"], str) or len(r["result"]) < min_len):
                r = {**r, "ok": False, "error": "wrong-result: %s" % str(r["result"])[:40]}
            rec("call/" + name, r)
            time.sleep(0.3)
        show("call/" + name)


def scen_logs_rt(reps):
    log("== eth_getLogs realtime (latest window) ==")
    cases = [
        ("OrderFilled both exchanges", [EXCHANGE, NEG_RISK_EXCHANGE], [TOPIC_ORDER_FILLED], (40, 130, 500)),
        ("CTF TransferSingle+Batch", CONDITIONAL_TOKENS, [[TOPIC_TRANSFER_SINGLE, TOPIC_TRANSFER_BATCH]], (40, 130, 500)),
        ("collateral Transfer", COLLATERAL, [TOPIC_TRANSFER], (40, 130)),
        ("USDC.e Transfer", USDCE, [TOPIC_TRANSFER], (40, 130)),
    ]
    for label, addr, topics, windows in cases:
        for w in windows:
            name = "logs_rt/%s w=%d" % (label, w)
            for _ in range(reps):
                h = head()
                r = get_logs(h - w + 1, h, addr, topics)
                rec(name, r, {"n_logs": n_logs(r), "head": h})
                time.sleep(1.0)
            ns = [x["n_logs"] for x in RESULTS[name] if x["ok"]]
            show(name, "logs/req=%s" % (round(sum(ns) / len(ns)) if ns else None))


def scen_archive(h, reps):
    log("== archive (historical heights) ==")
    holder = CTX["holder"]
    heights = [("head-500", h - 500), ("head-1000", h - 1000), ("head-2000", h - 2000), ("head-10k", h - 10_000),
               ("head-100k", h - 100_000), ("head-1M", h - 1_000_000), ("head-5M", h - 5_000_000),
               ("head-10M", h - 10_000_000), ("head-20M", h - 20_000_000), ("head-40M", h - 40_000_000)]
    CTX["heights"] = heights
    for label, n in heights:
        date = None
        tx = None
        for _ in range(reps):
            r1 = rpc_call(CHAIN, call_spec(USDCE, SEL_BALANCE_OF + pad_addr(holder), hx(n)))
            if r1["ok"] and (not isinstance(r1["result"], str) or len(r1["result"]) != 66):
                r1 = {**r1, "ok": False, "error": "wrong-result: %s" % str(r1["result"])[:40]}
            rec("archive/%s eth_call" % label, r1, {"block": n})
            r2 = rpc_call(CHAIN, {"method": "eth_getBalance", "params": [holder, hx(n)]})
            rec("archive/%s eth_getBalance" % label, r2, {"block": n})
            r3 = rpc_call(CHAIN, {"method": "eth_getBlockByNumber", "params": [hx(n), True], "nonnull": True})
            rec("archive/%s getBlockByNumber(full)" % label, r3, {"block": n})
            if r3["ok"]:
                date = date or ts_of(r3["result"])
                if tx is None and r3["result"]["transactions"]:
                    tx = r3["result"]["transactions"][0]["hash"]
            r4 = get_logs(n - 499, n, CONDITIONAL_TOKENS, [TOPIC_TRANSFER_SINGLE])
            rec("archive/%s getLogs500 TransferSingle" % label, r4, {"block": n, "n_logs": n_logs(r4)})
            if tx:
                r5 = rpc_call(CHAIN, {"method": "eth_getTransactionReceipt", "params": [tx], "nonnull": True})
                rec("archive/%s getTransactionReceipt" % label, r5, {"block": n})
            time.sleep(0.4)
        log("  -- %s = block %d (%s)" % (label, n, date or "date unknown"))
        for m in ("eth_call", "eth_getBalance", "getBlockByNumber(full)", "getLogs500 TransferSingle", "getTransactionReceipt"):
            if "archive/%s %s" % (label, m) in RESULTS:
                show("archive/%s %s" % (label, m))


def scen_logs_big(h):
    log("== eth_getLogs big ranges ==")
    for span in (2_000, 5_000, 10_000, 20_000, 50_000):
        name = "logs_big/OrderFilled span=%d" % span
        for _ in range(3):
            r = get_logs(h - span, h - 1, [EXCHANGE, NEG_RISK_EXCHANGE], [TOPIC_ORDER_FILLED], timeout=90)
            rec(name, r, {"n_logs": n_logs(r)})
            time.sleep(0.5)
        show(name, "logs=%s" % [x["n_logs"] for x in RESULTS[name]])
    for span in (1_000, 2_000, 5_000):
        name = "logs_big/TransferSingle span=%d" % span
        for _ in range(2):
            r = get_logs(h - span, h - 1, CONDITIONAL_TOKENS, [TOPIC_TRANSFER_SINGLE], timeout=90)
            rec(name, r, {"n_logs": n_logs(r)})
            time.sleep(0.5)
        show(name, "logs=%s" % [x["n_logs"] for x in RESULTS[name]])
    # the same at a historical height (head-10M) : narrow filter, big span
    n = h - 10_000_000
    for span in (5_000, 50_000):
        name = "logs_big/hist head-10M NegRiskAdapter span=%d" % span
        for _ in range(3):
            r = get_logs(n, n + span - 1, NEG_RISK_ADAPTER, None, timeout=90)
            rec(name, r, {"n_logs": n_logs(r)})
            time.sleep(0.5)
        show(name, "logs=%s" % [x["n_logs"] for x in RESULTS[name]])


def scen_batch():
    log("== JSON-RPC batch (unique addresses, 1s gap, ascending size) ==")
    rng = random.Random(3)
    for n in (10, 20, 40, 50, 100):
        name = "batch/eth_getBalance x%d" % n
        for _ in range(3):
            rec(name, rpc_call(CHAIN, {"batch": [("eth_getBalance", [rand_addr(rng), "latest"]) for _ in range(n)]}, timeout=60))
            time.sleep(1)
        show(name)
    for n in (10, 20, 40, 50, 100):
        name = "batch/eth_call x%d" % n
        for _ in range(3):
            rec(name, rpc_call(CHAIN, {"batch": [("eth_call", [{"to": USDCE, "data": SEL_BALANCE_OF + pad_addr(rand_addr(rng))}, "latest"]) for _ in range(n)]}, timeout=60))
            time.sleep(1)
        show(name)
    h = head()
    spec = {"batch": [("eth_blockNumber", []), ("eth_getBlockByNumber", [hx(h), False]),
                      ("eth_getLogs", [{"fromBlock": hx(h - 129), "toBlock": hx(h), "address": [EXCHANGE, NEG_RISK_EXCHANGE], "topics": [TOPIC_ORDER_FILLED]}]),
                      ("eth_call", [{"to": USDCE, "data": SEL_BALANCE_OF + pad_addr(CTX["holder"])}, "latest"]),
                      ("eth_getBalance", [CTX["holder"], "latest"]), ("eth_getTransactionCount", [CTX["holder"], "latest"])]}
    for _ in range(3):
        rec("batch/mixed x6", rpc_call(CHAIN, spec, timeout=60))
        time.sleep(1)
    show("batch/mixed x6")


def scen_sustain(seconds, rps):
    log("== sustained %.1f req/s mixed for %ds ==" % (rps, seconds))
    t_end = time.time() + seconds
    i = 0
    while time.time() < t_end:
        h = head()
        k = i % 4
        if k == 0:
            r = get_logs(h - 129, h, [EXCHANGE, NEG_RISK_EXCHANGE], [TOPIC_ORDER_FILLED]); name = "sustain/OrderFilled w=130"
        elif k == 1:
            r = rpc_call(CHAIN, call_spec(USDCE, SEL_BALANCE_OF + pad_addr(CTX["holder"]))); name = "sustain/eth_call"
        elif k == 2:
            r = rpc_call(CHAIN, {"method": "eth_getBlockByNumber", "params": [hx(h), True], "nonnull": True}); name = "sustain/getBlockByNumber(head,full)"
        else:
            r = get_logs(h - 499, h, CONDITIONAL_TOKENS, [[TOPIC_TRANSFER_SINGLE, TOPIC_TRANSFER_BATCH]]); name = "sustain/CTF transfers w=500"
        rec(name, r, {"head": h})
        i += 1
        time.sleep(max(0, 2.0 / rps - r["latency"]))
    for k in ("OrderFilled w=130", "eth_call", "getBlockByNumber(head,full)", "CTF transfers w=500"):
        show("sustain/" + k)


def scen_null_cache(h):
    log("== null-cache check on old blocks (6 tries, sequence) ==")
    rng = random.Random(5)
    for n in (h - 3_000_000 - rng.randrange(1000), h - 15_000_000 - rng.randrange(1000), h - 30_000_000 - rng.randrange(1000), h - 60_000_000 - rng.randrange(1000)):
        for full in (False, True):
            seq = []
            for _ in range(6):
                r = rpc_call(CHAIN, {"method": "eth_getBlockByNumber", "params": [hx(n), full], "nonnull": True})
                rec("nullcache/block full=%s" % full, r, {"block": n})
                seq.append("ok" if r["ok"] else ("null" if "null-result" in (r["error"] or "") else "err"))
                time.sleep(0.3)
            log("  %d full=%-5s : %s" % (n, full, " ".join(seq)))


def scen_burst(threads):
    log("== burst: 300 unique single eth_call + 3000 in batches of 40, %d threads ==" % threads)
    rng = random.Random(9)
    single = [rand_addr(rng) for _ in range(300)]
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=threads) as ex:
        for r in ex.map(lambda a: rpc_call(CHAIN, call_spec(USDCE, SEL_BALANCE_OF + pad_addr(a))), single):
            rec("burst/single eth_call", r)
    dt = time.perf_counter() - t0
    show("burst/single eth_call", "%.0f req/s " % (300 / dt))
    time.sleep(3)
    addrs = [rand_addr(rng) for _ in range(3000)]
    chunks = [addrs[i:i + 40] for i in range(0, 3000, 40)]
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=threads) as ex:
        for r in ex.map(lambda c: rpc_call(CHAIN, {"batch": [("eth_call", [{"to": USDCE, "data": SEL_BALANCE_OF + pad_addr(a)}, "latest"]) for a in c]}, timeout=60), chunks):
            rec("burst/batch40 eth_call", r)
    dt = time.perf_counter() - t0
    show("burst/batch40 eth_call", "%.0f calls/s " % (3000 / dt))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenarios", default="basic,call,logs_rt,archive,logs_big,nullcache,batch,sustain,burst")
    ap.add_argument("--reps", type=int, default=6)
    ap.add_argument("--sustain-seconds", type=int, default=90)
    ap.add_argument("--threads", type=int, default=6)
    ap.add_argument("--out", default="results_polygon_polymarket_%s.json" % datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    a = ap.parse_args()
    want = set(a.scenarios.split(","))
    h = head()
    log("Polygon head = %d (%s)" % (h, rpcutil.BASE))
    scen_basic(h)                                # always: fills CTX
    if "call" in want:
        scen_call()
    if "logs_rt" in want:
        scen_logs_rt(a.reps)
    if "archive" in want:
        scen_archive(h, a.reps)
    if "logs_big" in want:
        scen_logs_big(h)
    if "nullcache" in want:
        scen_null_cache(h)
    if "batch" in want:
        scen_batch()
    if "sustain" in want:
        scen_sustain(a.sustain_seconds, 1.5)
    if "burst" in want:
        scen_burst(a.threads)
    summary = {k: summarize(k) for k in RESULTS}
    json.dump({"chain": CHAIN, "head": h, "when": datetime.now(timezone.utc).isoformat(), "ctx": CTX,
               "summary": summary, "samples": RESULTS, "log": LOG, "conn": rpcutil.conn_stats()},
              open(a.out, "w"), indent=1, ensure_ascii=False)
    log("saved %s ; %s" % (a.out, rpcutil.conn_stats()))


if __name__ == "__main__":
    main()
