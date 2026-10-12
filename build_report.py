#!/usr/bin/env python3
"""Build the report data and render it into a new report page.

Inputs (newest results_* files are picked automatically; override with flags):
  chains_all.json                 chain catalog snapshot (/api/public/chains?limit=500)
  overview.json                   router overview snapshot (/api/public/overview), optional
  results_raw_*.json              output of rpc_bench.py      (availability, incl. history)
  results_latcache_raw_*.json     output of latency_cache.py  (stable latency + cache probe)
  report_template.html            the report page; its <script id="data"> block gets the data
  runs/*.json                     summaries of earlier runs, for the trend section

Outputs:
  report_data.json                merged per-chain record (everything, for digging)
  report_embed.json               the compact data the page renders
  runs/<bench ts>.json            this run's summary (KPIs + per-chain issues), read by later builds
  rpc_report_<date>_<time>.html   a new page (or --html); existing reports are never touched

Example
-------
  python3 build_report.py
  python3 build_report.py --bench results_raw_X.json --latcache results_latcache_raw_Y.json --html out.html
"""
import argparse
import glob
import json
import os
import re
import statistics
import sys
from collections import Counter
from datetime import datetime
from urllib.parse import urlparse

from rpcutil import (BASE, ERROR_KINDS, HISTORY_METHODS, METHOD_GROUP, METHOD_LABEL, classify_error, pct,
                     load_chains, head_regressions)
from classify_chains import redundancy_tier

TIER_LABELS = {"A-high": "A 高冗余 (≥10)", "B-medium": "B 中 (4–9)", "C-low": "C 低/单点 (≤3)"}
HIST = {m["id"]: m for m in HISTORY_METHODS}
SLOW_COLD_MS = 1000     # cold upstream fetch at/above this -> "上游慢"
JITTER_MS = 200         # eth_blockNumber stdev at/above this -> "抖动大"
BENEFIT_X = 1.3         # speedup at/above this counts as "cache helps"
HEAD_BACK_WARN = 3      # eth_blockNumber went back this many blocks or more -> warn (less -> info)
TREND_RUNS = 8          # runs shown in the trend section (this one included)
SHORT_TTL_S = 1.5       # latest answers cached shorter than this (s) are listed in section 08
ODD_SHARE = 0.1         # a method is "cached on some chains" when more than this share of chains disagree
_ok = lambda i, lo, hi: i["kind"] == "ok" and lo <= i["s"] < hi
TTL_BUCKETS = [         # (label, test on ttl_info) for the latest cache lifetime distribution
    ("不缓存", lambda i: i["kind"] == "none"),
    ("< 0.5 s", lambda i: _ok(i, 0, 0.5)),
    ("0.5–1.5 s", lambda i: _ok(i, 0.5, 1.5)),
    ("1.5–2.5 s", lambda i: _ok(i, 1.5, 2.5)),
    ("≥ 2.5 s", lambda i: _ok(i, 2.5, float("inf")) or (i["kind"] == "min" and i["min"] >= 2.5)),
    ("测不出", lambda i: i["kind"] in ("unknown", "skip") or (i["kind"] == "min" and i["min"] < 2.5)),
]
LEVEL_RANK = {"crit": 0, "warn": 1, "info": 2}
REGULAR = ("simple", "complex")


def newest(pattern):
    files = sorted(glob.glob(pattern))
    return files[-1] if files else None


def ms(x):
    return None if x is None else int(round(x * 1000))


def by_cid(d):
    return {int(k): v for k, v in d.items()}


def kind_label(kind):
    return ERROR_KINDS.get(kind or "other", ERROR_KINDS["other"])[0]


def top_error(errors):
    """Most common cause among [{error, n}] (re-classified with the current rules) + an example."""
    kinds = Counter()
    for e in errors or []:
        kinds[classify_error(e["error"])] += e["n"]
    if not kinds:
        return {"kind": None, "err": None}
    kind = kinds.most_common(1)[0][0]
    return {"kind": kind, "err": next(e["error"] for e in errors if classify_error(e["error"]) == kind)}


def method_label(mid):
    if mid in HIST:
        return "%s (%s)" % (HIST[mid]["label"], HIST[mid]["window"])
    return METHOD_LABEL.get(mid, mid)


def avail(methods, groups):
    """(success %, ok, total) over the methods of the given groups."""
    ms_ = [m for m in methods if m["group"] in groups]
    ok, tot = sum(m["ok"] for m in ms_), sum(m["total"] for m in ms_)
    return (round(ok / tot * 100, 1) if tot else None), ok, tot


def head_back(track):
    """rpc_bench head_track -> {n_back, max_back, n, heads, sec_back} (None for older runs)."""
    if not track or not track.get("heads"):
        return None
    ts = [t for t, _ in track["heads"]]
    bs = [b for _, b in track["heads"]]
    n_back, max_back = head_regressions(bs)
    # blocks per second from the running maximum, to express "blocks back" as time
    best = [max(bs[:i + 1]) for i in range(len(bs))]
    rate = (best[-1] - best[0]) / (ts[-1] - ts[0]) if ts[-1] > ts[0] and best[-1] > best[0] else None
    return {"n_back": n_back, "max_back": max_back, "n": len(bs), "heads": bs,
            "sec_back": round(max_back / rate, 1) if (rate and max_back) else None,
            "block_s": round(1 / rate, 2) if rate else None}


def method_cache(cache):
    """latency_cache test 3 -> {method: {label, cached, first, rep, ...}} (None for older runs / skipped chains)."""
    if not cache.get("methods"):
        return None
    return {mid: {"label": m["label"], "cached": m["cached"], "first": ms(m["first"]), "rep": ms(m["rep_med"]),
                  "ok": m["ok"], "total": m["total"], "kind": m["kind"], "err": m["error"]}
            for mid, m in cache["methods"].items()}


def latest_ttl(cache):
    """latency_cache test 4 -> lifetime summary (None for older runs / skipped chains)."""
    t = cache.get("ttl")
    if not t:
        return None
    return {"s": None if t["ttl"] is None else round(t["ttl"], 2), "min": t["ttl_min"], "none": t["all_miss"],
            "gaps": t["gaps"], "poll": t["poll"], "window": t["window"],
            "n": t["n"], "n_miss": t["n_miss"], "n_err": t["n_err"],
            "err": t.get("error"), "kind": classify_error(t.get("error"))}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bench", help="results_raw_*.json from rpc_bench.py (default: newest)")
    ap.add_argument("--latcache", help="results_latcache_raw_*.json from latency_cache.py (default: newest)")
    ap.add_argument("--template", default="report_template.html", help="report page to fill (default: %(default)s)")
    ap.add_argument("--html", help="output page (default: rpc_report_<bench date>_<time>.html)")
    ap.add_argument("--runs", default="runs", help="run-summary directory for the trend section (default: runs)")
    args = ap.parse_args()

    bench_path = args.bench or newest("results_raw_*.json")
    lc_path = args.latcache or newest("results_latcache_raw_*.json")
    if not bench_path or not lc_path:
        print("need both a results_raw_*.json and a results_latcache_raw_*.json", file=sys.stderr)
        return 1
    print("bench    :", bench_path)
    print("latcache :", lc_path)

    bench = json.load(open(bench_path))
    lc = json.load(open(lc_path))
    overview = json.load(open("overview.json")) if os.path.exists("overview.json") else {}
    catalog = {x["chainId"]: x for x in load_chains()}

    bres = by_cid(bench["results"])
    if any("methods" not in r for r in bres.values()):
        print("%s was written by an older rpc_bench.py (no error details); re-run it" % bench_path, file=sys.stderr)
        return 1
    lat = by_cid(lc["latency"])
    cache = by_cid(lc["cache"])
    cids = sorted(set(bres) & set(lat) & set(cache))
    missing = (set(bres) | set(lat) | set(cache)) - set(cids)
    if missing:
        print("warning: %d chains not present in both runs, skipped: %s" % (len(missing), sorted(missing)))

    chains = []
    for cid in cids:
        cat = catalog.get(cid, {})
        active = cat.get("active", 0) or 0
        pm = bres[cid]["methods"]
        lm = lat[cid]
        cf = cache[cid]["fixed"]
        cl = cache[cid]["latest"]

        speed = (cf["cold"] / cf["warm_med"]) if (cf["cold"] and cf["warm_med"]) else None

        c = {
            "chainId": cid,
            "name": cat.get("name") or str(cid),
            "sym": cat.get("nativeSymbol"),
            "tier": redundancy_tier(active),
            "active": active,
            "single_point": active <= 1,
            # router's cumulative request counter; it includes this toolkit's own test requests
            "ingress_incl_tests": cat.get("ingressTotal") or 0,
            "head": bres[cid].get("head"),
            "head_source": bres[cid].get("head_source"),
            "avail_regular": avail(pm.values(), REGULAR)[0],
            "avail_advanced": avail(pm.values(), ("advanced",))[0],
            "avail_history": avail(pm.values(), ("history",))[0],
            "tx_found": bres[cid].get("tx_found"),
            "head_back": head_back(bres[cid].get("head_track")),
            "methods": {mid: dict({
                "group": m["group"], "ok": m["ok"], "total": m["total"],
                "avg": ms(m["lat_avg"]), "p50": ms(m["lat_p50"]),
                "errors": m["errors"], "blocks": m.get("blocks"), "skipped": m.get("skipped"),
            }, **top_error(m["errors"])) for mid, m in pm.items()},
            "lat": {mid: {"ok": v["ok"], "total": v["total"], "p50": ms(v["p50"]), "p95": ms(v["p95"]),
                          "jitter": ms(v["stdev"]), "err": v["err"], "kind": classify_error(v["err"])}
                    for mid, v in lm.items()},
            "cache": {
                # block = the first block whose cold call failed, else the first block probed
                "block": cache[cid]["block"], "blocks": cache[cid].get("blocks", [cache[cid]["block"]]),
                "cold": ms(cf["cold"]), "warm": ms(cf["warm_med"]), "warm_min": ms(cf["warm_min"]),
                "speedup": round(speed, 1) if speed else None,
                # older latency_cache.py runs probed a single block
                "n_blocks": cf.get("n_blocks", 1),
                "cold_ok": cf.get("cold_ok", int(cf["cold"] is not None)),
                "cold_all": [ms(x) for x in cf.get("cold_all", [cf["cold"]])],
                "ok": cf["ok"], "total": cf.get("total", cf["reps"]), "reps": cf["reps"],
                "cold_kind": classify_error(cf.get("cold_error")), "cold_err": cf.get("cold_error"),
                "cold_fail_ms": ms(cf.get("cold_fail_latency")),
                "retry_call": cf.get("retry_ok_call"), "retry_ms": ms(cf.get("retry_ok_latency")),
                "probes": [{"block": p["block"], "calls": [
                    {"ok": x["ok"], "ms": ms(x["latency"]), "kind": classify_error(x["error"]), "err": x["error"]}
                    for x in p["calls"]]}
                    for p in cf.get("probes", [{"block": cache[cid]["block"], "calls": cf.get("calls", [])}])],
                "latest": {"cold": ms(cl["cold"]), "warm": ms(cl["warm_med"]), "ok": cl["ok"],
                           "total": cl.get("total", cl["reps"]), "reps": cl["reps"],
                           "err": cl.get("cold_error") or next((x["error"] for x in cl.get("calls", []) if x["error"]), None)},
                # which methods are cached / how long a latest answer stays cached; skip = why not tested
                "methods": method_cache(cache[cid]), "ttl": latest_ttl(cache[cid]), "skip": cache[cid].get("skip"),
            },
        }
        c["block_s"] = c["head_back"]["block_s"] if c["head_back"] else None
        c["issues"] = chain_issues(c)
        c["level"] = min((LEVEL_RANK[i["level"]] for i in c["issues"]), default=None)
        chains.append(c)

    embed = build_embed(chains, bench, lc, overview)

    # trend: this run against the earlier run summaries in --runs
    run = run_summary(embed, chains, bench)
    runs = load_runs(args.runs, run["ts"]) + [run]
    embed["trend"] = build_trend(runs)
    for r in embed["issue_rows"]:
        r["seen"] = embed["trend"]["seen"].get(str(r["chainId"]))

    with open("report_data.json", "w") as f:
        json.dump(chains, f, ensure_ascii=False, indent=1)
    with open("report_embed.json", "w") as f:
        json.dump(embed, f, ensure_ascii=False)
    os.makedirs(args.runs, exist_ok=True)
    run_path = os.path.join(args.runs, "%s.json" % run["ts"])
    with open(run_path, "w") as f:
        json.dump(run, f, ensure_ascii=False, indent=1)
    out = args.html or "rpc_report_%s.html" % datetime.strptime(run["ts"], "%Y%m%dT%H%M%SZ").strftime("%Y%m%d_%H%M")
    render(args.template, out, embed)

    print("chains   : %d  (issues: %s)" % (len(chains), dict(Counter(
        i["code"] for c in chains for i in c["issues"]))))
    print("trend    : %d runs (%s)" % (len(embed["trend"]["runs"]), ", ".join(r["ts"] for r in embed["trend"]["runs"])))
    print("wrote report_data.json, report_embed.json, %s, %s" % (run_path, out))
    return 0


def run_summary(embed, chains, bench):
    """What later builds need from this run: how it was measured, the KPIs, per-chain issues."""
    m, cs, bm = embed["meta"], embed["cache_summary"], bench.get("meta", {})
    return {
        "ts": bm.get("ts"), "date": datetime.strptime(bm["ts"], "%Y%m%dT%H%M%SZ").strftime("%Y-%m-%d %H:%M UTC"),
        "method": {"samples": bm.get("samples"), "interval": bm.get("interval") or 0,
                   "keepalive": "connections" in bm,   # rpc_bench reuses connections since it reports them
                   "cache_blocks": cs["blocks"], "advanced": m["avail_advanced"] is not None},
        "kpi": {"n_chains": m["n_chains"], "avail_regular": m["avail_regular"],
                "avail_advanced": m["avail_advanced"], "avail_history": m["avail_history"],
                "hot_p50": m["hot_p50"], "cold_p50": cs["cold_p50"], "warm_p50": cs["warm_p50"],
                "speedup_med": cs["speedup_med"], "n_issue": len(embed["issue_rows"]),
                # None for runs from before latency_cache checked methods / the latest cache lifetime
                "ttl_p50": embed["cache_ttl"]["p50"] if embed["cache_ttl"] else None,
                "n_uncached": sum(1 for x in embed["cache_methods"] if x["verdict"] == "uncached")
                if embed["cache_methods"] else None,
                "n_crit": sum(1 for r in embed["issue_rows"] if r["issues"][0]["level"] == "crit"),
                # None for runs from before rpc_bench tracked eth_blockNumber answers
                "n_head_back": sum(1 for c in chains if c["head_back"] and c["head_back"]["n_back"])
                if any(c["head_back"] for c in chains) else None},
        "chains": {str(c["chainId"]): {"name": c["name"], "level": c["level"],
                                       "issues": [[i["code"], i["level"], i["label"]] for i in c["issues"]]}
                   for c in chains},
    }


def load_runs(path, before_ts):
    runs = []
    for f in sorted(glob.glob(os.path.join(path, "*.json"))):
        r = json.load(open(f))
        if r.get("ts") and r["ts"] < before_ts:
            runs.append(r)
    return sorted(runs, key=lambda r: r["ts"])


def method_text(m):
    parts = ["连接复用" if m["keepalive"] else "每次新建连接", "%s 次采样" % m["samples"],
             "限速 %ss" % m["interval"] if m["interval"] else "不限速"]
    if (m.get("cache_blocks") or 1) > 1:
        parts.append("缓存测 %d 个区块" % m["cache_blocks"])
    if m.get("advanced"):
        parts.append("含进阶方法")
    return " · ".join(parts)


def build_trend(runs):
    """KPIs of the last TREND_RUNS runs + which chains became / stopped being problems since the previous one."""
    runs = runs[-TREND_RUNS:]
    cur = runs[-1]
    prev = runs[-2] if len(runs) > 1 else None

    def problems(r):   # chains with a warn/crit issue
        return {cid: c for cid, c in r["chains"].items() if c["level"] is not None and c["level"] <= LEVEL_RANK["warn"]}

    def labels(c):
        return [i[2] for i in c["issues"] if i[1] != "info"]

    now = problems(cur)
    out = {
        "runs": [dict(r["kpi"], ts=r["ts"], date=r["date"], method=method_text(r["method"])) for r in runs],
        "n": len(runs),
        # in how many of the shown runs each current problem chain was a problem
        "seen": {cid: sum(1 for r in runs if cid in problems(r)) for cid in now},
        "prev": None,
    }
    if prev:
        before = problems(prev)
        key = lambda kv: int(kv[0])
        out["prev"] = {
            "date": prev["date"], "same_method": prev["method"] == cur["method"],
            "new": [{"chainId": int(cid), "name": c["name"], "issues": labels(c)}
                    for cid, c in sorted(now.items(), key=key) if cid not in before],
            "resolved": [{"chainId": int(cid), "name": c["name"], "issues": labels(c),
                          "untested": cid not in cur["chains"]}
                         for cid, c in sorted(before.items(), key=key) if cid not in now],
        }
    return out


def fmt_fails(items):
    """[(what, ok, total, kind, err)] -> one line per distinct (kind, err): 'a 0/3、b 0/3 —— 原因：原文'."""
    groups = {}
    for what, ok, total, kind, err in items:
        groups.setdefault((kind, err), []).append("%s %d/%d" % (what, ok, total))
    return "；".join("%s —— %s：%s" % ("、".join(v), kind_label(k[0]), k[1] or "") for k, v in groups.items())


def chain_issues(c):
    """Per-chain findings, each {code, level, label, detail}. Levels: crit > warn > info."""
    out = []
    pm, lm, ca = c["methods"], c["lat"], c["cache"]

    def add(code, level, label, detail):
        out.append({"code": code, "level": level, "label": label, "detail": detail})

    # 1) chain unreachable: a whole test phase got zero successful answers
    reg = [m for m in pm.values() if m["group"] != "history" and m["total"]]
    phases = [
        ("可用性测试", sum(m["ok"] for m in reg), sum(m["total"] for m in reg),
         next((m["err"] for m in reg if m["err"]), None)),
        ("延迟测试", sum(v["ok"] for v in lm.values()), sum(v["total"] for v in lm.values()),
         next((v["err"] for v in lm.values() if v["err"]), None)),
        ("缓存测试", ca["ok"] + ca["latest"]["ok"], ca["total"] + ca["latest"]["total"],
         ca["cold_err"] or ca["latest"]["err"]),
    ]
    dead = [p for p in phases if p[2] and p[1] == 0]
    if dead:
        label = "整链不可用" if len(dead) == len(phases) else "测试期间曾整链不可用"
        add("down", "crit", label, fmt_fails(
            [(n + "阶段", ok, tot, classify_error(e), e) for n, ok, tot, e in dead]))
    dead_names = {p[0] for p in dead}

    # 2) eth_chainId returned another chain
    if pm.get("eth_chainId", {}).get("kind") == "wrong_chain":
        add("wrong_chain", "crit", "返回了错误的链", pm["eth_chainId"]["err"])

    # 3) regular (latest-block) and advanced methods with failures
    if "可用性测试" not in dead_names:
        for code, label, groups in (("regular_fail", "常规请求失败", REGULAR),
                                    ("advanced_fail", "进阶方法失败", ("advanced",))):
            bad = [(mid, m) for mid, m in pm.items() if m["group"] in groups and m["ok"] < m["total"]
                   and m["kind"] != "wrong_chain"]
            if bad:
                add(code, "warn", label, fmt_fails(
                    [(method_label(mid), m["ok"], m["total"], m["kind"], m["err"]) for mid, m in bad]))

    # 3b) eth_blockNumber went backwards: the router mixes upstreams at different heights
    hb = c["head_back"]
    if hb and hb["n_back"]:
        add("head_back", "warn" if hb["max_back"] >= HEAD_BACK_WARN else "info", "块高回退",
            "eth_blockNumber %d 次调用中有 %d 次比之前返回的块高还低，最多低 %s 块%s"
            "——用户可能先看到新块、再看到旧块" % (
                hb["n"], hb["n_back"], format(hb["max_back"], ","),
                "(约 %s 秒)" % hb["sec_back"] if hb["sec_back"] else ""))

    # 4) historical blocks: a depth never answered / answered only sometimes
    hist = [(mid, pm[mid]) for mid in HIST if mid in pm and pm[mid]["total"]]
    if "可用性测试" not in dead_names:
        none = [(mid, m) for mid, m in hist if m["ok"] == 0]
        some = [(mid, m) for mid, m in hist if 0 < m["ok"] < m["total"]]
        if none:
            level = "crit" if any(mid == "hist_recent" for mid, _ in none) else "warn"
            add("history_missing", level, "历史区块查不到", fmt_fails(
                [(method_label(mid), m["ok"], m["total"], m["kind"], m["err"]) for mid, m in none]))
        if some:
            add("history_flaky", "warn", "历史区块偶发失败", fmt_fails(
                [(method_label(mid), m["ok"], m["total"], m["kind"], m["err"]) for mid, m in some]))

    # 5) cache probe: the first (cold) call failed on some block(s); the cold_* fields describe the first one
    n_fail = ca["n_blocks"] - ca["cold_ok"]
    if n_fail and "缓存测试" not in dead_names:
        where = "缓存测试 %d 个区块中有 %d 个" % (ca["n_blocks"], n_fail) if ca["n_blocks"] > 1 else "缓存测试"
        if ca["retry_call"]:
            add("cold_fail", "warn", "首次请求失败", "%s第 1 次请求失败(如区块 #%s:%s，耗时 %s ms：%s)，第 %d 次才成功(%s ms)"
                "——路由器遇到这个错误没有自动换上游重试" % (
                    where, ca["block"], kind_label(ca["cold_kind"]), ca["cold_fail_ms"], ca["cold_err"],
                    ca["retry_call"], ca["retry_ms"]))
        else:
            add("cache_all_fail", "warn", "缓存区块全部失败", "%s第 1 次请求失败;区块 #%s 连续 %d 次请求全部失败(%s：%s)" % (
                where, ca["block"], ca["reps"], kind_label(ca["cold_kind"]), ca["cold_err"]))

    # 6) slow upstream, jitter, single point
    if ca["cold"] is not None and ca["cold"] >= SLOW_COLD_MS:
        add("slow_upstream", "warn", "上游慢", "冷回源 %s ms(缓存命中后 %s ms)" % (
            format(ca["cold"], ","), ca["warm"]))
    j = lm.get("eth_blockNumber", {}).get("jitter")
    if j is not None and j >= JITTER_MS:
        add("jitter", "info", "延迟抖动大", "eth_blockNumber 连续请求耗时标准差 %d ms" % j)
    if c["single_point"]:
        add("single_point", "info", "单点", "只有 %d 个活跃上游节点" % c["active"])

    out.sort(key=lambda i: LEVEL_RANK[i["level"]])
    return out


def build_cache_methods(chains):
    """Per method of latency_cache test 3: on how many chains it is cached, and the chains that differ."""
    tested = [c for c in chains if c["cache"]["methods"]]
    mids = list(dict.fromkeys(mid for c in tested for mid in c["cache"]["methods"]))
    out = []
    for mid in mids:
        rows = [(c, c["cache"]["methods"][mid]) for c in tested if mid in c["cache"]["methods"]]
        n_c = sum(1 for _, m in rows if m["cached"])
        n_u = sum(1 for _, m in rows if m["cached"] is False)
        major = n_c >= n_u
        # the majority's verdict unless more than ODD_SHARE of the chains disagree
        verdict = ("unknown" if not (n_c or n_u) else "mixed" if min(n_c, n_u) > ODD_SHARE * (n_c + n_u)
                   else "cached" if major else "uncached")
        out.append({
            "id": mid, "label": rows[0][1]["label"], "verdict": verdict,
            "calls": max(m["total"] for _, m in rows),
            "n_cached": n_c, "n_uncached": n_u, "n_unknown": len(rows) - n_c - n_u,
            "first_p50": pct([m["first"] for _, m in rows if m["first"] is not None], 50),
            "rep_p50": pct([m["rep"] for _, m in rows if m["rep"] is not None], 50),
            # chains on the minority side, and chains where the test failed
            "odd": [{"chainId": c["chainId"], "name": c["name"], "first": m["first"], "rep": m["rep"]}
                    for c, m in rows if m["cached"] is not None and m["cached"] != major],
            "unknown": [{"chainId": c["chainId"], "name": c["name"], "kind": m["kind"] or "other", "msg": m["err"],
                         "ok": m["ok"], "total": m["total"]} for c, m in rows if m["cached"] is None],
        })
    return out


def ttl_kind(t):
    if t is None:
        return None
    return "none" if t["none"] else "ok" if t["s"] is not None else "min" if t["min"] is not None else "unknown"


def ttl_sort(t):
    """Sort key of a chain's latest cache lifetime: not cached = 0, a lower bound counts as its value."""
    k = ttl_kind(t)
    return 0 if k == "none" else t["s"] if k == "ok" else t["min"] if k == "min" else None


def ttl_info(c):
    t, skip = c["cache"]["ttl"], c["cache"]["skip"]
    if t is None:
        return {"kind": "skip", "reason": skip} if skip else None
    return {"kind": ttl_kind(t), "s": t["s"], "min": t["min"], "gaps": t["gaps"], "poll": t["poll"],
            "n": t["n"], "n_miss": t["n_miss"], "n_err": t["n_err"], "block_s": c["block_s"],
            "err": t["err"], "ekind": t["kind"]}


def build_cache_ttl(chains):
    """Distribution of the latest cache lifetime (latency_cache test 4); None for older runs."""
    infos = [(c, ttl_info(c)) for c in chains]
    if not any(i and i["kind"] != "skip" for _, i in infos):
        return None
    t0 = next(c["cache"]["ttl"] for c in chains if c["cache"]["ttl"])
    buckets = []
    for label, test in TTL_BUCKETS:
        rows = [c for c, i in infos if i and test(i)]
        buckets.append({"label": label, "chains": [{"chainId": c["chainId"], "name": c["name"],
                                                    "ttl": ttl_sort(c["cache"]["ttl"])} for c in rows]})
    vals = [i["s"] for _, i in infos if i and i["kind"] == "ok"]
    # chains where latest answers are barely cached, or where the lifetime could not be measured
    short = [dict(i, chainId=c["chainId"], name=c["name"], tier=c["tier"], ttl=ttl_sort(c["cache"]["ttl"]))
             for c, i in infos if i and (i["kind"] in ("none", "unknown", "skip")
                                         or (i["kind"] == "ok" and i["s"] < SHORT_TTL_S))]
    short.sort(key=lambda r: (r["ttl"] is None, r["ttl"] or 0, r["chainId"]))
    return {"p50": pct(vals, 50), "n": len(vals), "poll": t0["poll"], "window": t0["window"],
            "short_s": SHORT_TTL_S, "buckets": buckets, "short": short}


def build_embed(chains, bench, lc, overview):
    bmeta = bench.get("meta", {})

    def fmt_ts(ts):
        try:
            return datetime.strptime(ts, "%Y%m%dT%H%M%SZ").strftime("%Y-%m-%d %H:%M UTC")
        except ValueError:
            return ts

    when = fmt_ts(bmeta.get("ts") or lc.get("ts") or "")
    lc_when = fmt_ts(lc.get("ts") or "")
    if lc_when and lc_when != when:
        # the two scripts ran at different times: show both
        when = "%s(延迟/缓存 %s)" % (when, lc_when[11:] if lc_when[:10] == when[:10] else lc_when)

    # ---- availability per method ----
    mids = list(chains[0]["methods"]) if chains else []
    methods_avail = []
    for mid in mids:
        ok = sum(c["methods"][mid]["ok"] for c in chains)
        tot = sum(c["methods"][mid]["total"] for c in chains)
        methods_avail.append({"name": mid, "label": method_label(mid), "group": METHOD_GROUP[mid],
                              "ok": ok, "total": tot, "sr": round(ok / tot * 100, 1) if tot else None})

    def group_avail(groups):
        ok = sum(m["ok"] for m in methods_avail if m["group"] in groups)
        tot = sum(m["total"] for m in methods_avail if m["group"] in groups)
        return (round(ok / tot * 100, 1) if tot else None), ok, tot

    reg_sr, reg_ok, reg_tot = group_avail(set(REGULAR))
    a_sr, a_ok, a_tot = group_avail({"advanced"})
    h_sr, h_ok, h_tot = group_avail({"history"})

    # ---- latency per method (median across chains of per-chain p50 / p95) ----
    lat_mids = list(chains[0]["lat"]) if chains else []
    methods_latency = []
    for mid in lat_mids:
        p50s = [c["lat"][mid]["p50"] for c in chains if c["lat"][mid]["p50"] is not None]
        p95s = [c["lat"][mid]["p95"] for c in chains if c["lat"][mid]["p95"] is not None]
        methods_latency.append({"name": mid, "p50": pct(p50s, 50), "p95": pct(p95s, 50), "n": len(p50s)})
    hot_p50 = pct([m["p50"] for m in methods_latency if m["p50"] is not None], 50)
    lat_samples = max((c["lat"][mid]["total"] for c in chains for mid in lat_mids), default=0)

    # ---- regular + advanced method failures ----
    failures = []
    for c in chains:
        for mid, m in c["methods"].items():
            if m["group"] != "history" and m["total"] and m["ok"] < m["total"]:
                failures.append({"chainId": c["chainId"], "name": c["name"], "method": method_label(mid),
                                 "group": m["group"], "ok": m["ok"], "total": m["total"],
                                 "kind": m["kind"] or "other", "msg": m["err"]})

    # ---- historical blocks ----
    depths = []
    for mid, spec in HIST.items():
        cells = [c["methods"][mid] for c in chains if mid in c["methods"]]
        ok = sum(m["ok"] for m in cells)
        tot = sum(m["total"] for m in cells)
        depths.append({
            "id": mid, "label": spec["label"], "window": spec["window"],
            "ok": ok, "total": tot, "sr": round(ok / tot * 100, 1) if tot else None,
            "chains_none": sum(1 for m in cells if m["total"] and m["ok"] == 0),
            "chains_some": sum(1 for m in cells if 0 < m["ok"] < m["total"]),
            "chains_skipped": sum(1 for m in cells if not m["total"]),
        })
    hist_rows = []
    for c in chains:
        cells = {mid: c["methods"][mid] for mid in HIST if mid in c["methods"]}
        if any(m["total"] and m["ok"] < m["total"] for m in cells.values()):
            hist_rows.append({"chainId": c["chainId"], "name": c["name"], "tier": c["tier"],
                              "cells": {mid: {"ok": m["ok"], "total": m["total"], "kind": m["kind"],
                                              "msg": m["err"], "skipped": m["skipped"],
                                              "blocks": m["blocks"]} for mid, m in cells.items()}})
    hist_rows.sort(key=lambda r: (sum(x["ok"] for x in r["cells"].values()) /
                                  max(1, sum(x["total"] for x in r["cells"].values())), r["chainId"]))

    # ---- cache ----
    with_speed = [c for c in chains if c["cache"]["speedup"] is not None]
    colds = [c["cache"]["cold"] for c in chains if c["cache"]["cold"] is not None]
    warms = [c["cache"]["warm"] for c in chains if c["cache"]["warm"] is not None]
    speeds = [c["cache"]["speedup"] for c in with_speed]
    cache_summary = {
        "cold_p50": pct(colds, 50), "warm_p50": pct(warms, 50),
        "speedup_med": round(statistics.median(speeds), 1) if speeds else None,
        "benefit": sum(1 for s in speeds if s >= BENEFIT_X), "benefit_x": BENEFIT_X,
        "n": len(with_speed), "n_total": len(chains),
        # chains where at least one block's cold call failed / where every one did (no cold latency)
        "n_cold_fail": sum(1 for c in chains if c["cache"]["cold_ok"] < c["cache"]["n_blocks"]),
        "n_cold_none": sum(1 for c in chains if c["cache"]["cold"] is None),
        "reps": max((c["cache"]["reps"] for c in chains), default=0),   # calls per block
        "blocks": max((c["cache"]["n_blocks"] for c in chains), default=0),
    }
    cache_top = [{"chainId": c["chainId"], "name": c["name"], "cold": c["cache"]["cold"],
                  "warm": c["cache"]["warm"], "speedup": c["cache"]["speedup"]}
                 for c in sorted(with_speed, key=lambda c: -c["cache"]["speedup"])[:12]]
    cache_fail = [{"chainId": c["chainId"], "name": c["name"], "block": c["cache"]["block"],
                   "kind": c["cache"]["cold_kind"] or "other", "msg": c["cache"]["cold_err"],
                   "fail_ms": c["cache"]["cold_fail_ms"], "retry_call": c["cache"]["retry_call"],
                   "retry_ms": c["cache"]["retry_ms"], "ok": c["cache"]["ok"], "reps": c["cache"]["reps"],
                   "n_fail": c["cache"]["n_blocks"] - c["cache"]["cold_ok"], "n_blocks": c["cache"]["n_blocks"]}
                  for c in chains if c["cache"]["cold_ok"] < c["cache"]["n_blocks"]]
    cache_methods = build_cache_methods(chains)
    cache_ttl = build_cache_ttl(chains)

    # ---- jitter ----
    jit = [c for c in chains if c["lat"].get("eth_blockNumber", {}).get("jitter") is not None]
    jitter_top = [{"chainId": c["chainId"], "name": c["name"], "jitter": c["lat"]["eth_blockNumber"]["jitter"],
                   "p50": c["lat"]["eth_blockNumber"]["p50"], "p95": c["lat"]["eth_blockNumber"]["p95"]}
                  for c in sorted(jit, key=lambda c: -c["lat"]["eth_blockNumber"]["jitter"])[:10]]

    # ---- head went backwards ----
    head_back_rows = sorted([dict(c["head_back"], chainId=c["chainId"], name=c["name"], tier=c["tier"])
                             for c in chains if c["head_back"] and c["head_back"]["n_back"]],
                            key=lambda r: (-r["max_back"], r["chainId"]))

    # ---- per-chain issues ----
    issue_rows = [{"chainId": c["chainId"], "name": c["name"], "tier": c["tier"], "active": c["active"],
                   "issues": [i for i in c["issues"] if i["level"] != "info"]}
                  for c in chains if c["level"] is not None and c["level"] <= LEVEL_RANK["warn"]]
    issue_rows.sort(key=lambda r: (LEVEL_RANK[r["issues"][0]["level"]], -len(r["issues"]), r["chainId"]))
    issue_counts = Counter(i["code"] for c in chains for i in c["issues"])
    issue_meta = {}
    for c in chains:
        for i in c["issues"]:
            issue_meta.setdefault(i["code"], {"label": i["label"], "level": i["level"]})

    tiers = Counter(c["tier"] for c in chains)

    return {
        "meta": {
            "target": urlparse(BASE).netloc, "date": when,
            "n_chains": len(chains),
            "catalog": overview.get("chains", {}).get("catalog"),
            "endpoints_active": overview.get("endpoints", {}).get("active"),
            "bench_calls": reg_tot + a_tot + h_tot, "bench_samples": bmeta.get("samples"),
            "bench_interval": bmeta.get("interval"),
            "latcache_calls": sum(c["lat"][m]["total"] for c in chains for m in lat_mids)
                              + sum(c["cache"]["total"] + c["cache"]["latest"]["total"] for c in chains),
            "lat_samples": lat_samples,
            "n_regular_methods": sum(1 for m in methods_avail if m["group"] in REGULAR),
            "n_advanced_methods": sum(1 for m in methods_avail if m["group"] == "advanced"),
            "avail_regular": reg_sr, "avail_regular_ok": reg_ok, "avail_regular_total": reg_tot,
            "avail_advanced": a_sr, "avail_advanced_ok": a_ok, "avail_advanced_total": a_tot,
            "avail_history": h_sr, "avail_history_ok": h_ok, "avail_history_total": h_tot,
            "hot_p50": hot_p50,
            "thresholds": {"slow_cold_ms": SLOW_COLD_MS, "jitter_ms": JITTER_MS, "head_back": HEAD_BACK_WARN},
        },
        "error_kinds": {k: {"label": v[0], "desc": v[1]} for k, v in ERROR_KINDS.items()},
        "tiers": [{"tier": t, "label": TIER_LABELS[t], "count": tiers.get(t, 0)} for t in ("A-high", "B-medium", "C-low")],
        "single_point": sum(1 for c in chains if c["single_point"]),
        "methods_avail": methods_avail,
        "methods_latency": methods_latency,
        "failures": failures,
        "history": {"depths": depths, "rows": hist_rows},
        "cache_summary": cache_summary,
        "cache_top": cache_top,
        "cache_fail": cache_fail,
        "cache_methods": cache_methods,
        "cache_ttl": cache_ttl,
        "jitter_top": jitter_top,
        "head_back": head_back_rows,
        "issue_rows": issue_rows,
        "issue_counts": issue_counts,
        "issue_meta": issue_meta,
        "all_chains": [{
            "chainId": c["chainId"], "name": c["name"], "sym": c["sym"], "tier": c["tier"],
            "active": c["active"], "sp": c["single_point"],
            "avail": c["avail_regular"], "aavail": c["avail_advanced"], "havail": c["avail_history"],
            "afail": [{"label": method_label(mid), "ok": m["ok"], "total": m["total"],
                       "kind": m["kind"], "msg": m["err"], "skipped": m["skipped"]}
                      for mid, m in c["methods"].items()
                      if m["group"] == "advanced" and (m["ok"] < m["total"] or m["skipped"])],
            "p50": c["lat"].get("eth_blockNumber", {}).get("p50"),
            "cold": c["cache"]["cold"], "warm": c["cache"]["warm"], "speedup": c["cache"]["speedup"],
            "coldf": None if c["cache"]["cold"] is not None else {
                "kind": c["cache"]["cold_kind"] or "other", "msg": c["cache"]["cold_err"],
                "ms": c["cache"]["cold_fail_ms"], "retry_call": c["cache"]["retry_call"],
                "retry_ms": c["cache"]["retry_ms"]},
            "ttl": ttl_sort(c["cache"]["ttl"]), "ttlf": ttl_info(c),
            "hfail": [{"label": method_label(mid), "ok": m["ok"], "total": m["total"],
                       "kind": m["kind"], "msg": m["err"]}
                      for mid, m in c["methods"].items() if m["group"] == "history" and m["ok"] < m["total"]],
            "issues": [{"code": i["code"], "level": i["level"], "label": i["label"], "detail": i["detail"]}
                       for i in c["issues"]],
            "level": c["level"],
        } for c in chains],
    }


def render(template, out, embed):
    """Copy the template page to `out` with the data inlined into its <script id="data"> block."""
    html = open(template, encoding="utf-8").read()
    data = json.dumps(embed, ensure_ascii=False).replace("</", "<\\/")
    pat = re.compile(r'(<script id="data" type="application/json">)(.*?)(</script>)', re.S)
    if not pat.search(html):
        raise SystemExit("no <script id=\"data\"> block in %s" % template)
    html = pat.sub(lambda m: m.group(1) + data + m.group(3), html, count=1)
    with open(out, "w", encoding="utf-8") as f:
        f.write(html)


if __name__ == "__main__":
    sys.exit(main())
