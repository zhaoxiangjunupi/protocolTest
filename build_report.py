#!/usr/bin/env python3
"""Build the report data and inline it into rpc_report.html.

Inputs (newest results_* files are picked automatically; override with flags):
  chains_all.json                 chain catalog snapshot (/api/public/chains?limit=500)
  overview.json                   router overview snapshot (/api/public/overview), optional
  results_raw_*.json              output of rpc_bench.py      (availability, incl. history)
  results_latcache_raw_*.json     output of latency_cache.py  (stable latency + cache probe)

Outputs:
  report_data.json                merged per-chain record (everything, for digging)
  report_embed.json               the compact data the page renders
  rpc_report.html                 <script id="data"> block replaced in place

Example
-------
  python3 build_report.py
  python3 build_report.py --bench results_raw_X.json --latcache results_latcache_raw_Y.json
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

from rpcutil import BASE, ERROR_KINDS, HISTORY_METHODS, METHOD_GROUP, classify_error, pct, load_chains
from classify_chains import redundancy_tier

TIER_LABELS = {"A-high": "A 高冗余 (≥10)", "B-medium": "B 中 (4–9)", "C-low": "C 低/单点 (≤3)"}
HIST = {m["id"]: m for m in HISTORY_METHODS}
SLOW_COLD_MS = 1000     # cold upstream fetch at/above this -> "上游慢"
JITTER_MS = 200         # eth_blockNumber stdev at/above this -> "抖动大"
BENEFIT_X = 1.3         # speedup at/above this counts as "cache helps"
LEVEL_RANK = {"crit": 0, "warn": 1, "info": 2}


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
    return mid


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bench", help="results_raw_*.json from rpc_bench.py (default: newest)")
    ap.add_argument("--latcache", help="results_latcache_raw_*.json from latency_cache.py (default: newest)")
    ap.add_argument("--html", default="rpc_report.html")
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

        reg = [m for m in pm.values() if m["group"] != "history"]
        hist = {k: pm[k] for k in HIST if k in pm}
        reg_ok, reg_tot = sum(m["ok"] for m in reg), sum(m["total"] for m in reg)
        h_ok, h_tot = sum(m["ok"] for m in hist.values()), sum(m["total"] for m in hist.values())
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
            "avail_regular": round(reg_ok / reg_tot * 100, 1) if reg_tot else None,
            "avail_history": round(h_ok / h_tot * 100, 1) if h_tot else None,
            "methods": {mid: dict({
                "group": m["group"], "ok": m["ok"], "total": m["total"],
                "avg": ms(m["lat_avg"]), "p50": ms(m["lat_p50"]),
                "errors": m["errors"], "blocks": m.get("blocks"), "skipped": m.get("skipped"),
            }, **top_error(m["errors"])) for mid, m in pm.items()},
            "lat": {mid: {"ok": v["ok"], "total": v["total"], "p50": ms(v["p50"]), "p95": ms(v["p95"]),
                          "jitter": ms(v["stdev"]), "err": v["err"], "kind": classify_error(v["err"])}
                    for mid, v in lm.items()},
            "cache": {
                "block": cache[cid]["block"],
                "cold": ms(cf["cold"]), "warm": ms(cf["warm_med"]), "warm_min": ms(cf["warm_min"]),
                "speedup": round(speed, 1) if speed else None,
                "ok": cf["ok"], "reps": cf["reps"],
                "cold_kind": classify_error(cf.get("cold_error")), "cold_err": cf.get("cold_error"),
                "cold_fail_ms": ms(cf.get("cold_fail_latency")),
                "retry_call": cf.get("retry_ok_call"), "retry_ms": ms(cf.get("retry_ok_latency")),
                "calls": [{"ok": x["ok"], "ms": ms(x["latency"]), "kind": classify_error(x["error"]), "err": x["error"]}
                          for x in cf.get("calls", [])],
                "latest": {"cold": ms(cl["cold"]), "warm": ms(cl["warm_med"]), "ok": cl["ok"], "reps": cl["reps"],
                           "err": cl.get("cold_error") or next((x["error"] for x in cl.get("calls", []) if x["error"]), None)},
            },
        }
        c["issues"] = chain_issues(c)
        c["level"] = min((LEVEL_RANK[i["level"]] for i in c["issues"]), default=None)
        chains.append(c)

    embed = build_embed(chains, bench, lc, overview)

    with open("report_data.json", "w") as f:
        json.dump(chains, f, ensure_ascii=False, indent=1)
    with open("report_embed.json", "w") as f:
        json.dump(embed, f, ensure_ascii=False)
    inject(args.html, embed)

    print("chains   : %d  (issues: %s)" % (len(chains), dict(Counter(
        i["code"] for c in chains for i in c["issues"]))))
    print("wrote report_data.json, report_embed.json, %s" % args.html)
    return 0


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
    reg = [m for m in pm.values() if m["group"] != "history"]
    phases = [
        ("可用性测试", sum(m["ok"] for m in reg), sum(m["total"] for m in reg),
         next((m["err"] for m in reg if m["err"]), None)),
        ("延迟测试", sum(v["ok"] for v in lm.values()), sum(v["total"] for v in lm.values()),
         next((v["err"] for v in lm.values() if v["err"]), None)),
        ("缓存测试", ca["ok"] + ca["latest"]["ok"], ca["reps"] + ca["latest"]["reps"],
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

    # 3) regular (latest-block) methods with failures
    if "可用性测试" not in dead_names:
        bad = [(mid, m) for mid, m in pm.items() if m["group"] != "history" and m["ok"] < m["total"]
               and m["kind"] != "wrong_chain"]
        if bad:
            add("regular_fail", "warn", "常规请求失败", fmt_fails(
                [(mid, m["ok"], m["total"], m["kind"], m["err"]) for mid, m in bad]))

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

    # 5) cache probe: first (cold) call failed
    if ca["cold"] is None and "缓存测试" not in dead_names:
        if ca["retry_call"]:
            add("cold_fail", "warn", "首次请求失败", "缓存测试第 1 次请求失败(%s，耗时 %s ms：%s)，第 %d 次才成功(%s ms)"
                "——路由器遇到这个错误没有自动换上游重试" % (
                    kind_label(ca["cold_kind"]), ca["cold_fail_ms"], ca["cold_err"], ca["retry_call"], ca["retry_ms"]))
        else:
            add("cache_all_fail", "warn", "缓存测试全部失败", "区块 #%s 连续 %d 次请求全部失败(%s：%s)" % (
                ca["block"], ca["reps"], kind_label(ca["cold_kind"]), ca["cold_err"]))

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

    reg_sr, reg_ok, reg_tot = group_avail({"simple", "complex"})
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

    # ---- regular-method failures ----
    failures = []
    for c in chains:
        for mid, m in c["methods"].items():
            if m["group"] != "history" and m["total"] and m["ok"] < m["total"]:
                failures.append({"chainId": c["chainId"], "name": c["name"], "method": mid, "group": m["group"],
                                 "ok": m["ok"], "total": m["total"], "kind": m["kind"] or "other", "msg": m["err"]})

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
        "n_cold_fail": sum(1 for c in chains if c["cache"]["cold"] is None),
        "reps": max((c["cache"]["reps"] for c in chains), default=0),
    }
    cache_top = [{"chainId": c["chainId"], "name": c["name"], "cold": c["cache"]["cold"],
                  "warm": c["cache"]["warm"], "speedup": c["cache"]["speedup"]}
                 for c in sorted(with_speed, key=lambda c: -c["cache"]["speedup"])[:12]]
    cache_fail = [{"chainId": c["chainId"], "name": c["name"], "block": c["cache"]["block"],
                   "kind": c["cache"]["cold_kind"] or "other", "msg": c["cache"]["cold_err"],
                   "fail_ms": c["cache"]["cold_fail_ms"], "retry_call": c["cache"]["retry_call"],
                   "retry_ms": c["cache"]["retry_ms"], "ok": c["cache"]["ok"], "reps": c["cache"]["reps"]}
                  for c in chains if c["cache"]["cold"] is None]

    # ---- jitter ----
    jit = [c for c in chains if c["lat"].get("eth_blockNumber", {}).get("jitter") is not None]
    jitter_top = [{"chainId": c["chainId"], "name": c["name"], "jitter": c["lat"]["eth_blockNumber"]["jitter"],
                   "p50": c["lat"]["eth_blockNumber"]["p50"], "p95": c["lat"]["eth_blockNumber"]["p95"]}
                  for c in sorted(jit, key=lambda c: -c["lat"]["eth_blockNumber"]["jitter"])[:10]]

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
            "bench_calls": reg_tot + h_tot, "bench_samples": bmeta.get("samples"),
            "latcache_calls": sum(c["lat"][m]["total"] for c in chains for m in lat_mids)
                              + sum(c["cache"]["reps"] + c["cache"]["latest"]["reps"] for c in chains),
            "lat_samples": lat_samples,
            "avail_regular": reg_sr, "avail_regular_ok": reg_ok, "avail_regular_total": reg_tot,
            "avail_history": h_sr, "avail_history_ok": h_ok, "avail_history_total": h_tot,
            "hot_p50": hot_p50,
            "thresholds": {"slow_cold_ms": SLOW_COLD_MS, "jitter_ms": JITTER_MS},
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
        "jitter_top": jitter_top,
        "issue_rows": issue_rows,
        "issue_counts": issue_counts,
        "issue_meta": issue_meta,
        "all_chains": [{
            "chainId": c["chainId"], "name": c["name"], "sym": c["sym"], "tier": c["tier"],
            "active": c["active"], "sp": c["single_point"],
            "avail": c["avail_regular"], "havail": c["avail_history"],
            "p50": c["lat"].get("eth_blockNumber", {}).get("p50"),
            "cold": c["cache"]["cold"], "warm": c["cache"]["warm"], "speedup": c["cache"]["speedup"],
            "coldf": None if c["cache"]["cold"] is not None else {
                "kind": c["cache"]["cold_kind"] or "other", "msg": c["cache"]["cold_err"],
                "ms": c["cache"]["cold_fail_ms"], "retry_call": c["cache"]["retry_call"],
                "retry_ms": c["cache"]["retry_ms"]},
            "hfail": [{"label": method_label(mid), "ok": m["ok"], "total": m["total"],
                       "kind": m["kind"], "msg": m["err"]}
                      for mid, m in c["methods"].items() if m["group"] == "history" and m["ok"] < m["total"]],
            "issues": [{"code": i["code"], "level": i["level"], "label": i["label"], "detail": i["detail"]}
                       for i in c["issues"]],
            "level": c["level"],
        } for c in chains],
    }


def inject(path, embed):
    html = open(path, encoding="utf-8").read()
    data = json.dumps(embed, ensure_ascii=False).replace("</", "<\\/")
    pat = re.compile(r'(<script id="data" type="application/json">)(.*?)(</script>)', re.S)
    if not pat.search(html):
        raise SystemExit("no <script id=\"data\"> block in %s" % path)
    html = pat.sub(lambda m: m.group(1) + data + m.group(3), html, count=1)
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)


if __name__ == "__main__":
    sys.exit(main())
