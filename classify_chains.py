#!/usr/bin/env python3
"""Classify chains served by the cryptostack RPC router.

Reads chains_all.json (from /api/public/chains?limit=500) and buckets each chain
by endpoint redundancy + live traffic, writes chains_classified.csv and prints a
summary. This is the input for targeting the latency/availability test runs.
"""
import csv
import sys
from rpcutil import load_chains

# well-known "major" EVM chains (for a convenience tag)
MAJORS = {
    1: "ethereum", 56: "bnb", 137: "polygon", 8453: "base", 42161: "arbitrum",
    10: "optimism", 43114: "avalanche", 250: "fantom", 100: "gnosis", 324: "zksync",
    59144: "linea", 534352: "scroll", 5000: "mantle", 81457: "blast", 534: "various",
}


def redundancy_tier(active):
    if active >= 10:
        return "A-high"      # many live endpoints -> most resilient
    if active >= 4:
        return "B-medium"
    return "C-low"           # 1-3 live endpoints -> fragile / single point


def classify(items):
    rows = []
    for x in items:
        active = x.get("active", 0) or 0
        rows.append({
            "chainId": x["chainId"],
            "name": x["name"],
            "shortName": x.get("shortName"),
            "nativeSymbol": x.get("nativeSymbol"),
            "catalogEndpoints": x.get("catalogEndpoints"),
            "endpoints": x.get("endpoints"),
            "active": active,
            "head": x.get("head"),
            "ingressTotal": x.get("ingressTotal", 0) or 0,
            "tier": redundancy_tier(active),
            "single_point": active <= 1,
            "has_traffic": (x.get("ingressTotal", 0) or 0) > 0,
            "is_major": x["chainId"] in MAJORS,
        })
    return rows


def main():
    items = load_chains()
    rows = classify(items)
    rows.sort(key=lambda r: (-r["ingressTotal"], -r["active"]))

    out = "chains_classified.csv"
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    # summary
    from collections import Counter
    tiers = Counter(r["tier"] for r in rows)
    print("=== chain classification (%d chains) ===" % len(rows))
    for t in ("A-high", "B-medium", "C-low"):
        print("  tier %-9s : %3d chains" % (t, tiers.get(t, 0)))
    print("  single-point (active<=1): %d" % sum(1 for r in rows if r["single_point"]))
    print("  with live traffic       : %d" % sum(1 for r in rows if r["has_traffic"]))
    print("  known majors            : %d" % sum(1 for r in rows if r["is_major"]))

    print("\n--- chains with live traffic (ingress>0) ---")
    print("  %-8s %-3s %-7s %-10s %s" % ("chainId", "act", "ingress", "head", "name"))
    for r in rows:
        if r["has_traffic"]:
            print("  %-8s %-3s %-7s %-10s %s" % (
                r["chainId"], r["active"], r["ingressTotal"], r["head"], r["name"]))

    print("\nwrote %s" % out)


if __name__ == "__main__":
    sys.exit(main())
