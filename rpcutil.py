"""Shared helpers for testing the cryptostack RPC router.

Stdlib only (urllib + json). No third-party deps.

The router forwards JSON-RPC to upstream nodes at:  BASE/rpc/{chainId}
Public read-only metadata is at:                    BASE/api/public/...
"""
import json
import time
import urllib.request
import urllib.error

BASE = "https://rpc.cryptostack.ai"
RPC_PATH = BASE + "/rpc/{chainId}"
UA = "rpc-bench/1.0 (cryptostack latency+availability tester)"

ZERO_ADDR = "0x0000000000000000000000000000000000000000"

# ---- method catalogue -------------------------------------------------------
# Each spec: name, jsonrpc method, params (static) or a builder taking `ctx`.
# ctx = {"chainId": int}  (kept simple; "latest" tags avoid extra round-trips)

SIMPLE_METHODS = [
    {"id": "eth_chainId",        "method": "eth_chainId",        "params": []},
    {"id": "eth_blockNumber",    "method": "eth_blockNumber",    "params": []},
    {"id": "net_version",        "method": "net_version",        "params": []},
    {"id": "eth_gasPrice",       "method": "eth_gasPrice",       "params": []},
    {"id": "web3_clientVersion", "method": "web3_clientVersion", "params": []},
]

COMPLEX_METHODS = [
    # full latest block incl. all transactions -> large payload, hits upstream
    {"id": "eth_getBlockByNumber", "method": "eth_getBlockByNumber", "params": ["latest", True]},
    # balance of zero address -> valid on every EVM chain
    {"id": "eth_getBalance", "method": "eth_getBalance", "params": [ZERO_ADDR, "latest"]},
    # tx count (nonce) of zero address
    {"id": "eth_getTransactionCount", "method": "eth_getTransactionCount", "params": [ZERO_ADDR, "latest"]},
    # logs of the latest block (no topic filter) -> exercises the log index path
    {"id": "eth_getLogs", "method": "eth_getLogs", "params": [{"fromBlock": "latest", "toBlock": "latest"}]},
]

ALL_METHODS = SIMPLE_METHODS + COMPLEX_METHODS


def _http_json(url, payload, timeout):
    """POST json, return (status, body_obj_or_text, elapsed_s, error_str)."""
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=data, method="POST",
        headers={"content-type": "application/json", "accept": "application/json", "user-agent": UA},
    )
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            dt = time.perf_counter() - t0
            try:
                return resp.status, json.loads(raw), dt, None
            except json.JSONDecodeError:
                return resp.status, raw.decode("utf-8", "replace")[:300], dt, "non-json-body"
    except urllib.error.HTTPError as e:
        dt = time.perf_counter() - t0
        body = e.read().decode("utf-8", "replace")[:300] if e.fp else ""
        return e.code, body, dt, "http-%s" % e.code
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        dt = time.perf_counter() - t0
        return None, None, dt, "conn:%s" % (getattr(e, "reason", e),)


def rpc_call(chain_id, spec, timeout=15.0, req_id=1):
    """Single JSON-RPC call through the router.

    Returns dict: ok(bool), latency(s), status, error(str|None), result_preview.
    ok == a valid JSON-RPC success response ("result" present, no "error").
    """
    url = RPC_PATH.format(chainId=chain_id)
    payload = {"jsonrpc": "2.0", "id": req_id, "method": spec["method"], "params": spec["params"]}
    status, body, dt, err = _http_json(url, payload, timeout)

    if err:
        return {"ok": False, "latency": dt, "status": status, "error": err, "result": None}
    if not isinstance(body, dict):
        return {"ok": False, "latency": dt, "status": status, "error": "bad-shape", "result": None}
    if "error" in body and body["error"] is not None:
        e = body["error"]
        msg = e.get("message") if isinstance(e, dict) else str(e)
        return {"ok": False, "latency": dt, "status": status, "error": "rpc:%s" % (msg or "")[:120], "result": None}
    if "result" not in body:
        return {"ok": False, "latency": dt, "status": status, "error": "no-result", "result": None}

    res = body["result"]
    # small preview for sanity / correctness checks
    if isinstance(res, (dict, list)):
        preview = "<%s len=%s>" % (type(res).__name__, len(res))
    else:
        preview = str(res)[:40]
    return {"ok": True, "latency": dt, "status": status, "error": None, "result": res, "preview": preview}


def pct(values, p):
    """Percentile (nearest-rank) of a list of numbers; None if empty."""
    if not values:
        return None
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round((p / 100.0) * (len(s) - 1)))))
    return s[k]


def load_chains(path="chains_all.json"):
    with open(path) as f:
        return json.load(f)["items"]
