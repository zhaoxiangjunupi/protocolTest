"""Shared helpers for testing the cryptostack RPC router.

Stdlib only (http.client + json). No third-party deps.

The router forwards JSON-RPC to upstream nodes at:  BASE/rpc/{chainId}
Public read-only metadata is at:                    BASE/api/public/...

Each worker thread keeps one keep-alive HTTPS connection to the router and
reuses it for all its calls; the TCP+TLS handshake happens before the timer
starts, so a call's latency is request -> full response only.
"""
import http.client
import json
import ssl
import threading
import time
from urllib.parse import urlsplit

BASE = "https://rpc.cryptostack.ai"
RPC_PATH = "/rpc/{chainId}"
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

# historical blocks at three depths below the live head. Params are built per
# sample by history_params(): every sample picks a different random block in the
# window, so each call misses the router cache and really reaches an upstream.
# "nonnull": a null result means the upstream has no such block -> failure.
HISTORY_METHODS = [
    {"id": "hist_recent", "method": "eth_getBlockByNumber", "nonnull": True,
     "label": "近期区块", "window": "head−16…128"},
    {"id": "hist_mid",    "method": "eth_getBlockByNumber", "nonnull": True,
     "label": "中期区块", "window": "head−5k…20k"},
    {"id": "hist_early",  "method": "eth_getBlockByNumber", "nonnull": True,
     "label": "早期区块", "window": "链前 1/4 段"},
]

ALL_METHODS = SIMPLE_METHODS + COMPLEX_METHODS + HISTORY_METHODS

METHOD_GROUP = {}
for _g, _ms in (("simple", SIMPLE_METHODS), ("complex", COMPLEX_METHODS), ("history", HISTORY_METHODS)):
    for _m in _ms:
        METHOD_GROUP[_m["id"]] = _g


def history_params(spec, head, rng):
    """Params for a HISTORY_METHODS spec at the given head, or None if the chain is too short."""
    if spec["id"] == "hist_recent":
        if head <= 200:
            return None
        blk = head - rng.randint(16, 128)
    elif spec["id"] == "hist_mid":
        if head <= 25_000:
            return None
        blk = head - rng.randint(5_000, 20_000)
    else:  # hist_early
        if head <= 10_000:
            return None
        blk = rng.randint(1_000, head // 4)
    return [hex(blk), False]


class Pacer:
    """Keeps calls to one chain at least `interval` seconds apart (start to start).

    Bursts of uncached calls trip upstream rate limits and the router then reports
    "all upstream endpoints exhausted"; pacing keeps the test from causing that.
    """

    def __init__(self, interval):
        self.interval = interval
        self.next = 0.0

    def wait(self):
        now = time.perf_counter()
        if now < self.next:
            time.sleep(self.next - now)
        self.next = max(now, self.next) + self.interval


def live_head(chain, timeout):
    """Current head via eth_blockNumber; falls back to the catalog snapshot."""
    r = rpc_call(chain["chainId"], {"method": "eth_blockNumber", "params": []}, timeout=timeout)
    if r["ok"]:
        try:
            return int(r["result"], 16), "live"
        except (ValueError, TypeError):
            pass
    return (chain.get("head") or 0), "snapshot"


# ---- error classification ---------------------------------------------------
# code -> (short label, one-line explanation). Order of ERROR_RULES matters:
# first match wins.
ERROR_KINDS = {
    "exhausted":    ("上游节点耗尽", "路由器把该链的上游节点都试过了仍失败:节点宕机、限流或都不支持该请求"),
    "no_data":      ("上游缺少该区块", "上游节点只保留近期区块(或非完整历史节点),查不到更早的区块"),
    "unsupported":  ("上游不支持该方法", "上游节点没有开放这个 JSON-RPC 方法"),
    "rate_limited": ("上游限流", "请求被上游按频率限制拒绝"),
    "timeout":      ("请求超时", "超过超时时间仍未收到响应"),
    "conn":         ("连接失败", "与路由器建立连接失败(网络/DNS/TLS)"),
    "http":         ("HTTP 错误", "路由器返回了非 200 的 HTTP 状态码"),
    "bad_response": ("响应格式异常", "返回的不是合法的 JSON-RPC 响应"),
    "wrong_chain":  ("返回了错误的链", "eth_chainId 返回值与请求的 chainId 不一致"),
    "other":        ("其他上游错误", "上游返回了未归类的 JSON-RPC 错误"),
}

ERROR_RULES = [
    ("exhausted",    ("all upstream endpoints exhausted", "no healthy upstream", "no upstream")),
    # plan / quota limits first: their messages often also mention "archive"
    ("rate_limited", ("rate limit", "ratelimit", "too many requests", "http-429", "limit exceeded",
                      "quota", "calls/hour", "calls/min", "calls per")),
    ("unsupported",  ("does not exist", "is not available", "method not available", "not available on this plan",
                      "method not found", "not supported", "unsupported", "not allowed", "-32601")),
    ("no_data",      ("null-result", "outside available upstream range", "only serves recent blocks",
                      "header not found", "block not found", "unknown block", "missing trie node",
                      "pruned", "historical", "archive")),
    ("timeout",      ("timed out", "timeout")),
    ("conn",         ("conn:",)),
    ("http",         ("http-",)),
    ("bad_response", ("non-json-body", "bad-shape", "no-result")),
    ("wrong_chain",  ("wrong-chain",)),
]


def classify_error(err):
    """Map an rpc_call error string to an ERROR_KINDS code (None if no error)."""
    if not err:
        return None
    e = err.lower()
    for code, needles in ERROR_RULES:
        if any(n in e for n in needles):
            return code
    return "other"


# ---- keep-alive transport ---------------------------------------------------
_HOST = urlsplit(BASE).hostname
_SSL_CTX = ssl.create_default_context()
_local = threading.local()          # .conn: this thread's HTTPSConnection
_stats_lock = threading.Lock()
_stats = {"requests": 0, "connects": 0}


def conn_stats():
    """{requests, connects} since start: connects << requests means connections were reused."""
    with _stats_lock:
        return dict(_stats)


def _drop_connection():
    conn = getattr(_local, "conn", None)
    if conn is not None:
        conn.close()


def _connection(timeout):
    """This thread's connection, (re)opened if needed. Returns (conn, reused)."""
    conn = getattr(_local, "conn", None)
    if conn is None:
        conn = _local.conn = http.client.HTTPSConnection(_HOST, timeout=timeout, context=_SSL_CTX)
    if conn.sock is not None:
        conn.sock.settimeout(timeout)
        return conn, True
    conn.timeout = timeout
    conn.connect()
    with _stats_lock:
        _stats["connects"] += 1
    return conn, False


def _http_json(path, payload, timeout):
    """POST json over this thread's keep-alive connection.

    Returns (status, body_obj_or_text, elapsed_s, error_str). elapsed_s excludes
    the handshake. A reused connection the server has already closed is reopened
    and the request sent again, once.
    """
    data = json.dumps(payload).encode()
    headers = {"content-type": "application/json", "accept": "application/json", "user-agent": UA}
    with _stats_lock:
        _stats["requests"] += 1
    for attempt in (1, 2):
        t0 = time.perf_counter()
        try:
            conn, reused = _connection(timeout)
        except OSError as e:   # DNS / TCP / TLS failure or connect timeout
            _drop_connection()
            return None, None, time.perf_counter() - t0, "conn:%s" % (str(e) or type(e).__name__)
        t0 = time.perf_counter()
        try:
            conn.request("POST", path, body=data, headers=headers)
            resp = conn.getresponse()
            raw = resp.read()
        except (http.client.HTTPException, OSError) as e:
            dt = time.perf_counter() - t0
            _drop_connection()
            if reused and attempt == 1 and isinstance(e, ConnectionError):
                continue   # idle connection closed by the server: reconnect and resend
            return None, None, dt, "conn:%s" % (str(e) or type(e).__name__)
        dt = time.perf_counter() - t0
        if not 200 <= resp.status < 300:
            return resp.status, raw.decode("utf-8", "replace")[:300], dt, "http-%s" % resp.status
        try:
            return resp.status, json.loads(raw), dt, None
        except ValueError:
            return resp.status, raw.decode("utf-8", "replace")[:300], dt, "non-json-body"


def rpc_call(chain_id, spec, timeout=15.0, req_id=1):
    """Single JSON-RPC call through the router.

    Returns dict: ok(bool), latency(s), status, error(str|None), result_preview.
    ok == a valid JSON-RPC success response ("result" present, no "error");
    with spec["nonnull"], a null result is a failure too ("null-result").
    """
    payload = {"jsonrpc": "2.0", "id": req_id, "method": spec["method"], "params": spec["params"]}
    status, body, dt, err = _http_json(RPC_PATH.format(chainId=chain_id), payload, timeout)

    if err:
        return {"ok": False, "latency": dt, "status": status, "error": err, "result": None}
    if not isinstance(body, dict):
        return {"ok": False, "latency": dt, "status": status, "error": "bad-shape", "result": None}
    if "error" in body and body["error"] is not None:
        e = body["error"]
        msg = e.get("message") if isinstance(e, dict) else str(e)
        return {"ok": False, "latency": dt, "status": status, "error": "rpc:%s" % (msg or "")[:160], "result": None}
    if "result" not in body:
        return {"ok": False, "latency": dt, "status": status, "error": "no-result", "result": None}

    res = body["result"]
    if res is None and spec.get("nonnull"):
        return {"ok": False, "latency": dt, "status": status,
                "error": "null-result (upstream returned no data for this block)", "result": None}
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
