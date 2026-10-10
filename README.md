# rpc-router-bench

多链 RPC 路由器的**分类、可用性(含历史区块)、失败原因、延迟与缓存**压测工具集。针对 [rpc.cryptostack.ai](https://rpc.cryptostack.ai/) 这类统一转发入口(`POST /rpc/{chainId}`)的聚合路由器,批量评估它服务的每条链的健康度。

纯 Python 标准库实现(`urllib` + `concurrent.futures`),**零第三方依赖**。

## 能做什么

- **分类**:按活跃节点冗余度把链分成 A 高 / B 中 / C 低(单点)三层,标记单点风险链。
- **常规可用性 + 延迟**:对每条链跑一组简单方法(元数据)和复杂方法(最新区块 / 状态 / 日志),统计成功率与延迟分布。
- **历史区块可用性**:在三个深度(近期 / 中期 / 早期)查历史区块,每次随机换区块,暴露"最新块能查、历史块查不到"的上游。
- **失败原因**:每次失败都记录错误原文,并归入一类(上游节点耗尽、上游缺少该区块、不支持该方法、限流、超时……)。
- **稳定延迟 + 缓存冷热对比**:多轮采样得到稳定的 p50/p95/抖动;用随机历史区块区分"冷回源 vs 缓存命中",量化缓存带来的加速。
- **汇总报告**:`build_report.py` 合并结果,生成自包含的 HTML 报告页(`rpc_report.html`):问题链汇总、按方法可用性、失败原因、历史区块、延迟、缓存、全部链明细(搜索 / 排序 / 筛选,悬停看错误原文),附名词解释。

## 文件

| 文件 | 作用 |
|------|------|
| `rpcutil.py` | 共享库:JSON-RPC 调用、方法目录(简单 / 复杂 / 历史区块)、错误分类、百分位 |
| `classify_chains.py` | 读链目录 → 分层,输出 `chains_classified.csv` |
| `rpc_bench.py` | 可用性 + 延迟压测(逐链逐方法多次采样,含历史区块) |
| `latency_cache.py` | 稳定延迟(多轮)+ 缓存冷/热对比 |
| `build_report.py` | 合并上面两个脚本的结果 → `report_data.json` / `report_embed.json`,并写入 `rpc_report.html` |
| `rpc_report.html` | 自包含的汇总报告页(数据已内联,浏览器直接打开) |
| `chains_all.json` | 链目录快照(来自 `/api/public/chains?limit=500`) |
| `overview.json` | 路由器概览快照(来自 `/api/public/overview`,报告里的目录总数 / 活跃节点数) |
| `chains_classified.csv` | 分层结果 |
| `report_data.json` | 每条链的完整合并记录(所有方法、错误原文、缓存逐次结果、问题列表) |
| `report_embed.json` | 报告页实际渲染的精简数据 |

## 快速开始

```bash
# 1) 拉取快照(可选，仓库里已带一份)
curl -s "https://rpc.cryptostack.ai/api/public/chains?limit=500" -o chains_all.json
curl -s "https://rpc.cryptostack.ai/api/public/overview"         -o overview.json

# 2) 分类
python3 classify_chains.py                # -> chains_classified.csv

# 3) 可用性(常规 + 历史区块)+ 延迟
python3 rpc_bench.py --chains all --methods all --samples 3
python3 rpc_bench.py --chains all --methods history --samples 3      # 只测历史区块
python3 rpc_bench.py --chains 1,56,8453 --methods complex            # 指定 chainId

# 4) 稳定延迟 + 缓存冷/热
python3 latency_cache.py --chains all --samples 12 --cache-reps 5

# 5) 生成报告(自动取最新的 results_raw_* 和 results_latcache_raw_*)
python3 build_report.py
```

### `rpc_bench.py` 参数

| 参数 | 说明 | 默认 |
|------|------|------|
| `--chains` | `all` / `traffic` / `majors` / 逗号分隔的 chainId | `traffic` |
| `--methods` | `simple` / `complex` / `history` / `all` | `all` |
| `--samples` | 每个(链,方法)采样次数 | `3` |
| `--concurrency` | 并行测试的链数 | `16` |
| `--timeout` | 单次请求超时(秒) | `15` |
| `--seed` | 历史区块随机选取的种子(0 = 按时间) | `0` |

输出:`results_raw_*.json`(逐方法聚合 + 全部错误原文及分类)、`results_summary_*.csv`(逐链逐方法,含 `error_kind` / `sample_error`)。

### `build_report.py` 参数

| 参数 | 说明 | 默认 |
|------|------|------|
| `--bench` | `rpc_bench.py` 的 `results_raw_*.json` | 最新一个 |
| `--latcache` | `latency_cache.py` 的 `results_latcache_raw_*.json` | 最新一个 |
| `--html` | 要写入数据的报告页 | `rpc_report.html` |

## 方法分组

- **简单(轻量/元数据,多可缓存)**:`eth_chainId`、`eth_blockNumber`、`net_version`、`eth_gasPrice`、`web3_clientVersion`
- **复杂(最新区块 / 状态 / 日志)**:`eth_getBlockByNumber(latest,true)`、`eth_getBalance`、`eth_getTransactionCount`、`eth_getLogs(latest)`
- **历史区块**:`eth_getBlockByNumber(区块号,false)`,以测试开始时实时查到的块高为基准:
  - 近期区块:head − 16…128
  - 中期区块:head − 5k…20k(超出"只保留最近 1024 块"的公共节点)
  - 早期区块:链的前 1/4 段(需要保留完整历史的节点)

**成功判定**:HTTP 200 且 JSON-RPC 响应含 `result`、无 `error`;`eth_chainId` 返回值须等于 chainId,否则算失败;历史区块和缓存测试中**返回 `null` 也算失败**(上游没有该区块)。

## 错误分类

按上游返回的错误原文归类(`rpcutil.classify_error`,规则见 `ERROR_RULES`):

| 代码 | 含义 |
|------|------|
| `exhausted` | 上游节点耗尽:路由器把该链的上游都试过了仍失败 |
| `no_data` | 上游缺少该区块:节点只保留近期区块,或返回 `null` |
| `unsupported` | 上游不支持该方法(含套餐限制) |
| `rate_limited` | 上游限流 |
| `timeout` / `conn` / `http` / `bad_response` | 超时 / 连接失败 / 非 200 / 响应格式异常 |
| `wrong_chain` | `eth_chainId` 返回了别的链 |
| `other` | 其他未归类错误 |

## 缓存冷/热的测法

路由器没有显式的缓存命中响应头,因此用延迟推断:每条链取一个**随机历史区块**(极可能未被缓存),对 `eth_getBlockByNumber(block,false)` 连打 K+1 次:第 1 次为**冷回源**,之后成功的请求取中位数为**缓存命中**,**加速 = 冷回源 ÷ 缓存命中**。第 1 次失败时记录失败原因、耗时,以及第几次才成功(说明路由器没有自动换上游重试)。同时对 `latest` 标签做对照。

## 问题判定(报告 02 节)

| 级别 | 问题 | 条件 |
|------|------|------|
| 严重 | 整链不可用 / 测试期间曾整链不可用 | 某个测试阶段该链请求全部失败 |
| 严重 | 返回了错误的链 | `eth_chainId` ≠ 请求的 chainId |
| 严重 | 历史区块查不到(近期) | 连最近 16–128 块都查不到 |
| 警告 | 历史区块查不到 / 偶发失败 | 中期或早期区块全部 / 部分失败 |
| 警告 | 常规请求失败 | 常规方法有任意一次失败 |
| 警告 | 首次请求失败 / 缓存测试全部失败 | 缓存测试第 1 次失败 / 全部失败 |
| 警告 | 上游慢 | 冷回源 ≥ 1000 ms |
| 提示 | 延迟抖动大 / 单点 | `eth_blockNumber` 标准差 ≥ 200 ms / 仅 1 个活跃节点 |

## 一次实测结论(135 条链,2026-10-10)

- 常规请求可用率 **99.7%**(3645 次);5 处失败全部来自上游(不支持该方法 / 节点耗尽 / 限流)。
- 历史区块可用率只有 **93.7%**:近期 99.0%、中期 96.0%、早期 **85.9%**;14 条链完全查不到早期区块,多条链的上游池混有"只保留最近 1024 块"的公共节点。
- 命中缓存后的延迟 p50 **≈30ms**,跨链高度一致。
- 缓存中位加速 **9.4×**(冷回源 258ms → 命中 28ms),最高 **349×**(Moonriver 9646ms → 28ms);25 条链冷请求失败,未计入。
- 39 条链有严重或警告级问题,明细见报告 02 节。

> 注意:路由器的 `ingressTotal` 是累计计数,会把本工具自己的测试请求也算进去,所以跑过测试后 `--chains traffic` 和"有流量"标记都不再准确,报告里已不再使用。
