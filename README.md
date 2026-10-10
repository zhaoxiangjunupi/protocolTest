# rpc-router-bench

多链 RPC 路由器的**分类、可用性、延迟与缓存**压测工具集。针对 [rpc.cryptostack.ai](https://rpc.cryptostack.ai/) 这类统一转发入口(`POST /rpc/{chainId}`)的聚合路由器,批量评估它服务的每条链的健康度。

纯 Python 标准库实现(`urllib` + `concurrent.futures`),**零第三方依赖**。

## 能做什么

- **分类**:按活跃节点冗余度把链分成 A 高 / B 中 / C 低(单点)三层,标记单点风险链和有真实流量的链。
- **可用性 + 延迟**:对每条链跑一组简单方法(元数据)和复杂方法(回源/大载荷),统计成功率与延迟分布(min/avg/p50/p95)。
- **稳定延迟 + 缓存冷热对比**:多轮采样得到稳定的 p50/p95/jitter;用随机历史区块区分"冷回源 vs 热命中",量化路由器缓存带来的加速。
- **汇总报告**:一个自包含的 HTML 报告页(`rpc_report.html`),含分类表、可用性/延迟图表、缓存价值可视化,支持明暗主题、搜索/排序/筛选。

## 文件

| 文件 | 作用 |
|------|------|
| `rpcutil.py` | 共享库:JSON-RPC 调用、方法目录(简单/复杂)、百分位 |
| `classify_chains.py` | 读链目录 → 分层,输出 `chains_classified.csv` |
| `rpc_bench.py` | 可用性 + 延迟压测(逐链逐方法多次采样) |
| `latency_cache.py` | 稳定延迟(多轮)+ 缓存冷/热对比 |
| `rpc_report.html` | 自包含的汇总报告页(数据已内联) |
| `chains_all.json` | 链目录快照(来自 `/api/public/chains?limit=500`) |
| `chains_classified.csv` | 分层结果 |
| `report_data.json` / `report_embed.json` | 报告用的合并/精简数据 |

## 快速开始

```bash
# 1) 拉取链目录快照(可选，仓库里已带一份)
curl -s "https://rpc.cryptostack.ai/api/public/chains?limit=500" -o chains_all.json

# 2) 分类
python3 classify_chains.py                # -> chains_classified.csv

# 3) 可用性 + 延迟
python3 rpc_bench.py --chains all --methods all --samples 3
python3 rpc_bench.py --chains traffic --methods simple --samples 5   # 只测有流量的链
python3 rpc_bench.py --chains 1,56,8453 --methods complex            # 指定 chainId

# 4) 稳定延迟 + 缓存冷/热
python3 latency_cache.py --chains all --samples 12 --cache-reps 5
```

### `rpc_bench.py` 参数

| 参数 | 说明 | 默认 |
|------|------|------|
| `--chains` | `all` / `traffic` / `majors` / 逗号分隔的 chainId | `traffic` |
| `--methods` | `simple` / `complex` / `all` | `all` |
| `--samples` | 每个(链,方法)采样次数 | `3` |
| `--concurrency` | 并行测试的链数 | `16` |
| `--timeout` | 单次请求超时(秒) | `15` |

输出:`results_raw_*.json`(逐样本)、`results_summary_*.csv`(逐链逐方法聚合)。

## 方法分组

- **简单(轻量/元数据,多可缓存)**:`eth_chainId`、`eth_blockNumber`、`net_version`、`eth_gasPrice`、`web3_clientVersion`
- **复杂(回源/大载荷)**:`eth_getBlockByNumber(latest,true)`、`eth_getBalance`、`eth_getTransactionCount`、`eth_getLogs(latest)`

**成功判定**:HTTP 200 且 JSON-RPC 响应含 `result`、无 `error`;`eth_chainId` 额外做正确性校验(返回值须等于 chainId)。

## 缓存冷/热的测法

路由器无显式缓存命中响应头,因此用延迟推断:每条链取一个**随机历史区块**(极可能未被缓存),对 `eth_getBlockByNumber(block,false)` 连打 K+1 次——第 1 次为**冷**(回源),其余取中位为**热**(命中)。同时对 `latest` 标签做对照。

## 一次实测结论(136 条链)

- 整体可用性 **99.7%**(3672 次调用);简单方法全部满额。
- 热路径 p50 **≈28ms**,跨链高度一致。
- 缓存中位加速 **10.4×**;上游越慢救得越多,最高 **311×**(7200ms → 23ms)。
- 失败全部来自上游(不支持该方法 / 节点池瞬时耗尽),非路由器本身。
