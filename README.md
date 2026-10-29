# rpc-router-bench

多链 RPC 路由器的**分类、可用性(含历史区块)、失败原因、延迟与缓存**压测工具集。针对 [rpc.cryptostack.ai](https://rpc.cryptostack.ai/) 这类统一转发入口(`POST /rpc/{chainId}`)的聚合路由器,批量评估它服务的每条链的健康度。

纯 Python 标准库实现(`http.client` + `concurrent.futures`),**零第三方依赖**。

**连接复用**:每个工作线程和路由器保持一条 keep-alive HTTPS 连接，所有请求复用它;TCP+TLS 握手在计时开始前完成，所以延迟只算"发请求 → 读完响应"。服务端关掉空闲连接时自动重连并重发一次。脚本结束时会打印 `N requests over M connections`,M 远小于 N 说明复用生效。(2026-10-12 之前的结果每次请求都新建连接，延迟里含约 20ms 握手。)

## 能做什么

- **分类**:按活跃节点冗余度把链分成 A 高 / B 中 / C 低(单点)三层,标记单点风险链。
- **常规可用性 + 延迟**:对每条链跑一组简单方法(元数据)和复杂方法(最新区块 / 状态 / 日志),统计成功率与延迟分布。
- **历史区块可用性**:在三个深度(近期 / 中期 / 早期)查历史区块,每次随机换区块,暴露"最新块能查、历史块查不到"的上游。
- **进阶方法**:钱包 / dApp 常用的 `eth_call`、`eth_estimateGas`、交易回执、500 块区间的 `eth_getLogs`、批量请求。
- **块高回退**:按顺序记录 `eth_blockNumber` 的返回值，发现"后一次比前一次还低"(路由器混用了高度不同的上游)。
- **失败原因**:每次失败都记录错误原文,并归入一类(上游节点耗尽、上游缺少该区块、不支持该方法、限流、超时……)。
- **稳定延迟 + 缓存冷热对比**:多轮采样得到稳定的 p50/p95/抖动;用随机近期区块区分"冷回源 vs 缓存命中",量化缓存带来的加速。
- **哪些方法缓存 + latest 缓存时间**:逐个方法判断路由器缓不缓存(不缓存的方法每次都回源);测出 latest 类请求(最新块高等)在缓存里保留多久，并和出块间隔对照。
- **汇总报告**:`build_report.py` 合并结果，每次生成一份**新的**自包含 HTML 报告页(`rpc_report_<日期>_<时间>.html`,旧报告不动):问题链汇总、和上次对比、按方法可用性、失败原因、历史区块、延迟、缓存、抖动与块高回退、全部链明细(搜索 / 排序 / 筛选,悬停看错误原文),附名词解释。
- **趋势对比**:每次运行的摘要存进 `runs/`,报告里列出最近 8 次的关键指标，以及和上次相比新出现 / 消失的问题链;问题链标出"持续"还是"偶发"。`run_all.sh` 一条命令跑完整轮，可以挂到 cron 上定时跑。

## 文件

| 文件 | 作用 |
|------|------|
| `rpcutil.py` | 共享库:JSON-RPC 调用、方法目录(简单 / 复杂 / 历史区块)、错误分类、百分位 |
| `classify_chains.py` | 读链目录 → 分层,输出 `chains_classified.csv` |
| `rpc_bench.py` | 可用性 + 延迟压测(逐链逐方法多次采样,含历史区块) |
| `latency_cache.py` | 稳定延迟(多轮)+ 缓存冷/热对比 + 方法是否缓存 + latest 缓存时间 |
| `build_report.py` | 合并上面两个脚本的结果 → `report_data.json` / `report_embed.json` / `runs/<时间>.json`,并用模板生成新的报告页 |
| `run_all.sh` | 一条命令跑完整轮(拉快照 → 分类 → 可用性 → 延迟/缓存 → 报告) |
| `bsc_use.md` / `bsc_use_check.py` | BSC 业务需求(方法、频率、归档范围)和按这份需求做的专项核对脚本,见下面"BSC 业务需求核对" |
| `report_template.html` | 报告页模板(数据块为空，由 `build_report.py` 填入) |
| `rpc_report*.html` | 自包含的汇总报告页(数据已内联，浏览器直接打开),每次运行一份 |
| `runs/` | 每次运行的摘要(测法、关键指标、每条链的问题),报告的趋势对比读它 |
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
python3 rpc_bench.py --chains all --methods all --samples 10 --interval 0.5
python3 rpc_bench.py --chains all --methods history --samples 10     # 只测历史区块
python3 rpc_bench.py --chains 1,56,8453 --methods complex            # 指定 chainId

# 4) 稳定延迟 + 缓存冷/热
python3 latency_cache.py --chains all --samples 12 --cache-blocks 10 --cache-reps 10

# 5) 生成报告(自动取最新的 results_raw_* 和 results_latcache_raw_*,输出 rpc_report_<日期>_<时间>.html)
python3 build_report.py

# 或者一条命令跑完 1)–5)
./run_all.sh
```

定时跑(例如每 6 小时一次,`crontab -e`):

```cron
0 */6 * * * /home/ubuntu/zxj/protocolTest/run_all.sh >> /home/ubuntu/zxj/protocolTest/runs/cron.log 2>&1
```

### `rpc_bench.py` 参数

| 参数 | 说明 | 默认 |
|------|------|------|
| `--chains` | `all` / `traffic` / `majors` / 逗号分隔的 chainId | `traffic` |
| `--methods` | `simple` / `complex` / `advanced` / `history` / `all` | `all` |
| `--samples` | 每个(链,方法)采样次数 | `10` |
| `--concurrency` | 并行测试的链数 | `32` |
| `--interval` | 同一条链两次请求之间的最小间隔(秒),0 = 不限速 | `0.5` |
| `--timeout` | 单次请求超时(秒) | `15` |
| `--seed` | 历史区块随机选取的种子(0 = 按时间) | `0` |

同一条链的请求**按轮穿插**发送(每轮把所有方法各发 1 次)并按 `--interval` 限速。不限速时，一条链几秒内会有几十个不走缓存的请求打到上游，触发上游限流，路由器随后报"上游节点耗尽",测试结果被自己污染(实测 10 次采样不限速时，历史区块的"节点耗尽"失败从 0.3% 涨到 2.7%)。

输出:`results_raw_*.json`(逐方法聚合 + 全部错误原文及分类)、`results_summary_*.csv`(逐链逐方法,含 `error_kind` / `sample_error`)。

### `latency_cache.py` 参数

| 参数 | 说明 | 默认 |
|------|------|------|
| `--chains` | 同上 | `traffic` |
| `--samples` | 延迟测试：每个方法连续请求次数 | `12` |
| `--rounds` / `--gap` | 延迟测试重复轮数 / 轮间隔(秒) | `1` / `15` |
| `--cache-blocks` | 缓存测试：每条链随机取几个区块(= 冷回源样本数) | `10` |
| `--cache-reps` | 缓存测试：每个区块冷请求之后再连发几次(热) | `10` |
| `--interval` | 同一条链两次冷回源请求之间的最小间隔(秒) | `0.5` |
| `--method-reps` | 方法是否缓存：第 1 次之后再连发几次 | `3` |
| `--ttl-poll` / `--ttl-window` | latest 缓存时间：`eth_blockNumber` 的轮询间隔(= 精度)/ 每条链最多测几秒 | `0.1` / `15` |
| `--concurrency` / `--timeout` | 并行链数 / 单次超时 | `12` / `15` |

### `build_report.py` 参数

| 参数 | 说明 | 默认 |
|------|------|------|
| `--bench` | `rpc_bench.py` 的 `results_raw_*.json` | 最新一个 |
| `--latcache` | `latency_cache.py` 的 `results_latcache_raw_*.json` | 最新一个 |
| `--template` | 报告页模板 | `report_template.html` |
| `--html` | 输出的报告页(已存在会被覆盖) | `rpc_report_<日期>_<时间>.html`(按可用性测试的时间) |
| `--runs` | 运行摘要目录(趋势对比用) | `runs` |

趋势对比只和**更早**的运行比较(按可用性测试的时间),所以用旧结果重新生成报告也不会拿新数据去比。测法(采样次数、限速、是否复用连接等)也记在摘要里，测法不同的运行之间数字不能直接比较，报告里会标出来。

## BSC 业务需求核对(`bsc_use_check.py`)

`bsc_use.md` 是 BSC 线上程序的真实用法(带过滤的 `eth_getLogs`、`eth_call` 读余额 / Multicall3 / Safe 多签、几十到一百条的批量、2026-04-01 以后的归档、每分钟约 80 次 + 日报高峰)。`bsc_use_check.py` 按这份需求对 `/rpc/56` 逐项实测:

```bash
python3 bsc_use_check.py                      # 第一轮:basic,call,logs_rt,archive,logs_big,batch,sustain,burst
python3 bsc_use_check.py --followup --reps 8  # 第二轮:固定深度归档、batch 上限(不重复地址)、窄过滤日志、不重复地址突发
```

输出 `results_bsc_use*_<时间>.json`(逐次样本 + 汇总 + 日志)。突发测试放在最后跑，因为大批量失败会让这条链熔断几分钟。

2026-10-29 结论:最新状态 + 窄过滤实时日志 + 单条 / ≤40 条批量都能满足;不能满足的有四点 ——
历史块高请求会随机落到非归档上游(成功率 60～100% 波动，个别高度 0%);老区块 `eth_getBlockByNumber` 的 null 会被缓存超过 10 分钟，重试无效;
`eth_getLogs` 上游范围限制不一(部分只允许 5000 块,结果上限 20,000 条),5 万块回溯大多失败;batch 路由器硬限 100 条、上游 50 条左右就拒绝且失败后熔断。

## 方法分组

- **简单(轻量/元数据,多可缓存)**:`eth_chainId`、`eth_blockNumber`、`net_version`、`eth_gasPrice`、`web3_clientVersion`
- **复杂(最新区块 / 状态 / 日志)**:`eth_getBlockByNumber(latest,true)`、`eth_getBalance`、`eth_getTransactionCount`、`eth_getLogs(latest)`
- **进阶(钱包 / dApp 常用)**:
  - `eth_call`:调用 `0x04` 预编译合约(identity),返回值必须原样回显输入，任何 EVM 链都不需要部署合约
  - `eth_estimateGas`:零地址转 0 给 `0x…dEaD`
  - `eth_getTransactionReceipt`:测试前从最近的区块(往回 2…1000 块)里取交易，都是空块时改用最近 500 块的日志里的交易;返回 `null` 也算失败(上游没建交易索引)
  - `eth_getLogs` 区间：500 块区间，过滤一个不会产生日志的地址(响应很小，但上游必须支持区间查询);区间终点跟着最新看到的块高走(保证落在"只保留最近 1024 块"的节点范围内，不和历史区块测试混在一起),每次再错开 1 块，避免命中缓存
  - 批量请求：一次 POST 发 `eth_chainId` / `eth_blockNumber` / `eth_gasPrice`,3 个都要有结果
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
| `unsupported` | 上游不支持该方法(含套餐限制、方法不在白名单) |
| `range_limit` | 查询范围超限:`eth_getLogs` 等的区块范围或返回条数被上游限制 |
| `rate_limited` | 上游限流 |
| `timeout` / `conn` / `http` / `bad_response` | 超时 / 连接失败 / 非 200 / 响应格式异常或结果与预期不符(如 `eth_call` 回显不对) |
| `wrong_chain` | `eth_chainId` 返回了别的链 |
| `other` | 其他未归类错误 |

## 缓存冷/热的测法

路由器没有显式的缓存命中响应头,因此用延迟推断:每条链以实时块高为基准取 N 个(`--cache-blocks`,默认 10)**随机近期区块**(最新块往前 200–800 块:极可能未被缓存,又在只保留最近 ~1024 块的普通节点范围内,避免把"上游没有老区块"误当成缓存测试失败;老区块的可用性由历史区块测试负责),每个区块对 `eth_getBlockByNumber(block,false)` 连打 K+1 次:第 1 次为**冷回源**,之后的为**缓存命中**。**冷回源 = 各区块第 1 次耗时的中位数**,**缓存命中 = 其余成功请求的中位数**,**加速 = 冷回源 ÷ 缓存命中**。冷回源请求之间按 `--interval` 限速。某个区块第 1 次失败时记录失败原因、耗时,以及第几次才成功(说明路由器没有自动换上游重试),该区块不计入冷回源中位数。同时对 `latest` 标签做对照。

(2026-10-12 之前每条链只测 1 个区块，冷回源只有 1 个样本，容易被单次波动带偏。)

### 哪些方法缓存

`latency_cache.CACHE_METHODS` 里的 12 个方法(简单方法、余额、nonce、带地址过滤的 latest 日志、latest / 固定区块、`eth_call`、`eth_estimateGas`),每条链各紧挨着请求 1+`--method-reps` 次。第 1 次之后的请求耗时 ≤ 这条链缓存命中耗时(上面固定区块测试的中位数)+ max(2.5 ms, 命中耗时的一半)算**命中**,多数命中判为**缓存**,否则判为**不缓存**(每次都去上游)。阈值要紧：有的上游 5–10 ms 就能返回(2026-10-12 实测命中 99.8% 落在阈值内，冷回源只有 0.8% 落在阈值内)。选的都是响应很小的请求，避免把传输大响应的时间误当成回源。缓存的 key 包含参数：换地址、换区块号都要重新回源。报告按多数链的结果下结论，超过 10% 的链结论不同时标为"部分链缓存"。固定区块加速 < 1.3× 的链缓存本身没生效，这两项不测。第 1 次就失败的方法不再重复(记为测不出):2026-10-12 实测在 zkSync 上紧挨着连发 4 次会失败的 `eth_estimateGas`,路由器随后对这条链的**所有**请求都直接返回 `all upstream endpoints exhausted`,持续好几分钟。

### latest 缓存时间

每条链用 `eth_blockNumber` 每 `--ttl-poll` 秒请求一次，最多 `--ttl-window` 秒，收集到 5 个间隔就停。耗时超过"缓存命中耗时和冷回源耗时的几何平均"(且不低于上面的命中阈值)算回源。这里故意放宽：负载下命中偶尔慢几毫秒，误当成回源会把一个周期切成两段，把中位数拉低;返回的块高和上一次不同也算回源(缓存里的答案不会变),这样几毫秒就能返回的快上游也能认出来;连续几次回源算同一次刷新(过期后路由器有时会连着回源几次)。**缓存时间 = 一次刷新结束到下一次回源开始的间隔取中位数 − 轮询间隔的一半**(过期发生在最后一次命中和这次回源之间)。前 10 次全部回源判为不缓存，前 10 次全部失败就停(记下错误原文);窗口内只刷新一次时只给下限。报告里同时列出出块间隔(由可用性测试中 `eth_blockNumber` 的增长速度估算)。

注意：延迟测试("热路径")里同一方法连发 12 次，都在 latest 缓存时间内，测到的基本是缓存命中延迟;`rpc_bench.py` 按 0.5s 限速、同一方法约 8 秒才轮到一次，已超过缓存时间，每次都是回源。

## 问题判定(报告 02 节)

| 级别 | 问题 | 条件 |
|------|------|------|
| 严重 | 整链不可用 / 测试期间曾整链不可用 | 某个测试阶段该链请求全部失败 |
| 严重 | 返回了错误的链 | `eth_chainId` ≠ 请求的 chainId |
| 严重 | 历史区块查不到(近期) | 连最近 16–128 块都查不到 |
| 警告 | 历史区块查不到 / 偶发失败 | 中期或早期区块全部 / 部分失败 |
| 警告 | 常规请求失败 | 常规方法有任意一次失败 |
| 警告 | 进阶方法失败 | 进阶方法有任意一次失败 |
| 警告 / 提示 | 块高回退 | `eth_blockNumber` 比之前返回的块高低 ≥ 3 块为警告，1–2 块为提示 |
| 警告 | 首次请求失败 / 缓存区块全部失败 | 缓存测试某个区块第 1 次失败 / 该区块全部失败 |
| 警告 | 上游慢 | 冷回源 ≥ 1000 ms |
| 提示 | 延迟抖动大 / 单点 | `eth_blockNumber` 标准差 ≥ 200 ms / 仅 1 个活跃节点 |

## 最近一次实测(136 条链,2026-10-12 06:33 UTC,延迟/缓存 06:59 UTC,连接复用 · 10 次采样 · 限速 0.5s)

- 常规请求可用率 **99.6%**,进阶方法 **97.5%**,历史区块 **93.7%**(近期 100% / 中期 97.0% / 早期 **84.0%**)。
- 进阶方法的问题集中在:ZK Stack 系的链(zkSync、Lens、Cronos zkEVM、Abstract)和 GuapcoinX 的 `eth_estimateGas` 全部失败;11 条链的 500 块区间 `eth_getLogs` 失败(区间超限、不支持、上游缺数据等);Filecoin、Unique、NV-CHAIN 的 `eth_call` 调预编译合约返回空。
- 命中缓存后的延迟 p50 **≈2ms**;冷回源 204ms,缓存中位加速 **92×**。
- **不缓存的方法**:`net_version`、`web3_clientVersion`、`eth_getTransactionCount`、`eth_getLogs`、`eth_estimateGas` 在几乎所有链上每次都回源(≈190ms);链 ID、块高、gas 价格、余额、区块、`eth_call` 都缓存。
- **latest 缓存时间**:中位数 **2.0s**,120/136 条链在 1.5–2.5s;出块比 2s 快的链更短，接近出块间隔(Arbitrum 0.26s、Monad 0.36s、BSC 0.66s、Avalanche 0.56s);Cronos zkEVM(约 78s 出块)、Polygon zkEVM、Moonbase Alpha 在约 13–15s 的测试窗口里一次都没刷新(只给下限)。
- **熔断**:同一条链连发几次会失败的请求后，路由器会对这条链的所有请求返回 `all upstream endpoints exhausted`,zkSync 上持续约 5 分钟(测法已改成失败不重复，避免测试自己触发)。
- 55 条链有警告级问题(没有严重问题),其中 26 条在最近 6 次运行里每次都有。

## 一次实测结论(135 条链,2026-10-10)

- 常规请求可用率 **99.7%**(3645 次);5 处失败全部来自上游(不支持该方法 / 节点耗尽 / 限流)。
- 历史区块可用率只有 **93.7%**:近期 99.0%、中期 96.0%、早期 **85.9%**;14 条链完全查不到早期区块,多条链的上游池混有"只保留最近 1024 块"的公共节点。
- 命中缓存后的延迟 p50 **≈30ms**,跨链高度一致。
- 缓存中位加速 **8.1×**(冷回源 233ms → 命中 29ms),最高 **207×**(Centrifuge 5873ms → 28ms);135 条链冷回源全部拿到。
- 32 条链有严重或警告级问题,明细见报告 02 节。

> 注意:路由器的 `ingressTotal` 是累计计数,会把本工具自己的测试请求也算进去,所以跑过测试后 `--chains traffic` 和"有流量"标记都不再准确,报告里已不再使用。
