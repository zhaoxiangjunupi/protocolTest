主要方法：
eth_getLogs：按代币合约地址 + Transfer 等事件 topic 过滤，扫最新区块，这是用量最大的；
eth_call：ERC20 balanceOf、Multicall3 批量读余额、Safe 多签的 getThreshold / getOwners 等只读函数；
其余：eth_blockNumber、eth_getBlockByNumber、eth_getCode、eth_getTransactionCount、eth_getTransactionReceipt、eth_getBalance、eth_chainId。
大量用 JSON-RPC 批量请求，一次几十到一百条。也会按历史块高做 eth_call、扫历史日志，需要 2026-04-01 以后的归档数据完整可查。
2. 频率：
实时监控 7×24 小时跑：约 12 个程序每分钟各一轮，每轮扫最近约 130 个块，getLogs 单次一般 100～500 块。合计常态约每分钟 70～90 次，约每秒 1～1.5 次。
两台服务器各每 20 秒查一次 eth_blockNumber，做节点健康检查。
每天 10:00～10:20 集中出日报和并表，要批量读 2,000～3,000 个地址的余额，这段时间是峰值，批量请求短时可到每秒几十条。
不定期补数或回溯历史：getLogs 单次最多 5 万块，单次结果控制在 2 万条以内，偶尔连续跑几十分钟到几小时。
