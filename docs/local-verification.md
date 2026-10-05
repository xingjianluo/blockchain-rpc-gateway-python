# 本地验证指南

## 1. 环境准备

```bash
# 需要 Python 3.14+，使用 uv 管理依赖
uv sync
```

## 2. 运行测试（推荐入口）

```bash
uv run pytest -q
```

测试使用真实 TCP 假节点（uvicorn 守护线程），共 46 个用例，覆盖：

- `tests/test_selector.py`：延迟/健康度选择、故障摘除与恢复；
- `tests/test_dispatch_retry.py`：连接故障切换、超时、**业务错误不重试**、
  全上游失败的 `-32003/-32004`、瞬时故障恢复；
- `tests/test_cache_reorg.py`：finalized 缓存命中、重组哈希失效、
  回滚高度不返回过期结果、未最终高度不缓存；
- `tests/test_singleflight.py`：12 个并发相同只读请求仅 1 次上游调用；
- `tests/test_batch.py`：批量 id 对齐、单项失败隔离、重复 id、通知 204；
- `tests/test_policy.py`：受限方法 `-32001`、白名单、批量超限/畸形错误码；
- `tests/test_wait_tx.py`：**等待交易确认**链路（11 个用例）：
  - 交易已上链立即确认；
  - 8 个调用方并发等同一笔交易：轮询合并为一份、finalized 命中缓存、
    结果各自按 req_id 归属；
  - 同一批次里成功/超时/普通只读/最终确定混排，结果对号入座互不串；
  - 等待期间重组后重新上链：按新位置确认且 `reorged=true`；
  - 重组后交易彻底回不来：`-32021 TX_REORGED`；
  - 预算耗尽未上链/确认深度不足：`-32020 WAIT_TIMEOUT`，不提前返回、不挂起；
  - 等到最终确定 `finalized=true`；
  - 等待期间 node-a 传输故障自动降级切换；
  - 等待方法受白名单限制、非法参数 `-32602`；
- `tests/test_e2e.py`：基于 `main.py` 内置三节点集群的综合场景。

### 查看可观测日志

测试默认以 INFO 打印**等待轮询过程、所选上游、缓存命中/未命中、
每次重试/降级原因**，例如：

```
tx=0xaa.. 加入等待 条件=confirmations>=2 预算=3000ms 间隔=20ms 当前等待方=8
tx=0xaa.. 启动预热：finalized 缓存未命中，已回填，后续轮询走缓存
method=eth_getTransactionReceipt 选择上游 node=node-b score=0.0
tx=0xaa.. poll#2 finalized 查询命中缓存 height=7
tx=0xaa.. poll#3 head=14 finalized=10 receipt=height=12 node=node-b
tx=0xaa.. 条件满足(confirmations>=2) 落定高度=12 确认数=3 重组=False 上游=node-b req_id=w0
node=node-a 传输失败(23ms): RemoteProtocolError，判定为传输故障，切换节点重试
tx=0xaa.. 观察到链重组：交易曾在 height=12 hash=0x..，现已从链上消失，继续在预算内观察
tx=0xaa.. 等待超时(confirmations>=1) 用时=250ms 轮询=12 req_id=t1 -> -32020
tx=0xdd.. 等待结束：交易被重组且未在预算内回来 -> -32021
链重组 depth=3 head=11 缓存失效=2/5
```

也可以只看关键行：

```bash
uv run pytest -q 2>&1 | grep -E "加入等待|poll#|条件满足|选择上游|缓存|重试|重组|超时"
```

## 3. 手动启动本地集群

```bash
uv run fastapi dev main.py
# 网关地址: http://127.0.0.1:8000/
# 启动日志会打印 node-a / node-b / node-c 三个假节点端口
```

### 3.1 正常只读请求

```bash
curl -s http://127.0.0.1:8000/ \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}'

curl -s http://127.0.0.1:8000/ \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":2,"method":"eth_getBlockByNumber","params":["finalized",false]}'
```

再发一次相同的 finalized 请求，观察日志出现 `缓存命中`。

### 3.2 传输故障自动切换

node-b 前 2 次调用注入连接重置：

```bash
# 前两次可能看到日志 "node=node-b ... 切换节点重试"，结果仍成功
for i in 1 2 3; do
  curl -s http://127.0.0.1:8000/ \
    -H 'content-type: application/json' \
    -d '{"jsonrpc":"2.0","id":'$i',"method":"eth_chainId","params":[]}'; echo
done
```

### 3.3 业务错误不被重试/掩盖

```bash
# 未知方法：上游 -32601 原样透传
curl -s http://127.0.0.1:8000/ -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":3,"method":"eth_doesNotExist","params":[]}'

# 受限方法：网关直接 -32001，不转发
curl -s http://127.0.0.1:8000/ -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":4,"method":"eth_sendRawTransaction","params":["0x1"]}'
```

### 3.4 链重组与缓存失效

```bash
curl -s http://127.0.0.1:8000/health
curl -s -X POST http://127.0.0.1:8000/admin/reorg \
  -H 'content-type: application/json' -d '{"depth":3,"seed":777}'
# => {"head":...,"invalidated":<失效条数>,"cached":...}
```

出块推进（finalized 前移，已最终缓存不受影响）：

```bash
curl -s -X POST http://127.0.0.1:8000/admin/mine \
  -H 'content-type: application/json' -d '{"count":5}'
```

### 3.5 等待交易确认（gw_waitForTransaction）

`main.py` 在高度 8 预置了演示交易
`0xabc0000000000000000000000000000000000000000000000000000000000001`。

```bash
# 等 2 个确认（已满足，立即返回 confirmed，可看 node/确认数/reorged=false）
curl -s http://127.0.0.1:8000/ -H 'content-type: application/json' -d '{
  "jsonrpc":"2.0","id":"w1","method":"gw_waitForTransaction",
  "params":{"transactionHash":"0xabc0000000000000000000000000000000000000000000000000000000000001",
            "confirmations":2,"timeoutMs":3000,"pollIntervalMs":100}}'; echo

# 等一笔不存在的交易，预算 300ms：明确返回 -32020 WAIT_TIMEOUT，而不是挂起
curl -s http://127.0.0.1:8000/ -H 'content-type: application/json' -d '{
  "jsonrpc":"2.0","id":"w2","method":"gw_waitForTransaction",
  "params":{"transactionHash":"0xdead000000000000000000000000000000000000000000000000000000000000",
            "confirmations":1,"timeoutMs":300,"pollIntervalMs":50}}'; echo
```

观察“等到最终确定 / 重组”可以先发起一个较长预算的等待，
再从另一个终端触发出块或重组：

```bash
# 终端1：等到最终确定（finalized 随出块前移，观察中途不提前返回）
curl -s http://127.0.0.1:8000/ -H 'content-type: application/json' -d '{
  "jsonrpc":"2.0","id":"w3","method":"gw_waitForTransaction",
  "params":{"transactionHash":"0xabc0000000000000000000000000000000000000000000000000000000000001",
            "finalized":true,"timeoutMs":15000,"pollIntervalMs":100}}'; echo

# 终端2：推进出块直到最终确定
curl -s -X POST http://127.0.0.1:8000/admin/mine \
  -H 'content-type: application/json' -d '{"count":5}'
```

等待期间网关日志会持续打印 `poll#`、链顶/最终高度、收据所在高度、
落定上游、finalized 缓存命中与降级重试原因。

### 3.6 批量请求（单项失败不影响整批）```bash
curl -s http://127.0.0.1:8000/ -H 'content-type: application/json' -d '[
  {"jsonrpc":"2.0","id":"ok","method":"eth_blockNumber","params":[]},
  {"jsonrpc":"2.0","id":"bad","method":"eth_nope","params":[]},
  {"jsonrpc":"2.0","id":"denied","method":"eth_accounts","params":[]},
  {"jsonrpc":"2.0","id":null,"method":"eth_chainId","params":[]}
]'
```

## 4. 错误码速查

| 错误码 | 含义 | 可重试 |
|---|---|---|
| -32700 | JSON 解析失败 | —（协议层） |
| -32600 | 非法请求/空批量 | — |
| -32601 | 方法不存在（业务） | 否，原样透传 |
| -32602 | 参数非法（业务） | 否，原样透传 |
| -32001 | 方法被网关策略拒绝 | 否 |
| -32003 | 所有上游传输故障 | 网关内部已逐节点重试 |
| -32004 | 所有上游超时 | 网关内部已逐节点重试 |
| -32010 | 批量条数超限 | — |
| -32011 | 批量元素畸形（仅该项） | — |
| -32020 | 等待交易确认超时（预算耗尽仍未达标） | 网关在预算内已持续轮询 |
| -32021 | 交易等待期间被重组回滚且未在预算内重新上链 | 与成功确认/普通超时明确区分 |
