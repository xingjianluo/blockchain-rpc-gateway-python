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

测试使用真实 TCP 假节点（uvicorn 守护线程），共 45 个用例，覆盖：

- `tests/test_selector.py`：延迟/健康度选择、故障摘除与恢复；
- `tests/test_dispatch_retry.py`：连接故障切换、超时、**业务错误不重试**、
  全上游失败的 `-32003/-32004`、瞬时故障恢复；
- `tests/test_cache_reorg.py`：finalized 缓存命中、重组哈希失效、
  回滚高度不返回过期结果、未最终高度不缓存；
- `tests/test_singleflight.py`：12 个并发相同只读请求仅 1 次上游调用；
- `tests/test_wait_tx.py`：等待交易确认——成功/最终确定、超时 `-32020`、
  重组回滚未恢复 `-32021`、重组后重新上链按新位置确认、
  并发等同一笔交易合并轮询且结果各自对位、批量内不同项互不串扰、
  等待期间上游故障自动切换、方法策略对等待方法生效；
- `tests/test_batch.py`：批量 id 对齐、单项失败隔离、重复 id、通知 204；
- `tests/test_policy.py`：受限方法 `-32001`、白名单、批量超限/畸形错误码；
- `tests/test_e2e.py`：基于 `main.py` 内置三节点集群的综合场景。

### 查看可观测日志

测试默认以 INFO 打印**所选上游、缓存命中/未命中、每次重试原因**，例如：

```
method=eth_getBlockByNumber 缓存命中 key=eth_getBlockByNumber:7:[...]
method=eth_blockNumber 选择上游 node=node-b score=1.0
node=node-b 传输失败(7ms): RemoteProtocolError，判定为传输故障，切换节点重试
method=eth_blockNumber 选择上游 node=node-a score=8.0
node=good 返回业务错误 code=-32601 ... 确定性结论，不重试
链重组 depth=3 head=11 缓存失效=2/5
tx=0xaaaa... 第1轮轮询：上链 height=11 hash=0xd491a4c5df (此前 pending)
tx=0xaaaa... 达成条件：height=11 confirmations=1 finalized=False (第1轮轮询后)
```

也可以只看关键行：

```bash
uv run pytest -q 2>&1 | grep -E "选择上游|缓存|重试|业务错误|重组|轮询|等待"
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

### 3.5 批量请求（单项失败不影响整批）

```bash
curl -s http://127.0.0.1:8000/ -H 'content-type: application/json' -d '[
  {"jsonrpc":"2.0","id":"ok","method":"eth_blockNumber","params":[]},
  {"jsonrpc":"2.0","id":"bad","method":"eth_nope","params":[]},
  {"jsonrpc":"2.0","id":"denied","method":"eth_accounts","params":[]},
  {"jsonrpc":"2.0","id":null,"method":"eth_chainId","params":[]}
]'
```

### 3.6 等待交易确认（gw_waitForTransaction）

```bash
# 先把一笔交易打包进链顶（测试辅助端点）
curl -s -X POST http://127.0.0.1:8000/admin/include_tx \
  -H 'content-type: application/json' -d '{"txHash":"0xabcd"}'

# 等待它达到 2 个确认；另开一个终端出块可看到确认数增长
curl -s http://127.0.0.1:8000/ -H 'content-type: application/json' -d \
  '{"jsonrpc":"2.0","id":1,"method":"gw_waitForTransaction","params":
    {"txHash":"0xabcd","confirmations":2,"timeoutMs":10000}}'
# => {"result":{"txHash":"0xabcd","status":"confirmed","blockNumber":"0x..",
#     "blockHash":"0x..","confirmations":2,"finalized":false}, ...}

# 等一笔永不上链的交易：预算耗尽返回 -32020，不会一直挂起
curl -s http://127.0.0.1:8000/ -H 'content-type: application/json' -d \
  '{"jsonrpc":"2.0","id":2,"method":"gw_waitForTransaction","params":
    {"txHash":"0xdead","timeoutMs":2000}}'
# => {"error":{"code":-32020,"message":"timed out waiting ...", ...}}

# 等待期间触发重组（回滚包含该交易的区块）且不再重新打包：
# 预算耗尽返回 -32021，与正常确认明确区分
curl -s -X POST http://127.0.0.1:8000/admin/reorg \
  -H 'content-type: application/json' -d '{"depth":2}'
```

等待期间网关日志可见每一轮轮询、所选上游、重组判定与最终结论。

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
| -32020 | 等待交易确认超出时间预算 | 可由调用方重新发起等待 |
| -32021 | 等待期间交易被重组回滚且未恢复 | 否（与正常确认明确区分） |
