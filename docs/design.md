# 区块链 JSON-RPC 网关设计说明

面向本地部署的多上游节点场景，提供**节点选择与故障切换、最终性缓存与重组失效、
方法策略与批量约束**。所有上游均为确定性的本地假节点，故障、延迟、重组均可复现。

## 1. 模块组成

```
gateway/
├── errors.py       # JSON-RPC 错误码、可重试判定依据
├── models.py       # RpcRequest/RpcResponse/UpstreamNode/DispatchResult
├── fakechain.py    # 确定性假链：出块、重组(reorg)、回滚(rollback)
├── fakeupstream.py # 假节点 ASGI 应用 + 真实 TCP 端口服务器 + 故障注入
├── selector.py     # 健康度/延迟节点选择
├── policy.py       # 方法白/黑名单 + 批量大小约束
├── cache.py        # 最终性缓存 FinalityCache + SingleFlight
├── waiter.py       # 等待交易确认：按 tx 合并轮询、按等待方独立判定
├── dispatcher.py   # 分发：缓存 -> 选节点 -> 传输重试 / 业务短路 / 等待编排
└── app.py          # FastAPI 协议入口（单条/批量/通知/管理端点）
main.py             # 启动 3 个假节点 + 网关的本地示例
```

## 2. 上游选择与降级策略

### 2.1 选择算法（`selector.NodeSelector`）

每个节点维护：

- `latency_ewma_ms`：成功调用延迟的指数加权移动平均（α=0.4）；
- `consecutive_failures`：连续传输失败次数，每次带来 500ms 惩罚分；
- `healthy`：连续失败达到阈值（3 次）后摘除，一次成功即恢复。

```
score(node) = ∞                       # 不健康
            = latency_ewma_ms + 500ms * consecutive_failures
```

每次分发按 `score` 升序选择；同分按轮转序号打散，避免流量集中。
已尝试失败的节点在本次请求内通过 `exclude` 集合排除。

### 2.2 两类失败的区分（关键）

| 类别 | 触发条件 | 判定依据 | 行为 |
|---|---|---|---|
| 传输故障 | 连接被拒/被重置、读超时、非法 JSON、非 JSON-RPC 结构 | 客户端拿不到任何业务结论，换节点可能成功 | 切换下一节点重试，记录失败并降低健康度 |
| 业务错误 | 上游合法返回 `-32601`/`-32602`/`-32600` 等 JSON-RPC error | 确定性结论，换节点结论不变 | **立即原样透传，不重试、不掩盖** |

实现中具体的异常映射：

- `httpx.TimeoutException` → 传输故障，全失败后返回 `-32004 UPSTREAM_TIMEOUT`；
- `httpx.TransportError`（含注入的 `RemoteProtocolError`、`ConnectError`）
  → 传输故障，全失败后返回 `-32003 UPSTREAM_UNAVAILABLE`；
- HTTP 响应体无法解析为 JSON / 缺少 `result|error` 字段 → 传输故障；
- JSON-RPC `error` 字段（`is_retryable_upstream(code)=False`）→ 业务错误，短路返回。

所有判定都会写入 `DispatchResult.reasons` 并打印日志，例如：

```
method=eth_blockNumber 选择上游 node=node-b score=12.0
node=node-b 传输失败(7ms): RemoteProtocolError，判定为传输故障，切换节点重试
method=eth_blockNumber 选择上游 node=node-a score=8.0
node=node-a 返回业务错误 code=-32601 (method x not found)，确定性结论，不重试
```

当所有候选节点均传输失败时，返回带 `data.reason` 的 `-32003/-32004`，
而非伪造业务结果。

## 3. 缓存与失效规则

### 3.1 仅缓存已最终确定的只读数据

可缓存方法集合：`eth_getBlockByNumber`、`eth_getBalance`。

| 区块参数 | 是否缓存 |
|---|---|
| `finalized` | 是（解析为当前 `finalized_number`） |
| 显式十六进制高度 `<= finalized_number` | 是 |
| `latest` / `pending` / `safe` / `earliest` | 否，一律透传上游 |
| 显式高度高于最终高度 | 否（未最终确定，可能重组） |

缓存键绑定解析后的**最终高度**与参数：
`method:resolvedHeight:canonicalParams`。finalized 向前推进后自然形成新键。
每个条目记录抓取时刻的 `(block_number, block_hash)`。

### 3.2 链重组/回滚失效（`on_new_head`）

链推进（`/admin/mine`）或重组（`/admin/reorg`）后逐条校验：

1. 缓存高度 **> 当前链顶**（高度已被回滚，rollback 场景）→ 删除；
2. 缓存高度的 **当前哈希 != 记录哈希**（该高度被重组重出）→ 删除；
3. 其余保留。已最终确定且未被深重组触及的数据保持命中。

`FinalityCache.get` 还会做一次防御式哈希校验，即使未触发失效流程也绝不返回
过期结果。假链的哈希/余额由 `(seed, epoch, 高度, 账户)` 确定性生成，
重组后同高度哈希与余额必然变化，测试可稳定断言。

### 3.3 并发相同只读请求合并（`SingleFlight`）

同一事件循环内，相同缓存键的并发只读请求只允许一个回源 producer 执行，
其余 await 同一个 future 共享结果（成功或异常均传播）。
不同键（不同方法/参数/高度）互不影响。

> 说明：测试中 `fastapi.TestClient` 每个请求使用独立事件循环，因此
> `single-flight` 的端到端测试使用 `httpx.ASGITransport` 在单一事件循环内
> 并发发起（见 `tests/test_singleflight.py`，12 个并发请求仅 1 次上游调用）。

## 4. 方法策略与批量约束（`policy.MethodPolicy`）

- **黑名单**（默认）：`eth_sendRawTransaction`、`eth_sendTransaction`、
  `eth_accounts`、`personal_sendTransaction`、`miner_start`、`admin_nodeInfo`
  在网关侧直接返回 `-32001 METHOD_FORBIDDEN`，**不产生任何上游调用**；
- **白名单模式**：`allowed` 非空时，不在白名单（且不在黑名单）的方法也拒绝；
- **未知方法**默认放行：由上游返回标准 `-32601`，作为业务错误透传，不重试。

### 批量协议（`app.py`）

| 场景 | 状态/错误码 |
|---|---|
| 非法 JSON | `-32700 PARSE_ERROR`（单个错误对象） |
| 单条非法结构 / `jsonrpc!=2.0` / method 非字符串 | `-32600 INVALID_REQUEST` |
| `params` 非数组非对象 | `-32602 INVALID_PARAMS` |
| 空数组批量 | `-32600 INVALID_REQUEST` |
| 批量条数超过 `max_batch_size` | `-32010 BATCH_TOO_LARGE` |
| 批量中元素不是对象 | 该项 `-32011 BATCH_MALFORMED`，不影响其他项 |
| 通知（无 `id`） | 不产生响应；全通知批量返回 `204` |

批内各项**并发独立分发、按下标位对齐写回**：任何单项（含传输全失败、业务错误、
策略拒绝、畸形项）都不会拖垮或截断整批，调用方原始 `id`（数字/字符串/null/重复）
原样回显，不错位、不互相污染。

## 5. 等待交易确认（`waiter.py`，方法 `gw_waitForTransaction`）

调用方提交交易标识、确认条件与总等待预算，网关在预算内向**上游**轮询
`eth_getTransactionReceipt`，满足条件即返回落块位置与确认程度。

### 5.1 请求与结果

```json
{"jsonrpc":"2.0","id":1,"method":"gw_waitForTransaction","params":{
  "txHash": "0x...",          // 必填，0x 前缀
  "confirmations": 3,         // 可选，确认数阈值，默认 1
  "finalized": true,          // 可选，为 true 时要求所在高度已最终确定
  "timeoutMs": 10000          // 可选，总等待预算，默认 30000，上限 300000
}}
```

也兼容位置参数形态 `[txHash, {options...}]`。参数非法返回 `-32602`。

成功结果（`status` 为 `confirmed` 或 `finalized`）：

```json
{"txHash":"0x...","status":"confirmed","blockNumber":"0xb",
 "blockHash":"0x...","confirmations":3,"finalized":false}
```

失败结果与成功严格区分，**绝不返回未达标的结果**：

| 情形 | 错误码 |
|---|---|
| 预算耗尽仍未达标（从未上链 / 确认数不足） | `-32020 WAIT_TIMEOUT` |
| 等待期间被重组回滚且预算内未重新上链 | `-32021 TX_REORGED`（`data.lastIncluded` 给出回滚前位置） |

### 5.2 确认深度与最终性的判定

- 确认数 = 当前链顶高度 − 交易所在高度 + 1，链顶/最终高度取自与最终性缓存
  相同的本地链镜像；
- `confirmations` 与 `finalized` 同时给出时需**同时满足**；
- 预算从请求进入等待时开始计时，到期立即返回对应错误，不挂起、不延期。

### 5.3 轮询合并（按交易 single-flight）

同一 `tx_hash` 无论多少并发等待方（含同一批次内的多项），只存在**一个**
`TxWatch` 轮询任务，按固定间隔（`Dispatcher(wait_poll_interval=...)`，
默认 1s）产生一路上游调用；每个等待方持有自己的条件与预算，独立对共享
观测做判定，各自拿到的结果严格对应自己的请求 `id` 与条件，互不影响。
最后一个等待方结束后轮询任务即停止。

### 5.4 重组处理

轮询发现回执从“已上链”变为“不存在”即判定重组，观测转为 `dropped`：

- 仍在等待的调用方**不会**拿到回滚前的确认结论；
- 预算内交易重新上链（新高度/新哈希）→ 按**新位置**继续判定并返回；
- 预算耗尽仍未重新上链 → `-32021 TX_REORGED`，与正常确认明确区分。

### 5.5 与既有能力的兼容

- 每一轮轮询都走 `Dispatcher.dispatch_single`：节点选择、传输故障切换
  重试、业务错误短路、方法策略（如把 `gw_waitForTransaction` 或
  `eth_getTransactionReceipt` 加入黑名单即 `-32001`）全部生效；
- 等待方法可出现在批量请求中，与其他项并发独立分发、按 `id` 对齐；
- 轮询方法 `eth_getTransactionReceipt` 不属于最终性缓存的可缓存集合，
  不占用也不绕过缓存；既有只读缓存与 single-flight 行为不变。

## 6. 假节点故障模型（`fakeupstream`）

每个节点以守护线程运行**真实 uvicorn + TCP 端口**，支持确定性注入：

- `latency_ms` / `jitter_ms`（带种子的抖动）；
- `fail_first=N`：前 N 次请求在 ASGI 层发出响应头后异常断连，
  客户端稳定得到 `httpx.RemoteProtocolError`（与业务错误完全可区分），之后恢复；
- `always_connection_reset`：持续连接重置；
- `always_timeout`：读完请求后静默挂起，客户端稳定得到读超时。

`main.py` 默认画像：node-a 低抖动、node-b 前 2 次故障后恢复、node-c 高延迟。
