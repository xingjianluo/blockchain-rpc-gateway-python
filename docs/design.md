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
├── waiter.py       # 等待交易确认 WaitCoordinator：预算轮询/同交易合并/重组感知
├── dispatcher.py   # 分发：缓存 -> 选节点 -> 传输重试 / 业务短路 / 等待确认
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

## 5. 等待交易确认（`gw_waitForTransaction`，`waiter.py`）

### 5.1 请求与确认条件

网关在标准 JSON-RPC 上新增一个方法 `gw_waitForTransaction`，用于“在预算内
等某笔交易达到指定确认深度、或等到已最终确定”。支持具名与位置两种参数：

```jsonc
// 具名
{"transactionHash": "0x..", "confirmations": 3,
 "timeoutMs": 8000, "pollIntervalMs": 100}
// 等到最终确定（与 confirmations 二选一，finalized 优先）
{"transactionHash": "0x..", "finalized": true, "timeoutMs": 12000}
// 位置：[txHash] / [txHash, confirmations] / [txHash, confirmations, timeoutMs]
```

| 参数 | 含义 | 缺省/约束 |
|---|---|---|
| `transactionHash` | 交易标识 | 必填，`0x` 前缀字符串，否则 `-32602` |
| `confirmations` | 期望确认深度 | 缺省 1；正整数（十进制或 `0x`） |
| `finalized` | 是否等到所在区块最终确定 | `true` 时忽略确认深度 |
| `timeoutMs` | 总等待预算 | 缺省 10000；正整数 |
| `pollIntervalMs` | 轮询间隔 | 缺省 100；会被网关下限收敛 |

确认数按惯例定义：`confirmations = head - tx_block + 1`。
`finalized` 条件要求 `tx_block <= finalized_number`。

### 5.2 轮询、预算与结论

等待循环在预算内周期性地通过**既有分发链**（`dispatch_poll`：最终性缓存 →
节点选择 → 传输重试）依次发起三个标准只读调用：

1. `eth_blockNumber`：当前链顶高度；
2. `eth_getBlockByNumber(["finalized", false])`：当前最终高度，首次回填
   最终性缓存后，后续每轮稳定**命中缓存**；
3. `eth_getTransactionReceipt([txHash])`：交易落地位置（未上链按惯例为 `null`）。

每轮拿到链上事实后，**逐个等待方按自己的条件与预算独立结算**：

- 条件满足 → 返回 `confirmed`（结构见 5.5）；
- 预算已到而条件未满足 → 返回 `-32020 WAIT_TIMEOUT`，`data` 明确标注当前
  链顶/最终高度、交易是否已上链、已用时间与轮询次数；
- 既不会无限挂起，也不会在确认深度不足 / 未最终确定时提前伪造成功结果。

内部轮询拿到上游的**业务错误**（如收据方法 `-32602`）是确定性结论：
原样透传给所有等待方并结束该交易的等待；**传输故障**（连接重置/超时/
非法响应）只记录降级原因、切换节点重试，不改变等待结论。若链顶查询持续
传输失败，到达预算的等待方仍会被结算为超时，不会被挂住。

### 5.3 同一交易的等待合并（single-flight 等待）

`WaitCoordinator` 按 `tx_hash` 为每笔交易维护唯一一个 `_TxWatcher`：

- 同一时刻任意多个调用方（含同一批次的多个项）等待同一笔交易，
  只产生**一份**链顶/最终高度/收据轮询流量，不重复打上游；
- 共享的是“链上事实”，**结论不共享**：每个等待方用自己的条件、预算、
  原始 `id` 独立构造响应；成功、超时、不同条件互不串味；
- 不同交易各有独立 watcher，互不影响；
- 最后一个等待方结束/离开后摘除 watcher 并取消轮询；客户端中途断开
  只移除自己，不影响其余等待方。

### 5.4 重组期间的表现（关键）

watcher 记住上一轮交易所在 `(block_number, block_hash)`：

- **曾上链 → 本轮收据变 `null`**：判定被重组回滚，置 `reorged_seen`，
  绝不再把回滚前的位置当作已确认/已最终结果返回；
- 在**剩余预算内继续观察**：
  - 交易在新区块重新上链（`blockHash` 变化）→ 按**新位置**重新计确认数，
    达标后返回 `confirmed` 且 `reorged: true`（与一路正常确认可区分）；
  - 预算耗尽时交易仍未回到链上 → 返回 **`-32021 TX_REORGED`**，
    与“正常确认”“普通超时”都明确可区分，`data.reorged=true`，
    并给出最后一次见到的区块高度与原因。

假链为此提供：`send_tx`（入交易池）、`mine`（把池中交易确定性打进
第一个新区块）、`reorg(depth, dropped_txs=..., extra=...)`
（回滚未最终区块、以新 epoch 重出；交易默认退回池中等待重新打包，
`dropped_txs` 内的交易彻底丢弃），详见 `fakechain.py`。

### 5.5 成功结果结构

```jsonc
{
  "status": "confirmed",
  "transactionHash": "0x..",
  "blockNumber": "0x..", "blockHash": "0x..",
  "confirmations": "0x..",         // 当前确认深度
  "finalized": true,               // 所在区块当前是否已最终确定
  "reorged": false,                // 等待期间是否经历过重组
  "condition": "confirmations>=3", // 或 "finalized"
  "waitedMs": 142, "polls": 3,
  "node": "node-b"                 // 收据结论最终落在哪个上游
}
```

### 5.6 与现有能力的兼容范围

- **方法策略**：`gw_waitForTransaction` 本身先走方法级访问限制（黑名单/
  白名单），被限制时网关直接 `-32001`、不发起任何轮询；内部三个只读方法
  使用固定标准方法，经 `dispatch_poll` 走缓存与重试，不重复过策略；
- **上游选择 / 故障降级重试**：每一轮的每个只读调用都经过节点打分排序与
  传输故障切换，健康度、延迟统计正常累计；
- **最终性缓存**：finalized 高度查询纳入既有 `FinalityCache`，随链推进
  形成新键、随重组失效；
- **批量协议**：等待方法可与普通只读项混在同一批次，各项并发独立分发、
  按槽位对齐回写，单项超时/重组/错误不拖垮或串改其他项；
- **新增错误码**：`-32020 WAIT_TIMEOUT`、`-32021 TX_REORGED`（见错误码速查）。

## 6. 假节点故障模型（`fakeupstream`）

每个节点以守护线程运行**真实 uvicorn + TCP 端口**，支持确定性注入：
- `fail_first=N`：前 N 次请求在 ASGI 层发出响应头后异常断连，
  客户端稳定得到 `httpx.RemoteProtocolError`（与业务错误完全可区分），之后恢复；
- `always_connection_reset`：持续连接重置；
- `always_timeout`：读完请求后静默挂起，客户端稳定得到读超时。

`main.py` 默认画像：node-a 低抖动、node-b 前 2 次故障后恢复、node-c 高延迟。
