# 区块链节点本地 JSON-RPC 网关

在多个（本地假）区块链节点之上的只读 JSON-RPC 2.0 网关。内置**多上游故障切换**、
**按最终性划分的缓存与链重组失效**、**相同只读请求并发合并**、**方法级访问策略与
批量约束**，并以本地假节点模拟多节点、网络抖动（延迟/超时/断连）与链重组，
使故障与并发场景下的结果**可解释、可复现**。

> 运行要求：Python ≥ 3.14，依赖 FastAPI + httpx，测试用 pytest/pytest-asyncio。

---

## 1. 快速开始

```bash
uv sync                        # 安装依赖
uv run pytest -s               # 运行全部测试（-s 可看到决策日志）
uv run uvicorn main:app --reload   # 启动网关（默认内置 3 个内存假节点）
```

启动后：

```bash
# 单条请求
curl -s localhost:8000 -H 'content-type: application/json' -d '{
  "jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}'

# 批量请求（各项独立、保序、互不污染）
curl -s localhost:8000 -H 'content-type: application/json' -d '[
  {"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]},
  {"jsonrpc":"2.0","id":2,"method":"eth_getBlockByNumber","params":[3,false]},
  {"jsonrpc":"2.0","id":3,"method":"eth_sendRawTransaction","params":["0x1"]}
]'

curl -s localhost:8000/health   # 节点健康度 / 缓存命中 / 合并次数
```

### 独立进程方式启动多个假上游（可选）

假节点本身也是一个 HTTP 服务，带故障/重组控制端点，可启动多份模拟真实多节点：

```bash
uv run python -m gateway.cli_fake_server --name node-a --port 9101
uv run python -m gateway.cli_fake_server --name node-b --port 9102
uv run python -m gateway.cli_fake_server --name node-c --port 9103
```

控制端点（用于演练，非 JSON-RPC）：

```bash
curl -X POST localhost:9101/__control/grow  -H 'content-type: application/json' -d '{"n":3}'
curl -X POST localhost:9101/__control/reorg -H 'content-type: application/json' -d '{"fork_height":5,"depth":3}'
curl -X POST localhost:9101/__control/fault -H 'content-type: application/json' \
  -d '{"timeout_methods":["eth_blockNumber"]}'      # 或 {"fail_connect":true} / {"latency":0.5}
curl -s localhost:9101/__chain
```

将 `config.example.json` 中 `use_fake_upstreams` 设为 `false` 并指向这些地址，
网关即通过真实 HTTP（`gateway/upstream/http_transport.py`）访问。

---

## 2. 上游选择与降级策略

### 2.1 两类失败的明确区分（核心判定依据）

| 失败类型 | 触发条件 | 是否重试 | 错误码 |
|---|---|---|---|
| **传输层故障** `TransportError` | 未得到上游的 JSON-RPC 应答：TCP 连接失败/拒绝、连接重置、读写超时、HTTP 5xx/4xx、非对象响应 | **切换节点重试** | `-32001` 超时 / `-32002` 连接失败 / `-32000` 全部节点失败 |
| **业务错误** `RpcBusinessError` | 上游返回了结构合法的 JSON-RPC `error`：`-32601` 方法不存在、`-32602` 参数非法等 | **不重试、不掩盖** | 透传上游错误码（如 `-32601`/`-32602`） |

判定逻辑见 `gateway/engine/retry.py::classify_failure`，每次决策都会打印 reason。
依据是：业务错误是**确定性语义结果**，换节点结论相同，重试只会放大负载；
传输故障时请求语义未知，对只读调用可安全地在另一节点重试。

### 2.2 节点选择与熔断

- 每个节点维护健康状态（`gateway/upstream/health.py`）：连续失败计数、熔断截止
  时间、最近 20 次响应延迟滑动窗口。
- 选择评分 = `-(平均延迟 + 连续失败惩罚)`，熔断中的节点不可选；评分相同按节点名
  稳定排序，保证选择顺序可复现（`gateway/upstream/selector.py`）。
- 连续失败达到阈值（默认 3 次）熔断，冷却时间按指数退避（1x、2x、4x…）；冷却后
  节点重新进入候选做半开探测，一次成功即关闭熔断并清零失败计数。
- 一次请求按健康顺序逐节点尝试；传输故障→记录健康并切换下一节点；业务错误→立即返回。
  所有节点均传输失败时返回 `-32000`，响应 `data.attempts` 给出尝试过的节点序列。

---

## 3. 缓存与失效规则（按最终性）

实现在 `gateway/cache/`，最终性判定见 `cache/finality.py::classify`：

- **可缓存（FINALIZED）**：按明确高度读取的方法
  （`eth_getBlockByNumber`、`eth_getBalance`、`eth_getCode`、`eth_getStorageAt` 等），
  且请求高度 `<= finalized_height`。
- **不缓存（UNFINALIZED）**：高度高于最终高度、`latest/pending/safe/finalized` 标签、
  以及 `eth_call`、`eth_blockNumber` 等随链头变化的请求——每次都访问上游，
  **绝不返回过期结果**。
- **按哈希内容寻址**（`eth_getBlockByHash` 等）视为可缓存：重组后旧哈希查不到数据，
  不会“张冠李戴”。
- **错误响应不缓存**。
- **链重组失效**：网关在每次请求前观测最新 `finalized_height`；一旦发现最终高度
  回退（重组），即调用 `FinalityCache.invalidate_reorg(fork_height-1)`，
  **凡关联高度 > 回滚点的缓存条目一律失效**。因此即使高度此前被标记为“已最终确定”，
  一旦被回滚，也不会再返回旧哈希。失效条数会打印日志。
- 缓存条目带 TTL（默认 300s，惰性淘汰）。

### 并发请求合并（singleflight）

同一时刻键（方法 + 规范化参数 + 请求 id/通知标记）完全相同的并发只读请求，
只发起**一次**上游调用，其余等待者共享同一个 Future（`cache/singleflight.py`）。
业务异常会原样传播给每个等待者，不被吞掉。合并键包含 id，因此通知（无 id）、
`id=null`、`id=1` 不会相互借用响应。

---

## 4. 方法策略与批量约束

实现于 `gateway/policy/`，配置见 `config.example.json`。

- **方法级访问策略**（`method_policy.py`）：`forbidden_methods` 中的方法在网关侧
  直接拒绝，返回 **`-32010 METHOD_FORBIDDEN`**，绝不转发上游。可选 `allowed_methods`
  白名单；白名单外的未知方法默认放行，交由上游按 `-32601` 判定，避免网关掩盖真实错误。
- **批量约束**（`batch_policy.py` + `engine/batch.py`）：
  - 数量超过 `max_batch`：整批拒绝 **`-32011 BATCH_TOO_LARGE`**（HTTP 413）。
  - 空批量：`-32600`（HTTP 400）。
  - 单项畸形（非对象、缺/非法 `method`、`params` 类型错、jsonrpc 版本错）：
    **仅该项**返回 `-32600`（能提取就回填其 id，否则 id=null），兄弟项照常执行，
    整批不失败、不缺项、不错位。
  - 单项内部异常被隔离为该项的 `-32603`，不影响其它项。
  - 无 `id` 的通知项不产生响应（遵循 JSON-RPC 2.0）；全部为通知时返回 HTTP 204。
  - 即使多项 `id` 重复，结果也按原始位置保序返回，不相互覆盖。

### 错误码速查

| 码 | 含义 | 可重试 |
|---|---|---|
| `-32700` | JSON 解析错误 | 否 |
| `-32600` | 非法请求 / 批量畸形项 | 否 |
| `-32601` | 方法不存在（上游业务错误） | 否 |
| `-32602` | 参数非法（上游业务错误） | 否 |
| `-32603` | 内部错误 | 否 |
| `-32000` | 所有上游传输层故障 | 是（已逐节点尝试） |
| `-32001` | 上游超时 | 是（切换节点） |
| `-32002` | 上游连接失败/重置/5xx | 是（切换节点） |
| `-32010` | 方法被网关策略禁止 | 否 |
| `-32011` | 批量超限 | 否 |
| `-32013` | 限流 | 否 |

---

## 5. 可观测性与“可解释、可复现”

- 所有关键决策都用 logger `gateway.dispatcher` / `gateway.batch` 打印，测试中
  以 `TEST-LOG` 前缀实时输出：
  - 所选上游：`upstream=node-a SUCCESS method=... latency=... (attempts=[...])`
  - 缓存命中：`cache HIT/MISS method=... key=...`；重组：`reorg detected: ... N entries invalidated`
  - 重试原因：`upstream=node-a transport FAILED ... retryable -> trying next node`
  - 业务错误不重试：`business error code=-32601 ... deterministic -> NOT retrying`
- 假节点的抖动基于调用计数确定性生成，测试通过注入的**假时钟**控制熔断，
  避免 wall-clock 造成的偶发失败。
- `GET /health` 暴露各节点可用性、连续失败数、平均延迟、缓存条目/命中数、合并次数。

---

## 6. 本地验证方法

```bash
uv run pytest -s                       # 全量测试并打印决策日志
uv run pytest tests/test_dispatcher.py -s    # 多上游故障切换 / 两类失败区分 / 熔断
uv run pytest tests/test_cache_reorg.py -s   # 最终性缓存 / 链重组失效 / 并发合并
uv run pytest tests/test_batch_policy.py -s  # 方法策略 / 批量隔离 / id 不错位
uv run pytest tests/test_api.py -s           # HTTP 端到端错误码 / 故障切换 / 重组
```

测试矩阵（`tests/`）：

| 文件 | 覆盖重点 |
|---|---|
| `test_dispatcher.py` | 延迟选节点、连接拒绝/超时切换、业务错误不重试、全失败 `-32000`、熔断与半开恢复、故障分类 reason |
| `test_cache_reorg.py` | 最终数据缓存命中、未最终/latest 不缓存、重组回滚失效、深重组、错误不缓存、8 并发合并为 1 次上游调用 |
| `test_batch_policy.py` | `-32010` 不转发、混合批逐项隔离、超限整批 `-32011`、重复 id 不串位、通知无响应、畸形项隔离 |
| `test_singleflight.py` | 同键合并、异键不合并、异常广播、调用结束后再次执行 |
| `test_api.py` | 协议字段、`-32700/-32600/-32011/-32010/-32000` 错误码、HTTP 故障切换透明、重组后不取旧值、health |
| `test_http_transport.py` | 进程内 uvicorn 假节点 + `HttpTransport`：连接拒绝切换、业务错误不重试、全失败 `-32000` |

---

## 7. 代码结构

```
gateway/
  models.py               # RpcRequest/RpcResponse/Finality/NodeInfo
  errors.py               # 错误码 + TransportError / RpcBusinessError 分类
  config.py               # 配置加载（含本地演示配置）
  app.py                  # 组装根 GatewayRuntime / create_app
  upstream/
    base.py               # UpstreamTransport 协议
    http_transport.py     # httpx 真实 HTTP 上游（传输故障映射）
    fake_node.py          # 本地假节点 + FakeChainState（抖动/故障/重组）
    health.py             # 健康度/熔断/延迟窗口
    selector.py           # 按健康度与延迟排序选节点
  cache/
    finality.py           # 最终性判定
    store.py              # FINALIZED 缓存 + 重组失效
    singleflight.py       # 并发相同请求合并
  policy/
    method_policy.py      # 方法允许/受限
    batch_policy.py       # 批量上限/畸形检测
    rate_limit.py         # 令牌桶（默认关闭）
  engine/
    retry.py              # 故障分类与重试判定
    dispatcher.py         # 单条分发：策略→缓存→合并→故障切换
    batch.py              # 批量编排：逐项独立、保序、隔离
  api/
    rpc_handler.py        # JSON-RPC 解析/错误编码
    routes.py             # POST / 与 GET /health
  cli_fake_server.py      # 可独立启动的假节点 HTTP 服务
tests/                    # 上述测试矩阵 + conftest（多假节点/假时钟/日志）
main.py                   # create_app() 供 uvicorn 加载
```
