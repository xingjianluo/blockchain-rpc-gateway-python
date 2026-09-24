"""假上游 JSON-RPC 节点：注入延迟、抖动、超时、连接故障。

两种部署形态：
- create_fake_app：FastAPI 应用，可用 TestClient 直连做纯逻辑测试；
- FakeUpstreamServer：在守护线程内以真实 TCP 端口运行 uvicorn，
  使 httpx 能真实经历连接失败 (ConnectError) 与读取超时。
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import socket
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Self

import uvicorn
from fastapi import FastAPI, Request, Response

from gateway.errors import RpcErrorCode, error_payload
from gateway.fakechain import FakeChain

logger = logging.getLogger("gateway.fakeupstream")


@dataclass
class FaultConfig:
    """单节点的故障注入配置（确定性，便于复现）。"""

    always_timeout: bool = False
    always_connection_reset: bool = False
    fail_first: int = 0       # 前 N 次调用注入连接故障，之后恢复
    latency_ms: int = 0
    jitter_ms: int = 0
    seed: int = 0
    calls: int = field(default=0)
    _rng: random.Random = field(default_factory=random.Random, repr=False)

    def __post_init__(self) -> None:
        self._rng = random.Random(self.seed or 1)

    def sleep_delay_s(self) -> float:
        jitter = self._rng.randint(0, self.jitter_ms) if self.jitter_ms else 0
        return (self.latency_ms + jitter) / 1000.0

    def should_break_connection(self) -> bool:
        self.calls += 1
        return self.always_connection_reset or self.calls <= self.fail_first


def _resolve_block_tag(chain: FakeChain, tag: Any) -> int | None:
    if tag == "latest" or tag == "pending":
        return chain.head_number()
    if tag == "finalized":
        return chain.finalized_number()
    if tag == "earliest":
        return 0
    if isinstance(tag, str) and tag.startswith("0x"):
        try:
            height = int(tag, 16)
        except ValueError:
            return None
        return height if 0 <= height <= chain.head_number() else None
    return None


def _handle_method(chain: FakeChain, method: str, params: Any) -> Any:
    """执行业务方法；抛 ValueError 表示参数非法（确定性业务错误）。

    返回 (result, ) 或 RpcError 信息。
    """
    if method == "eth_chainId":
        return hex(chain.seed)
    if method == "eth_blockNumber":
        return hex(chain.head_number())
    if method == "eth_getBlockByNumber":
        if not isinstance(params, list) or not params:
            raise ValueError("expected [blockTag, includeTransactions]")
        height = _resolve_block_tag(chain, params[0])
        if height is None:
            return None  # 高度越界：按以太坊惯例返回 null（非错误）
        return chain.block_at(height)
    if method == "eth_getBalance":
        if not isinstance(params, list) or len(params) < 2 or not isinstance(
            params[0], str
        ):
            raise ValueError("expected [account, blockTag]")
        height = _resolve_block_tag(chain, params[1])
        if height is None:
            return None
        return hex(chain.balance_at(height, params[0]))
    # 未知方法：标准业务错误，网关不得重试。
    return _MethodNotFound(method)


class _MethodNotFound:
    def __init__(self, method: str):
        self.method = method


def _result_or_error(method: str, params: Any, chain: FakeChain) -> dict[str, Any]:
    try:
        out = _handle_method(chain, method, params)
    except ValueError as exc:
        return {"error": error_payload(
            RpcErrorCode.INVALID_PARAMS, f"invalid params: {exc}")}
    if isinstance(out, _MethodNotFound):
        return {"error": error_payload(
            RpcErrorCode.METHOD_NOT_FOUND, f"method {method} not found")}
    return {"result": out}


async def _handle_json(
    chain: FakeChain, name: str, fault: FaultConfig, payload: Any
) -> Any:
    # 传输层故障（连接重置/超时）统一由 _TransportFaultWrapper 在
    # ASGI 层注入；这里只处理正常延迟与业务逻辑。
    delay = fault.sleep_delay_s()
    if delay:
        await asyncio.sleep(delay)

    def one(item: Any) -> dict[str, Any] | None:
        if not isinstance(item, dict) or item.get("jsonrpc") != "2.0" or "method" not in item:
            return {"jsonrpc": "2.0", "id": None,
                    "error": error_payload(RpcErrorCode.INVALID_REQUEST, "invalid request")}
        method = item["method"]
        req_id = item.get("id")
        if "id" not in item:
            return None  # 通知：上游不返回
        body = _result_or_error(method, item.get("params", []), chain)
        return {"jsonrpc": "2.0", "id": req_id, **body}

    if isinstance(payload, list):
        responses = [r for r in (one(i) for i in payload) if r is not None]
        return responses
    return one(payload)


def create_fake_app(chain: FakeChain, name: str, fault: FaultConfig) -> FastAPI:
    """构造一个假节点 FastAPI 应用。

    外层 ASGI 包装负责真正的传输层故障注入：
    - 连接重置：直接向底层连接写入非法 HTTP 字节，客户端稳定收到
      RemoteProtocolError（区别于任何 JSON-RPC 业务错误）；
    - 超时：读完请求后静默不发送响应，客户端稳定收到 ReadTimeout。
    """
    app = FastAPI(title=f"fake-upstream-{name}")

    @app.post("/")
    async def rpc(request: Request) -> Response:
        delay = fault.sleep_delay_s()
        if delay:
            await asyncio.sleep(delay)
        try:
            payload = await request.json()
        except Exception:  # noqa: BLE001 - 任何 JSON 解析失败统一为 parse error
            import json
            return Response(
                content=json.dumps({"jsonrpc": "2.0", "id": None, "error": error_payload(
                    RpcErrorCode.PARSE_ERROR, "parse error")}),
                media_type="application/json",
            )
        out = await _handle_json(chain, name, fault, payload)
        import json
        return Response(content=json.dumps(out), media_type="application/json")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"node": name, "status": "ok"}

    return _TransportFaultWrapper(app, name, fault)


class _TransportFaultWrapper:
    """在 ASGI 层注入连接重置/超时，绕过应用层错误处理。"""

    def __init__(self, app: FastAPI, name: str, fault: FaultConfig):
        self.app = app
        self.name = name
        self.fault = fault

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http" or scope.get("path") != "/":
            await self.app(scope, receive, send)
            return
        # 先读完请求体，保证调用计数与真实“服务端收到后出故障”一致。
        buffered: dict[str, bytes | None] = {"body": await _drain_body(receive)}
        if self.fault.should_break_connection():
            logger.info("node=%s 注入连接重置（响应中途异常断连）", self.name)
            # 先发出 chunked 响应头，再以异常终止 ASGI 应用：
            # uvicorn 来不及发送合法 chunk 与结束帧，客户端读取到
            # 截断/非法的 HTTP 响应，稳定得到 RemoteProtocolError。
            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"application/json")],
            })
            raise ConnectionResetError(f"node {self.name}: injected connection reset")
        if self.fault.always_timeout:
            logger.info("node=%s 注入超时（读完请求后挂起）", self.name)
            await asyncio.sleep(30)
            return

        async def replay_receive():
            if buffered["body"] is None:
                return await receive()
            data = buffered["body"]
            buffered["body"] = None
            return {"type": "http.request", "body": data, "more_body": False}

        await self.app(scope, replay_receive, send)


async def _drain_body(receive) -> bytes:
    chunks = []
    while True:
        message = await receive()
        if message["type"] == "http.disconnect":
            break
        chunks.append(message.get("body", b""))
        if not message.get("more_body"):
            break
    return b"".join(chunks)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class FakeUpstreamServer:
    """以真实 TCP 端口运行假节点，用于模拟连接失败/超时。"""

    def __init__(self, chain: FakeChain, name: str, fault: FaultConfig):
        self._chain = chain
        self._name = name
        self._fault = fault
        self._port = _free_port()
        self._server: uvicorn.Server | None = None
        self._thread: threading.Thread | None = None

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._port}/"

    @property
    def port(self) -> int:
        return self._port

    def start(self) -> None:
        app = create_fake_app(self._chain, self._name, self._fault)
        config = uvicorn.Config(
            app, host="127.0.0.1", port=self._port,
            log_level="error", lifespan="off",
        )
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(
            target=self._server.run, kwargs={"sockets": None}, daemon=True
        )
        self._thread.start()
        self._wait_ready(timeout=5.0)

    def _wait_ready(self, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with contextlib.suppress(OSError), socket.create_connection(
                ("127.0.0.1", self._port), timeout=0.2
            ):
                return
            time.sleep(0.01)
        raise RuntimeError(f"fake upstream {self._name} did not start")

    def stop(self) -> None:
        if self._server is not None:
            self._server.should_exit = True
        if self._thread is not None:
            self._thread.join(timeout=3)

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.stop()
