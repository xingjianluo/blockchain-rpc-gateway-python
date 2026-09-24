"""真实 HTTP 传输链路测试：进程内启动假节点服务，经 HttpTransport 访问。

验证 HTTP 层的连接拒绝/业务错误分类，与内存假节点走同一套重试判定。
"""
from __future__ import annotations

import asyncio
import socket

import httpx
import pytest
import uvicorn

from gateway.app import GatewayRuntime
from gateway.cli_fake_server import build_fake_app
from gateway.config import GatewayConfig
from gateway.errors import AllUpstreamsFailed
from gateway.models import NodeInfo, RpcRequest
from gateway.upstream.http_transport import HttpTransport


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


async def _no_finalized():
    return -1


async def _wait_port(port: int) -> None:
    for _ in range(100):
        try:
            _reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.close()
            await writer.wait_closed()
            return
        except OSError:
            await asyncio.sleep(0.02)
    raise RuntimeError("fake server did not start")


@pytest.fixture
async def http_env():
    port = _free_port()
    server = uvicorn.Server(uvicorn.Config(
        build_fake_app("http-node"),
        host="127.0.0.1", port=port, log_level="error",
    ))
    task = asyncio.ensure_future(server.serve())
    await _wait_port(port)

    cfg = GatewayConfig(
        nodes=[
            NodeInfo("dead", f"http://127.0.0.1:{_free_port()}"),  # 无监听
            NodeInfo("live", f"http://127.0.0.1:{port}"),
        ],
        use_fake_upstreams=False, timeout=2.0,
    )
    rt = GatewayRuntime(cfg)
    client = httpx.AsyncClient()
    rt.transports["dead"] = HttpTransport(cfg.nodes[0], client)
    rt.transports["live"] = HttpTransport(cfg.nodes[1], client)
    rt.dispatcher._transports = rt.transports
    rt.dispatcher._finalized_probe = _no_finalized
    rt.dispatcher._last_finalized = -1
    yield rt
    await client.aclose()
    server.should_exit = True
    await task


async def test_http_failover_from_dead_to_live(http_env):
    resp = await http_env.dispatcher.dispatch(
        RpcRequest("eth_blockNumber", [], 1)
    )
    assert resp.error is None
    assert resp.upstream == "live"


async def test_http_business_error_not_retried(http_env):
    resp = await http_env.dispatcher.dispatch(
        RpcRequest("eth_does_not_exist", [], 2)
    )
    assert resp.is_error and resp.error["code"] == -32601


async def test_http_all_dead_raises_all_upstreams_failed():
    cfg = GatewayConfig(nodes=[
        NodeInfo("d1", f"http://127.0.0.1:{_free_port()}"),
        NodeInfo("d2", f"http://127.0.0.1:{_free_port()}"),
    ], use_fake_upstreams=False)
    rt = GatewayRuntime(cfg)
    client = httpx.AsyncClient()
    rt.transports["d1"] = HttpTransport(cfg.nodes[0], client)
    rt.transports["d2"] = HttpTransport(cfg.nodes[1], client)
    rt.dispatcher._transports = rt.transports
    rt.dispatcher._finalized_probe = _no_finalized
    rt.dispatcher._last_finalized = -1
    try:
        with pytest.raises(AllUpstreamsFailed) as exc:
            await rt.dispatcher.dispatch(RpcRequest("eth_blockNumber", [], 3))
        assert exc.value.code == -32000
    finally:
        await client.aclose()
