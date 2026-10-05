"""共享 fixtures：假链、多假节点集群、网关 TestClient。"""
from __future__ import annotations

import asyncio
import logging

import pytest
from fastapi.testclient import TestClient

from gateway.app import create_app
from gateway.dispatcher import Dispatcher
from gateway.fakechain import FakeChain
from gateway.fakeupstream import FakeUpstreamServer, FaultConfig
from gateway.models import UpstreamNode
from gateway.policy import MethodPolicy

# 打印测试日志（所选上游/缓存命中/重试原因均通过 logging 输出）。
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
)


@pytest.fixture
def chain() -> FakeChain:
    return FakeChain(seed=20260924, initial_blocks=12, finalized_offset=4)


@pytest.fixture
def cluster(chain):
    """启动 3 个真实 TCP 假节点，测试结束后停止。"""
    servers = [
        FakeUpstreamServer(chain, "node-a", FaultConfig(latency_ms=4, seed=11)),
        FakeUpstreamServer(chain, "node-b", FaultConfig(latency_ms=4, seed=22)),
        FakeUpstreamServer(chain, "node-c", FaultConfig(latency_ms=4, seed=33)),
    ]
    for s in servers:
        s.start()
    yield servers
    for s in servers:
        s.stop()


@pytest.fixture
def nodes(cluster) -> list[UpstreamNode]:
    return [UpstreamNode(name=f"node-{c}", url=s.url)
            for c, s in zip(("a", "b", "c"), cluster)]


@pytest.fixture
def policy() -> MethodPolicy:
    return MethodPolicy(max_batch_size=20)


@pytest.fixture
def gateway_factory(nodes, chain, policy):
    """工厂 fixture：允许测试自定义节点子集/故障画像后构造网关。"""
    created: list[Dispatcher] = []

    def _factory(node_list=None, pol: MethodPolicy | None = None,
                 timeout: float = 2.0,
                 wait_poll_interval: float = 0.05) -> tuple:
        dispatcher = Dispatcher(
            node_list or nodes, chain, pol or policy, timeout=timeout,
            wait_poll_interval=wait_poll_interval,
        )
        created.append(dispatcher)
        app = create_app(chain=chain, policy=pol or policy,
                         dispatcher=dispatcher)
        return TestClient(app), dispatcher

    yield _factory
    for d in created:
        try:
            asyncio.run(d.aclose())
        except RuntimeError:
            pass


@pytest.fixture
def gateway(gateway_factory):
    client, _ = gateway_factory()
    return client


@pytest.fixture
async def async_gateway(gateway_factory):
    """同一事件循环内的异步 client，用于检验 single-flight 真实合并。"""
    import httpx
    from httpx import ASGITransport

    client, dispatcher = gateway_factory()
    transport = ASGITransport(app=client.app)
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://gw") as ac:
        yield ac, dispatcher
    await dispatcher.aclose()
