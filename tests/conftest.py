"""pytest 公共夹具：本地多假上游网关环境。

单测日志通过 caplog 打印三类关键信息：
- 所选上游（upstream=... SUCCESS / FAILED）
- 缓存命中（cache HIT/MISS）
- 重试原因（transport FAILED ... retryable）
"""
from __future__ import annotations

import logging

import pytest

from gateway.app import GatewayRuntime
from gateway.config import GatewayConfig
from gateway.models import NodeInfo
from gateway.upstream.fake_node import FakeNode


class FakeClock:
    """确定性单调钟：默认冻结在 1000.0，测试可手动推进。"""

    def __init__(self, start: float = 1000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


@pytest.fixture
def env():
    """构建含 3 个内存假节点的网关运行时（共享同一条链）。"""
    config = GatewayConfig(
        nodes=[
            NodeInfo("node-a", "local://a"),
            NodeInfo("node-b", "local://b"),
            NodeInfo("node-c", "local://c"),
        ],
        timeout=0.5,
        cooldown=10.0,
        forbidden_methods={"eth_sendRawTransaction", "miner_start"},
        max_batch=5,
    )
    rt = GatewayRuntime(config)
    clock = FakeClock()
    # 替换共享链上默认 transport，注入差异化延迟
    chain = rt.chain
    rt.transports["node-a"] = FakeNode(
        config.nodes[0], chain, latency=0.001, jitter=0.0
    )
    rt.transports["node-b"] = FakeNode(
        config.nodes[1], chain, latency=0.02, jitter=0.0
    )
    rt.transports["node-c"] = FakeNode(
        config.nodes[2], chain, latency=0.05, jitter=0.0
    )
    rt.dispatcher._clock = clock
    rt.clock = clock
    yield rt


@pytest.fixture(autouse=True)
def _gateway_logs(caplog):
    """让 gateway.* 日志在 INFO 级别被捕获并实时打印到 stdout。

    覆盖三类关键决策：所选上游、缓存命中、重试原因。
    使用 ``uv run pytest -s`` 可直接观察，满足“单测日志可解释”要求。
    """
    import sys

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("TEST-LOG %(name)s | %(message)s"))
    root = logging.getLogger("gateway")
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    caplog.set_level(logging.INFO, logger="gateway")
    yield
    root.removeHandler(handler)
