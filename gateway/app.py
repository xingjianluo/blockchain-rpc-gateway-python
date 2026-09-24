"""网关组装根（依赖注入入口）。

默认构建本地演示环境：3 个共享同一条假链的内存上游节点，
可通过返回对象上的 chain / transports / dispatcher 注入故障与链重组。
"""
from __future__ import annotations

import logging

from fastapi import FastAPI

from gateway.api.routes import build_router
from gateway.cache.singleflight import SingleFlight
from gateway.cache.store import FinalityCache
from gateway.config import GatewayConfig
from gateway.engine.batch import BatchExecutor
from gateway.engine.dispatcher import Dispatcher
from gateway.policy.batch_policy import BatchPolicy
from gateway.policy.method_policy import MethodPolicy
from gateway.upstream.fake_node import FakeChainState, FakeNode
from gateway.upstream.health import NodeHealth
from gateway.upstream.http_transport import HttpTransport
from gateway.upstream.selector import NodeSelector

logger = logging.getLogger("gateway")


class GatewayRuntime:
    """组装后的运行时句柄，供测试注入故障/重组并访问观测计数。"""

    def __init__(self, config: GatewayConfig) -> None:
        self.config = config
        self.chain = FakeChainState(8)
        self.transports: dict = {}
        self.health: dict[str, NodeHealth] = {}

        for node in config.nodes:
            if config.use_fake_upstreams:
                transport = FakeNode(node, self.chain)
            else:
                transport = HttpTransport(node)
            self.transports[node.name] = transport
            self.health[node.name] = NodeHealth(node)

        self.cache = FinalityCache(ttl=config.cache_ttl)
        self.flights = SingleFlight()
        self.selector = NodeSelector(self.health)
        self.policy = MethodPolicy(forbidden=set(config.forbidden_methods),
                                   allowed=config.allowed_methods)
        self.batch_policy = BatchPolicy(max_batch=config.max_batch)

        async def probe_finalized() -> int:
            return self.chain.finalized_height

        self.dispatcher = Dispatcher(
            self.transports, self.selector, self.cache, self.flights,
            self.policy, self.health,
            timeout=config.timeout, cooldown=config.cooldown,
            finalized_probe=probe_finalized,
        )
        self.executor = BatchExecutor(self.dispatcher, self.batch_policy)


def create_app(config: GatewayConfig | None = None,
               *, runtime: GatewayRuntime | None = None) -> FastAPI:
    config = config or GatewayConfig.demo()
    rt = runtime or GatewayRuntime(config)
    rt.app = FastAPI(title="blockchain-jsonrpc-gateway")
    rt.app.state.runtime = rt
    rt.app.include_router(build_router(rt.executor))
    return rt.app
