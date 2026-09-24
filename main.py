"""本地启动：3 个假上游节点（含故障注入）+ JSON-RPC 网关。

启动方式：uv run fastapi dev main.py
- 网关:        http://127.0.0.1:8000/
- 假节点:      127.0.0.1 的独立端口（启动日志打印）
- 故障画像:    node-a 正常+抖动；node-b 前 2 次连接故障后恢复；
              node-c 固定高延迟，用于验证按延迟/健康度选择。
"""
from __future__ import annotations

import atexit
import logging

from gateway.app import create_app
from gateway.fakechain import FakeChain
from gateway.fakeupstream import FakeUpstreamServer, FaultConfig
from gateway.models import UpstreamNode
from gateway.policy import MethodPolicy

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
)

_chain = FakeChain(seed=20260924, initial_blocks=12, finalized_offset=4)

_FAULTS = {
    "node-a": FaultConfig(latency_ms=8, jitter_ms=4, seed=11),
    "node-b": FaultConfig(fail_first=2, latency_ms=5, seed=22),
    "node-c": FaultConfig(latency_ms=120, jitter_ms=30, seed=33),
}

_servers: list[FakeUpstreamServer] = []
for _name, _fault in _FAULTS.items():
    _server = FakeUpstreamServer(_chain, _name, _fault)
    _server.start()
    _servers.append(_server)
    logging.getLogger("main").info("假上游 %s -> %s", _name, _server.url)

atexit.register(lambda: [s.stop() for s in _servers])

_nodes = [UpstreamNode(name=n, url=s.url) for n, s in zip(_FAULTS, _servers)]

app = create_app(
    nodes=_nodes,
    chain=_chain,
    policy=MethodPolicy(max_batch_size=20),
    timeout=1.0,
)
