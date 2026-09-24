"""网关配置：节点、缓存、策略参数。

可从 JSON 文件加载，未提供时返回本地演示配置（三个假上游节点）。
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from gateway.models import NodeInfo


@dataclass
class GatewayConfig:
    nodes: list[NodeInfo] = field(default_factory=list)
    timeout: float = 2.0
    cache_ttl: float = 300.0
    forbidden_methods: set[str] = field(default_factory=set)
    allowed_methods: set[str] | None = None
    max_batch: int = 100
    use_fake_upstreams: bool = True
    cooldown: float = 1.0
    host: str = "127.0.0.1"
    port: int = 8000

    @staticmethod
    def demo() -> GatewayConfig:
        return GatewayConfig(
            nodes=[
                NodeInfo("node-a", "http://127.0.0.1:9101"),
                NodeInfo("node-b", "http://127.0.0.1:9102"),
                NodeInfo("node-c", "http://127.0.0.1:9103"),
            ],
            forbidden_methods={
                # 写/管理类方法默认在网关侧禁止，绝不转发上游
                "eth_sendRawTransaction",
                "eth_sendTransaction",
                "miner_start",
                "miner_stop",
                "admin_nodeInfo",
            },
        )

    @staticmethod
    def load(path: str | Path | None = None) -> GatewayConfig:
        if path is None:
            return GatewayConfig.demo()
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        nodes = [
            NodeInfo(
                name=n["name"],
                url=n["url"],
                weight=float(n.get("weight", 1.0)),
            )
            for n in data.get("nodes", [])
        ]
        allowed = data.get("allowed_methods")
        return GatewayConfig(
            nodes=nodes,
            timeout=float(data.get("timeout", 2.0)),
            cache_ttl=float(data.get("cache_ttl", 300.0)),
            forbidden_methods=set(data.get("forbidden_methods", [])),
            allowed_methods=set(allowed) if allowed is not None else None,
            max_batch=int(data.get("max_batch", 100)),
            use_fake_upstreams=bool(data.get("use_fake_upstreams", True)),
            cooldown=float(data.get("cooldown", 1.0)),
            host=str(data.get("host", "127.0.0.1")),
            port=int(data.get("port", 8000)),
        )
