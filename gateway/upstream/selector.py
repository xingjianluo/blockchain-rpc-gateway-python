"""节点选择：按健康度与响应延迟排序。"""
from __future__ import annotations

from gateway.upstream.health import NodeHealth


class NodeSelector:
    """给定当前时间，输出节点尝试顺序。

    排序依据为 NodeHealth.score（平均延迟 + 连续失败惩罚，熔断节点不可选）；
    评分相同时按节点名稳定排序，保证选择结果可复现。
    """

    def __init__(self, health: dict[str, NodeHealth]) -> None:
        self._health = health

    def ordered(self, now: float) -> list[str]:
        candidates = [
            (name, h) for name, h in self._health.items() if h.available(now)
        ]
        candidates.sort(key=lambda kv: (-kv[1].score(now), kv[0]))
        return [name for name, _ in candidates]
