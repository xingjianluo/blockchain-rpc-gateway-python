"""上游节点选择：按健康度与响应延迟在可用节点间排序。

策略：
- 不健康节点（连续失败达到阈值）排除在候选之外；
- 健康节点按 score = 延迟EWMA + 失败惩罚 升序；
- 分数相同按轮转序号打破，避免所有流量压在同一节点。
"""
from __future__ import annotations

import itertools

from gateway.models import UpstreamNode

# 连续失败达到该阈值即摘除，后续可由探测恢复（本实现保留在列表末尾）。
_UNHEALTHY_THRESHOLD = 3
_EWMA_ALPHA = 0.4


class NodeSelector:
    def __init__(self, nodes: list[UpstreamNode]):
        self._nodes = list(nodes)
        self._tie = itertools.count()

    def ordered_nodes(self, exclude: set[str] | None = None) -> list[UpstreamNode]:
        """返回按选择优先级排序的节点，exclude 中的节点本次跳过。"""
        exclude = exclude or set()
        candidates = [n for n in self._nodes if n.name not in exclude]
        return sorted(
            candidates,
            key=lambda n: (
                n.score(),
                # 同分时稳定轮转：用名称顺序 + 当前轮转计数取模
                (self._nodes.index(n) + next(self._tie)) % len(self._nodes),
            ),
        )

    def record_success(self, name: str, latency_ms: float) -> None:
        node = self._find(name)
        if node is None:
            return
        node.consecutive_failures = 0
        node.healthy = True
        if node.latency_ewma_ms == 0.0:
            node.latency_ewma_ms = latency_ms
        else:
            node.latency_ewma_ms = (
                _EWMA_ALPHA * latency_ms + (1 - _EWMA_ALPHA) * node.latency_ewma_ms
            )

    def record_failure(self, name: str) -> None:
        node = self._find(name)
        if node is None:
            return
        node.consecutive_failures += 1
        if node.consecutive_failures >= _UNHEALTHY_THRESHOLD:
            node.healthy = False

    def _find(self, name: str) -> UpstreamNode | None:
        for n in self._nodes:
            if n.name == name:
                return n
        return None
