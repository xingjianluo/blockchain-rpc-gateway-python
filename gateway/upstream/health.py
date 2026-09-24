"""节点健康度与响应延迟统计。

判定依据：
- 连续失败达到阈值后熔断一段时间（open_until 之前不可选），探测恢复后自动半开；
- 评分综合成功率与滑动窗口平均延迟，健康且更快的节点排序更靠前。
"""
from __future__ import annotations

from dataclasses import dataclass, field

from gateway.models import NodeInfo

_FAILURE_THRESHOLD = 3          # 连续失败次数达到该值即熔断
_LATENCY_WINDOW = 20            # 延迟滑动窗口大小
_PENALTY_PER_FAILURE = 50.0     # 每次连续失败折算的延迟惩罚（毫秒）


@dataclass
class NodeHealth:
    """单节点健康状态：连续失败计数、熔断截止时间、延迟窗口。"""

    node: NodeInfo
    consecutive_failures: int = 0
    open_until: float = 0.0
    latencies: list[float] = field(default_factory=list)

    def record_success(self, latency: float, now: float) -> None:
        self.consecutive_failures = 0
        self.open_until = 0.0
        self.latencies.append(latency)
        if len(self.latencies) > _LATENCY_WINDOW:
            self.latencies = self.latencies[-_LATENCY_WINDOW:]

    def record_failure(self, now: float, *, cooldown: float) -> None:
        self.consecutive_failures += 1
        if self.consecutive_failures >= _FAILURE_THRESHOLD:
            # 指数退避：1x、2x、4x ... cooldown
            factor = 2 ** (self.consecutive_failures - _FAILURE_THRESHOLD)
            self.open_until = now + cooldown * factor

    def available(self, now: float) -> bool:
        return now >= self.open_until

    def avg_latency(self, default: float = 1.0) -> float:
        if not self.latencies:
            return default
        return sum(self.latencies) / len(self.latencies)

    def score(self, now: float) -> float:
        """健康度评分，越高越优先。

        基础分为平均延迟（秒，越小越好），叠加连续失败惩罚；
        熔断中的节点返回负无穷（不可选）。
        """
        if not self.available(now):
            return float("-inf")
        penalty = self.consecutive_failures * _PENALTY_PER_FAILURE / 1000.0
        # 取负：延迟/惩罚越小，得分越高
        return -(self.avg_latency() + penalty)
