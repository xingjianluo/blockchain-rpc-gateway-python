"""方法级访问策略与批量约束。

- denied 集合中的方法在网关侧直接返回 -32001，不占用上游调用；
- allowed 非空时采用白名单语义：不在白名单也不在黑名单的方法拒绝，
  allowed 为空时仅黑名单生效（默认转发未知方法，由上游判定 -32601）；
- 批量请求条数超过 max_batch_size 返回 -32010。
"""
from __future__ import annotations

from dataclasses import dataclass, field

# 默认受限方法：写操作 / 管理接口不得通过只读网关转发。
DEFAULT_DENIED: frozenset[str] = frozenset({
    "eth_sendRawTransaction",
    "eth_sendTransaction",
    "eth_accounts",
    "personal_sendTransaction",
    "miner_start",
    "admin_nodeInfo",
})


@dataclass
class MethodPolicy:
    allowed: set[str] = field(default_factory=set)
    denied: set[str] = field(default_factory=lambda: set(DEFAULT_DENIED))
    max_batch_size: int = 100

    def is_forbidden(self, method: str) -> bool:
        """受限方法在网关侧直接拒绝，不转发上游。"""
        if method in self.denied:
            return True
        return bool(self.allowed and method not in self.allowed)
