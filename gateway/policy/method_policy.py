"""方法级访问策略。

- forbidden 中的方法在网关侧直接拒绝（错误码 -32010），绝不转发上游；
- allowed 非空时为白名单，只有其中方法可转发；
- 白名单外但不在 forbidden 的未知方法默认放行，交由上游按 -32601 判定，
  避免网关与上游的方法集不一致而掩盖真实业务错误。
"""
from __future__ import annotations

from dataclasses import dataclass, field

from gateway.errors import MethodForbidden


@dataclass
class MethodPolicy:
    allowed: set[str] | None = None
    forbidden: set[str] = field(default_factory=set)
    unknown_is_allowed: bool = True

    def check(self, method: str) -> None:
        if method in self.forbidden:
            raise MethodForbidden(
                f"method {method!r} is forbidden by gateway policy"
            )
        if self.allowed is not None and method not in self.allowed:
            if self.unknown_is_allowed:
                # 放行让上游给 -32601，保持业务错误语义
                return
            raise MethodForbidden(f"method {method!r} not in allow-list")
