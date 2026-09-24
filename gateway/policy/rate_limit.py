"""每客户端令牌桶限流（默认不启用，rate<=0 表示关闭）。"""
from __future__ import annotations

import time


class TokenBucket:
    def __init__(self, rate: float = 0.0, burst: float = 0.0) -> None:
        self.rate = rate
        self.burst = burst
        self._tokens: dict[str, float] = {}
        self._last: dict[str, float] = {}

    def allow(self, key: str, now: float | None = None) -> bool:
        if self.rate <= 0:
            return True
        now = time.monotonic() if now is None else now
        last = self._last.get(key, now)
        tokens = min(
            self.burst, self._tokens.get(key, self.burst) + (now - last) * self.rate
        )
        self._last[key] = now
        if tokens >= 1.0:
            self._tokens[key] = tokens - 1.0
            return True
        self._tokens[key] = tokens
        return False
