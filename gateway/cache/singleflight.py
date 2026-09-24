"""请求合并（singleflight）：同一时刻相同 key 的只读请求共享一次上游调用。

- 首个调用创建 asyncio.Task 并登记；并发同 key 调用 await 同一个 Task；
- 调用完成（成功或业务异常）后清理登记，后续请求会重新发起；
- 业务异常同样传播给每个等待者，不会被合并逻辑吞掉或掩盖；
- merged_count 统计被合并的等待次数，供日志/测试打印。
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

T = TypeVar("T")


class SingleFlight:
    def __init__(self) -> None:
        self._inflight: dict[str, asyncio.Task[Any]] = {}
        self.merged_count = 0

    async def do(
        self, key: str, coro_factory: Callable[[], Awaitable[T]]
    ) -> T:
        existing = self._inflight.get(key)
        if existing is not None:
            self.merged_count += 1
            return await asyncio.shield(existing)

        task = asyncio.create_task(coro_factory())
        self._inflight[key] = task
        try:
            return await task
        finally:
            # 仅在仍是自己登记的任务时移除，避免竞态覆盖
            if self._inflight.get(key) is task:
                self._inflight.pop(key, None)
