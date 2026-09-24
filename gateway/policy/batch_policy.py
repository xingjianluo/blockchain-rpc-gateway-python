"""批量请求约束：数量上限与单项畸形检测。

- 整批超限：抛 BatchTooLarge（-32011），拒绝整批（协议级资源保护）；
- 单项畸形：validate_item 抛 MalformedRequest（-32600），仅该项产生独立
  error 响应（id 尽力提取），不影响兄弟项，整批不失败、不缺项。
"""
from __future__ import annotations

from typing import Any

from gateway.errors import BatchTooLarge, MalformedRequest

DEFAULT_MAX_BATCH = 100


class BatchPolicy:
    def __init__(self, max_batch: int = DEFAULT_MAX_BATCH) -> None:
        if max_batch <= 0:
            raise ValueError("max_batch must be positive")
        self.max_batch = max_batch

    def validate_size(self, n: int) -> None:
        if n > self.max_batch:
            raise BatchTooLarge(
                f"batch size {n} exceeds limit {self.max_batch}",
                data={"max_batch": self.max_batch, "actual": n},
            )

    def validate_item(self, item: Any, index: int) -> None:
        if not isinstance(item, dict):
            raise MalformedRequest(
                f"batch item #{index} is not an object",
                data={"index": index},
            )
        if item.get("jsonrpc") not in (None, "2.0"):
            raise MalformedRequest(
                f"batch item #{index} has unsupported jsonrpc version",
                data={"index": index},
            )
        method = item.get("method")
        if not isinstance(method, str) or not method:
            raise MalformedRequest(
                f"batch item #{index} missing string 'method'",
                data={"index": index},
            )
        params = item.get("params", [])
        if not isinstance(params, (list, dict)):
            raise MalformedRequest(
                f"batch item #{index} 'params' must be array or object",
                data={"index": index},
            )
