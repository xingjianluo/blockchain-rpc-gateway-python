"""最终确定数据缓存，支持链重组失效。

规则：
- 仅写入被判定为 FINALIZED 且成功（无 error）的响应；
- 每条记录关联其读取高度与写入时间，TTL 到期惰性淘汰；
- 链重组时调用 invalidate_reorg(fork_height-1)：凡关联高度 > 该值的
  条目一律失效，确保已回滚高度绝不返回过期结果；
- 命中/未命中与失效条数均通过 stats 暴露，供日志与测试打印。
"""
from __future__ import annotations

from dataclasses import dataclass

from gateway.models import Finality, RpcRequest, RpcResponse


def _canonical_params(params) -> str:
    import json
    return json.dumps(params, sort_keys=True, separators=(",", ":"))


def request_key(req: RpcRequest) -> str:
    """规范化的只读请求键（方法 + 参数），缓存与请求合并共用。"""
    return f"{req.method}:{_canonical_params(req.params)}"


@dataclass
class CacheEntry:
    value: RpcResponse
    height: int | None
    finalized_at: float


class FinalityCache:
    def __init__(self, *, ttl: float = 300.0) -> None:
        self._ttl = ttl
        self._store: dict[str, CacheEntry] = {}
        self.hits = 0
        self.misses = 0
        self.invalidations = 0

    def _key(self, req: RpcRequest) -> str:
        return request_key(req)

    def get(self, req: RpcRequest, now: float) -> RpcResponse | None:
        key = self._key(req)
        entry = self._store.get(key)
        if entry is None:
            self.misses += 1
            return None
        if now - entry.finalized_at > self._ttl:
            self._store.pop(key, None)
            self.misses += 1
            return None
        self.hits += 1
        return entry.value

    def is_safe(self, req: RpcRequest, finalized_height: int) -> bool:
        """该 key 的缓存条目关联高度是否仍 <= 当前最终高度。

        按哈希内容寻址（height=None）始终安全；高度未知（-1，尚无探测结果）
        时保守放行，交由重组失效逻辑在下一请求兜底。
        """
        if finalized_height < 0:
            return True
        entry = self._store.get(self._key(req))
        if entry is None:
            return False
        if entry.height is None:
            return True
        return entry.height <= finalized_height

    def put(self, req: RpcRequest, resp: RpcResponse, finality: Finality,
            height: int | None, now: float) -> None:
        # 未最终确定的数据、错误响应一律不缓存
        if finality is not Finality.FINALIZED or resp.is_error:
            return
        self._store[self._key(req)] = CacheEntry(
            value=resp, height=height, finalized_at=now
        )

    def evict(self, req: RpcRequest) -> None:
        self._store.pop(self._key(req), None)

    def invalidate_reorg(self, rolled_back_to: int, now: float) -> int:
        """失效所有关联高度 > rolled_back_to 的条目，返回失效条数。"""
        doomed = [
            k for k, e in self._store.items()
            if e.height is not None and e.height > rolled_back_to
        ]
        for k in doomed:
            self._store.pop(k, None)
        self.invalidations += len(doomed)
        return len(doomed)

    def __len__(self) -> int:
        return len(self._store)
