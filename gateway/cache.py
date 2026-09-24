"""按最终性划分的缓存 + 并发相同只读请求合并 (single-flight)。

缓存规则：
- 仅只读方法且其引用区块已最终确定（tag=finalized 或显式高度
  <= finalized_number）才可缓存；latest/pending/未最终高度一律透传；
- 缓存项绑定抓取时刻的 (block_number, block_hash)；
- on_new_head 逐条校验：高度已被回滚（超出当前链顶）或哈希发生
  变化（重组）则失效，绝不返回过期结果。
"""
from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from gateway.fakechain import FakeChain
from gateway.models import RpcRequest

# 允许进入最终性缓存的只读方法（其余一律不缓存）。
CACHEABLE_METHODS = frozenset({
    "eth_getBlockByNumber",
    "eth_getBalance",
})
_FINAL_TAGS = frozenset({"finalized"})
_NON_CACHEABLE_TAGS = frozenset({"latest", "pending", "earliest", "safe"})


@dataclass
class CacheEntry:
    value: Any
    block_number: int
    block_hash: str


# 各方法中区块参数的位置（位置参数形态）。
_BLOCK_PARAM_INDEX = {
    "eth_getBlockByNumber": 0,
    "eth_getBalance": 1,
}


def _block_arg(req: RpcRequest) -> Any:
    """取出请求中的区块参数（位置/具名两种形态）。"""
    if isinstance(req.params, dict):
        return req.params.get("blockTag") or req.params.get("blockNumber")
    if isinstance(req.params, list):
        idx = _BLOCK_PARAM_INDEX.get(req.method)
        if idx is not None and idx < len(req.params):
            return req.params[idx]
    return None


def _hex_to_int(value: Any) -> int | None:
    if isinstance(value, str) and value.startswith("0x"):
        try:
            return int(value, 16)
        except ValueError:
            return None
    return None


def referenced_block_number(
    req: RpcRequest, chain: FakeChain
) -> tuple[int, str] | None:
    """解析请求引用的区块 (高度, 规范化标签)；不可缓存返回 None。"""
    if req.method not in CACHEABLE_METHODS:
        return None
    arg = _block_arg(req)
    if not isinstance(arg, str):
        return None
    if arg in _FINAL_TAGS:
        return chain.finalized_number(), "finalized"
    if arg in _NON_CACHEABLE_TAGS:
        return None
    height = _hex_to_int(arg)
    if height is None or height > chain.finalized_number():
        return None  # 显式高度但尚未最终确定 -> 不缓存
    return height, hex(height)


def cache_key(req: RpcRequest, chain: FakeChain) -> str | None:
    """返回可缓存请求的稳定键；None 表示该请求不可缓存。"""
    resolved = referenced_block_number(req, chain)
    if resolved is None:
        return None
    height, _tag = resolved
    # 键绑定到解析后的最终高度：finalized 推进后自然形成新键。
    suffix = json.dumps(req.params, sort_keys=True, separators=(",", ":"))
    return f"{req.method}:{height}:{suffix}"


class FinalityCache:
    def __init__(self, chain: FakeChain):
        self._chain = chain
        self._store: dict[str, CacheEntry] = {}
        self.hits = 0
        self.misses = 0

    def get(self, key: str) -> CacheEntry | None:
        entry = self._store.get(key)
        if entry is None:
            self.misses += 1
            return None
        # 防御式校验：即便未触发 on_new_head 也不返回过期数据。
        current_hash = self._chain.block_hash_at(entry.block_number)
        if current_hash is None or current_hash != entry.block_hash:
            self._store.pop(key, None)
            self.misses += 1
            return None
        self.hits += 1
        return entry

    def put(self, key: str, entry: CacheEntry) -> None:
        self._store[key] = entry

    def on_new_head(self) -> int:
        """链推进/重组后失效过期项，返回被失效的条数。"""
        stale: list[str] = []
        for key, entry in self._store.items():
            current_hash = self._chain.block_hash_at(entry.block_number)
            if current_hash is None:
                stale.append(key)  # 高度已被回滚
            elif current_hash != entry.block_hash:
                stale.append(key)  # 同高度哈希变化 -> 重组
        for key in stale:
            self._store.pop(key, None)
        return len(stale)

    def __len__(self) -> int:
        return len(self._store)


class SingleFlight:
    """同一时刻相同键的只读请求只产生一次上游调用，其余等待共享结果。"""

    def __init__(self) -> None:
        self._inflight: dict[str, asyncio.Future[Any]] = {}

    async def run(
        self, key: str, producer: Callable[[], Awaitable[Any]]
    ) -> Any:
        existing = self._inflight.get(key)
        if existing is not None:
            return await existing  # 合并到进行中的同键调用
        future: asyncio.Future[Any] = asyncio.get_event_loop().create_future()
        self._inflight[key] = future
        try:
            value = await producer()
            future.set_result(value)
            return value
        except BaseException as exc:
            if not future.done():
                future.set_exception(exc)
            raise
        finally:
            self._inflight.pop(key, None)

    @property
    def inflight_count(self) -> int:
        return len(self._inflight)
