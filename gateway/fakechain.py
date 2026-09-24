"""本地假链：确定性区块数据 + 链重组模拟。

设计目标：
- 所有哈希/余额由 (种子, epoch, 高度, 账户) 确定性推导，保证测试可复现；
- reorg 回滚后以新 epoch 重新出块，相同高度的哈希与余额随之变化，
  从而能真实检验缓存是否在重组后失效；
- 回滚点之前的区块 epoch 不变，哈希保持稳定。
"""
from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass, field
from typing import Any

_GENESIS_TS = 1_700_000_000
_BLOCK_INTERVAL = 2


def _deterministic_hash(*parts: object) -> str:
    raw = "|".join(str(p) for p in parts).encode()
    return "0x" + hashlib.sha256(raw).hexdigest()[:40]


@dataclass
class Block:
    number: int
    block_hash: str
    parent_hash: str
    timestamp: int
    epoch: int = 0


@dataclass
class FakeChain:
    """所有假节点共享的链状态；可确定性推进与重组。"""

    seed: int = 1
    finalized_offset: int = 4
    initial_blocks: int = 8
    blocks: list[Block] = field(default_factory=list)
    rng: random.Random = field(default_factory=random.Random)
    epoch: int = 0

    def __post_init__(self) -> None:
        self.rng = random.Random(self.seed)
        if not self.blocks:
            self.mine(self.initial_blocks)

    # ---- 基础查询 ----
    def head_number(self) -> int:
        return self.blocks[-1].number

    def finalized_number(self) -> int:
        return max(0, self.head_number() - self.finalized_offset)

    def _has(self, number: int) -> bool:
        return 0 <= number <= self.head_number()

    def block_hash_at(self, number: int) -> str | None:
        if not self._has(number):
            return None
        return self.blocks[number].block_hash

    def block_at(self, number: int) -> dict[str, Any] | None:
        if not self._has(number):
            return None
        b = self.blocks[number]
        return {
            "number": hex(b.number),
            "hash": b.block_hash,
            "parentHash": b.parent_hash,
            "timestamp": hex(b.timestamp),
        }

    def balance_at(self, number: int, account: str) -> int | None:
        if not self._has(number):
            return None
        # 余额依赖当前高度区块哈希，重组后同高度余额会变化。
        digest = _deterministic_hash(
            "balance", self.seed, self.blocks[number].epoch,
            self.blocks[number].block_hash, account.lower(),
        )
        return int(digest[2:18], 16)

    # ---- 状态推进 ----
    def _make_block(self, number: int) -> Block:
        parent = self.blocks[-1].block_hash if self.blocks else "0x" + "0" * 40
        return Block(
            number=number,
            block_hash=_deterministic_hash("block", self.seed, self.epoch, number),
            parent_hash=parent,
            timestamp=_GENESIS_TS + number * _BLOCK_INTERVAL,
            epoch=self.epoch,
        )

    def mine(self, count: int = 1) -> None:
        """在链顶追加 count 个区块。"""
        for _ in range(max(0, count)):
            number = len(self.blocks)
            self.blocks.append(self._make_block(number))

    def rollback(self, count: int) -> int:
        """直接截断链顶 count 个区块（模拟高度被回滚、不再重出）。

        返回回滚后的链顶高度。用于检验“已回滚高度不得返回过期结果”。
        """
        count = min(max(0, count), len(self.blocks))
        if count:
            del self.blocks[-count:]
        return self.head_number()

    def reorg(self, depth: int, rng_seed: int | None = None) -> int:
        """回滚 depth 个区块后以新 epoch 重新挖出，制造链重组。

        返回回滚后链顶高度。已最终确定的区块不允许被重组
        （与 finalized 语义一致），depth 会自动收敛到安全范围。
        """
        max_depth = max(0, self.head_number() - self.finalized_number())
        depth = min(max(0, depth), max_depth)
        if depth:
            del self.blocks[-depth:]
            self.epoch += 1 if rng_seed is None else rng_seed
            self.mine(depth)
        return self.head_number()
