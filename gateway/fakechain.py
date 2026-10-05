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
    transactions: list[str] = field(default_factory=list)


@dataclass
class FakeChain:
    """所有假节点共享的链状态；可确定性推进与重组。"""

    seed: int = 1
    finalized_offset: int = 4
    initial_blocks: int = 8
    blocks: list[Block] = field(default_factory=list)
    rng: random.Random = field(default_factory=random.Random)
    epoch: int = 0
    # 交易池：尚未打包的交易标识；已上链交易通过所在区块反查。
    pending: set[str] = field(default_factory=set)
    # 预置交易：在初始化出块时就打进指定高度（1-based 相对首个区块），
    # 仅用于本地/演示；为空时行为与原先完全一致。
    seeded_txs: dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.rng = random.Random(self.seed)
        if not self.blocks:
            self.mine(self.initial_blocks)
        # 预置交易落到已生成的区块上。
        for tx_hash, block_index in self.seeded_txs.items():
            if 0 <= block_index < len(self.blocks):
                txs = self.blocks[block_index].transactions
                if tx_hash not in txs:
                    txs.append(tx_hash)

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
            "transactions": list(b.transactions),
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

    # ---- 交易 ----
    def send_tx(self, tx_hash: str) -> None:
        """提交交易到交易池；已被重组丢弃的交易也可重新提交。"""
        self.pending.add(tx_hash)

    def locate_tx(self, tx_hash: str) -> tuple[int, str] | None:
        """返回交易当前所在 (高度, 区块哈希)；不在链上（仅在池中/未知）返回 None。"""
        for b in self.blocks:
            if tx_hash in b.transactions:
                return b.number, b.block_hash
        return None

    def receipt(self, tx_hash: str) -> dict[str, Any] | None:
        """构造 eth_getTransactionReceipt 形态结果；未上链返回 None（惯例）。"""
        located = self.locate_tx(tx_hash)
        if located is None:
            return None
        number, block_hash = located
        return {
            "transactionHash": tx_hash,
            "blockNumber": hex(number),
            "blockHash": block_hash,
            "confirmations": hex(self.head_number() - number + 1),
            "status": "0x1",
        }

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

    def mine(self, count: int = 1, pack_pending: bool = True) -> None:
        """在链顶追加 count 个区块。

        pack_pending=True 时，交易池中的全部交易确定性地打进第一个新区块
        （按标识排序，保证测试可复现），随后从交易池移除。
        """
        packed = False
        for _ in range(max(0, count)):
            number = len(self.blocks)
            block = self._make_block(number)
            if pack_pending and not packed and self.pending:
                block.transactions = sorted(self.pending)
                self.pending.clear()
                packed = True
            self.blocks.append(block)

    def rollback(self, count: int, dropped: set[str] | None = None) -> int:
        """直接截断链顶 count 个区块（模拟高度被回滚、不再重出）。

        返回回滚后的链顶高度。用于检验“已回滚高度不得返回过期结果”。
        被回滚区块里的交易默认退回交易池；出现在 dropped 中的交易不再回池，
        用于模拟“确认期间交易被重组且再未上链”。
        """
        count = min(max(0, count), len(self.blocks))
        dropped = dropped or set()
        if count:
            for b in self.blocks[-count:]:
                for tx in b.transactions:
                    if tx not in dropped:
                        self.pending.add(tx)
            del self.blocks[-count:]
        return self.head_number()

    def reorg(
        self,
        depth: int,
        rng_seed: int | None = None,
        dropped_txs: set[str] | None = None,
        extra: int = 0,
    ) -> int:
        """回滚 depth 个区块后以新 epoch 重新挖出，制造链重组。

        返回回滚后链顶高度。已最终确定的区块不允许被重组
        （与 finalized 语义一致），depth 会自动收敛到安全范围。

        被回滚区块中的交易默认退回交易池，后续 mine 才会重新打包；
        dropped_txs 中的交易被彻底丢弃（不再上链）；
        extra>0 时在重出 depth 块之后再追加 extra 个空块，
        可让重新打包的交易立刻获得更多确认。
        """
        dropped_txs = dropped_txs or set()
        max_depth = max(0, self.head_number() - self.finalized_number())
        depth = min(max(0, depth), max_depth)
        if depth:
            detached = [tx for b in self.blocks[-depth:]
                        for tx in b.transactions if tx not in dropped_txs]
            del self.blocks[-depth:]
            self.epoch += 1 if rng_seed is None else rng_seed
            # 先重出 depth 个空块（新 epoch -> 同高度哈希全部改变），
            # 退回池中的交易留给随后的 mine 打包，从而拿到“新位置”。
            self.mine(depth, pack_pending=False)
            for tx in detached:
                self.pending.add(tx)
            if extra:
                self.mine(extra)
        return self.head_number()
