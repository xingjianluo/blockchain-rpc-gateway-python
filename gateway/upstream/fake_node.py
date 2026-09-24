"""本地假上游节点：模拟多节点、网络抖动与链重组。

设计要点：
- FakeChainState 是一条可被多节点共享/分叉的内存链，区块按高度存储，含哈希与父哈希；
-  finalized_height 表示已最终确定高度；reorg() 从分叉点替换规范链并产生新哈希；
- FakeNode 在链之上注入延迟、超时、连接拒绝等传输层行为，以及
  方法不存在/参数非法等业务错误，供网关区分两类失败。
"""
from __future__ import annotations

import asyncio
import hashlib
import time

from gateway.errors import (
    INVALID_PARAMS,
    METHOD_NOT_FOUND,
    UpstreamConnRefused,
    UpstreamTimeout,
)
from gateway.models import NodeInfo, RpcRequest, RpcResponse

_CONFIRMATIONS = 2  # head 落后该区块数即视为最终确定


def _hash(height: int, parent: str | None, payload: str = "") -> str:
    h = hashlib.sha256(f"{height}:{parent or 'GENESIS'}:{payload}".encode()).hexdigest()
    return "0x" + h[:40]


def _parse_height(tag: object) -> int | None:
    """把区块标签解析为高度；latest/pending/earliest 返回 None。"""
    if isinstance(tag, int):
        return tag
    if isinstance(tag, str):
        t = tag.lower()
        if t in ("latest", "pending", "earliest", "safe", "finalized"):
            return None
        if t.startswith("0x"):
            try:
                return int(t, 16)
            except ValueError:
                return None
        try:
            return int(t)
        except ValueError:
            return None
    return None


class FakeChainState:
    """假链状态：区块数据、最终高度、重组历史。"""

    def __init__(self, initial_blocks: int = 5) -> None:
        self.blocks: dict[int, dict] = {}
        self.finalized_height: int = 0
        for height in range(initial_blocks):
            self._append(height)
        self.finalized_height = max(0, self.head_height() - _CONFIRMATIONS)

    def head_height(self) -> int:
        return max(self.blocks, default=-1)

    def build_block(self, height: int, parent_hash: str | None = None,
                    payload: str = "") -> dict:
        return {
            "number": hex(height),
            "hash": _hash(height, parent_hash, payload),
            "parentHash": parent_hash or ("0x" + "00" * 20 if height == 0 else ""),
            "timestamp": hex(1_700_000_000 + height),
            "payload": payload,
        }

    def _append(self, height: int, payload: str = "") -> dict:
        parent = self.blocks[height - 1]["hash"] if height > 0 else None
        blk = self.build_block(height, parent, payload)
        self.blocks[height] = blk
        return blk

    def grow(self, n: int = 1) -> None:
        """正常出块 n 个，并推进最终确定高度。"""
        for _ in range(n):
            self._append(self.head_height() + 1)
        self.finalized_height = max(0, self.head_height() - _CONFIRMATIONS)

    def reorg(self, fork_height: int, depth: int, *, payload: str | None = None
              ) -> list[dict]:
        """从 fork_height 起用新哈希链替换 depth 个区块（模拟链重组）。

        返回新链区块。所有高度 >= fork_height 的旧哈希失效。
        最终确定高度若被回滚则下调，保证“已回滚高度不返回过期结果”。
        """
        if fork_height < 0:
            raise ValueError("fork_height must be >= 0")
        if depth <= 0:
            raise ValueError("depth must be > 0")
        new_blocks: list[dict] = []
        for i in range(depth):
            height = fork_height + i
            parent = new_blocks[-1]["hash"] if i > 0 else (
                self.blocks[height - 1]["hash"] if height > 0 else None
            )
            mark = payload if payload is not None else f"reorg@{time.time_ns()}"
            blk = self.build_block(height, parent, f"{mark}:{i}")
            self.blocks[height] = blk
            new_blocks.append(blk)
        self.finalized_height = min(
            self.finalized_height, max(0, fork_height - 1)
        )
        return new_blocks


class FakeNode:
    """内存假节点，实现 UpstreamTransport 协议。"""

    def __init__(
        self,
        node: NodeInfo,
        state: FakeChainState,
        *,
        latency: float = 0.0,
        jitter: float = 0.0,
        fail_connect: bool = False,
        timeout_methods: set[str] | None = None,
        unknown_methods: set[str] | None = None,
        bad_params_methods: set[str] | None = None,
    ) -> None:
        self.node = node
        self.state = state
        self.latency = latency
        self.jitter = jitter
        self.fail_connect = fail_connect
        self.timeout_methods = timeout_methods or set()
        self.unknown_methods = unknown_methods or set()
        self.bad_params_methods = bad_params_methods or set()
        self.calls = 0  # 记录实际到达该节点的调用数，便于断言重试/合并

    # ---- 业务语义（纯内存，不注入传输故障） ----
    async def handle(self, req: RpcRequest) -> RpcResponse:
        method = req.method
        if method in self.unknown_methods:
            return RpcResponse.fail(
                req.id, METHOD_NOT_FOUND, f"method {method} not found",
                upstream=self.node.name,
            )
        if method in self.bad_params_methods:
            return RpcResponse.fail(
                req.id, INVALID_PARAMS, "invalid params",
                upstream=self.node.name,
            )
        state = self.state
        if method == "eth_blockNumber":
            return RpcResponse.ok(req.id, hex(state.head_height()),
                                  upstream=self.node.name)
        if method == "eth_getBlockByNumber":
            params = req.params
            if isinstance(params, dict):
                tag = params.get("blockNumber", params.get("tag", "latest"))
                full = bool(params.get("fullTransactions", False))
            else:
                params = params or []
                tag = params[0] if params else "latest"
                full = bool(params[1]) if len(params) > 1 else False
            height = _parse_height(tag)
            if height is None:
                height = state.head_height()
            blk = state.blocks.get(height)
            if blk is not None and not full:
                blk = {k: v for k, v in blk.items() if k != "payload"}
            return RpcResponse.ok(req.id, blk, upstream=self.node.name)
        if method == "eth_getBlockByHash":
            if isinstance(req.params, dict):
                target = req.params.get("blockHash")
            else:
                target = (req.params or [None])[0]
            found = next(
                (b for b in state.blocks.values() if b["hash"] == target), None
            )
            return RpcResponse.ok(req.id, found, upstream=self.node.name)
        if method == "web3_clientVersion":
            return RpcResponse.ok(
                req.id, f"FakeNode/{self.node.name}", upstream=self.node.name
            )
        # 未实现的方法：标准业务错误 -32601（不重试）
        return RpcResponse.fail(
            req.id, METHOD_NOT_FOUND, f"method {method} not found",
            upstream=self.node.name,
        )

    # ---- 传输层：注入连接失败/延迟/超时 ----
    async def send(self, req: RpcRequest, *, timeout: float) -> RpcResponse:
        if self.fail_connect:
            raise UpstreamConnRefused(
                f"node {self.node.name} connection refused"
            )
        if req.method in self.timeout_methods:
            # 模拟超过 deadline 的响应
            await asyncio.sleep(max(timeout + 0.05, 0.01))
            raise UpstreamTimeout(f"node {self.node.name} timed out")
        wait = self.latency
        if self.jitter:
            # 确定性伪抖动：基于调用计数，避免测试不可复现
            wait += ((self.calls % 3) * self.jitter)
        if wait > 0:
            await asyncio.sleep(wait)
        self.calls += 1
        return await self.handle(req)

    async def healthcheck(self) -> bool:
        return not self.fail_connect
