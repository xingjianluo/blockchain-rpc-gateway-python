"""等待交易上链：按交易合并轮询（single-flight per tx），各等待方独立判定。

语义：
- 调用方提交 (tx_hash, 确认条件, 时间预算)；网关在预算内轮询上游
  eth_getTransactionReceipt，确认数/最终性由本地链镜像（与缓存同一来源）计算；
- 同一 tx_hash 无论多少等待方只产生一路上游轮询（TxWatch），
  每个等待方按自己的条件与预算独立得出结论，互不影响；
- 轮询期间发现回执从“已上链”变为“不存在”即判定发生重组，观测转为
  dropped，绝不把回滚前的状态当作确认结果返回；预算内重新上链则按
  新位置继续判定；
- 预算耗尽时：曾被包含但被回滚且未重新上链 -> -32021 TX_REORGED；
  其余（从未上链 / 已上链但确认数不足）-> -32020 WAIT_TIMEOUT。
  两种失败都与成功确认严格区分，不会返回未达标的结果。

轮询走 Dispatcher.dispatch_single，因此节点选择、传输故障切换重试、
方法策略与最终性缓存等既有约束对每一轮轮询同样生效。
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from gateway.errors import RpcErrorCode
from gateway.models import DispatchResult, RpcRequest, RpcResponse

if TYPE_CHECKING:
    from gateway.dispatcher import Dispatcher
    from gateway.fakechain import FakeChain

logger = logging.getLogger("gateway.waiter")

#: 网关自定义的等待确认方法名。
WAIT_FOR_TX_METHOD = "gw_waitForTransaction"

#: 等待方预算的默认值与上限（秒），防止调用方遗漏预算导致永久挂起。
DEFAULT_TIMEOUT_S = 30.0
MAX_TIMEOUT_S = 300.0


@dataclass
class WaitCondition:
    """单个等待方的确认条件与时间预算。"""

    tx_hash: str
    confirmations: int = 1
    require_finalized: bool = False
    timeout_s: float = DEFAULT_TIMEOUT_S


def parse_wait_condition(req: RpcRequest) -> WaitCondition:
    """解析 gw_waitForTransaction 参数；非法时抛 ValueError/TypeError（-> -32602）。

    支持两种形态：
    - 具名：{"txHash": ..., "confirmations": N, "finalized": bool, "timeoutMs": M}
    - 位置：[txHash, {options...}]
    """
    params = req.params
    options: dict[str, Any]
    if isinstance(params, dict):
        options = dict(params)
    elif isinstance(params, list) and params:
        options = {"txHash": params[0]}
        if len(params) > 1:
            if not isinstance(params[1], dict):
                raise ValueError("second positional param must be an options object")
            options.update(params[1])
    else:
        raise ValueError("expected {txHash, ...} or [txHash, options]")

    tx_hash = options.get("txHash")
    if not isinstance(tx_hash, str) or not tx_hash.startswith("0x"):
        raise ValueError("txHash must be a 0x-prefixed string")

    confirmations = options.get("confirmations", 1)
    if not isinstance(confirmations, int) or isinstance(confirmations, bool) \
            or confirmations < 1:
        raise ValueError("confirmations must be an integer >= 1")

    require_finalized = options.get("finalized", False)
    if not isinstance(require_finalized, bool):
        raise TypeError("finalized must be a boolean")

    timeout_ms = options.get("timeoutMs", DEFAULT_TIMEOUT_S * 1000)
    if not isinstance(timeout_ms, (int, float)) or isinstance(timeout_ms, bool) \
            or timeout_ms <= 0:
        raise ValueError("timeoutMs must be a positive number")
    timeout_s = min(timeout_ms / 1000.0, MAX_TIMEOUT_S)

    return WaitCondition(
        tx_hash=tx_hash,
        confirmations=confirmations,
        require_finalized=require_finalized,
        timeout_s=timeout_s,
    )


@dataclass
class Observation:
    """最近一次轮询得到的交易落地观测。"""

    state: str = "pending"  # pending | included | dropped
    block_number: int | None = None
    block_hash: str | None = None


class TxWatch:
    """同一笔交易的共享轮询器：无论多少等待方，只产生一路上游轮询。"""

    def __init__(
        self,
        dispatcher: Dispatcher,
        chain: FakeChain,
        tx_hash: str,
        poll_interval_s: float,
    ):
        self.dispatcher = dispatcher
        self.chain = chain
        self.tx_hash = tx_hash
        self.poll_interval_s = poll_interval_s
        self.condition = asyncio.Condition()
        self.observation = Observation()
        self.last_included: tuple[int, str] | None = None
        self.waiters = 0
        self.polls = 0
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._poll_loop())

    def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            self._task = None

    async def _poll_loop(self) -> None:
        try:
            while True:
                await self._poll_once()
                await asyncio.sleep(self.poll_interval_s)
        except asyncio.CancelledError:
            pass

    async def _poll_once(self) -> None:
        self.polls += 1
        result = await self.dispatcher.dispatch_single(RpcRequest(
            method="eth_getTransactionReceipt",
            params=[self.tx_hash],
            req_id=f"gw-poll-{self.tx_hash[:10]}-{self.polls}",
            is_notification=False,
        ))
        async with self.condition:
            if not result.response.is_success:
                # 上游全部传输故障：保留上次观测，等待下一轮/预算耗尽。
                logger.warning(
                    "tx=%s 第%d轮轮询未获得上游结论（%s），保留上次观测 state=%s",
                    self.tx_hash, self.polls,
                    result.response.error, self.observation.state)
            else:
                receipt = result.response.result
                if receipt is None:
                    if self.observation.state == "included":
                        logger.info(
                            "tx=%s 第%d轮轮询：回执消失，判定被重组回滚 "
                            "(曾在 height=%s)", self.tx_hash, self.polls,
                            self.observation.block_number)
                        self.observation = Observation(state="dropped")
                else:
                    number = int(receipt["blockNumber"], 16)
                    block_hash = receipt["blockHash"]
                    prev = self.observation
                    self.observation = Observation(
                        state="included", block_number=number,
                        block_hash=block_hash)
                    self.last_included = (number, block_hash)
                    if prev.state != "included" or prev.block_hash != block_hash:
                        logger.info(
                            "tx=%s 第%d轮轮询：上链 height=%s hash=%s (此前 %s)",
                            self.tx_hash, self.polls, number,
                            block_hash[:12], prev.state)
            # 每轮都唤醒等待方：链顶推进后确认数可能已达标。
            self.condition.notify_all()


class WaiterRegistry:
    """管理所有进行中的交易轮询；按 tx_hash 合并。"""

    def __init__(self, dispatcher: Dispatcher, chain: FakeChain,
                 poll_interval_s: float = 1.0):
        self._dispatcher = dispatcher
        self._chain = chain
        self._poll_interval_s = poll_interval_s
        self._watches: dict[str, TxWatch] = {}

    @property
    def active_watches(self) -> int:
        return len(self._watches)

    async def wait(self, req: RpcRequest, cond: WaitCondition) -> DispatchResult:
        watch = self._watches.get(cond.tx_hash)
        if watch is None:
            watch = TxWatch(self._dispatcher, self._chain, cond.tx_hash,
                            self._poll_interval_s)
            self._watches[cond.tx_hash] = watch
            watch.start()
            logger.info("tx=%s 新建共享轮询（interval=%.2fs）",
                        cond.tx_hash, self._poll_interval_s)
        else:
            logger.info("tx=%s 合并到进行中的共享轮询（当前等待方=%d）",
                        cond.tx_hash, watch.waiters)
        watch.waiters += 1
        try:
            return await self._wait_loop(watch, req, cond)
        finally:
            watch.waiters -= 1
            if watch.waiters == 0:
                watch.stop()
                self._watches.pop(cond.tx_hash, None)
                logger.info("tx=%s 全部等待方结束，停止共享轮询（共轮询%d轮）",
                            cond.tx_hash, watch.polls)

    async def _wait_loop(
        self, watch: TxWatch, req: RpcRequest, cond: WaitCondition
    ) -> DispatchResult:
        reasons = [
            (f"等待 tx={cond.tx_hash} confirmations>={cond.confirmations} "
             f"finalized={cond.require_finalized} 预算={cond.timeout_s:.2f}s")
        ]
        logger.info("tx=%s %s", cond.tx_hash, reasons[0])
        loop = asyncio.get_running_loop()
        deadline = loop.time() + cond.timeout_s

        while True:
            async with watch.condition:
                obs = watch.observation
                if obs.state == "included":
                    head = self._chain.head_number()
                    confirmations = head - obs.block_number + 1
                    finalized = obs.block_number <= self._chain.finalized_number()
                    if confirmations >= cond.confirmations and (
                        not cond.require_finalized or finalized
                    ):
                        return self._success(req, cond, obs, confirmations,
                                             finalized, watch, reasons)
                remaining = deadline - loop.time()
                if remaining <= 0:
                    return self._budget_exhausted(watch, req, cond, reasons)
                try:
                    await asyncio.wait_for(watch.condition.wait(),
                                           timeout=remaining)
                except TimeoutError:
                    pass  # 预算到期，回到循环做最终判定

    def _success(
        self, req: RpcRequest, cond: WaitCondition, obs: Observation,
        confirmations: int, finalized: bool, watch: TxWatch,
        reasons: list[str],
    ) -> DispatchResult:
        status = "finalized" if finalized else "confirmed"
        payload = {
            "txHash": cond.tx_hash,
            "status": status,
            "blockNumber": hex(obs.block_number),
            "blockHash": obs.block_hash,
            "confirmations": confirmations,
            "finalized": finalized,
        }
        reason = (f"tx={cond.tx_hash} 达成条件：height={obs.block_number} "
                  f"confirmations={confirmations} finalized={finalized} "
                  f"(第{watch.polls}轮轮询后)")
        reasons.append(reason)
        logger.info("%s", reason)
        return DispatchResult(
            RpcResponse.ok(req.req_id, payload),
            node=None, retries=0, cache_hit=False, reasons=reasons,
        )

    def _budget_exhausted(
        self, watch: TxWatch, req: RpcRequest, cond: WaitCondition,
        reasons: list[str],
    ) -> DispatchResult:
        obs = watch.observation
        if obs.state == "dropped":
            # 曾被包含但被重组回滚，预算内未重新上链：与正常确认明确区分。
            last_height, last_hash = watch.last_included or (None, None)
            reason = (f"tx={cond.tx_hash} 曾在 height={last_height} 上链，"
                      f"等待期间被重组回滚，预算 {cond.timeout_s:.2f}s 内"
                      f"未重新上链，返回 -32021")
            reasons.append(reason)
            logger.info("%s", reason)
            return DispatchResult(
                RpcResponse.fail(
                    req.req_id, RpcErrorCode.TX_REORGED,
                    "transaction was reverted by chain reorg and not "
                    "re-included before timeout",
                    data={
                        "txHash": cond.tx_hash,
                        "lastIncluded": {
                            "blockNumber": hex(last_height)
                            if last_height is not None else None,
                            "blockHash": last_hash,
                        },
                        "reason": reason,
                    }),
                node=None, retries=0, cache_hit=False, reasons=reasons,
            )
        reason = (f"tx={cond.tx_hash} 预算 {cond.timeout_s:.2f}s 耗尽仍未达标"
                  f"(lastKnown={obs.state}, polls={watch.polls})，返回 -32020")
        reasons.append(reason)
        logger.info("%s", reason)
        return DispatchResult(
            RpcResponse.fail(
                req.req_id, RpcErrorCode.WAIT_TIMEOUT,
                "timed out waiting for transaction confirmation",
                data={
                    "txHash": cond.tx_hash,
                    "timeoutMs": int(cond.timeout_s * 1000),
                    "lastKnown": obs.state,
                    "reason": reason,
                }),
            node=None, retries=0, cache_hit=False, reasons=reasons,
        )
