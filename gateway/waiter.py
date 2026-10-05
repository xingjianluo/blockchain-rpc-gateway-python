"""等待交易确认：预算内轮询、同交易轮询合并、重组感知。

语义概述（详见 docs/design.md 第 6 节）：
- 调用方提交交易标识 + 期望条件（确认深度 N 或已最终确定）+ 等待预算；
- 网关在预算内通过既有分发链（选节点/重试/最终性缓存/方法策略）轮询：
  每轮依次发 eth_blockNumber、eth_getBlockByNumber(finalized)、
  eth_getTransactionReceipt，finalized 查询正常命中最终性缓存；
- 同一笔交易的多个等待方共享同一个轮询循环（single-flight 等待），
  观察到的链上事实共享，但结论严格按每个等待方自己的条件/预算独立结算，
  响应各自构造、id 原样回显，互不串味；
- 等待期间发生重组（曾观察到上链的交易消失）时不返回过期结论，
  在剩余预算内继续观察；重新上链按新位置结算（结果标注 reorged=true），
  预算耗尽仍未回来则返回与正常确认明确可区分的 -32021 TX_REORGED；
- 预算耗尽仍未满足条件返回 -32020 WAIT_TIMEOUT，
  绝不挂起、绝不返回未达标结果。
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any

from gateway.errors import RpcErrorCode
from gateway.models import DispatchResult, RpcRequest, RpcResponse

logger = logging.getLogger("gateway.waiter")

WAIT_METHOD = "gw_waitForTransaction"

# 轮询固定经过既有分发链的三个只读方法。
_HEAD_METHOD = "eth_blockNumber"
_FINALIZED_METHOD = "eth_getBlockByNumber"
_RECEIPT_METHOD = "eth_getTransactionReceipt"


def _to_int(value: Any) -> int | None:
    """接受十进制 int 或 0x 十六进制字符串。"""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value, 16) if value.startswith("0x") else int(value)
        except ValueError:
            return None
    return None


@dataclass
class WaitParams:
    """单个等待项的条件与预算。"""

    tx_hash: str
    confirmations: int | None = None
    finalized: bool = False
    timeout_ms: int = 10_000
    poll_ms: int = 100

    def condition_label(self) -> str:
        if self.finalized:
            return "finalized"
        return f"confirmations>={self.confirmations}"


@dataclass
class ChainObservation:
    """一轮轮询得到的链上事实（所有等待方共享）。"""

    head_number: int
    finalized_number: int
    receipt: dict[str, Any] | None  # None = 交易当前不在链上
    node: str | None                # 本轮收据请求落定的上游
    polls: int


@dataclass
class _Subscriber:
    """一个具体等待方：自己的条件、预算与结算 future。"""

    params: WaitParams
    req_id: Any
    deadline: float
    future: asyncio.Future
    started: float
    settled: bool = False


@dataclass
class WaitOutcome:
    """协调器给分发层的内部结果（再由分发层包成 DispatchResult）。"""

    response: RpcResponse
    node: str | None
    reasons: list[str]


class _TxWatcher:
    """单笔交易的共享轮询循环。

    任何时刻该交易只有一个 poller 在回源；等待方只管 await 自己的 future。
    """

    def __init__(self, tx_hash: str, dispatch, min_poll_ms: int) -> None:
        self.tx_hash = tx_hash
        self._dispatch = dispatch
        self._min_poll_s = min_poll_ms / 1000.0
        self._subscribers: list[_Subscriber] = []
        self._task: asyncio.Task | None = None
        self._last_obs: ChainObservation | None = None
        # 重组追踪：上一轮交易所在区块；reorg_seen 表示等待期间见过消失。
        self._last_block_hash: str | None = None
        self._last_block_number: int | None = None
        self._reorg_seen = False
        self._reasons: list[str] = []

    # ---- 等待方生命周期 ----
    def add(self, sub: _Subscriber) -> None:
        self._subscribers.append(sub)
        if self._task is None:
            self._task = asyncio.create_task(self._run())
        logger.info(
            "tx=%s 加入等待 条件=%s 预算=%sms 间隔=%sms 当前等待方=%d",
            self.tx_hash, sub.params.condition_label(),
            sub.params.timeout_ms, sub.params.poll_ms, len(self._subscribers),
        )

    def discard(self, sub: _Subscriber) -> None:
        """等待方提前离开（如客户端断开），不影响其他等待方。"""
        if sub in self._subscribers:
            self._subscribers.remove(sub)
            logger.info("tx=%s 一个等待方离开，剩余=%d",
                        self.tx_hash, len(self._subscribers))

    # ---- 轮询循环 ----
    async def _run(self) -> None:
        try:
            await self._warm_finalized_cache()
            while self._subscribers:
                await self._poll_once()
                self._settle_all()
                self._subscribers = [s for s in self._subscribers if not s.settled]
                if not self._subscribers:
                    break
                await asyncio.sleep(self._next_delay())
        except asyncio.CancelledError:
            logger.info("tx=%s 轮询循环被取消", self.tx_hash)
            raise
        finally:
            # 兜底：所有未结算 future 一律给内部错误，绝不允许挂起。
            for sub in self._subscribers:
                if not sub.settled and not sub.future.done():
                    sub.future.set_result(self._error_response(
                        sub, RpcErrorCode.INTERNAL_ERROR,
                        "wait loop terminated",
                        {"reason": "poller stopped before settlement"},
                    ))

    async def _warm_finalized_cache(self) -> None:
        """等待开始时预热 finalized 缓存，使后续轮询稳定命中、不重复回源。"""
        res = await self._dispatch(_FINALIZED_METHOD, ["finalized", False])
        if res.cache_hit:
            logger.info("tx=%s 启动预热：finalized 缓存命中", self.tx_hash)
        elif res.response.is_success:
            logger.info("tx=%s 启动预热：finalized 缓存未命中，已回填，"
                        "后续轮询走缓存", self.tx_hash)

    def _next_delay(self) -> float:
        delays = []
        for s in self._subscribers:
            remaining = s.deadline - time.monotonic()
            delays.append(max(0.0, remaining))
            delays.append(max(self._min_poll_s, s.params.poll_ms / 1000.0))
        return max(0.0, min(delays))

    async def _poll_once(self) -> None:
        """一轮三次只读调用，全部走既有分发链（选节点/重试/缓存/策略）。"""
        poll_n = (self._last_obs.polls + 1) if self._last_obs else 1

        head_res: DispatchResult = await self._dispatch(_HEAD_METHOD, [])
        if not head_res.response.is_success:
            self._reasons.extend(head_res.reasons)
            logger.info("tx=%s poll#%d 链顶查询失败，本轮跳过评估：%s",
                        self.tx_hash, poll_n,
                        head_res.response.error.get("message"))
            # 不能因持续传输故障让等待方挂过预算：到期者直接按超时结算。
            for sub in list(self._subscribers):
                if (not sub.settled
                        and time.monotonic() >= sub.deadline):
                    self._finish(sub, self._timeout_response_without_obs(
                        sub, poll_n,
                        f"upstream unavailable while polling: "
                        f"{head_res.response.error.get('message')}",
                    ))
            return
        head_number = _to_int(head_res.response.result)

        fin_res: DispatchResult = await self._dispatch(
            _FINALIZED_METHOD, ["finalized", False]
        )
        finalized_number = head_number  # 保守：拿不到最终高度就不按最终满足
        if fin_res.response.is_success and isinstance(
            fin_res.response.result, dict
        ):
            finalized_number = _to_int(
                fin_res.response.result.get("number")
            ) or head_number
            if fin_res.cache_hit:
                logger.info("tx=%s poll#%d finalized 查询命中缓存 height=%s",
                            self.tx_hash, poll_n, finalized_number)
        else:
            self._reasons.extend(fin_res.reasons)

        rx_res: DispatchResult = await self._dispatch(
            _RECEIPT_METHOD, [self.tx_hash]
        )
        self._reasons.extend(rx_res.reasons)

        # 收据方法的业务错误是确定性结论：通知所有等待方，不再轮询。
        if rx_res.response.error is not None and rx_res.node is not None:
            logger.info("tx=%s poll#%d 收据查询返回业务错误 node=%s，"
                        "结束全部等待", self.tx_hash, poll_n, rx_res.node)
            err = rx_res.response.error
            for sub in list(self._subscribers):
                self._finish(sub, RpcResponse(sub.req_id, error=err))
            return

        receipt = rx_res.response.result if rx_res.response.is_success else None
        self._track_reorg(receipt, poll_n)

        self._last_obs = ChainObservation(
            head_number=head_number,
            finalized_number=finalized_number,
            receipt=receipt,
            node=rx_res.node,
            polls=poll_n,
        )
        logger.info(
            "tx=%s poll#%d head=%s finalized=%s receipt=%s node=%s",
            self.tx_hash, poll_n, head_number, finalized_number,
            "None(未上链)" if receipt is None
            else f"height={_to_int(receipt.get('blockNumber'))}",
            rx_res.node,
        )

    def _track_reorg(self, receipt: dict[str, Any] | None, poll_n: int) -> None:
        """根据收据变化识别重组：曾上链 -> 消失 -> （可能）重新上链。"""
        if receipt is None:
            if self._last_block_hash is not None:
                self._reorg_seen = True
                logger.warning(
                    "tx=%s poll#%d 观察到链重组：交易曾在 height=%s hash=%s，"
                    "现已从链上消失，继续在预算内观察",
                    self.tx_hash, poll_n, self._last_block_number,
                    self._last_block_hash,
                )
                self._last_block_hash = None
                self._last_block_number = None
            return
        new_hash = receipt.get("blockHash")
        new_number = _to_int(receipt.get("blockNumber"))
        if self._last_block_hash is not None and new_hash != self._last_block_hash:
            self._reorg_seen = True
            logger.info(
                "tx=%s poll#%d 交易在新位置重新上链 height=%s hash=%s（旧 height=%s）",
                self.tx_hash, poll_n, new_number, new_hash,
                self._last_block_number,
            )
        self._last_block_hash = new_hash
        self._last_block_number = new_number

    # ---- 结算 ----
    def _settle_all(self) -> None:
        obs = self._last_obs
        for sub in list(self._subscribers):
            if sub.settled or obs is None:
                continue
            elapsed_ms = int((time.monotonic() - sub.started) * 1000)
            timed_out = time.monotonic() >= sub.deadline

            if obs.receipt is not None and self._condition_met(sub.params, obs):
                self._finish(sub, self._confirmed_response(sub, obs, elapsed_ms))
                continue

            if timed_out:
                self._finish(sub, self._timeout_response(sub, obs, elapsed_ms))

    def _condition_met(self, p: WaitParams, obs: ChainObservation) -> bool:
        block_number = _to_int(obs.receipt.get("blockNumber"))
        if block_number is None:
            return False
        if p.finalized:
            return block_number <= obs.finalized_number
        need = p.confirmations if p.confirmations is not None else 1
        return obs.head_number - block_number + 1 >= need

    def _confirmed_response(
        self, sub: _Subscriber, obs: ChainObservation, elapsed_ms: int
    ) -> RpcResponse:
        block_number = _to_int(obs.receipt.get("blockNumber"))
        confirmations = obs.head_number - block_number + 1
        result = {
            "status": "confirmed",
            "transactionHash": self.tx_hash,
            "blockNumber": hex(block_number),
            "blockHash": obs.receipt.get("blockHash"),
            "confirmations": hex(confirmations),
            "finalized": block_number <= obs.finalized_number,
            "reorged": self._reorg_seen,
            "condition": sub.params.condition_label(),
            "waitedMs": elapsed_ms,
            "polls": obs.polls,
            "node": obs.node,
        }
        logger.info(
            "tx=%s 条件满足(%s) 落定高度=%s 确认数=%s finalized=%s "
            "重组=%s 上游=%s 用时=%sms 轮询=%d req_id=%s",
            self.tx_hash, sub.params.condition_label(), block_number,
            confirmations, result["finalized"], self._reorg_seen,
            obs.node, elapsed_ms, obs.polls, sub.req_id,
        )
        return RpcResponse.ok(sub.req_id, result)

    def _timeout_response_without_obs(
        self, sub: _Subscriber, polls: int, note: str
    ) -> RpcResponse:
        elapsed_ms = int((time.monotonic() - sub.started) * 1000)
        data = {
            "transactionHash": self.tx_hash,
            "condition": sub.params.condition_label(),
            "budgetMs": sub.params.timeout_ms,
            "elapsedMs": elapsed_ms,
            "polls": polls,
            "included": False,
            "reason": note,
        }
        logger.warning(
            "tx=%s 等待超时(%s) 期间链顶持续不可用 用时=%sms 轮询=%d "
            "req_id=%s -> -32020",
            self.tx_hash, sub.params.condition_label(), elapsed_ms,
            polls, sub.req_id,
        )
        return self._error_response(
            sub, RpcErrorCode.WAIT_TIMEOUT,
            "wait for transaction confirmation timed out", data,
        )

    def _timeout_response(
        self, sub: _Subscriber, obs: ChainObservation, elapsed_ms: int
    ) -> RpcResponse:
        # 关键区分：曾经上链后被重组掉、且预算耗尽时仍未回来 -> TX_REORGED，
        # 绝不与正常确认或普通超时混淆。
        if self._reorg_seen and obs.receipt is None:
            data = {
                "transactionHash": self.tx_hash,
                "condition": sub.params.condition_label(),
                "budgetMs": sub.params.timeout_ms,
                "elapsedMs": elapsed_ms,
                "polls": obs.polls,
                "reorged": True,
                "lastSeenBlockNumber": (
                    None if self._last_block_number is None
                    else hex(self._last_block_number)
                ),
                "reason": "transaction was included, then reorged out and "
                          "did not re-enter the chain within the wait budget",
            }
            logger.warning(
                "tx=%s 等待结束：交易被重组且未在预算内回来 用时=%sms 轮询=%d "
                "req_id=%s -> -32021", self.tx_hash, elapsed_ms, obs.polls,
                sub.req_id,
            )
            return self._error_response(
                sub, RpcErrorCode.TX_REORGED,
                "transaction reorged out within wait budget", data,
            )

        data = {
            "transactionHash": self.tx_hash,
            "condition": sub.params.condition_label(),
            "budgetMs": sub.params.timeout_ms,
            "elapsedMs": elapsed_ms,
            "polls": obs.polls,
            "head": obs.head_number,
            "finalized": obs.finalized_number,
            "included": obs.receipt is not None,
            "reason": "condition not satisfied before the wait budget expired",
        }
        logger.warning(
            "tx=%s 等待超时(%s) 用时=%sms 轮询=%d req_id=%s -> -32020",
            self.tx_hash, sub.params.condition_label(), elapsed_ms,
            obs.polls, sub.req_id,
        )
        return self._error_response(
            sub, RpcErrorCode.WAIT_TIMEOUT,
            "wait for transaction confirmation timed out", data,
        )

    def _error_response(
        self, sub: _Subscriber, code: int, message: str, data: Any
    ) -> RpcResponse:
        return RpcResponse.fail(sub.req_id, code, message, data=data)

    def _finish(self, sub: _Subscriber, response: RpcResponse) -> None:
        sub.settled = True
        if not sub.future.done():
            sub.future.set_result(WaitOutcome(
                response=response,
                node=self._last_obs.node if self._last_obs else None,
                reasons=list(self._reasons),
            ))


@dataclass
class WaitCoordinator:
    """按交易标识合并等待：同 tx 同一时刻只有一个 _TxWatcher。"""

    dispatcher: Any = None
    min_poll_ms: int = 10
    _watchers: dict[str, _TxWatcher] = field(default_factory=dict)

    async def _dispatch_via_chain(self, method: str, params: list) -> DispatchResult:
        req = RpcRequest(
            method=method, params=params, req_id=None, is_notification=True,
        )
        # 内部轮询请求绕过方法策略无意义：直接走分发链的缓存/选节点/重试。
        return await self.dispatcher.dispatch_poll(req)

    async def wait(self, params: WaitParams, req_id: Any = None) -> WaitOutcome:
        watcher = self._watchers.get(params.tx_hash)
        if watcher is None:
            watcher = _TxWatcher(
                params.tx_hash, self._dispatch_via_chain, self.min_poll_ms
            )
            self._watchers[params.tx_hash] = watcher

        loop = asyncio.get_running_loop()
        future: asyncio.Future[WaitOutcome] = loop.create_future()
        sub = _Subscriber(
            params=params,
            req_id=req_id,
            deadline=time.monotonic() + params.timeout_ms / 1000.0,
            future=future,
            started=time.monotonic(),
        )
        watcher.add(sub)
        try:
            return await future
        except asyncio.CancelledError:
            watcher.discard(sub)
            self._cleanup(params.tx_hash, watcher)
            raise
        finally:
            self._cleanup(params.tx_hash, watcher)

    def _cleanup(self, tx_hash: str, watcher: _TxWatcher) -> None:
        # 仅当 watcher 已无存活等待方时摘除；poller 在下轮自检退出。
        current = self._watchers.get(tx_hash)
        if current is watcher and not watcher._subscribers:
            self._watchers.pop(tx_hash, None)
            if watcher._task is not None and not watcher._task.done():
                watcher._task.cancel()


def parse_wait_request(
    req: RpcRequest,
) -> tuple[WaitParams | None, RpcResponse | None]:
    """解析 gw_waitForTransaction 参数；非法时返回 (None, 错误响应)。

    接受具名参数：
      {"transactionHash": "0x..", "confirmations": 3 | "0x3",
       "finalized": true, "timeoutMs": 5000, "pollIntervalMs": 100}
    也接受位置参数：
      [txHash] / [txHash, confirmations] / [txHash, confirmations, timeoutMs]
    缺省：confirmations=1、timeoutMs=10000、pollIntervalMs=100。
    """
    p = req.params
    tx_hash: Any = None
    confirmations: Any = None
    finalized: Any = None
    timeout_ms: Any = None
    poll_ms: Any = None

    if isinstance(p, dict):
        tx_hash = (p.get("transactionHash") or p.get("txHash")
                   or p.get("hash"))
        confirmations = p.get("confirmations")
        finalized = p.get("finalized", p.get("requireFinalized"))
        timeout_ms = p.get("timeoutMs", p.get("timeout"))
        poll_ms = p.get("pollIntervalMs", p.get("pollMs"))
    elif isinstance(p, list):
        if p and isinstance(p[0], dict):
            return parse_wait_request(
                RpcRequest(req.method, p[0], req.req_id, False)
            )
        if len(p) >= 1:
            tx_hash = p[0]
        if len(p) >= 2:
            confirmations = p[1]
        if len(p) >= 3:
            timeout_ms = p[2]
        if len(p) >= 4:
            poll_ms = p[3]
    elif p is None:
        pass
    else:
        return None, RpcResponse.fail(
            req.req_id, RpcErrorCode.INVALID_PARAMS,
            "params must be array or object",
        )

    if not isinstance(tx_hash, str) or not tx_hash.startswith("0x"):
        return None, RpcResponse.fail(
            req.req_id, RpcErrorCode.INVALID_PARAMS,
            "missing or invalid transactionHash (expected 0x-prefixed string)",
        )

    want_finalized = bool(finalized)
    want_conf: int | None = None
    if not want_finalized:
        if confirmations is None:
            want_conf = 1
        else:
            want_conf = _to_int(confirmations)
            if want_conf is None or want_conf < 1:
                return None, RpcResponse.fail(
                    req.req_id, RpcErrorCode.INVALID_PARAMS,
                    "confirmations must be a positive integer",
                )

    budget = _to_int(timeout_ms) if timeout_ms is not None else 10_000
    if budget is None or budget <= 0:
        return None, RpcResponse.fail(
            req.req_id, RpcErrorCode.INVALID_PARAMS,
            "timeoutMs must be a positive integer",
        )

    interval = _to_int(poll_ms) if poll_ms is not None else 100
    if interval is None or interval <= 0:
        return None, RpcResponse.fail(
            req.req_id, RpcErrorCode.INVALID_PARAMS,
            "pollIntervalMs must be a positive integer",
        )

    return WaitParams(
        tx_hash=tx_hash,
        confirmations=want_conf,
        finalized=want_finalized,
        timeout_ms=budget,
        poll_ms=interval,
    ), None
