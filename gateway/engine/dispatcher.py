"""单条请求分发。

流程（每一步都通过 gateway.dispatcher logger 打印，便于单测观察）：
1. 方法策略检查：受限方法直接拒绝（-32010），不转发；
2. 从上游观察最新最终确定高度，检测链重组并失效相关缓存；
3. 最终性判定：FINALIZED 先查缓存；命中即返回（打印 cache hit）；
4. 未命中/未最终确定：在 singleflight 内按健康度排序逐节点尝试；
5. 传输层故障 -> 记录健康、切换下一节点重试（打印重试原因）；
   业务错误（JSON-RPC error）-> 立即返回，不重试、不掩盖；
6. 成功且 FINALIZED 的结果写入缓存。
"""
from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable

from gateway.cache.finality import classify
from gateway.cache.singleflight import SingleFlight
from gateway.cache.store import FinalityCache, request_key
from gateway.errors import (
    AllUpstreamsFailed,
    TransportError,
)
from gateway.models import Finality, RpcRequest, RpcResponse
from gateway.policy.method_policy import MethodPolicy
from gateway.upstream.selector import NodeSelector

logger = logging.getLogger("gateway.dispatcher")


class Dispatcher:
    def __init__(
        self,
        transports: dict,
        selector: NodeSelector,
        cache: FinalityCache,
        flights: SingleFlight,
        policy: MethodPolicy,
        health: dict,
        *,
        timeout: float = 2.0,
        cooldown: float = 1.0,
        finalized_probe: Callable[[], Awaitable[int]] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._transports = transports
        self._selector = selector
        self._cache = cache
        self._flights = flights
        self._policy = policy
        self._health = health
        self._timeout = timeout
        self._cooldown = cooldown
        self._finalized_probe = finalized_probe
        self._last_finalized = -1
        # 可替换时钟（测试注入确定性假钟）
        self._clock = clock

    # ---- 最终确定高度观测 + 重组失效 ----
    async def _observe_finality(self) -> int:
        if self._finalized_probe is None:
            return -1
        try:
            h = await self._finalized_probe()
        except Exception:  # 探测失败不影响转发
            logger.debug("finalized probe failed", exc_info=True)
            return self._last_finalized
        if self._last_finalized >= 0 and h < self._last_finalized:
            n = self._cache.invalidate_reorg(h, self._clock())
            logger.warning(
                "reorg detected: finalized height %s -> %s; %d cache entries invalidated",
                self._last_finalized, h, n,
            )
        self._last_finalized = h
        return h

    async def dispatch(self, req: RpcRequest) -> RpcResponse:
        # 1. 方法策略（网关侧拒绝，不转发）
        self._policy.check(req.method)

        now = self._clock()
        finalized_height = await self._observe_finality()
        finality, assoc_height = classify(req, finalized_height)

        # 2. 最终确定数据读缓存
        if finality is Finality.FINALIZED:
            cached = self._cache.get(req, now)
            if cached is not None:
                # 安全护栏：仅当缓存条目的关联高度仍不高于当前最终高度时返回。
                # 这样即使重组失效尚未跑完，也不会把已回滚高度的旧值交给客户端。
                if self._cache.is_safe(req, finalized_height):
                    logger.info(
                        "cache HIT method=%s key=%s upstream=%s",
                        req.method, request_key(req), cached.upstream,
                    )
                    return RpcResponse.ok(
                        req.id, cached.result, upstream=cached.upstream,
                        from_cache=True,
                    )
                # 已高于最终高度：淘汰过期条目并走上游重读
                self._cache.evict(req)
                logger.warning(
                    "cache entry for key=%s is above finalized height %s "
                    "(possible reorg) -> bypassing stale value",
                    request_key(req), finalized_height,
                )
            else:
                logger.info(
                    "cache MISS method=%s key=%s", req.method, request_key(req)
                )

        key = request_key(req)

        async def upstream_call() -> RpcResponse:
            return await self._try_upstreams(req, finality, assoc_height)

        # 3. 并发相同只读请求合并为一次上游调用。
        # 合并键必须包含请求 id 与通知标记：无 id 的通知、id=null 与 id=1
        # 的请求不得共享同一个 Future，否则会“借用”彼此响应造成标识错位。
        merge_key = f"{key}#id={req.id!r}#notif={req.is_notification}"
        return await self._flights.do(merge_key, upstream_call)

    async def _try_upstreams(
        self,
        req: RpcRequest,
        finality: Finality,
        assoc_height: int | None,
    ) -> RpcResponse:
        now = self._clock()
        order = self._selector.ordered(now)
        if not order:
            raise AllUpstreamsFailed("no available upstream nodes")

        attempts: list[str] = []
        last_exc: BaseException | None = None

        for node_name in order:
            transport = self._transports[node_name]
            attempts.append(node_name)
            start = self._clock()
            try:
                resp = await transport.send(req, timeout=self._timeout)
            except TransportError as exc:
                latency = self._clock() - start
                self._health[node_name].record_failure(
                    self._clock(), cooldown=self._cooldown
                )
                decision_reason = (
                    f"transport error {type(exc).__name__}: {exc}"
                )
                logger.warning(
                    "upstream=%s transport FAILED after %.4fs; %s; "
                    "retryable -> trying next node (attempts=%s)",
                    node_name, latency, decision_reason, attempts,
                )
                last_exc = exc
                continue
            except Exception as exc:  # 非传输类未知异常：不重试
                logger.error(
                    "upstream=%s non-transport error %s; NOT retrying",
                    node_name, type(exc).__name__,
                )
                raise

            latency = self._clock() - start
            self._health[node_name].record_success(latency, self._clock())

            # 业务错误：不重试、不掩盖
            if resp.is_error:
                logger.info(
                    "upstream=%s business error code=%s message=%s; "
                    "deterministic -> NOT retrying (attempts=%s)",
                    node_name, resp.error.get("code"),
                    resp.error.get("message"), attempts,
                )
                return RpcResponse.fail(
                    req.id, resp.error["code"], resp.error.get("message", ""),
                    data=resp.error.get("data"), upstream=node_name,
                )

            logger.info(
                "upstream=%s SUCCESS method=%s latency=%.4fs (attempts=%s)",
                node_name, req.method, latency, attempts,
            )
            out = RpcResponse.ok(req.id, resp.result, upstream=node_name)
            # 仅缓存已最终确定的成功结果
            self._cache.put(req, out, finality, assoc_height, self._clock())
            return out

        raise AllUpstreamsFailed(
            f"all {len(attempts)} node(s) failed: {attempts}; "
            f"last={type(last_exc).__name__ if last_exc else 'unknown'}",
            data={"attempts": attempts},
        )
