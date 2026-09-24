"""请求分发：节点选择、传输故障重试、最终性缓存、single-flight。

失败区分（明确判定依据）：
- 传输故障：httpx.TimeoutException / TransportError（连接被拒、被重置、
  读取超时），或响应不是合法 JSON-RPC。这类失败拿不到业务结论，
  记录原因后切换下一节点重试，并用 record_failure 降低其健康度。
- 业务错误：上游合法返回 JSON-RPC error（-32601/-32602 等），
  是确定性结论，立即原样返回，绝不重试、不掩盖。
"""
from __future__ import annotations

import asyncio
import logging
import time

import httpx

from gateway.cache import CacheEntry, FinalityCache, SingleFlight, cache_key
from gateway.errors import RpcErrorCode
from gateway.fakechain import FakeChain
from gateway.models import (
    DispatchResult,
    RpcRequest,
    RpcResponse,
    UpstreamNode,
)
from gateway.policy import MethodPolicy
from gateway.selector import NodeSelector

logger = logging.getLogger("gateway.dispatcher")


class Dispatcher:
    def __init__(
        self,
        nodes: list[UpstreamNode],
        chain: FakeChain,
        policy: MethodPolicy,
        timeout: float = 2.0,
        cache: FinalityCache | None = None,
    ):
        self._nodes = nodes
        self._chain = chain
        self._policy = policy
        self._timeout = timeout
        self._selector = NodeSelector(nodes)
        self.cache = cache or FinalityCache(chain)
        self._single_flight = SingleFlight()
        # AsyncClient 绑定事件循环；TestClient 每次请求使用不同循环，
        # 因此按运行循环惰性创建并缓存，循环结束自动关闭。
        self._clients: dict[asyncio.AbstractEventLoop, httpx.AsyncClient] = {}

    def _get_client(self) -> httpx.AsyncClient:
        loop = asyncio.get_running_loop()
        client = self._clients.get(loop)
        if client is None or client.is_closed:
            client = httpx.AsyncClient(timeout=self._timeout)
            self._clients[loop] = client
        return client

    async def aclose(self) -> None:
        for client in self._clients.values():
            if not client.is_closed:
                await client.aclose()
        self._clients.clear()

    # ---- 单条分发 ----
    async def dispatch_single(self, req: RpcRequest) -> DispatchResult:
        reasons: list[str] = []

        # 1) 方法策略：受限方法网关侧拒绝，不转发上游。
        if self._policy.is_forbidden(req.method):
            reason = f"方法 {req.method} 在拒绝名单，网关直接返回 -32001"
            reasons.append(reason)
            logger.info("method=%s 拒绝转发：%s", req.method, reason)
            return DispatchResult(
                RpcResponse.fail(req.req_id, RpcErrorCode.METHOD_FORBIDDEN,
                                 f"method {req.method} is forbidden",
                                 data={"reason": reason}),
                node=None, retries=0, cache_hit=False, reasons=reasons,
            )

        # 2) 最终性缓存 + single-flight（仅只读、已最终确定）。
        key = cache_key(req, self._chain)
        if key is not None:
            return await self._single_flight.run(
                key, lambda: self._dispatch_cached(req, key, reasons)
            )

        # 3) 非缓存请求直接走节点重试链。
        return await self._dispatch_with_retry(req, reasons)

    async def _dispatch_cached(
        self, req: RpcRequest, key: str, reasons: list[str]
    ) -> DispatchResult:
        entry = self.cache.get(key)
        if entry is not None:
            reasons.append(f"缓存命中 key={key} height={entry.block_number}")
            logger.info("method=%s 缓存命中 key=%s", req.method, key)
            return DispatchResult(
                RpcResponse.ok(req.req_id, entry.value),
                node=None, retries=0, cache_hit=True, reasons=reasons,
            )
        reasons.append(f"缓存未命中 key={key}")
        logger.info("method=%s 缓存未命中 key=%s，回源", req.method, key)
        result = await self._dispatch_with_retry(req, reasons)
        if result.response.is_success:
            height = int(key.split(":")[1])
            block_hash = self._chain.block_hash_at(height)
            if block_hash is not None:
                self.cache.put(
                    key, CacheEntry(result.response.result, height, block_hash)
                )
        return result

    async def _dispatch_with_retry(
        self, req: RpcRequest, reasons: list[str]
    ) -> DispatchResult:
        attempted: set[str] = set()
        retries = 0
        saw_timeout = False
        ordered = self._selector.ordered_nodes()

        for node in ordered:
            attempted.add(node.name)
            started = time.perf_counter()
            logger.info("method=%s 选择上游 node=%s url=%s score=%.1f",
                        req.method, node.name, node.url, node.score())
            try:
                http_resp = await self._get_client().post(node.url, json={
                    "jsonrpc": "2.0", "id": req.req_id,
                    "method": req.method,
                    "params": req.params if req.params is not None else [],
                })
                payload = http_resp.json()
            except httpx.TimeoutException as exc:
                elapsed = (time.perf_counter() - started) * 1000
                saw_timeout = True
                self._selector.record_failure(node.name)
                reason = (f"node={node.name} 读取/连接超时({elapsed:.0f}ms): "
                          f"{type(exc).__name__}，判定为传输故障，切换节点重试")
                reasons.append(reason)
                logger.info("method=%s %s", req.method, reason)
                retries += 1
                continue
            except httpx.TransportError as exc:
                elapsed = (time.perf_counter() - started) * 1000
                self._selector.record_failure(node.name)
                reason = (f"node={node.name} 传输失败({elapsed:.0f}ms): "
                          f"{type(exc).__name__}，判定为传输故障，切换节点重试")
                reasons.append(reason)
                logger.info("method=%s %s", req.method, reason)
                retries += 1
                continue
            except ValueError as exc:  # 响应体不是合法 JSON
                self._selector.record_failure(node.name)
                reason = f"node={node.name} 返回非法JSON({exc})，判定为传输故障，重试"
                reasons.append(reason)
                retries += 1
                continue

            elapsed = (time.perf_counter() - started) * 1000
            self._selector.record_success(node.name, elapsed)

            # 业务层结果：成功或业务错误都立即返回（业务错误不重试）。
            if isinstance(payload, dict) and ("result" in payload or "error" in payload):
                if "error" in payload:
                    err = payload["error"]
                    reason = (f"node={node.name} 返回业务错误 code={err.get('code')} "
                              f"({err.get('message')})，确定性结论，不重试")
                    reasons.append(reason)
                    logger.info("method=%s %s", req.method, reason)
                    return DispatchResult(
                        RpcResponse(req.req_id, error=err),
                        node=node.name, retries=retries, cache_hit=False,
                        reasons=reasons,
                    )
                return DispatchResult(
                    RpcResponse.ok(req.req_id, payload["result"]),
                    node=node.name, retries=retries, cache_hit=False,
                    reasons=reasons,
                )

            reason = f"node={node.name} 返回非JSON-RPC结构，判定为传输故障，重试"
            reasons.append(reason)
            retries += 1
            self._selector.record_failure(node.name)

        # 所有候选节点均传输失败。
        code = (RpcErrorCode.UPSTREAM_TIMEOUT if saw_timeout
                else RpcErrorCode.UPSTREAM_UNAVAILABLE)
        message = ("all upstreams timed out" if saw_timeout
                   else "all upstreams unavailable")
        reason = f"已尝试 {sorted(attempted)}，均为传输故障，共重试 {retries} 次"
        reasons.append(reason)
        logger.error("method=%s 全部上游失败：%s", req.method, reason)
        return DispatchResult(
            RpcResponse.fail(req.req_id, code, message, data={"reason": reason}),
            node=None, retries=retries, cache_hit=False, reasons=reasons,
        )

    # ---- 批处理 ----
    async def dispatch_batch(
        self, requests: list[RpcRequest]
    ) -> list[DispatchResult]:
        """并发分发批内各项；单项失败不影响其他项，顺序与入参一致。"""
        results = await asyncio.gather(
            *(self.dispatch_single(r) for r in requests),
            return_exceptions=False,
        )
        return list(results)
