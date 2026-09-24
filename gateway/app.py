"""FastAPI 应用：JSON-RPC 入口、批量协议校验与缓存失效触发。

协议约定（可区分的错误码）：
- 非法 JSON / 非对象结构      -> -32700 / -32600（单错误对象）；
- 批量为空或非数组            -> -32600；批量畸形（元素不是对象）-> -32011；
- 批量条数超 max_batch_size   -> -32010；
- 批量中每个元素独立判定：单条畸形返回该项的错误对象，不拖垮整批；
- 通知（无 id）不产生响应；某通知项畸形时按 -32600 占位（罕见）。
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from gateway.dispatcher import Dispatcher
from gateway.errors import RpcErrorCode, error_payload
from gateway.fakechain import FakeChain
from gateway.models import RpcRequest, UpstreamNode
from gateway.policy import MethodPolicy

logger = logging.getLogger("gateway")


def _error_response(req_id: Any, code: int, message: str, data: Any = None):
    return {"jsonrpc": "2.0", "id": req_id,
            "error": error_payload(code, message, data)}


def _parse_single_item(item: Any) -> tuple[RpcRequest | None, dict | None]:
    """返回 (请求, 错误响应)；通知返回请求（由上层过滤输出）。"""
    if not isinstance(item, dict):
        return None, _error_response(None, RpcErrorCode.BATCH_MALFORMED,
                                     "batch item must be an object")
    if item.get("jsonrpc") != "2.0" or not isinstance(item.get("method"), str):
        return None, _error_response(item.get("id") if isinstance(item, dict) else None,
                                     RpcErrorCode.INVALID_REQUEST,
                                     "invalid jsonrpc request")
    params = item.get("params")
    if params is not None and not isinstance(params, (list, dict)):
        return None, _error_response(item.get("id"), RpcErrorCode.INVALID_PARAMS,
                                     "params must be array or object")
    req = RpcRequest(
        method=item["method"],
        params=params,
        req_id=item.get("id"),
        is_notification="id" not in item,
    )
    return req, None


def create_app(
    nodes: list[UpstreamNode] | None = None,
    chain: FakeChain | None = None,
    policy: MethodPolicy | None = None,
    dispatcher: Dispatcher | None = None,
    timeout: float = 2.0,
) -> FastAPI:
    chain = chain or FakeChain()
    policy = policy or MethodPolicy()
    if dispatcher is None:
        dispatcher = Dispatcher(nodes or [], chain, policy, timeout=timeout)

    app = FastAPI(title="blockchain-rpc-gateway")
    app.state.dispatcher = dispatcher
    app.state.chain = chain

    @app.post("/")
    async def rpc_entry(request: Request):
        raw = await request.body()
        try:
            import json
            payload = json.loads(raw)
        except Exception:  # noqa: BLE001 - 任何 JSON 解析失败统一为 parse error
            return JSONResponse(_error_response(
                None, RpcErrorCode.PARSE_ERROR, "parse error"))

        # ---- 批量 ----
        if isinstance(payload, list):
            if not payload:
                return JSONResponse(_error_response(
                    None, RpcErrorCode.INVALID_REQUEST,
                    "empty batch is invalid"))
            if len(payload) > policy.max_batch_size:
                return JSONResponse(_error_response(
                    None, RpcErrorCode.BATCH_TOO_LARGE,
                    f"batch size {len(payload)} exceeds {policy.max_batch_size}",
                    data={"max": policy.max_batch_size, "size": len(payload)}))

            responses: list[dict[str, Any]] = []
            requests: list[RpcRequest] = []
            slots: list[int] = []  # 每个合法请求在 responses 的位置
            for item in payload:
                req, err = _parse_single_item(item)
                if err is not None:
                    responses.append(err)
                    continue
                assert req is not None
                if req.is_notification:
                    requests.append(req)
                    slots.append(-1)  # 通知不落位
                    continue
                requests.append(req)
                slots.append(len(responses))
                responses.append({})  # 占位

            results = await dispatcher.dispatch_batch(requests)
            for result, slot in zip(results, slots):
                if slot >= 0:
                    responses[slot] = result.response.to_dict()
            # 通知不产生任何输出；整批全是通知则按规范返回 204。
            if not responses:
                from fastapi import Response
                return Response(status_code=204)
            return JSONResponse(responses)

        # ---- 单条 ----
        if not isinstance(payload, dict):
            return JSONResponse(_error_response(
                None, RpcErrorCode.INVALID_REQUEST, "request must be object"))
        req, err = _parse_single_item(payload)
        if err is not None:
            return JSONResponse(err)
        assert req is not None
        if req.is_notification:
            from fastapi import Response
            return Response(status_code=204)
        result = await dispatcher.dispatch_single(req)
        return JSONResponse(result.response.to_dict())

    @app.post("/admin/reorg")
    async def admin_reorg(request: Request):
        """测试/运维辅助：触发链重组或出块，并失效过期缓存。"""
        body = await request.json()
        depth = int(body.get("depth", 1))
        before = len(dispatcher.cache)
        head = chain.reorg(depth, rng_seed=body.get("seed"))
        invalidated = dispatcher.cache.on_new_head()
        logger.info("链重组 depth=%s head=%s 缓存失效=%s/%s",
                    depth, head, invalidated, before)
        return {"head": head, "invalidated": invalidated, "cached": len(dispatcher.cache)}

    @app.post("/admin/mine")
    async def admin_mine(request: Request):
        body = await request.json()
        count = int(body.get("count", 1))
        chain.mine(count)
        invalidated = dispatcher.cache.on_new_head()
        return {"head": chain.head_number(),
                "finalized": chain.finalized_number(),
                "invalidated": invalidated}

    @app.get("/health")
    async def health():
        return {"status": "ok", "head": chain.head_number(),
                "finalized": chain.finalized_number(),
                "cached": len(dispatcher.cache)}

    return app
