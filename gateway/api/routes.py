"""FastAPI 路由：POST /（单条/批量 JSON-RPC）、GET /health。"""
from __future__ import annotations

import json
import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from gateway.api.rpc_handler import encode_error, parse_error_response, parse_single
from gateway.errors import (
    BATCH_TOO_LARGE,
    GatewayError,
)

logger = logging.getLogger("gateway.api")


def build_router(executor) -> APIRouter:
    router = APIRouter()

    @router.post("/")
    async def rpc_entry(request: Request):
        raw_bytes = await request.body()
        try:
            payload = json.loads(raw_bytes)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.warning("parse error: %s", exc)
            return JSONResponse(parse_error_response(exc))

        # 批量请求
        if isinstance(payload, list):
            if not payload:
                # 空批量：按规范返回 -32600
                return JSONResponse(
                    encode_error(None, -32600,
                                 "invalid request: empty batch"),
                    status_code=400,
                )
            try:
                results = await executor.execute(payload)
            except GatewayError as exc:
                # 整批级失败（如超限 -32011）：单个 error 对象
                logger.warning("batch rejected: %s", exc.message)
                status = 413 if exc.code == BATCH_TOO_LARGE else 400
                return JSONResponse(
                    encode_error(None, exc.code, exc.message, data=exc.data),
                    status_code=status,
                )
            # 若全部为通知（结果为空），规范要求返回 204
            if not results:
                return JSONResponse(status_code=204, content=None)
            return JSONResponse(results)

        # 单条请求
        try:
            req = parse_single(payload)
        except GatewayError as exc:
            req_id = payload.get("id") if isinstance(payload, dict) else None
            logger.warning("invalid single request: %s", exc.message)
            return JSONResponse(
                encode_error(req_id, exc.code, exc.message, data=exc.data),
                status_code=400,
            )
        try:
            resp = await executor.dispatcher.dispatch(req)
        except GatewayError as exc:
            logger.warning("single request rejected: %s", exc.message)
            return JSONResponse(
                encode_error(req.id, exc.code, exc.message, data=exc.data)
            )
        return JSONResponse(resp.to_dict())

    @router.get("/health")
    async def health(request: Request):
        rt = request.app.state.runtime
        return {
            "status": "ok",
            "nodes": {
                name: {
                    "available": h.available(rt.dispatcher._clock()),
                    "consecutive_failures": h.consecutive_failures,
                    "avg_latency": round(h.avg_latency(), 6),
                }
                for name, h in rt.health.items()
            },
            "cache_entries": len(rt.cache),
            "cache_hits": rt.cache.hits,
            "merged": rt.flights.merged_count,
        }

    return router
