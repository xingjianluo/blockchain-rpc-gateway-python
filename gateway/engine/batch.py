"""批量请求编排。

保证：
- 先做整批大小校验（超限整批拒绝 -32011）；
- 逐项校验结构，畸形项独立产出 -32600 error（id 尽力提取，无法识别用 None），
  绝不拖垮兄弟项；
- 各项与原始位置一一对应、保序返回；各自独立分派，单项失败/异常被隔离，
  不会污染或挤占其它项的结果；
- 通知类请求（id 缺失）不产生响应条目（遵循 JSON-RPC 2.0 批量语义）。
"""
from __future__ import annotations

import asyncio
import logging

from gateway.errors import GatewayError, MalformedRequest
from gateway.models import RpcRequest, RpcResponse
from gateway.policy.batch_policy import BatchPolicy

from .dispatcher import Dispatcher

logger = logging.getLogger("gateway.batch")


def _error_dict(req_id, rpc_err: dict) -> dict:
    return {"jsonrpc": "2.0", "id": req_id, "error": rpc_err}


class BatchExecutor:
    def __init__(self, dispatcher: Dispatcher, batch_policy: BatchPolicy) -> None:
        self._dispatcher = dispatcher
        self._policy = batch_policy

    @property
    def dispatcher(self) -> Dispatcher:
        return self._dispatcher

    def precheck_size(self, raw_items: list) -> None:
        self._policy.validate_size(len(raw_items))

    async def execute(self, raw_items: list) -> list[dict]:
        self.precheck_size(raw_items)

        async def handle_one(index: int, item: object) -> dict | None:
            # 结构校验：畸形项只影响自身
            try:
                self._policy.validate_item(item, index)
            except MalformedRequest as exc:
                req_id = item.get("id") if isinstance(item, dict) else None
                logger.warning(
                    "batch item #%d malformed: %s -> -32600", index, exc.message
                )
                return _error_dict(req_id, exc.to_rpc_error())

            assert isinstance(item, dict)
            raw_id = item.get("id")
            # 通知：原始对象中完全没有 "id" 键；id=null 仍是合法请求并回 null
            is_notification = "id" not in item
            req = RpcRequest(
                method=item["method"],
                params=item.get("params", []),
                id=raw_id,
                jsonrpc=item.get("jsonrpc", "2.0"),
                is_notification=is_notification,
            )
            try:
                resp = await self._dispatcher.dispatch(req)
            except GatewayError as exc:
                rpc_err = exc.to_rpc_error()
                logger.warning(
                    "batch item #%d method=%s rejected: code=%s %s",
                    index, req.method, rpc_err["code"], rpc_err["message"],
                )
                resp = RpcResponse(id=req.id, error=rpc_err)
            except Exception as exc:  # 单项意外异常隔离为该项 error
                logger.exception(
                    "batch item #%d method=%s unexpected error",
                    index, req.method,
                )
                resp = RpcResponse.fail(req.id, -32603, f"internal error: {exc}")
            if is_notification:
                return None
            return resp.to_dict()

        results = await asyncio.gather(
            *(handle_one(i, it) for i, it in enumerate(raw_items))
        )
        # 保序、剔除通知项的 None
        return [r for r in results if r is not None]
