"""JSON-RPC 2.0 请求解析与错误响应编码。"""
from __future__ import annotations

from typing import Any

from gateway.errors import INVALID_REQUEST, PARSE_ERROR, MalformedRequest
from gateway.models import RpcRequest


def encode_error(req_id: Any, code: int, message: str, data: Any = None) -> dict:
    err: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        err["data"] = data
    return {"jsonrpc": "2.0", "id": req_id, "error": err}


def parse_single(raw: Any) -> RpcRequest:
    """把单个 JSON 对象解析为 RpcRequest。

    非法结构抛 MalformedRequest(-32600)；解析时保留原始 id 以便回填错误响应。
    """
    if not isinstance(raw, dict):
        raise MalformedRequest("request must be a JSON object")
    if raw.get("jsonrpc") != "2.0":
        raise MalformedRequest("missing or unsupported 'jsonrpc' version")
    method = raw.get("method")
    if not isinstance(method, str) or not method:
        raise MalformedRequest("missing or invalid 'method'")
    params = raw.get("params", [])
    if not isinstance(params, (list, dict)):
        raise MalformedRequest("'params' must be array or object")
    req_id = raw.get("id")
    if not isinstance(req_id, (str, int, type(None))) or isinstance(req_id, bool):
        raise MalformedRequest("'id' must be string, number or null")
    return RpcRequest(method=method, params=params, id=req_id)


def parse_error_response(raw_text_exc: Exception) -> dict:
    """JSON 解析失败（-32700）的标准响应，id 必须为 null。"""
    return encode_error(None, PARSE_ERROR, f"parse error: {raw_text_exc}")


def invalid_request_response(req_id: Any, message: str) -> dict:
    return encode_error(req_id, INVALID_REQUEST, message)
