"""网关内部领域模型与类型定义。"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

Json = dict[str, Any] | list[Any] | str | int | float | bool | None
RpcId = str | int | None


class Finality(str, Enum):
    """数据最终性等级。"""

    FINALIZED = "finalized"
    UNFINALIZED = "unfinalized"


@dataclass
class RpcRequest:
    """单条 JSON-RPC 请求。"""

    method: str
    params: list[Any] | dict[str, Any] = field(default_factory=list)
    id: RpcId = None
    jsonrpc: str = "2.0"
    is_notification: bool = False  # 原始报文缺少 "id" 键

    def to_dict(self) -> dict[str, Any]:
        return {
            "jsonrpc": self.jsonrpc,
            "method": self.method,
            "params": self.params,
            "id": self.id,
        }


@dataclass
class RpcResponse:
    """单条 JSON-RPC 响应（成功或失败）。成功时 error=None。"""

    id: RpcId = None
    result: Any = None
    error: dict[str, Any] | None = None
    jsonrpc: str = "2.0"
    # 观测信息（不参与 JSON-RPC 报文，仅用于日志/测试）
    upstream: str | None = None
    from_cache: bool = False

    @property
    def is_error(self) -> bool:
        return self.error is not None

    @staticmethod
    def ok(req_id: RpcId, result: Any, *, upstream: str | None = None,
           from_cache: bool = False) -> RpcResponse:
        return RpcResponse(id=req_id, result=result, error=None,
                           upstream=upstream, from_cache=from_cache)

    @staticmethod
    def fail(req_id: RpcId, code: int, message: str, *, data: Any = None,
             upstream: str | None = None) -> RpcResponse:
        err: dict[str, Any] = {"code": code, "message": message}
        if data is not None:
            err["data"] = data
        return RpcResponse(id=req_id, result=None, error=err, upstream=upstream)

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"jsonrpc": self.jsonrpc, "id": self.id}
        if self.error is not None:
            d["error"] = self.error
        else:
            d["result"] = self.result
        return d


@dataclass
class NodeInfo:
    """上游节点配置。"""

    name: str
    url: str
    weight: float = 1.0
