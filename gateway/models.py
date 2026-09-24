"""请求/响应/上游节点等数据模型。"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from gateway.errors import error_payload


@dataclass
class RpcRequest:
    """单条 JSON-RPC 2.0 请求。

    req_id 保留调用方的原始 id（数字/字符串/null 均合法），
    批量请求据此保证响应与请求不错位。
    """

    method: str
    params: list[Any] | dict[str, Any] | None
    req_id: Any
    is_notification: bool
    jsonrpc: str = "2.0"


@dataclass
class RpcResponse:
    """单条 JSON-RPC 2.0 响应（通知不产生响应）。"""

    req_id: Any
    result: Any = None
    error: dict[str, Any] | None = None
    jsonrpc: str = "2.0"

    @property
    def is_success(self) -> bool:
        return self.error is None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"jsonrpc": self.jsonrpc, "id": self.req_id}
        if self.error is not None:
            d["error"] = self.error
        else:
            d["result"] = self.result
        return d

    @staticmethod
    def ok(req_id: Any, result: Any) -> RpcResponse:
        return RpcResponse(req_id=req_id, result=result)

    @staticmethod
    def fail(req_id: Any, code: int, message: str, data: Any = None) -> RpcResponse:
        return RpcResponse(
            req_id=req_id, error=error_payload(code, message, data)
        )


@dataclass
class UpstreamNode:
    """一个上游节点的静态描述与运行时健康状态。

    score 综合健康度与延迟：
    - 不健康节点直接排在最后；
    - 连续失败产生惩罚分；
    - latency_ewma_ms 为响应延迟的指数加权移动平均。
    """

    name: str
    url: str
    healthy: bool = True
    latency_ewma_ms: float = 0.0
    consecutive_failures: int = 0
    _failure_penalty_ms: float = 500.0

    def score(self) -> float:
        if not self.healthy:
            return float("inf")
        return self.latency_ewma_ms + self._failure_penalty_ms * self.consecutive_failures


@dataclass
class DispatchResult:
    """一次分发的结果及可观测信息（测试日志依据）。"""

    response: RpcResponse
    node: str | None
    retries: int
    cache_hit: bool
    reasons: list[str] = field(default_factory=list)
