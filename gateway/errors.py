"""错误码与异常分类。

两类失败的明确判定依据：
- 传输层故障 TransportError：请求未得到上游的 JSON-RPC 应答
  （TCP 连接失败、读写超时、连接重置、HTTP 5xx 等）。请求语义未知，
  对只读调用可安全切换节点重试。
- 业务错误 RpcBusinessError：上游返回了结构合法的 JSON-RPC error 对象
  （如 -32601 方法不存在、-32602 参数非法）。这是确定性的语义结果，
  换节点结果相同，不得重试或掩盖。
"""
from __future__ import annotations

from typing import Any

# JSON-RPC 2.0 标准错误码
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603

# 网关扩展错误码（-32000 ~ -32099 为实现自定义服务端错误区间）
UPSTREAM_ALL_FAILED = -32000   # 所有节点传输层故障
UPSTREAM_TIMEOUT = -32001      # 上游超时
UPSTREAM_CONN_REFUSED = -32002  # 连接被拒绝/重置
METHOD_FORBIDDEN = -32010      # 方法被网关策略禁止
BATCH_TOO_LARGE = -32011       # 批量请求数量超限
BATCH_MALFORMED = -32012       # 批量请求畸形
RATE_LIMITED = -32013          # 限流
STALE_FINALIZED = -32020       # 缓存命中高度已被重组回滚


class GatewayError(Exception):
    """网关错误基类。"""

    code: int = INTERNAL_ERROR
    message: str = "gateway error"
    retryable: bool = False

    def __init__(self, message: str | None = None, *, data: Any = None) -> None:
        super().__init__(message or self.message)
        if message:
            self.message = message
        self.data = data

    def to_rpc_error(self) -> dict[str, Any]:
        err: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.data is not None:
            err["data"] = self.data
        return err


class TransportError(GatewayError):
    """传输层故障：请求未得到上游应答，可切换节点重试。"""

    retryable = True


class UpstreamTimeout(TransportError):
    code = UPSTREAM_TIMEOUT
    message = "upstream timeout"


class UpstreamConnRefused(TransportError):
    code = UPSTREAM_CONN_REFUSED
    message = "upstream connection failed"


class AllUpstreamsFailed(TransportError):
    code = UPSTREAM_ALL_FAILED
    message = "all upstreams failed"


class RpcBusinessError(GatewayError):
    """上游 JSON-RPC 业务错误：确定性语义结果，不得重试。"""

    retryable = False

    def __init__(self, code: int, message: str, *, data: Any = None) -> None:
        super().__init__(message, data=data)
        self.code = code


class MethodForbidden(GatewayError):
    code = METHOD_FORBIDDEN
    message = "method forbidden by gateway policy"
    retryable = False


class BatchTooLarge(GatewayError):
    code = BATCH_TOO_LARGE
    message = "batch size exceeds limit"
    retryable = False


class MalformedRequest(GatewayError):
    code = INVALID_REQUEST
    message = "invalid request"
    retryable = False


class RateLimited(GatewayError):
    code = RATE_LIMITED
    message = "rate limited"
    retryable = False


def is_retryable(exc: BaseException) -> bool:
    """传输层故障可重试；其余（业务错误、策略拒绝）不可重试。"""
    return isinstance(exc, TransportError)
