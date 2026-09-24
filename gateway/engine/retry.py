"""故障分类与节点切换重试决策。

判定依据（可解释、可复现）：
- TransportError：TCP/HTTP 传输层未取得 JSON-RPC 应答（超时、连接拒绝、重置、5xx）。
  请求对只读方法幂等，可切换到下一节点重试。
- RpcBusinessError：上游返回了结构合法的 JSON-RPC error（-32601/-32602 等）。
  这是方法/参数层面的确定性结果，所有节点结论一致，重试只会放大负载，故不重试。
- 其它未知异常：视为内部错误，不重试，避免非幂等副作用。
"""
from __future__ import annotations

from dataclasses import dataclass

from gateway.errors import RpcBusinessError, TransportError


@dataclass
class RetryDecision:
    should_retry: bool
    reason: str


def classify_failure(exc: BaseException) -> RetryDecision:
    if isinstance(exc, TransportError):
        return RetryDecision(
            True,
            f"transport failure ({type(exc).__name__}: {exc}); "
            "no JSON-RPC response received, safe to retry on another node",
        )
    if isinstance(exc, RpcBusinessError):
        return RetryDecision(
            False,
            f"business error code={exc.code}: deterministic JSON-RPC error; "
            "retrying on another node would yield the same result",
        )
    return RetryDecision(False, f"non-retryable internal error: {type(exc).__name__}")
