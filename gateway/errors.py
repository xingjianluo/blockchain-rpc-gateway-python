"""JSON-RPC 2.0 错误码与异常定义。

错误分类的判定依据：
- 业务错误（-32601 方法不存在、-32602 参数非法、-32600 非法请求等）：
  来自上游的合法 JSON-RPC error 响应，重试其他节点不会改变结论，
  必须原样透传，不得重试或掩盖。
- 传输故障（超时、连接失败、上游 5xx/空响应）：
  拿不到任何业务层结论，属于可重试故障，应切换节点重试。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class RpcErrorCode:
    # ---- JSON-RPC 2.0 标准错误码 ----
    PARSE_ERROR = -32700
    INVALID_REQUEST = -32600
    METHOD_NOT_FOUND = -32601
    INVALID_PARAMS = -32602
    INTERNAL_ERROR = -32603

    # ---- 网关自定义错误码（保留段 -32000 ~ -32099）----
    METHOD_FORBIDDEN = -32001        # 受限方法，网关侧拒绝
    UPSTREAM_UNAVAILABLE = -32003    # 所有上游均发生传输故障
    UPSTREAM_TIMEOUT = -32004        # 所有上游均超时
    BATCH_TOO_LARGE = -32010         # 批量请求条数超限
    BATCH_MALFORMED = -32011         # 批量请求结构畸形
    WAIT_TIMEOUT = -32020            # 等待交易确认超出时间预算
    TX_REORGED = -32021              # 等待期间交易被重组回滚且预算内未重新上链


# 业务错误码集合：上游返回这些错误时绝不重试。
_NON_RETRYABLE_UPSTREAM_CODES = frozenset({
    RpcErrorCode.INVALID_REQUEST,
    RpcErrorCode.METHOD_NOT_FOUND,
    RpcErrorCode.INVALID_PARAMS,
    RpcErrorCode.METHOD_FORBIDDEN,
})


@dataclass
class RpcError(Exception):
    """携带 JSON-RPC 错误码与判定原因的异常。

    retryable 标明该错误是否为传输层可重试故障；
    reason 给出明确判定依据，用于日志可解释性。
    """

    code: int
    message: str
    data: Any = None
    retryable: bool = False
    reason: str = ""

    def __str__(self) -> str:
        return f"RpcError({self.code}, retryable={self.retryable}): {self.message}"


def error_payload(code: int, message: str, data: Any = None) -> dict[str, Any]:
    """构造 JSON-RPC error 字段。"""
    err: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        err["data"] = data
    return err


def is_retryable_upstream(code: int) -> bool:
    """判断上游返回的错误码是否属于可重试故障。

    判定依据：业务错误（方法不存在/参数非法/非法请求/被策略拒绝）
    是确定性结论，切换节点无意义；仅网关自定义的传输类错误码
    （-32003/-32004 等）才允许重试。
    """
    return code not in _NON_RETRYABLE_UPSTREAM_CODES
