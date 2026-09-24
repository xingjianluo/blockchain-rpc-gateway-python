"""上游传输接口。"""
from __future__ import annotations

from typing import Protocol, runtime_checkable

from gateway.models import NodeInfo, RpcRequest, RpcResponse


@runtime_checkable
class UpstreamTransport(Protocol):
    """上游节点传输协议（HTTP 或本地假节点）。"""

    node: NodeInfo

    async def send(self, req: RpcRequest, *, timeout: float) -> RpcResponse:
        """发送单条请求；传输故障抛 gateway.errors.TransportError。"""
        raise NotImplementedError

    async def healthcheck(self) -> bool:
        """探测节点是否可用。"""
        raise NotImplementedError
