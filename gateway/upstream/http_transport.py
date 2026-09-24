"""基于 httpx 的真实 HTTP JSON-RPC 上游传输。

传输故障映射（可重试）：
- ConnectError / ConnectTimeout / ReadTimeout / PoolTimeout -> TransportError 子类
- HTTP 5xx 网关类响应 -> UpstreamConnRefused
业务错误不在此处理：上游返回的 JSON-RPC error 对象由上层当作确定性结果，不重试。
"""
from __future__ import annotations

import httpx

from gateway.errors import (
    UpstreamConnRefused,
    UpstreamTimeout,
)
from gateway.models import NodeInfo, RpcRequest, RpcResponse


class HttpTransport:
    def __init__(self, node: NodeInfo, client: httpx.AsyncClient | None = None) -> None:
        self.node = node
        self._client = client
        self._owns_client = client is None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient()
        return self._client

    async def send(self, req: RpcRequest, *, timeout: float) -> RpcResponse:
        client = self._get_client()
        try:
            resp = await client.post(
                self.node.url,
                json=req.to_dict(),
                timeout=timeout,
                headers={"content-type": "application/json"},
            )
        except (httpx.TimeoutException, httpx.PoolTimeout) as exc:
            raise UpstreamTimeout(f"{self.node.name}: {exc}") from exc
        except (httpx.ConnectError, httpx.NetworkError,
                httpx.RemoteProtocolError) as exc:
            raise UpstreamConnRefused(f"{self.node.name}: {exc}") from exc

        if resp.status_code >= 500:
            raise UpstreamConnRefused(
                f"{self.node.name}: HTTP {resp.status_code}"
            )
        if resp.status_code != 200:
            # 4xx 视为节点给不出正常应答（传输类），交由重试逻辑判定
            raise UpstreamConnRefused(
                f"{self.node.name}: HTTP {resp.status_code}"
            )
        data = resp.json()
        if not isinstance(data, dict):
            raise UpstreamConnRefused(f"{self.node.name}: non-object response")
        return RpcResponse(
            id=data.get("id"),
            result=data.get("result"),
            error=data.get("error"),
            upstream=self.node.name,
        )

    async def healthcheck(self) -> bool:
        try:
            client = self._get_client()
            resp = await client.post(
                self.node.url,
                json={"jsonrpc": "2.0", "id": 0,
                      "method": "web3_clientVersion", "params": []},
                timeout=1.0,
            )
            return resp.status_code == 200
        except httpx.HTTPError:
            return False

    async def aclose(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
