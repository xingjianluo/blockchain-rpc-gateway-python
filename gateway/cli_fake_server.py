"""本地假上游节点 HTTP 服务。

可启动多个进程模拟多节点，各自持有独立链状态；也支持通过管理端点
注入故障与链重组：
  POST /                 JSON-RPC 入口
  POST /__control/grow   {"n": 2}
  POST /__control/reorg  {"fork_height": 3, "depth": 2}
  POST /__control/fault  {"latency": 0.5} / {"timeout_methods": [...]}
                           / {"fail_connect": true}
"""
from __future__ import annotations

import argparse

import uvicorn
from fastapi import FastAPI, Request

from gateway.models import NodeInfo, RpcRequest
from gateway.upstream.fake_node import FakeChainState, FakeNode


def build_fake_app(node_name: str = "fake", initial_blocks: int = 8) -> FastAPI:
    app = FastAPI(title=f"fake-upstream-{node_name}")
    chain = FakeChainState(initial_blocks)
    node = FakeNode(NodeInfo(node_name, "local"), chain)
    app.state.chain = chain
    app.state.node = node

    @app.post("/")
    async def rpc(request: Request):
        data = await request.json()
        req = RpcRequest(
            method=data["method"],
            params=data.get("params", []),
            id=data.get("id"),
            jsonrpc=data.get("jsonrpc", "2.0"),
        )
        try:
            resp = await node.send(req, timeout=30.0)
        except Exception as exc:  # noqa: BLE001 - 进程边界：把假节点故障统一暴露为 502
            # 独立进程内以 HTTP 502 暴露传输故障，网关侧映射为连接类故障
            from fastapi.responses import JSONResponse
            return JSONResponse(
                {"jsonrpc": "2.0", "id": req.id,
                 "error": {"code": -32002, "message": str(exc)}},
                status_code=502,
            )
        return resp.to_dict()

    @app.post("/__control/grow")
    async def grow(request: Request):
        body = await request.json()
        chain.grow(int(body.get("n", 1)))
        return {"head": chain.head_height(),
                "finalized": chain.finalized_height}

    @app.post("/__control/reorg")
    async def reorg(request: Request):
        body = await request.json()
        new = chain.reorg(int(body["fork_height"]), int(body["depth"]))
        return {"replaced": [b["number"] for b in new],
                "finalized": chain.finalized_height}

    @app.post("/__control/fault")
    async def fault(request: Request):
        body = await request.json()
        if "latency" in body:
            node.latency = float(body["latency"])
        if "jitter" in body:
            node.jitter = float(body["jitter"])
        if "fail_connect" in body:
            node.fail_connect = bool(body["fail_connect"])
        if "timeout_methods" in body:
            node.timeout_methods = set(body["timeout_methods"])
        if "unknown_methods" in body:
            node.unknown_methods = set(body["unknown_methods"])
        return {"ok": True}

    @app.get("/__chain")
    async def dump_chain():
        return {
            "head": chain.head_height(),
            "finalized": chain.finalized_height,
            "blocks": {h: b["hash"] for h, b in chain.blocks.items()},
        }

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a fake blockchain upstream")
    parser.add_argument("--name", default="fake")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9101)
    args = parser.parse_args()
    uvicorn.run(build_fake_app(args.name), host=args.host, port=args.port)


if __name__ == "__main__":
    main()
