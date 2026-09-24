"""方法级访问策略与批量约束测试。"""
from __future__ import annotations

from gateway.errors import RpcErrorCode as C
from gateway.policy import MethodPolicy


def test_forbidden_method_rejected_at_gateway(gateway, nodes):
    # 受限方法必须 -32001 且不转发：通过禁用所有上游 url 仍应成功拒绝。
    resp = gateway.post("/", json={
        "jsonrpc": "2.0", "id": 1,
        "method": "eth_sendRawTransaction",
        "params": ["0xdeadbeef"],
    })
    body = resp.json()
    assert body["error"]["code"] == C.METHOD_FORBIDDEN
    assert "reason" in body["error"]["data"]


def test_allowed_read_method_passes(gateway):
    resp = gateway.post("/", json={
        "jsonrpc": "2.0", "id": 7,
        "method": "eth_blockNumber", "params": [],
    })
    assert resp.json()["result"].startswith("0x")


def test_whitelist_mode(gateway_factory, chain):
    policy = MethodPolicy(allowed={"eth_blockNumber"}, max_batch_size=20)
    client, _ = gateway_factory(pol=policy)
    ok = client.post("/", json={"jsonrpc": "2.0", "id": 1,
                                "method": "eth_blockNumber", "params": []})
    assert "result" in ok.json()
    denied = client.post("/", json={"jsonrpc": "2.0", "id": 2,
                                    "method": "eth_chainId", "params": []})
    assert denied.json()["error"]["code"] == C.METHOD_FORBIDDEN


def test_batch_too_large(gateway, policy):
    too_big = [{"jsonrpc": "2.0", "id": i,
                "method": "eth_blockNumber", "params": []}
               for i in range(policy.max_batch_size + 1)]
    resp = gateway.post("/", json=too_big)
    assert resp.json()["error"]["code"] == C.BATCH_TOO_LARGE


def test_empty_batch_invalid(gateway):
    assert gateway.post("/", json=[]).json()["error"]["code"] == C.INVALID_REQUEST


def test_batch_malformed_item_isolated(gateway):
    # 一个畸形项不得拖垮其他项：错误码可区分。
    resp = gateway.post("/", json=[
        {"jsonrpc": "2.0", "id": "good",
         "method": "eth_blockNumber", "params": []},
        "not-an-object",
        {"jsonrpc": "1.0", "id": "badver", "method": "eth_blockNumber"},
        {"jsonrpc": "2.0", "id": "forbid",
         "method": "eth_sendRawTransaction", "params": []},
    ])
    body = resp.json()
    assert len(body) == 4
    by_id = {r.get("id"): r for r in body}
    assert "result" in by_id["good"]
    assert by_id[None]["error"]["code"] == C.BATCH_MALFORMED
    assert by_id["badver"]["error"]["code"] == C.INVALID_REQUEST
    assert by_id["forbid"]["error"]["code"] == C.METHOD_FORBIDDEN
