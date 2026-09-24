"""批量请求：标识对齐、单项失败不污染整批、混合方法。"""
from __future__ import annotations

from gateway.errors import RpcErrorCode as C


def test_batch_responses_align_with_ids(gateway):
    resp = gateway.post("/", json=[
        {"jsonrpc": "2.0", "id": 100, "method": "eth_blockNumber", "params": []},
        {"jsonrpc": "2.0", "id": "str-id", "method": "eth_chainId", "params": []},
        {"jsonrpc": "2.0", "id": None, "method": "eth_blockNumber", "params": []},
    ])
    body = resp.json()
    assert [r["id"] for r in body] == [100, "str-id", None]
    assert all("result" in r for r in body)


def test_batch_single_item_failure_is_isolated(gateway):
    resp = gateway.post("/", json=[
        {"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []},
        {"jsonrpc": "2.0", "id": 2, "method": "eth_unknownMethod", "params": []},
        {"jsonrpc": "2.0", "id": 3, "method": "eth_chainId", "params": []},
        {"jsonrpc": "2.0", "id": 4, "method": "eth_sendRawTransaction",
         "params": ["0x1"]},
    ])
    by_id = {r["id"]: r for r in resp.json()}
    assert "result" in by_id[1]
    assert by_id[2]["error"]["code"] == C.METHOD_NOT_FOUND
    assert "result" in by_id[3]
    assert by_id[4]["error"]["code"] == C.METHOD_FORBIDDEN
    # 成功项结果完整，不残缺
    assert by_id[1]["result"].startswith("0x")


def test_batch_duplicate_ids_preserved(gateway):
    resp = gateway.post("/", json=[
        {"jsonrpc": "2.0", "id": "dup", "method": "eth_blockNumber", "params": []},
        {"jsonrpc": "2.0", "id": "dup", "method": "eth_chainId", "params": []},
    ])
    body = resp.json()
    assert len(body) == 2
    assert all(r["id"] == "dup" for r in body)
    # 两项结果不互相污染（方法不同结果不同）
    assert body[0]["result"] != body[1]["result"] or True
    assert len({r["result"] for r in body}) == 2


def test_batch_notification_returns_204(gateway):
    resp = gateway.post("/", json=[
        {"jsonrpc": "2.0", "method": "eth_blockNumber", "params": []},
    ])
    assert resp.status_code == 204
    assert resp.content == b""


def test_batch_mixed_notification_and_request(gateway):
    resp = gateway.post("/", json=[
        {"jsonrpc": "2.0", "method": "eth_blockNumber"},
        {"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []},
    ])
    body = resp.json()
    assert len(body) == 1 and body[0]["id"] == 1


def test_single_notification_204(gateway):
    assert gateway.post("/", json={
        "jsonrpc": "2.0", "method": "eth_blockNumber"
    }).status_code == 204


def test_parse_error_and_invalid_request(gateway):
    assert gateway.post("/", content=b"{not json",
                        headers={"content-type": "application/json"}
                        ).json()["error"]["code"] == C.PARSE_ERROR
    assert gateway.post("/", json={"foo": 1}).json()["error"]["code"] == C.INVALID_REQUEST
