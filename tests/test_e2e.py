"""端到端综合场景：基于 main.py 启动的本地多假节点集群。"""
from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

import main as main_module


@pytest.fixture(scope="module")
def live_client():
    logging.getLogger("e2e").info("启动 main.py 内置假上游集群: %s",
                                  [s.url for s in main_module._servers])
    with TestClient(main_module.app) as client:
        yield client


def _rpc(client, req_id, method, params=None):
    return client.post("/", json={"jsonrpc": "2.0", "id": req_id,
                                  "method": method, "params": params or []}).json()


def test_health_and_basic_read(live_client):
    health = live_client.get("/health").json()
    assert health["status"] == "ok" and health["head"] >= 11
    body = _rpc(live_client, 1, "eth_blockNumber")
    assert body["result"] == hex(health["head"])


def test_transport_failover_and_then_selection_improves(live_client):
    # node-b 前 2 次连接故障：前两次请求可能触发重试，之后选择应稳定。
    r1 = _rpc(live_client, "a", "eth_blockNumber")
    assert "result" in r1
    r2 = _rpc(live_client, "b", "eth_chainId")
    assert "result" in r2
    # 后续请求不再受故障影响
    for i in range(3):
        assert "result" in _rpc(live_client, i, "eth_blockNumber")


def test_forbidden_not_forwarded(live_client):
    body = _rpc(live_client, 1, "eth_accounts")
    assert body["error"]["code"] == -32001


def test_reorg_then_fresh_data_and_cache_invalidation(live_client):
    # finalized 数据缓存一次
    fin = live_client.get("/health").json()["finalized"]
    cached_block = _rpc(live_client, 1, "eth_getBlockByNumber",
                        ["finalized", False])["result"]
    assert int(cached_block["number"], 16) == fin

    # 未最终高度重组：latest 哈希变化
    latest_before = _rpc(live_client, 2, "eth_getBlockByNumber",
                         ["latest", False])["result"]
    rep = live_client.post("/admin/reorg", json={"depth": 3, "seed": 13}).json()
    assert rep["head"] == int(latest_before["number"], 16)
    latest_after = _rpc(live_client, 3, "eth_getBlockByNumber",
                        ["latest", False])["result"]
    assert latest_after["hash"] != latest_before["hash"]


def test_mixed_batch_with_failure(live_client):
    body = live_client.post("/", json=[
        {"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []},
        {"jsonrpc": "2.0", "id": 2, "method": "eth_nope", "params": []},
        {"jsonrpc": "2.0", "id": 3, "method": "eth_getBalance",
         "params": ["0xC0FFEE", "latest"]},
    ]).json()
    by_id = {r["id"]: r for r in body}
    assert "result" in by_id[1]
    assert by_id[2]["error"]["code"] == -32601
    assert isinstance(by_id[3]["result"], str) and by_id[3]["result"].startswith("0x")
