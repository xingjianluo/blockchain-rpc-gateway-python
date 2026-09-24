"""HTTP 端到端测试：协议错误码、单条/批量、故障切换与重组下的可复现行为。"""
from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from gateway.app import create_app

logger = logging.getLogger("test.api")


@pytest.fixture
def client(env):
    app = create_app(env.config, runtime=env)
    return TestClient(app)


def test_single_success_and_protocol_fields(client):
    r = client.post("/", json={
        "jsonrpc": "2.0", "id": 1,
        "method": "eth_blockNumber", "params": [],
    })
    assert r.status_code == 200
    body = r.json()
    assert body["jsonrpc"] == "2.0" and body["id"] == 1
    assert body["result"].startswith("0x")


def test_parse_error_returns_minus_32700_with_null_id(client):
    r = client.post("/", content=b"{not json",
                    headers={"content-type": "application/json"})
    assert r.status_code == 200
    err = r.json()["error"]
    assert err["code"] == -32700
    assert r.json()["id"] is None


def test_invalid_request_minus_32600_preserves_id(client):
    r = client.post("/", json={"jsonrpc": "1.0", "id": 55, "method": "x"})
    assert r.status_code == 400
    body = r.json()
    assert body["id"] == 55 and body["error"]["code"] == -32600


def test_empty_batch_is_invalid(client):
    r = client.post("/", json=[])
    assert r.status_code == 400
    assert r.json()["error"]["code"] == -32600


def test_batch_too_large_http_413_and_distinct_code(client):
    big = [{"jsonrpc": "2.0", "id": i, "method": "eth_blockNumber", "params": []}
           for i in range(6)]  # max_batch=5
    r = client.post("/", json=big)
    assert r.status_code == 413
    assert r.json()["error"]["code"] == -32011


def test_forbidden_method_http_minus_32010_not_forwarded(client, env):
    before = env.transports["node-a"].calls
    r = client.post("/", json={
        "jsonrpc": "2.0", "id": 9,
        "method": "miner_start", "params": [],
    })
    assert r.json()["error"]["code"] == -32010
    assert env.transports["node-a"].calls == before


def test_transport_failover_is_transparent_over_http(client, env, caplog):
    env.transports["node-a"].fail_connect = True
    env.transports["node-b"].fail_connect = True
    r = client.post("/", json={
        "jsonrpc": "2.0", "id": 10,
        "method": "web3_clientVersion", "params": [],
    })
    body = r.json()
    assert "result" in body
    # a、b 均传输失败，最终由 c 成功；日志含重试原因
    joined = "\n".join(x.getMessage() for x in caplog.records)
    assert "node-a" in joined and "node-b" in joined
    assert joined.count("transport FAILED") >= 2
    logger.info("failover over HTTP -> %s", body["result"])


def test_all_nodes_down_returns_minus_32000(client, env):
    for n in env.transports.values():
        n.fail_connect = True
    r = client.post("/", json={
        "jsonrpc": "2.0", "id": 11,
        "method": "eth_blockNumber", "params": [],
    })
    err = r.json()["error"]
    assert err["code"] == -32000
    assert set(err["data"]["attempts"]) == {"node-a", "node-b", "node-c"}


def test_reorg_then_http_returns_new_block_not_stale(client, env, caplog):
    # 先缓存高度 5（最终确定）
    r1 = client.post("/", json={
        "jsonrpc": "2.0", "id": 12,
        "method": "eth_getBlockByNumber", "params": [5, True],
    })
    old_hash = r1.json()["result"]["hash"]
    # 重组高度 5
    env.chain.reorg(5, 3)
    # 任意请求触发 finalized 回退探测与缓存失效
    client.post("/", json={"jsonrpc": "2.0", "id": 13,
                           "method": "eth_blockNumber", "params": []})
    r2 = client.post("/", json={
        "jsonrpc": "2.0", "id": 14,
        "method": "eth_getBlockByNumber", "params": [5, True],
    })
    new_hash = r2.json()["result"]["hash"]
    assert new_hash != old_hash
    joined = "\n".join(x.getMessage() for x in caplog.records)
    assert "reorg detected" in joined and "cache entries invalidated" in joined


def test_health_endpoint_reports_node_state(client, env):
    env.transports["node-a"].fail_connect = True
    client.post("/", json={"jsonrpc": "2.0", "id": 1,
                           "method": "web3_clientVersion", "params": []})
    h = client.get("/health").json()
    assert h["nodes"]["node-a"]["consecutive_failures"] >= 1
