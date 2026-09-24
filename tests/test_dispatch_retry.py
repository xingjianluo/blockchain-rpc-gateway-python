"""多上游故障切换重试；业务错误不重试不掩盖。

日志中会打印：所选上游、缓存命中情况、每次重试原因。
"""
from __future__ import annotations

import pytest

from gateway.errors import RpcErrorCode as C
from gateway.fakeupstream import FakeUpstreamServer, FaultConfig
from gateway.models import UpstreamNode

_BLOCK_NUMBER = {"jsonrpc": "2.0", "id": 1,
                 "method": "eth_blockNumber", "params": []}


def _start(chain, name, fault) -> FakeUpstreamServer:
    s = FakeUpstreamServer(chain, name, fault)
    s.start()
    return s


@pytest.fixture
def flaky_nodes(chain):
    # node-bad1/bad2 前若干次连接故障；node-good 始终正常。
    bad1 = _start(chain, "bad1", FaultConfig(fail_first=3, latency_ms=2, seed=1))
    good = _start(chain, "good", FaultConfig(latency_ms=2, seed=2))
    yield [UpstreamNode("bad1", bad1.url), UpstreamNode("good", good.url)], [bad1, good]
    bad1.stop(); good.stop()


def test_transport_failure_failover_succeeds(flaky_nodes, gateway_factory):
    nodes, _ = flaky_nodes
    # 让 bad1 排在前面（先记录小延迟），迫使先选它再切换 good。
    client, dispatcher = gateway_factory(node_list=nodes)
    dispatcher._selector.record_success("bad1", 1)
    dispatcher._selector.record_success("good", 50)

    resp = client.post("/", json=_BLOCK_NUMBER).json()
    assert "result" in resp  # 切换节点后成功
    # 第二次调用：bad1 已被惩罚，good 优先，不再重试
    r2 = client.post("/", json=_BLOCK_NUMBER)
    assert "result" in r2.json()


def test_business_error_method_not_found_not_retried(chain, gateway_factory):
    good = _start(chain, "good", FaultConfig(seed=3))
    try:
        nodes = [UpstreamNode("good", good.url)]
        client, _disp = gateway_factory(nodes)
        resp = client.post("/", json={
            "jsonrpc": "2.0", "id": 9,
            "method": "eth_doesNotExist", "params": [],
        }).json()
        assert resp["error"]["code"] == C.METHOD_NOT_FOUND
        assert good._fault.calls == 1  # 业务错误绝不重试
    finally:
        good.stop()


def test_business_error_invalid_params_not_retried(chain, gateway_factory):
    good = _start(chain, "good", FaultConfig(seed=4))
    try:
        client, _d = gateway_factory([UpstreamNode("good", good.url)])
        resp = client.post("/", json={
            "jsonrpc": "2.0", "id": 9,
            "method": "eth_getBalance", "params": [],
        }).json()
        assert resp["error"]["code"] == C.INVALID_PARAMS
        assert good._fault.calls == 1
    finally:
        good.stop()


def test_all_upstreams_down_returns_upstream_unavailable(chain, gateway_factory):
    dead1 = _start(chain, "dead1", FaultConfig(always_connection_reset=True))
    dead2 = _start(chain, "dead2", FaultConfig(fail_first=999))
    try:
        client, _ = gateway_factory(
            [UpstreamNode("dead1", dead1.url), UpstreamNode("dead2", dead2.url)],
            timeout=1.0,
        )
        body = client.post("/", json=_BLOCK_NUMBER).json()
        assert body["error"]["code"] == C.UPSTREAM_UNAVAILABLE
        assert "reason" in body["error"]["data"]
        assert "传输故障" in body["error"]["data"]["reason"]
    finally:
        dead1.stop(); dead2.stop()


def test_timeout_is_distinguishable_error(chain, gateway_factory):
    slow = _start(chain, "slow", FaultConfig(always_timeout=True, latency_ms=1))
    try:
        client, _ = gateway_factory([UpstreamNode("slow", slow.url)], timeout=0.3)
        body = client.post("/", json=_BLOCK_NUMBER).json()
        assert body["error"]["code"] == C.UPSTREAM_TIMEOUT
    finally:
        slow.stop()


def test_recovery_after_transient_failures(chain, gateway_factory):
    # fail_first=2：前两次传输故障，第三次恢复。验证重试链路能拿到恢复后结果。
    flaky = _start(chain, "flaky", FaultConfig(fail_first=2, seed=5))
    stable = _start(chain, "stable", FaultConfig(latency_ms=2, seed=6))
    try:
        client, dispatcher = gateway_factory(
            [UpstreamNode("flaky", flaky.url), UpstreamNode("stable", stable.url)],
            timeout=1.0,
        )
        # 第一次：flaky 失败 -> stable 成功
        assert "result" in client.post("/", json=_BLOCK_NUMBER).json()
        # flaky 被惩罚，stable 优先；制造 stable 不可用后，flaky 已恢复可接管
        dispatcher._selector.record_failure("stable")
        dispatcher._selector.record_failure("stable")
        dispatcher._selector.record_failure("stable")
        assert "result" in client.post("/", json=_BLOCK_NUMBER).json()
    finally:
        flaky.stop(); stable.stop()
