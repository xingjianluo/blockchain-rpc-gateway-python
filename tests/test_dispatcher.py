"""多上游故障切换与故障分类测试。"""
from __future__ import annotations

import logging

import pytest

from gateway.engine.retry import classify_failure
from gateway.errors import (
    INVALID_PARAMS,
    METHOD_NOT_FOUND,
    AllUpstreamsFailed,
    RpcBusinessError,
    UpstreamConnRefused,
    UpstreamTimeout,
)
from gateway.models import RpcRequest

logger = logging.getLogger("test.dispatcher")


async def test_healthy_request_prefers_lowest_latency_node(env):
    resp = await env.dispatcher.dispatch(
        RpcRequest("web3_clientVersion", [], 1)
    )
    assert resp.error is None
    # node-a 延迟最低，应被选中
    assert resp.upstream == "node-a"
    logger.info("selected upstream = %s", resp.upstream)


async def test_transport_failure_failover_to_next_node(env, caplog):
    # node-a 连接拒绝 -> 必须切换到 node-b 重试并成功
    env.transports["node-a"].fail_connect = True
    resp = await env.dispatcher.dispatch(
        RpcRequest("web3_clientVersion", [], 2)
    )
    assert resp.error is None, resp.error
    assert resp.upstream == "node-b"
    assert env.transports["node-a"].calls == 0
    assert env.transports["node-b"].calls >= 1
    # 日志中必须出现重试原因与所选节点
    joined = "\n".join(r.getMessage() for r in caplog.records)
    assert "transport FAILED" in joined
    assert "node-a" in joined and "retryable" in joined
    logger.info("failover selected upstream = %s", resp.upstream)


async def test_timeout_is_retried_but_business_error_is_not(env, caplog):
    # node-a 对该方法超时 -> 切 node-b
    env.transports["node-a"].timeout_methods = {"eth_blockNumber"}
    resp = await env.dispatcher.dispatch(
        RpcRequest("eth_blockNumber", [], 3)
    )
    assert resp.error is None and resp.upstream == "node-b"
    joined = "\n".join(r.getMessage() for r in caplog.records)
    assert "UpstreamTimeout" in joined
    logger.info("timeout failover -> %s", resp.upstream)

    caplog.clear()
    # 业务错误（方法不存在）：-32601 绝不切换/重试其它节点。
    # 上半段 node-a 超时后健康度下降，此请求首选已是 node-b；
    # 在 node-b 注入业务错误，验证“拿到 JSON-RPC error 即立即返回”。
    env.transports["node-b"].unknown_methods = {"eth_missingMethod"}
    before_c = env.transports["node-c"].calls
    resp2 = await env.dispatcher.dispatch(
        RpcRequest("eth_missingMethod", [], 4)
    )
    assert resp2.is_error
    assert resp2.error["code"] == METHOD_NOT_FOUND
    assert resp2.upstream == "node-b"
    # 业务错误不重试：node-c 不应被尝试
    assert env.transports["node-c"].calls == before_c
    joined2 = "\n".join(r.getMessage() for r in caplog.records)
    assert "business error" in joined2 and "NOT retrying" in joined2
    logger.info("business error code=%s not retried", resp2.error["code"])


async def test_invalid_params_business_error_not_masked(env):
    env.transports["node-a"].bad_params_methods = {"eth_getBlockByNumber"}
    resp = await env.dispatcher.dispatch(
        RpcRequest("eth_getBlockByNumber", ["bad-arg"], 5)
    )
    assert resp.is_error
    assert resp.error["code"] == INVALID_PARAMS
    # 未切换节点：node-b/c 不应收到该请求
    assert env.transports["node-b"].calls == 0
    assert env.transports["node-c"].calls == 0


async def test_all_nodes_transport_failure_returns_distinguishable_code(env):
    for name in ("node-a", "node-b", "node-c"):
        env.transports[name].fail_connect = True
    with pytest.raises(AllUpstreamsFailed) as exc:
        await env.dispatcher.dispatch(
            RpcRequest("eth_blockNumber", [], 6)
        )
    assert exc.value.code == -32000
    assert set(exc.value.data["attempts"]) == {"node-a", "node-b", "node-c"}
    logger.info("all-upstreams-failed attempts=%s", exc.value.data["attempts"])


async def test_circuit_breaker_opens_after_repeated_failures(env):
    for name in ("node-a", "node-b", "node-c"):
        env.transports[name].fail_connect = True
    # 每次请求都会按顺序尝试各节点，node-a 每轮失败一次
    for _ in range(3):
        with pytest.raises(AllUpstreamsFailed):
            await env.dispatcher.dispatch(RpcRequest("web3_clientVersion", [], 7))
    # 连续失败 3 次后全部熔断，选择顺序为空
    assert env.selector.ordered(env.clock()) == []
    logger.info("all nodes open after repeated failures")

    # 冷却时间过去后节点回到候选（半开探测）
    env.clock.advance(60.0)
    assert env.selector.ordered(env.clock())[0] == "node-a"
    # node-a 恢复：下一次请求探测成功并关闭熔断
    env.transports["node-a"].fail_connect = False
    resp = await env.dispatcher.dispatch(RpcRequest("web3_clientVersion", [], 8))
    assert resp.upstream == "node-a"
    assert env.health["node-a"].consecutive_failures == 0


def test_failure_classification_reasons():
    d_timeout = classify_failure(UpstreamTimeout())
    d_conn = classify_failure(UpstreamConnRefused())
    d_biz = classify_failure(RpcBusinessError(METHOD_NOT_FOUND, "nf"))
    assert d_timeout.should_retry and d_conn.should_retry
    assert not d_biz.should_retry
    # 判定依据必须明确出现在 reason 中
    assert "transport" in d_timeout.reason
    assert "deterministic" in d_biz.reason
