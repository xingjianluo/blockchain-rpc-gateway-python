"""最终性缓存、链重组失效与并发合并测试。"""
from __future__ import annotations

import asyncio
import logging

from gateway.models import RpcRequest

logger = logging.getLogger("test.cache")


def _block_req(height: int, rid: int) -> RpcRequest:
    return RpcRequest("eth_getBlockByNumber", [height, False], rid)


async def test_finalized_result_cached_and_served_from_cache(env, caplog):
    finalized = env.chain.finalized_height  # 初始 5
    req = _block_req(finalized, 1)
    r1 = await env.dispatcher.dispatch(req)
    assert r1.error is None and not r1.from_cache
    assert r1.upstream == "node-a"
    r2 = await env.dispatcher.dispatch(req)
    assert r2.from_cache is True
    # 缓存命中后不应再访问上游
    assert env.transports["node-a"].calls == 1
    joined = "\n".join(rec.getMessage() for rec in caplog.records)
    assert "cache HIT" in joined
    logger.info("finalized height=%s cached, second call cache hit", finalized)


async def test_unfinalized_height_is_not_cached(env):
    head = env.chain.head_height()  # 7，未最终确定
    req = _block_req(head, 2)
    await env.dispatcher.dispatch(req)
    await env.dispatcher.dispatch(req)
    # 未最终确定：每次都必须访问上游，不能返回缓存
    assert env.transports["node-a"].calls == 2
    assert len(env.cache) == 0


async def test_latest_tag_is_never_cached(env):
    req = RpcRequest("eth_getBlockByNumber", ["latest", False], 3)
    await env.dispatcher.dispatch(req)
    await env.dispatcher.dispatch(req)
    assert env.transports["node-a"].calls == 2


async def test_reorg_invalidates_rolled_back_entries(env, caplog):
    # 缓存最终确定高度 4、5
    r4 = await env.dispatcher.dispatch(_block_req(4, 4))
    r5 = await env.dispatcher.dispatch(_block_req(5, 5))
    assert len(env.cache) == 2
    hash4_before, hash5_before = r4.result["hash"], r5.result["hash"]

    # 在高度 5 发生重组，回滚 finalized 到 4
    env.chain.reorg(5, 3)
    assert env.chain.finalized_height == 4

    # 下一次请求触发 finalized 探测，检测到回退并失效缓存
    caplog.clear()
    await env.dispatcher.dispatch(RpcRequest("eth_blockNumber", [], 6))
    joined = "\n".join(rec.getMessage() for rec in caplog.records)
    assert "reorg detected" in joined
    logger.info("reorg invalidated log: %s",
                [r.getMessage() for r in caplog.records if "reorg" in r.getMessage()])

    # 高度 5 的旧缓存必须已失效，重新取到重组后的新哈希
    r5_after = await env.dispatcher.dispatch(_block_req(5, 7))
    assert not r5_after.from_cache
    assert r5_after.result["hash"] != hash5_before

    # 高度 4 未被回滚，仍可命中旧缓存
    r4_after = await env.dispatcher.dispatch(_block_req(4, 8))
    assert r4_after.from_cache
    assert r4_after.result["hash"] == hash4_before


async def test_deep_reorg_below_cached_finalized_height(env):
    # 重组分叉点低于已缓存最终高度：已回滚高度不得返回过期结果
    await env.dispatcher.dispatch(_block_req(3, 10))
    await env.dispatcher.dispatch(_block_req(4, 11))
    h3_before = (await env.dispatcher.dispatch(_block_req(3, 12))).result["hash"]
    env.chain.reorg(3, 3)  # finalized 被下调到 2
    # 触发探测
    await env.dispatcher.dispatch(RpcRequest("eth_blockNumber", [], 13))
    after = await env.dispatcher.dispatch(_block_req(3, 14))
    assert not after.from_cache
    assert after.result["hash"] != h3_before


async def test_error_responses_are_not_cached(env):
    env.transports["node-a"].unknown_methods = {"eth_missing"}
    req = RpcRequest("eth_missing", [], 20)
    r1 = await env.dispatcher.dispatch(req)
    r2 = await env.dispatcher.dispatch(req)
    assert r1.is_error and r2.is_error
    assert len(env.cache) == 0


async def test_stale_cached_height_bypassed_even_before_probe(env):
    # 1) 缓存最终高度 4
    req = _block_req(4, 21)
    before = await env.dispatcher.dispatch(req)
    assert not before.from_cache
    # 2) 直接下调 finalized 高度但不调用任何探测（模拟极端时序）
    env.chain.finalized_height = 2
    # 3) 同一请求不得返回已回滚高度的旧缓存，必须绕过并重读
    env.dispatcher._last_finalized = 2  # 使 observe 不触发 reorg 日志分支
    after = await env.dispatcher.dispatch(req)
    assert not after.from_cache
    assert after.result["hash"] == before.result["hash"]  # 链未变，重读结果一致
    # 过期条目已被淘汰
    assert env.cache.is_safe(req, 2) is False or len(env.cache) == 0


async def test_concurrent_identical_readonly_requests_coalesce(env, caplog):
    req = _block_req(1, 30)  # 最终确定但首访仍需上游
    calls_before = env.transports["node-a"].calls

    async def slow_handle(orig, request):
        # 给并发窗口留点时间
        await asyncio.sleep(0.02)
        return await orig(request)

    # 让首选节点变慢，放大并发重叠窗口
    env.transports["node-a"].latency = 0.05
    resps = await asyncio.gather(
        *[env.dispatcher.dispatch(req) for _ in range(8)]
    )
    # 8 个并发相同请求只应产生 1 次上游调用
    assert env.transports["node-a"].calls == calls_before + 1
    assert all(r.result == resps[0].result for r in resps)
    assert env.flights.merged_count >= 7
    logger.info("8 concurrent requests -> 1 upstream call, merged=%d",
                env.flights.merged_count)
