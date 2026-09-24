"""SingleFlight 请求合并的独立边界测试。"""
from __future__ import annotations

import asyncio

from gateway.cache.singleflight import SingleFlight


async def test_concurrent_same_key_single_invocation():
    sf = SingleFlight()
    invocations = 0

    async def factory():
        nonlocal invocations
        invocations += 1
        await asyncio.sleep(0.01)
        return "v"

    out = await asyncio.gather(*(sf.do("k", factory) for _ in range(20)))
    assert out == ["v"] * 20
    assert invocations == 1
    assert sf.merged_count == 19


async def test_distinct_keys_not_merged():
    sf = SingleFlight()
    invocations = {"a": 0, "b": 0}

    async def factory(key):
        async def f():
            invocations[key] += 1
            await asyncio.sleep(0.01)
            return key
        return f

    await asyncio.gather(
        sf.do("a", await factory("a")),
        sf.do("b", await factory("b")),
    )
    assert invocations == {"a": 1, "b": 1}


async def test_business_exception_propagates_to_all_waiters():
    sf = SingleFlight()

    class Biz(Exception):
        pass

    async def boom():
        await asyncio.sleep(0.01)
        raise Biz("deterministic business failure")

    async def call():
        try:
            await sf.do("k", boom)
        except Biz:
            return "caught"

    out = await asyncio.gather(*(call() for _ in range(5)))
    assert out == ["caught"] * 5


async def test_after_completion_next_call_runs_again():
    sf = SingleFlight()
    invocations = 0

    async def factory():
        nonlocal invocations
        invocations += 1
        return invocations

    assert await sf.do("k", factory) == 1
    # 上一个调用结束后，新调用应重新执行而非复用旧结果
    assert await sf.do("k", factory) == 2
    assert invocations == 2
