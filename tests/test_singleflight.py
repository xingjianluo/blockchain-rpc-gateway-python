"""并发相同只读请求合并 (single-flight) 测试。"""
from __future__ import annotations

import asyncio

import pytest

from gateway.cache import SingleFlight


def test_concurrent_same_key_single_producer_call():
    sf = SingleFlight()
    calls = {"n": 0}

    async def producer():
        calls["n"] += 1
        await asyncio.sleep(0.02)
        return "one-upstream-call"

    async def scenario():
        return await asyncio.gather(*[sf.run("k", producer) for _ in range(40)])

    results = asyncio.run(scenario())
    assert calls["n"] == 1
    assert results == ["one-upstream-call"] * 40


def test_different_keys_run_separately():
    sf = SingleFlight()
    seen: list[str] = []

    async def make(label: str):
        async def producer():
            seen.append(label)
            await asyncio.sleep(0.01)
            return label
        return await sf.run(label, producer)

    async def scenario():
        return await asyncio.gather(make("a"), make("b"))

    assert asyncio.run(scenario()) == ["a", "b"]
    assert sorted(seen) == ["a", "b"]


@pytest.mark.asyncio
async def test_gateway_coalesces_concurrent_finalized_requests(
    async_gateway, cluster
):
    """端到端：同一事件循环内 N 个相同 finalized 请求只产生一次上游调用。"""
    client, _dispatcher = async_gateway
    payload = {"jsonrpc": "2.0", "id": 0,
               "method": "eth_getBlockByNumber", "params": ["finalized", False]}

    before = sum(s._fault.calls for s in cluster)
    responses = await asyncio.gather(*[
        client.post("/", json=payload) for _ in range(12)
    ])
    upstream_calls = sum(s._fault.calls for s in cluster) - before

    assert all(r.status_code == 200 for r in responses)
    hashes = {r.json()["result"]["hash"] for r in responses}
    assert len(hashes) == 1
    # 12 个并发请求必须合并为 1 次上游调用。
    assert upstream_calls == 1, f"期望合并为1次，实际上游调用 {upstream_calls} 次"


@pytest.mark.asyncio
async def test_gateway_different_requests_not_coalesced(async_gateway, cluster):
    client, _ = async_gateway
    before = sum(s._fault.calls for s in cluster)
    await asyncio.gather(
        client.post("/", json={"jsonrpc": "2.0", "id": 1,
                               "method": "eth_blockNumber", "params": []}),
        client.post("/", json={"jsonrpc": "2.0", "id": 2,
                               "method": "eth_chainId", "params": []}),
    )
    calls = sum(s._fault.calls for s in cluster) - before
    assert calls == 2
