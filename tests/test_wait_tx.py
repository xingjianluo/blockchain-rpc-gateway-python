"""等待交易确认 (gw_waitForTransaction) 测试。

覆盖：成功确认、最终确定、超时、重组回滚、并发同一交易合并轮询、
批量内不同项互不串扰、故障切换在等待期间生效。
"""
from __future__ import annotations

import asyncio

from gateway.errors import RpcErrorCode

TX_A = "0x" + "aa" * 32
TX_B = "0x" + "bb" * 32
TX_C = "0x" + "cc" * 32


def _wait_req(req_id, tx_hash, **options):
    return {
        "jsonrpc": "2.0", "id": req_id,
        "method": "gw_waitForTransaction",
        "params": {"txHash": tx_hash, **options},
    }


async def test_wait_confirmed_success(async_gateway, chain):
    """交易已上链，确认数达标后返回落块高度与确认数。"""
    client, _ = async_gateway
    height = chain.include_tx(TX_A)

    async def mine_later():
        await asyncio.sleep(0.12)
        chain.mine(2)

    task = asyncio.create_task(mine_later())
    resp = await client.post("/", json=_wait_req(
        1, TX_A, confirmations=2, timeoutMs=3000))
    await task

    body = resp.json()
    assert body["id"] == 1
    result = body["result"]
    assert result["status"] == "confirmed"
    assert result["txHash"] == TX_A
    assert result["blockNumber"] == hex(height)
    assert result["confirmations"] >= 2
    assert result["finalized"] is False
    assert result["blockHash"] == chain.block_hash_at(height)


async def test_wait_finalized_success(async_gateway, chain):
    """finalized=true 时等到所在高度最终确定才返回。"""
    client, _ = async_gateway
    height = chain.include_tx(TX_A)

    async def mine_later():
        await asyncio.sleep(0.1)
        chain.mine(chain.finalized_offset + 1)

    task = asyncio.create_task(mine_later())
    resp = await client.post("/", json=_wait_req(
        7, TX_A, finalized=True, timeoutMs=5000))
    await task

    result = resp.json()["result"]
    assert result["status"] == "finalized"
    assert result["finalized"] is True
    assert int(result["blockNumber"], 16) == height
    assert height <= chain.finalized_number()


async def test_wait_timeout_never_included(async_gateway, chain):
    """交易始终未上链：预算耗尽返回 -32020，而不是挂起或伪造结果。"""
    client, _ = async_gateway
    resp = await client.post("/", json=_wait_req(
        2, TX_B, confirmations=1, timeoutMs=250))
    body = resp.json()
    assert body["id"] == 2
    assert body["error"]["code"] == RpcErrorCode.WAIT_TIMEOUT
    assert body["error"]["data"]["lastKnown"] == "pending"


async def test_wait_invalid_params(async_gateway):
    client, _ = async_gateway
    for bad in (
        {"txHash": "not-hex"},
        {"txHash": TX_A, "confirmations": 0},
        {"txHash": TX_A, "timeoutMs": -5},
        {},
    ):
        resp = await client.post("/", json={
            "jsonrpc": "2.0", "id": 9,
            "method": "gw_waitForTransaction", "params": bad})
        assert resp.json()["error"]["code"] == RpcErrorCode.INVALID_PARAMS, bad


async def test_concurrent_waiters_same_tx_coalesced(async_gateway, chain, cluster):
    """N 个调用方并发等同一笔交易：共享一路上游轮询，各自结果对齐自己的 id。"""
    client, dispatcher = async_gateway
    height = chain.include_tx(TX_A)

    async def mine_later():
        await asyncio.sleep(0.15)
        chain.mine(3)

    task = asyncio.create_task(mine_later())
    before = sum(s._fault.calls for s in cluster)
    n = 8
    responses = await asyncio.gather(*[
        client.post("/", json=_wait_req(i, TX_A, confirmations=2,
                                        timeoutMs=4000))
        for i in range(n)
    ])
    await task
    upstream_calls = sum(s._fault.calls for s in cluster) - before

    # 每个等待方拿到自己的 id，结果内容一致且都达标。
    for i, resp in enumerate(responses):
        body = resp.json()
        assert body["id"] == i
        assert body["result"]["txHash"] == TX_A
        assert body["result"]["blockNumber"] == hex(height)
        assert body["result"]["confirmations"] >= 2
    # 8 个等待方合并为一路轮询：上游调用远小于逐等待方轮询的规模。
    watch_polls = upstream_calls
    assert watch_polls < n * 2, f"轮询未合并？上游调用 {watch_polls} 次"
    assert dispatcher.waiters.active_watches == 0  # 全部结束后轮询已停止


async def test_batch_items_isolated(async_gateway, chain):
    """同一批次里：一项成功、一项超时、一项等另一笔交易，结果严格对位。"""
    client, _ = async_gateway
    height_a = chain.include_tx(TX_A)
    # TX_B 永不上链（会超时）；TX_C 稍后上链到另一个高度。
    c_height: dict[str, int] = {}

    async def include_c_later():
        await asyncio.sleep(0.2)
        chain.mine(1)
        c_height["h"] = chain.include_tx(TX_C)

    task = asyncio.create_task(include_c_later())
    batch = [
        _wait_req("ok-a", TX_A, confirmations=1, timeoutMs=3000),
        _wait_req("timeout-b", TX_B, confirmations=1, timeoutMs=300),
        _wait_req("ok-c", TX_C, confirmations=1, timeoutMs=3000),
        {"jsonrpc": "2.0", "id": "plain", "method": "eth_blockNumber",
         "params": []},
    ]
    resp = await client.post("/", json=batch)
    await task

    bodies = {item["id"]: item for item in resp.json()}
    assert set(bodies) == {"ok-a", "timeout-b", "ok-c", "plain"}

    ok_a = bodies["ok-a"]["result"]
    assert ok_a["txHash"] == TX_A
    assert ok_a["blockNumber"] == hex(height_a)

    err_b = bodies["timeout-b"]["error"]
    assert err_b["code"] == RpcErrorCode.WAIT_TIMEOUT
    assert err_b["data"]["txHash"] == TX_B  # 不能串到 TX_A 的结果

    ok_c = bodies["ok-c"]["result"]
    assert ok_c["txHash"] == TX_C
    # C 的结果必须对得上 C 自己的落块位置，而不是 A 的。
    assert int(ok_c["blockNumber"], 16) == c_height["h"] != height_a
    assert ok_c["blockHash"] == chain.block_hash_at(c_height["h"])

    assert "result" in bodies["plain"]


async def test_reorg_during_wait_then_reincluded(async_gateway, chain):
    """等待期间交易被重组回滚又重新上链：按新位置给出确认结论。"""
    client, _ = async_gateway
    old_height = chain.include_tx(TX_A)
    old_hash = chain.block_hash_at(old_height)

    async def reorg_and_reinclude():
        await asyncio.sleep(0.15)
        # 回滚包含 TX_A 的区块（交易随之掉落），再以新 epoch 重出。
        chain.reorg(chain.head_number() - old_height + 1)
        assert chain.tx_receipt(TX_A) is None
        await asyncio.sleep(0.15)
        chain.mine(1)
        chain.include_tx(TX_A)  # 重新上链到新链顶
        chain.mine(1)

    task = asyncio.create_task(reorg_and_reinclude())
    resp = await client.post("/", json=_wait_req(
        3, TX_A, confirmations=2, timeoutMs=5000))
    await task

    result = resp.json()["result"]
    new_height = int(result["blockNumber"], 16)
    assert result["txHash"] == TX_A
    # 必须按重组后的新位置确认，绝不能返回回滚前的旧高度/旧哈希。
    assert result["blockHash"] != old_hash
    assert result["blockHash"] == chain.block_hash_at(new_height)
    assert result["confirmations"] >= 2


async def test_reorg_not_recovered_returns_reorged(async_gateway, chain):
    """被重组回滚且预算内未重新上链：返回 -32021，与成功/普通超时区分。"""
    client, _ = async_gateway
    height = chain.include_tx(TX_A)

    async def reorg_only():
        await asyncio.sleep(0.15)
        chain.reorg(chain.head_number() - height + 1)

    task = asyncio.create_task(reorg_only())
    resp = await client.post("/", json=_wait_req(
        4, TX_A, confirmations=3, timeoutMs=1200))
    await task

    body = resp.json()
    assert body["error"]["code"] == RpcErrorCode.TX_REORGED
    assert body["error"]["data"]["txHash"] == TX_A
    assert body["error"]["data"]["lastIncluded"]["blockNumber"] == hex(height)


async def test_wait_respects_upstream_failover(async_gateway, cluster, chain):
    """等待轮询期间首选节点故障：自动切换到健康节点，等待仍正常完成。"""
    client, _ = async_gateway
    # 让全部节点里延迟最低的 node-a 永久连接故障，逼轮询走降级路径。
    cluster[0]._fault.always_connection_reset = True
    chain.include_tx(TX_A)

    resp = await client.post("/", json=_wait_req(
        5, TX_A, confirmations=1, timeoutMs=3000))
    result = resp.json()["result"]
    assert result["txHash"] == TX_A
    # node-a 被尝试过（并失败），最终由其他节点服务。
    assert cluster[0]._fault.calls > 0
    assert cluster[1]._fault.calls + cluster[2]._fault.calls > 0


async def test_wait_method_forbidden_by_policy(gateway_factory):
    """方法级访问限制对等待方法同样生效。"""
    from gateway.policy import MethodPolicy
    pol = MethodPolicy(denied={"gw_waitForTransaction"})
    client, _ = gateway_factory(pol=pol)
    resp = client.post("/", json=_wait_req(6, TX_A, timeoutMs=100))
    assert resp.json()["error"]["code"] == RpcErrorCode.METHOD_FORBIDDEN
