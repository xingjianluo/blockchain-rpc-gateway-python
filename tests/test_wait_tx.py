"""等待交易确认 (gw_waitForTransaction) 测试。

覆盖：
- 并发等同一笔交易：轮询合并、结果各自归属；
- 批次中不同项（不同交易/成功/超时）互不串味；
- 等待期间重组后重新上链（按新位置结算，标注 reorged）；
- 重组后交易彻底回不来（-32021 TX_REORGED，与正常确认/超时区分）；
- 预算耗尽仍未达标（-32020 WAIT_TIMEOUT，绝不提前返回未达标结果）；
- 等待过程沿用上游选择/重试、最终性缓存与方法访问限制。
"""
from __future__ import annotations

import asyncio

import pytest

TX1 = "0xaa00000000000000000000000000000000000000000000000000000000000001"
TX2 = "0xbb00000000000000000000000000000000000000000000000000000000000002"
TX3 = "0xcc00000000000000000000000000000000000000000000000000000000000003"


def _wait_payload(req_id, tx_hash, *, confirmations=None, finalized=None,
                  timeout_ms=4000, poll_ms=30):
    params: dict = {"transactionHash": tx_hash, "timeoutMs": timeout_ms,
                    "pollIntervalMs": poll_ms}
    if confirmations is not None:
        params["confirmations"] = confirmations
    if finalized is not None:
        params["finalized"] = finalized
    return {"jsonrpc": "2.0", "id": req_id,
            "method": "gw_waitForTransaction", "params": params}


def _upstream_calls(cluster) -> int:
    return sum(s._fault.calls for s in cluster)


@pytest.mark.asyncio
async def test_wait_already_included_confirmed(async_gateway, chain):
    """最小成功路径：交易已在链上且满足 1 确认，立即返回 confirmed。"""
    client, _ = async_gateway
    chain.send_tx(TX1)
    chain.mine(1)

    resp = await client.post("/", json=_wait_payload("s1", TX1,
                                                    confirmations=1,
                                                    timeout_ms=2000))
    body = resp.json()
    assert "error" not in body, body
    result = body["result"]
    assert result["status"] == "confirmed"
    assert result["transactionHash"] == TX1
    assert int(result["confirmations"], 16) >= 1
    assert result["reorged"] is False


@pytest.mark.asyncio
async def test_concurrent_waiters_same_tx_share_polls(async_gateway, chain, cluster):
    """N 个调用方等同一笔交易：收据轮询只打一份；每个结果归属各自 req_id。"""
    client, dispatcher = async_gateway
    chain.send_tx(TX1)

    async def miner():
        await asyncio.sleep(0.1)
        chain.mine(3)

    before = _upstream_calls(cluster)
    responses = await asyncio.gather(
        *[client.post("/", json=_wait_payload(f"w{i}", TX1,
                                             confirmations=2,
                                             timeout_ms=3000,
                                             poll_ms=20))
          for i in range(8)],
        miner(),
    )
    mid = _upstream_calls(cluster)
    bodies = [r.json() for r in responses[:-1]]

    # 每个等待方都拿到自己的 id 与同一笔交易的结论，绝不串。
    assert {b["id"] for b in bodies} == {f"w{i}" for i in range(8)}
    for b in bodies:
        assert "error" not in b, b
        r = b["result"]
        assert r["status"] == "confirmed"
        assert r["transactionHash"] == TX1
        assert int(r["confirmations"], 16) >= 2
        assert r["reorged"] is False

    shared_calls = mid - before

    # 等待期间 finalized 查询走最终性缓存：预热回填后后续轮询必须命中。
    assert dispatcher.cache.hits >= 1, "等待轮询的 finalized 查询应命中缓存"

    # 对照：单个等待方等另一笔交易，经历相近的等待时长与轮询节奏。
    chain.send_tx(TX2)

    async def miner2():
        await asyncio.sleep(0.1)
        chain.mine(2)

    await asyncio.gather(
        client.post("/", json=_wait_payload("solo", TX2,
                                           confirmations=1,
                                           timeout_ms=3000,
                                           poll_ms=20)),
        miner2(),
    )
    solo_calls = _upstream_calls(cluster) - mid

    # 合并：8 个等待方的调用量应与 1 个等待方处于同一量级
    # （未合并时会接近 8 倍）。允许单轮抖动的余量。
    assert solo_calls > 0
    assert shared_calls <= solo_calls * 2, (
        f"期望8等待方轮询被合并(≈{solo_calls}次)，实际 {shared_calls} 次"
    )


@pytest.mark.asyncio
async def test_batch_mixed_items_do_not_cross(async_gateway, chain):
    """同一批次：成功/等待超时/普通只读 三类项目并存，结果严格对号入座。"""
    client, _ = async_gateway
    # TX1 已上链并拥有足够确认；TX3 始终不上链。
    chain.send_tx(TX1)
    chain.mine(4)

    batch = [
        _wait_payload("ok", TX1, confirmations=2, timeout_ms=1500, poll_ms=20),
        _wait_payload("late", TX3, confirmations=1, timeout_ms=200, poll_ms=20),
        {"jsonrpc": "2.0", "id": "ro",
         "method": "eth_blockNumber", "params": []},
        _wait_payload("ok2", TX1, finalized=True, timeout_ms=1500, poll_ms=20),
    ]
    resp = await client.post("/", json=batch)
    by_id = {item["id"]: item for item in resp.json()}

    # 四项全部回位，无错位、无丢失。
    assert set(by_id) == {"ok", "late", "ro", "ok2"}

    ok = by_id["ok"]["result"]
    assert ok["status"] == "confirmed" and ok["transactionHash"] == TX1
    assert int(ok["confirmations"], 16) >= 2

    # TX1 落定在高度 head-3.. 处；finalized 项只有真正最终确定才成功，
    # 否则必须明确超时，不能把未最终结果当成功返回。
    tx_height = int(ok["blockNumber"], 16)
    if tx_height <= chain.finalized_number():
        assert "error" not in by_id["ok2"]
        assert by_id["ok2"]["result"]["finalized"] is True
    else:
        err = by_id["ok2"]["error"]
        assert err["code"] == -32020
        assert err["data"]["transactionHash"] == TX1

    # 超时项：独立错误、明确 -32020，且 id 不串。
    err = by_id["late"]["error"]
    assert err["code"] == -32020
    assert err["data"]["transactionHash"] == TX3
    assert err["data"]["included"] is False

    # 普通只读项照常返回，不被等待逻辑污染。
    assert int(by_id["ro"]["result"], 16) == chain.head_number()


@pytest.mark.asyncio
async def test_reorg_during_wait_reincluded_at_new_position(async_gateway, chain):
    """等待中交易先上链、再被重组回滚，随后重新上链：按新位置确认并标注 reorged。"""
    client, _ = async_gateway
    chain.send_tx(TX1)
    chain.mine(1)  # TX1 上链但只有 1 确认，而条件要 3 -> 等待方必须继续轮询
    old_height, old_hash = chain.locate_tx(TX1)
    head = chain.head_number()
    assert head - old_height + 1 == 1

    async def scenario():
        task = asyncio.create_task(client.post(
            "/", json=_wait_payload("r2", TX1, confirmations=3,
                                    timeout_ms=3000, poll_ms=20),
        ))
        await asyncio.sleep(0.06)   # watcher 至少观察到一次旧位置
        chain.reorg(depth=1, rng_seed=321)  # 回滚 -> 交易退回池中、链上消失
        await asyncio.sleep(0.06)   # watcher 观察到“曾上链 -> 消失”
        chain.mine(3)               # 重新打包 + 给足 3 确认
        return await task

    body = (await scenario()).json()
    assert "error" not in body, body
    r = body["result"]
    assert r["status"] == "confirmed"
    assert r["transactionHash"] == TX1
    assert r["reorged"] is True
    new_height = int(r["blockNumber"], 16)
    assert r["blockHash"] != old_hash
    assert int(r["confirmations"], 16) >= 3
    # 新位置必须是重组后链上的真实位置。
    assert chain.locate_tx(TX1) == (new_height, r["blockHash"])


@pytest.mark.asyncio
async def test_reorged_out_and_never_returns(async_gateway, chain):
    """等待中交易被重组且彻底丢弃：必须返回 -32021，与成功确认/普通超时区分。"""
    client, _ = async_gateway
    chain.send_tx(TX1)
    chain.mine(1)  # 1 确认，条件要 3，等待中
    old = chain.locate_tx(TX1)

    async def scenario():
        task = asyncio.create_task(client.post(
            "/", json=_wait_payload("d1", TX1, confirmations=3,
                                    timeout_ms=600, poll_ms=20),
        ))
        await asyncio.sleep(0.06)
        chain.reorg(depth=1, rng_seed=654, dropped_txs={TX1})  # 交易彻底丢失
        return await task

    body = (await scenario()).json()
    err = body["error"]
    assert err["code"] == -32021
    assert err["data"]["transactionHash"] == TX1
    assert err["data"]["reorged"] is True
    assert "reorged" in err["message"]
    # 绝不能把被重组前的位置当成功结果返回。
    assert chain.locate_tx(TX1) is None
    assert old is not None


@pytest.mark.asyncio
async def test_timeout_never_included(async_gateway, chain):
    """预算耗尽交易始终未上链：-32020，且不提前返回未达标结果、不挂起。"""
    client, _ = async_gateway

    async def scenario():
        t0 = asyncio.get_event_loop().time()
        resp = await client.post(
            "/", json=_wait_payload("t1", TX3, confirmations=1,
                                   timeout_ms=250, poll_ms=20),
        )
        elapsed = asyncio.get_event_loop().time() - t0
        return resp, elapsed

    resp, elapsed = await scenario()
    err = resp.json()["error"]
    assert err["code"] == -32020
    assert err["data"]["transactionHash"] == TX3
    assert err["data"]["included"] is False
    # 大致在预算之后很快返回，而不是一直挂着（容忍调度余量）。
    assert 0.25 <= elapsed < 1.0


@pytest.mark.asyncio
async def test_no_early_result_before_depth_reached(async_gateway, chain):
    """交易已上链但确认数不够时，绝不能提前返回 confirmed，应继续等到达标。"""
    client, _ = async_gateway
    chain.send_tx(TX2)
    chain.mine(1)  # 只有 1 确认

    async def scenario():
        task = asyncio.create_task(client.post(
            "/", json=_wait_payload("n1", TX2, confirmations=4,
                                    timeout_ms=2000, poll_ms=20),
        ))
        await asyncio.sleep(0.08)
        chain.mine(2)  # 累计 3 确认，仍不够
        await asyncio.sleep(0.08)
        chain.mine(1)  # 累计 4 确认
        return await task

    r = (await scenario()).json()["result"]
    assert r["status"] == "confirmed"
    assert int(r["confirmations"], 16) >= 4
    assert r["transactionHash"] == TX2


@pytest.mark.asyncio
async def test_wait_method_denied_by_policy(chain, nodes):
    """等待方法同样受方法级访问限制：白名单未包含时网关直接拒绝，不轮询上游。"""
    import httpx
    from httpx import ASGITransport

    from gateway.app import create_app
    from gateway.dispatcher import Dispatcher
    from gateway.policy import MethodPolicy

    # 只允许两个标准只读方法 -> gw_waitForTransaction 被挡在网关侧。
    pol = MethodPolicy(
        allowed={"eth_blockNumber", "eth_getTransactionReceipt"},
        max_batch_size=20,
    )
    dispatcher = Dispatcher(nodes, chain, pol, timeout=2.0)
    app = create_app(chain=chain, policy=pol, dispatcher=dispatcher)
    transport = ASGITransport(app=app)
    try:
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://gw") as ac:
            resp = await ac.post(
                "/", json=_wait_payload("p1", TX1, timeout_ms=300),
            )
            err = resp.json()["error"]
            assert err["code"] == -32001
            assert "gw_waitForTransaction" in err["message"]
    finally:
        await dispatcher.aclose()


@pytest.mark.asyncio
async def test_invalid_wait_params_async(async_gateway):
    client, dispatcher = async_gateway
    # 非法参数下不得创建任何 watcher。
    cases = [
        ({"confirmations": 2}, "missing hash"),
        ({"transactionHash": TX1, "confirmations": 0}, "zero conf"),
        ({"transactionHash": TX1, "timeoutMs": -5}, "bad timeout"),
        ({"transactionHash": "not-hex"}, "bad hash"),
    ]
    for params, _label in cases:
        body = (await client.post("/", json={
            "jsonrpc": "2.0", "id": "x",
            "method": "gw_waitForTransaction", "params": params,
        })).json()
        assert body["error"]["code"] == -32602, params
    assert dispatcher.wait_coordinator._watchers == {}


@pytest.mark.asyncio
async def test_wait_survives_upstream_failover(chain, policy, cluster):
    """等待轮询期间上游传输故障必须自动降级重试，等待方最终仍拿到结论。"""
    import httpx
    from httpx import ASGITransport

    from gateway.app import create_app
    from gateway.dispatcher import Dispatcher
    from gateway.models import UpstreamNode

    # node-a 前 3 次调用连接重置，之后恢复；其余节点正常。
    cluster[0]._fault.fail_first = 3
    nodes = [UpstreamNode(name=f"node-{c}", url=s.url)
             for c, s in zip(("a", "b", "c"), cluster)]
    dispatcher = Dispatcher(nodes, chain, policy, timeout=1.0)

    # 记录等待期间每一轮内部轮询的节点与降级原因。
    observed: list[str] = []
    original_poll = dispatcher.dispatch_poll

    async def tracked(req):
        res = await original_poll(req)
        observed.extend(res.reasons)
        return res

    dispatcher.dispatch_poll = tracked
    # 协调器在构造时已绑定 self，dispatch_poll 经由 self 调用 -> 需要同步替换
    # 协调器实际使用的入口。
    dispatcher.wait_coordinator.dispatcher = dispatcher

    app = create_app(chain=chain, policy=policy, dispatcher=dispatcher)
    transport = ASGITransport(app=app)
    try:
        chain.send_tx(TX1)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://gw") as ac:
            task = asyncio.create_task(ac.post(
                "/", json=_wait_payload("f1", TX1, confirmations=2,
                                        timeout_ms=3000, poll_ms=20),
            ))
            await asyncio.sleep(0.15)  # 覆盖 node-a 故障窗口内的若干轮
            chain.mine(2)
            body = (await task).json()
        assert "error" not in body, body
        r = body["result"]
        assert r["status"] == "confirmed"
        assert r["transactionHash"] == TX1
        # 必须真的观测到“传输故障 -> 切换节点重试”的降级轨迹。
        joined = "\n".join(observed)
        assert "node-a" in joined and "切换节点重试" in joined, joined
    finally:
        await dispatcher.aclose()


@pytest.mark.asyncio
async def test_wait_until_finalized(async_gateway, chain):
    """finalized=true：交易必须等到所在区块最终确定，未最终前不得报成功。"""
    client, _ = async_gateway
    chain.send_tx(TX1)

    async def scenario():
        task = asyncio.create_task(client.post(
            "/", json=_wait_payload("fin", TX1, finalized=True,
                                    timeout_ms=3000, poll_ms=20),
        ))
        await asyncio.sleep(0.08)
        chain.mine(2)  # 刚上链，远未最终确定
        await asyncio.sleep(0.1)
        assert not task.done(), "未最终确定不得提前返回"
        chain.mine(chain.finalized_offset + 1)  # 推进到交易所在区块最终确定
        return await task

    body = (await scenario()).json()
    assert "error" not in body, body
    r = body["result"]
    assert r["status"] == "confirmed"
    assert r["finalized"] is True
    assert int(r["blockNumber"], 16) <= chain.finalized_number()
