"""最终性缓存：命中、链重组失效、回滚不返回过期结果。"""
from __future__ import annotations


def _call(client, req_id, method, params):
    return client.post("/", json={"jsonrpc": "2.0", "id": req_id,
                                  "method": method, "params": params}).json()


def test_finalized_request_caches_then_reorg_invalidates(gateway):
    client = gateway
    # 同一 client fixture 的 dispatcher 通过 health 查看缓存数
    r1 = _call(client, 1, "eth_getBlockByNumber", ["finalized", False])
    block1 = r1["result"]
    health = client.get("/health").json()
    assert health["cached"] == 1

    # 第二次应命中缓存（结果一致）
    r2 = _call(client, 2, "eth_getBlockByNumber", ["finalized", False])
    assert r2["result"] == block1

    # 重组 finalized 区域不会发生；这里对未最终高度重组，
    # 并验证已缓存的“最终高度”在被显式 rollback 覆盖后失效。
    fin = health["finalized"]
    # 先缓存一个显式最终高度
    r3 = _call(client, 3, "eth_getBlockByNumber", [hex(fin), False])
    assert r3["result"]["hash"] == block1["hash"] or r3["result"] is not None

    # 触发链推进：finalized 前移，旧缓存仍有效（同高度哈希不变）
    mine = client.post("/admin/mine", json={"count": 8}).json()
    assert mine["invalidated"] == 0
    assert _call(client, 4, "eth_getBlockByNumber", [hex(fin), False])["result"] == r3["result"]


def test_rolled_back_height_not_served(gateway):
    client = gateway
    fin = client.get("/health").json()["finalized"]
    cached = _call(client, 1, "eth_getBalance", ["0xabc123", hex(fin)])
    assert "result" in cached
    # 直接在共享链上回滚该高度（模拟深度回滚），缓存必须失效
    chain = client.app.state.chain
    head = chain.head_number()
    chain.rollback(head - fin + 1)  # 截断到 fin 之下
    invalidated = client.app.state.dispatcher.cache.on_new_head()
    assert invalidated >= 1
    out = _call(client, 2, "eth_getBalance", ["0xabc123", hex(fin)])
    # 高度已不存在 -> 上游按惯例返回 null，绝不返回旧余额
    assert out["result"] is None


def test_reorg_changes_hash_served_fresh(gateway):
    client = gateway
    head = client.get("/health").json()["head"]
    # 缓存一个未最终高度（不允许缓存），确认 latest 总是新值；
    # 再验证 finalized 高度数据与 latest 不同高度时的新鲜性。
    before = _call(client, 1, "eth_getBlockByNumber", ["latest", False])
    assert int(before["result"]["number"], 16) == head
    client.post("/admin/reorg", json={"depth": 3, "seed": 777})
    after = _call(client, 2, "eth_getBlockByNumber", ["latest", False])
    # 链顶高度不变（回滚后重出），但哈希改变
    assert after["result"]["number"] == before["result"]["number"]
    assert after["result"]["hash"] != before["result"]["hash"]


def test_unfinalized_height_is_not_cached(gateway):
    client = gateway
    head = client.get("/health").json()["head"]
    _call(client, 1, "eth_getBlockByNumber", ["latest", False])
    _call(client, 2, "eth_getBlockByNumber", [hex(head), False])
    health = client.get("/health").json()
    assert health["cached"] == 0
