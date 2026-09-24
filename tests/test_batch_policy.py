"""方法级访问策略与批量约束测试。"""
from __future__ import annotations

import logging

from gateway.engine.batch import BatchExecutor
from gateway.models import RpcRequest

logger = logging.getLogger("test.policy")


def _executor(env):
    return BatchExecutor(env.dispatcher, env.batch_policy)


async def test_forbidden_single_method_rejected_and_not_forwarded(env):
    from pytest import raises

    from gateway.errors import MethodForbidden

    before = env.transports["node-a"].calls
    with raises(MethodForbidden) as exc:
        await env.dispatcher.dispatch(
            RpcRequest("eth_sendRawTransaction", ["0x01"], 1)
        )
    assert exc.value.code == -32010
    # 受限方法绝不转发上游
    assert env.transports["node-a"].calls == before


async def test_batch_mixed_items_are_isolated(env, caplog):
    executor = _executor(env)
    results = await executor.execute([
        {"jsonrpc": "2.0", "id": "a", "method": "eth_blockNumber", "params": []},
        {"jsonrpc": "2.0", "id": "b",
         "method": "eth_sendRawTransaction", "params": ["0x1"]},
        "garbage",
        {"jsonrpc": "2.0", "id": "d", "method": "eth_missing_xyz"},
        {"jsonrpc": "2.0", "id": "e",
         "method": "eth_getBlockByNumber", "params": [1, False]},
    ])
    # a/b/d/e 四个带 id 项 + 畸形项回 id=null，共 5 条、保序且 id 不错位
    assert len(results) == 5
    by_id = {r["id"]: r for r in results if r["id"] is not None}
    assert set(by_id) == {"a", "b", "d", "e"}
    assert "result" in by_id["a"]
    assert by_id["b"]["error"]["code"] == -32010       # 受限
    assert by_id["d"]["error"]["code"] == -32601       # 上游方法不存在
    assert by_id["e"]["result"]["number"] == hex(1)
    # 畸形项无 id -> 位置上独立返回 null id 错误，不拖垮其它项
    assert results[2]["id"] is None
    assert results[2]["error"]["code"] == -32600
    joined = "\n".join(r.getMessage() for r in caplog.records)
    assert "malformed" in joined
    logger.info("batch ids preserved: %s", list(by_id))


async def test_batch_too_large_rejected_whole(env):
    executor = _executor(env)
    items = [
        {"jsonrpc": "2.0", "id": i, "method": "eth_blockNumber", "params": []}
        for i in range(env.config.max_batch + 1)
    ]
    from pytest import raises

    from gateway.errors import BatchTooLarge

    with raises(BatchTooLarge) as exc:
        await executor.execute(items)
    assert exc.value.code == -32011
    # 整批拒绝：上游一次都不应被调用
    assert env.transports["node-a"].calls == 0


async def test_batch_duplicate_ids_do_not_collide(env):
    executor = _executor(env)
    results = await executor.execute([
        {"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []},
        {"jsonrpc": "2.0", "id": 1,
         "method": "eth_getBlockByNumber", "params": [2, False]},
        {"jsonrpc": "2.0", "id": 1,
         "method": "eth_getBlockByNumber", "params": [3, False]},
    ])
    # 即使 id 重复，结果也按位置返回，不相互覆盖
    assert len(results) == 3
    assert [r["id"] for r in results] == [1, 1, 1]
    assert results[0]["result"] == hex(env.chain.head_height())
    assert results[1]["result"]["number"] == hex(2)
    assert results[2]["result"]["number"] == hex(3)


async def test_notification_produces_no_response(env):
    executor = _executor(env)
    results = await executor.execute([
        {"jsonrpc": "2.0", "method": "eth_blockNumber", "params": []},  # 无 id
        {"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []},
    ])
    assert len(results) == 1 and results[0]["id"] == 1


async def test_malformed_batch_item_params_wrong_type_isolated(env):
    executor = _executor(env)
    results = await executor.execute([
        {"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": {}},
        {"jsonrpc": "2.0", "id": 2, "method": "eth_blockNumber", "params": "bad"},
    ])
    # params 为 dict 对该方法允许（交给上游）；字符串 params 畸形
    assert "result" in results[0]
    assert results[1]["error"]["code"] == -32600
