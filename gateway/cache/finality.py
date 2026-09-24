"""方法/高度的最终性判定。

判定依据：
- eth_getBlockByNumber 等“按高度”读取：第一个参数为高度标签。
  * 具体高度且 <= finalized_height -> FINALIZED，可缓存；
  * 具体高度 > finalized_height（未最终确定）-> UNFINALIZED，不缓存；
  * latest/pending/safe/finalized 标签 -> UNFINALIZED（高度随链头变化）；
- eth_getBlockByHash / eth_getTransactionByHash 按哈希内容寻址：
  重组后旧哈希查不到数据，不会返回“张冠李戴”的过期结果，视为 FINALIZED；
- eth_call 等状态查询默认 UNFINALIZED（状态随链改变，保守不缓存）；
- eth_blockNumber 为链头高度，UNFINALIZED。
"""
from __future__ import annotations

from gateway.models import Finality, RpcRequest

READ_BY_NUMBER_FULLY_FINALIZABLE = frozenset(
    {
        "eth_getBlockByNumber",
        "eth_getBlockTransactionCountByNumber",
        "eth_getBalance",
        "eth_getTransactionCount",
        "eth_getCode",
        "eth_getStorageAt",
    }
)
READ_BY_HASH_METHODS = frozenset(
    {"eth_getBlockByHash", "eth_getTransactionByHash",
     "eth_getTransactionReceipt"}
)
NON_FINALIZABLE_METHODS = frozenset({"eth_call", "eth_blockNumber"})

_TAG_HEIGHTS = {"latest", "pending", "earliest", "safe", "finalized"}


def _parse_tag(tag: object) -> tuple[bool, int | None]:
    """返回 (是否具体高度, 高度值)。标签 latest 等返回 (False, None)。"""
    if isinstance(tag, bool):
        return False, None
    if isinstance(tag, int):
        return True, tag
    if isinstance(tag, str):
        if tag.lower() in _TAG_HEIGHTS:
            return False, None
        try:
            return True, int(tag, 16) if tag.lower().startswith("0x") else int(tag)
        except ValueError:
            return False, None
    return False, None


def classify(req: RpcRequest, finalized_height: int) -> tuple[Finality, int | None]:
    """返回 (最终性, 关联高度)。关联高度用于重组时定向失效。"""
    method = req.method
    params = req.params if isinstance(req.params, (list, tuple)) else []

    if method in READ_BY_HASH_METHODS:
        return Finality.FINALIZED, None

    if method in READ_BY_NUMBER_FULLY_FINALIZABLE:
        tag = params[0] if params else "latest"
        concrete, height = _parse_tag(tag)
        if concrete and height is not None and height <= finalized_height:
            return Finality.FINALIZED, height
        if concrete and height is not None:
            return Finality.UNFINALIZED, height
        return Finality.UNFINALIZED, None

    # eth_call 等其它方法：保守视为未最终确定，不缓存
    return Finality.UNFINALIZED, None
