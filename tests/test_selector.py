"""节点选择：健康度 + 延迟优先 + 故障摘除/恢复。"""
from __future__ import annotations

from gateway.models import UpstreamNode
from gateway.selector import NodeSelector


def test_lower_latency_selected_first():
    nodes = [UpstreamNode("slow", "u1"), UpstreamNode("fast", "u2")]
    sel = NodeSelector(nodes)
    sel.record_success("slow", 120)
    sel.record_success("fast", 8)
    assert sel.ordered_nodes()[0].name == "fast"


def test_failures_demote_and_unhealthy_excluded_last():
    a = UpstreamNode("a", "u1")
    b = UpstreamNode("b", "u2")
    sel = NodeSelector([a, b])
    sel.record_success("a", 10)
    sel.record_success("b", 5)
    assert sel.ordered_nodes()[0].name == "b"
    # b 连续故障 -> 惩罚升高，a 被优先
    sel.record_failure("b")
    sel.record_failure("b")
    assert sel.ordered_nodes()[0].name == "a"
    # 达阈值摘除
    sel.record_failure("b")
    assert b.healthy is False
    assert sel.ordered_nodes()[0].name == "a"
    assert sel.ordered_nodes()[-1].name == "b"
    # 一次成功即恢复
    sel.record_success("b", 3)
    assert b.healthy and b.consecutive_failures == 0
    assert sel.ordered_nodes()[0].name == "b"


def test_exclude_used_nodes_for_retry():
    nodes = [UpstreamNode(f"n{i}", f"u{i}") for i in range(3)]
    sel = NodeSelector(nodes)
    ordered = sel.ordered_nodes(exclude={"n0"})
    assert all(n.name != "n0" for n in ordered)
    assert {n.name for n in ordered} == {"n1", "n2"}
