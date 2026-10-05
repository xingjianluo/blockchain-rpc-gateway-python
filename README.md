# 区块链 JSON-RPC 网关（Python Bootstrap）

本地多上游 JSON-RPC 网关：按健康度/延迟选节点、传输故障自动切换重试、
最终性缓存随链重组失效、并发相同只读请求合并、等待交易确认
（按交易合并轮询、超时/重组可区分）、方法策略与批量约束。
所有故障与重组均由本地确定性假节点注入，结果可解释、可复现。

## 快速开始

```bash
uv run pytest -q                      # 45 个测试（真实 TCP 假节点）
uv run fastapi run main.py            # 启动 3 假节点 + 网关 (8000)
```

## 文档

- [设计说明](docs/design.md)：上游选择/降级、缓存/失效、等待确认、方法策略、错误码
- [本地验证](docs/local-verification.md)：测试说明、curl 场景、错误码速查
