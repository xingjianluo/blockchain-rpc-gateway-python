"""uvicorn 加载入口：GATEWAY_CONFIG 指向配置文件时从文件加载，否则用本地演示配置。"""
import os

from gateway.app import create_app
from gateway.config import GatewayConfig

_config_path = os.environ.get("GATEWAY_CONFIG")
app = create_app(GatewayConfig.load(_config_path) if _config_path else None)
