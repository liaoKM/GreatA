"""配置文件"""
import json
import os
from pathlib import Path

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent

# 数据存储路径
DATA_PATH = PROJECT_ROOT / 'data'

# 日志配置
LOG_PATH = PROJECT_ROOT / 'logs'
LOG_LEVEL = 'INFO'

# 配置文件路径
_CONFIG_FILE = PROJECT_ROOT / 'config' / 'config.json'

# 配置缓存
_config_cache = None


def _load_config() -> dict:
    """加载配置文件"""
    global _config_cache
    if _config_cache is not None:
        return _config_cache

    if _CONFIG_FILE.exists():
        with open(_CONFIG_FILE, 'r', encoding='utf-8') as f:
            _config_cache = json.load(f)
    else:
        _config_cache = {}

    return _config_cache


def get_token(platform: str = 'tushare') -> str:
    """
    获取指定平台的 token

    优先级：环境变量 > 配置文件

    Args:
        platform: 平台名称，如 'tushare', 'akshare'

    Returns:
        token 字符串，未找到返回空字符串
    """
    # 优先从环境变量读取（格式：TUSHARE_TOKEN, AKSHARE_TOKEN）
    env_key = f'{platform.upper()}_TOKEN'
    token = os.environ.get(env_key, '')
    if token:
        return token

    # 从配置文件读取
    config = _load_config()
    tokens = config.get('tokens', {})
    return tokens.get(platform, '')


def get_config(key: str, default=None):
    """
    获取配置项

    Args:
        key: 配置键，支持点号分隔的路径，如 'tokens.tushare'
        default: 默认值

    Returns:
        配置值
    """
    config = _load_config()

    # 支持点号分隔的路径
    keys = key.split('.')
    value = config
    for k in keys:
        if isinstance(value, dict):
            value = value.get(k)
        else:
            return default
        if value is None:
            return default

    return value


def reload_config():
    """重新加载配置文件"""
    global _config_cache
    _config_cache = None
    return _load_config()
