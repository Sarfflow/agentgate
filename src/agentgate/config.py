from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class OneBotConfig:
    ws_port: int = 8765
    http_api: str = "http://127.0.0.1:3000"
    access_token: str = ""


@dataclass
class ClaudeCodeConfig:
    model: str = ""
    max_concurrent: int = 3
    max_budget: float = 0.0
    fallback_model: str = ""
    extra_flags: list[str] = field(default_factory=list)


@dataclass
class SecurityConfig:
    admin_users: list[int] = field(default_factory=list)
    whitelist_users: list[int] = field(default_factory=list)
    whitelist_groups: list[int] = field(default_factory=list)


@dataclass
class GatewayConfig:
    work_dir: str = "workspace"
    data_dir: str = "data"
    debounce_seconds: float = 10.0
    stall_timeout: int = 180
    max_message_length: int = 4500


@dataclass
class RateLimitConfig:
    max_messages: int = 30
    window_seconds: int = 60


@dataclass
class Config:
    onebot: OneBotConfig = field(default_factory=OneBotConfig)
    claude_code: ClaudeCodeConfig = field(default_factory=ClaudeCodeConfig)
    security: SecurityConfig = field(default_factory=SecurityConfig)
    gateway: GatewayConfig = field(default_factory=GatewayConfig)
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
    log_level: str = "INFO"

    @classmethod
    def load(cls, path: str | Path) -> Config:
        path = Path(path)
        if not path.exists():
            return cls()
        with open(path) as f:
            data = yaml.safe_load(f) or {}

        def _make(dc_cls, d):
            if not isinstance(d, dict):
                return dc_cls()
            names = {f.name for f in dc_cls.__dataclass_fields__.values()}
            return dc_cls(**{k: v for k, v in d.items() if k in names})

        return cls(
            onebot=_make(OneBotConfig, data.get("onebot")),
            claude_code=_make(ClaudeCodeConfig, data.get("claude_code")),
            security=_make(SecurityConfig, data.get("security")),
            gateway=_make(GatewayConfig, data.get("gateway")),
            rate_limit=_make(RateLimitConfig, data.get("rate_limit")),
            log_level=data.get("log_level", "INFO"),
        )
