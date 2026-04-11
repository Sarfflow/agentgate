from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Message:
    """Platform-agnostic incoming message."""

    text: str = ""
    images: list[str] = field(default_factory=list)  # image URLs
    sender_id: int = 0
    sender_name: str = ""
    chat_id: int = 0
    chat_type: str = ""  # "private" or "group"
    message_id: int = 0
    reply_text: str | None = None
    is_bot_mentioned: bool = False
    is_admin: bool = False


@dataclass
class AgentResult:
    """Result from a CLI agent execution."""

    text: str = ""
    session_id: str | None = None
    cost_usd: float = 0.0
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_creation_tokens: int = 0
    context_window: int = 0
    num_turns: int = 0
    duration_ms: int = 0
    model: str = ""


@dataclass
class HistoryMessage:
    """A message from chat history."""

    text: str = ""
    sender_id: int = 0
    sender_name: str = ""
    timestamp: int = 0
    message_id: int = 0
    is_from_bot: bool = False
    mentions_bot: bool = False
