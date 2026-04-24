from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Message:
    """Platform-agnostic incoming message."""

    text: str = ""
    images: list[str] = field(default_factory=list)  # image URLs
    files: list[dict] = field(default_factory=list)  # [{"name": ..., "url": ...}]
    sender_id: int = 0
    sender_name: str = ""
    chat_id: int = 0
    chat_type: str = ""  # "private" or "group"
    message_id: int = 0
    is_bot_mentioned: bool = False
    is_admin: bool = False
    received_at: float = 0.0  # unix timestamp; used for inbox TTL on replay

    def to_dict(self) -> dict:
        return {
            "text": self.text,
            "images": list(self.images),
            "files": list(self.files),
            "sender_id": self.sender_id,
            "sender_name": self.sender_name,
            "chat_id": self.chat_id,
            "chat_type": self.chat_type,
            "message_id": self.message_id,
            "is_bot_mentioned": self.is_bot_mentioned,
            "is_admin": self.is_admin,
            "received_at": self.received_at,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Message":
        return cls(
            text=d.get("text", ""),
            images=list(d.get("images", [])),
            files=list(d.get("files", [])),
            sender_id=int(d.get("sender_id", 0)),
            sender_name=d.get("sender_name", ""),
            chat_id=int(d.get("chat_id", 0)),
            chat_type=d.get("chat_type", ""),
            message_id=int(d.get("message_id", 0)),
            is_bot_mentioned=bool(d.get("is_bot_mentioned", False)),
            is_admin=bool(d.get("is_admin", False)),
            received_at=float(d.get("received_at", 0.0)),
        )


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
class PromptContext:
    """Context the gateway passes to the agent for prompt assembly."""

    group_context: str = ""
    image_paths: list[str] = field(default_factory=list)
    file_paths: list[str] = field(default_factory=list)
    is_replay: bool = False
    """True if this message was saved across a gateway restart and is being re-processed."""


@dataclass
class ResponseSegment:
    """A parsed segment of agent output, ready for sending."""

    type: str  # "text" or "render" (markdown to PNG)
    content: str


@dataclass
class AgentEvent:
    """One event yielded from Agent.run() as the agent streams.

    - kind="text": the agent produced a new assistant text chunk. `text`
      holds the raw text; the gateway runs it through parse_response and
      delivers the resulting segments immediately.
    - kind="result": final event. `result` holds the full AgentResult
      (session id, cost, tokens, context window, etc.) so the gateway
      can update stats. No user-visible text here — any final text was
      already delivered via preceding "text" events.
    - kind="error": agent failed or was interrupted. `text` is a short
      diagnostic suitable for delivery to the user (may be empty, in
      which case the gateway can stay silent).
    """

    kind: str
    text: str = ""
    result: "AgentResult | None" = None


@dataclass
class SessionSummary:
    """Summary of a past agent session, for /resume listing."""

    session_id: str = ""
    last_modified: datetime | None = None
    last_user_message: str = ""


@dataclass
class HistoryMessage:
    """A message from chat history."""

    text: str = ""
    sender_id: int = 0
    sender_name: str = ""
    timestamp: int = 0
    message_id: int = 0
    mentions_bot: bool = False
