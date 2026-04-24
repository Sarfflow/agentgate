from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..types import HistoryMessage


@runtime_checkable
class ChatPlatform(Protocol):
    """Interface that chat platform adapters must implement.

    To add a new platform (Telegram, Discord, etc.), create a class that
    implements this protocol and wire it up in main.py.
    """

    @property
    def bot_id(self) -> int | None: ...

    async def send_text(
        self,
        chat_id: int,
        chat_type: str,
        text: str,
        reply_to: int | None = None,
        mentions: list[int] | None = None,
    ) -> None: ...

    async def send_image(
        self,
        chat_id: int,
        chat_type: str,
        image_path: str,
    ) -> None: ...

    async def send_forward(
        self,
        chat_id: int,
        chat_type: str,
        chunks: list[str],
        sender_name: str = "Bot",
    ) -> None: ...

    async def fetch_history(
        self,
        chat_id: int,
        limit: int = 1000,
    ) -> list[HistoryMessage]: ...

    def get_platform_rules(self) -> str:
        """Return platform-specific instructions for the agent workspace."""
        ...
