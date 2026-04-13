"""Response sending: takes parsed ResponseSegments and delivers them via the platform."""
from __future__ import annotations

import asyncio
import logging

from .platforms.base import ChatPlatform
from .render import render_md_to_png
from .session import SessionManager
from .types import ResponseSegment

logger = logging.getLogger(__name__)


class ResponseSender:
    def __init__(
        self,
        platform: ChatPlatform,
        session_mgr: SessionManager,
        max_message_length: int = 4500,
    ):
        self.platform = platform
        self.session_mgr = session_mgr
        self.max_len = max_message_length

    async def send(
        self,
        segments: list[ResponseSegment],
        chat_id: int,
        chat_type: str,
        reply_msg_id: int,
        sender_id: int,
    ):
        """Send a batch of segments; first text uses reply_msg_id as reply anchor."""
        if not segments:
            return

        first_text = True
        for i, seg in enumerate(segments):
            await self.send_segment(
                seg,
                chat_id,
                chat_type,
                reply_msg_id if (first_text and seg.type == "text") else None,
                sender_id,
            )
            if seg.type == "text":
                first_text = False
            if i < len(segments) - 1:
                await asyncio.sleep(0.5)

    async def send_segment(
        self,
        seg: ResponseSegment,
        chat_id: int,
        chat_type: str,
        reply_msg_id: int | None,
        sender_id: int,
    ):
        """Send a single segment. `reply_msg_id` only used for text segments."""
        if seg.type == "render":
            await self._send_rendered_image(seg.content, chat_id, chat_type)
        else:
            await self._send_text(
                seg.content, chat_id, chat_type, reply_msg_id, sender_id
            )

    async def _send_text(
        self,
        text: str,
        chat_id: int,
        chat_type: str,
        reply_msg_id: int | None,
        sender_id: int,
    ):
        if len(text) > self.max_len:
            await self._send_as_forward(text, chat_id, chat_type)
            return

        if chat_type == "group" and reply_msg_id is not None:
            await self.platform.send_text(
                chat_id,
                chat_type,
                text,
                reply_to=reply_msg_id,
                mention=sender_id,
            )
        else:
            await self.platform.send_text(chat_id, chat_type, text)

    async def _send_rendered_image(
        self, md_text: str, chat_id: int, chat_type: str
    ):
        session_key = SessionManager.make_key(chat_type, chat_id)
        work_dir = self.session_mgr.get_work_dir(session_key)
        ts = int(asyncio.get_event_loop().time() * 1000)
        png_path = work_dir / f"_render_{ts}.png"

        ok = await render_md_to_png(md_text, png_path)
        if ok:
            await self.platform.send_image(
                chat_id, chat_type, str(png_path.resolve())
            )
        else:
            await self.platform.send_text(chat_id, chat_type, md_text)

    async def _send_as_forward(
        self, text: str, chat_id: int, chat_type: str
    ):
        chunks: list[str] = []
        while text:
            chunks.append(text[: self.max_len])
            text = text[self.max_len :]

        await self.platform.send_forward(
            chat_id, chat_type, chunks, sender_name="Agent"
        )
