"""Response formatting and sending logic."""
from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path

from .platforms.base import ChatPlatform
from .render import render_md_to_png
from .session import SessionManager

logger = logging.getLogger(__name__)

_RENDER_RE = re.compile(r"<!--render-->(.*?)<!--/render-->", re.DOTALL)


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
        text: str,
        chat_id: int,
        chat_type: str,
        reply_msg_id: int,
        sender_id: int,
    ):
        if not text:
            return

        segments = [s.strip() for s in text.split("<!--SPLIT-->") if s.strip()]

        first = True
        for seg in segments:
            await self._send_segment(
                seg,
                chat_id,
                chat_type,
                reply_msg_id if first else None,
                sender_id,
            )
            first = False
            if len(segments) > 1:
                await asyncio.sleep(0.5)

    async def _send_segment(
        self,
        text: str,
        chat_id: int,
        chat_type: str,
        reply_msg_id: int | None,
        sender_id: int,
    ):
        """Parse <!--render-->...<!--/render--> blocks, send text and images."""
        parts = _RENDER_RE.split(text)
        is_render = False

        first_text = True
        for part in parts:
            part = part.strip()
            if not part:
                is_render = not is_render
                continue

            if is_render:
                await self._send_rendered_image(part, chat_id, chat_type)
            else:
                await self._send_text(
                    part,
                    chat_id,
                    chat_type,
                    reply_msg_id if first_text else None,
                    sender_id,
                )
                first_text = False

            is_render = not is_render

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
                chat_id, chat_type, text,
                reply_to=reply_msg_id, mention=sender_id,
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
