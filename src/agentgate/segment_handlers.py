"""Dispatch table for :class:`ResponseSegment` types.

Each handler declares whether it needs admin-level authorization; the
gateway's dispatcher checks that once per segment. Adding a new side-effect
tag is: add a regex in :mod:`agentgate.parser`, add a handler class here,
register it in :func:`build_registry`. Nothing else changes.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import ClassVar, Protocol

from .response import ResponseSender
from .security import SecurityChecker
from .types import ResponseSegment

logger = logging.getLogger(__name__)


@dataclass
class SegmentContext:
    """Per-run state passed to every handler invocation.

    ``is_admin`` is batch-wide: True iff every message in the current
    aggregated batch came from an admin. Handlers that mutate gateway
    state (restart, future lockdown, etc.) should gate on it.
    ``first_text_pending`` flips to False after the first text segment
    so subsequent text does not re-quote the reply anchor.
    ``schedule_restart`` is set by :class:`RestartHandler` and read by
    the gateway after the run completes.
    """

    chat_id: int
    chat_type: str
    sender_id: int
    is_admin: bool
    session_key: str
    response_sender: ResponseSender
    security: SecurityChecker
    reply_anchor: int | None
    first_text_pending: bool = True
    schedule_restart: bool = False


class SegmentHandler(Protocol):
    type: ClassVar[str]
    needs_admin: ClassVar[bool]

    async def handle(
        self, seg: ResponseSegment, ctx: SegmentContext
    ) -> None: ...


class TextHandler:
    type: ClassVar[str] = "text"
    needs_admin: ClassVar[bool] = False

    async def handle(
        self, seg: ResponseSegment, ctx: SegmentContext
    ) -> None:
        anchor = ctx.reply_anchor if ctx.first_text_pending else None
        await ctx.response_sender.send_segment(
            seg, ctx.chat_id, ctx.chat_type, anchor, ctx.sender_id,
        )
        ctx.first_text_pending = False


class RenderHandler:
    type: ClassVar[str] = "render"
    needs_admin: ClassVar[bool] = False

    async def handle(
        self, seg: ResponseSegment, ctx: SegmentContext
    ) -> None:
        await ctx.response_sender.send_segment(
            seg, ctx.chat_id, ctx.chat_type, None, ctx.sender_id,
        )


class MuteHandler:
    """Mute a user. Not admin-gated at the gateway layer: the agent
    decides when to emit this based on its own bot-detection heuristics,
    and non-admin prompt-injection attempts targeting this tag were
    judged acceptable risk vs. the agent's autonomy."""

    type: ClassVar[str] = "mute"
    needs_admin: ClassVar[bool] = False

    async def handle(
        self, seg: ResponseSegment, ctx: SegmentContext
    ) -> None:
        if ctx.chat_type != "group":
            return
        try:
            uid = int(seg.content)
        except ValueError:
            logger.warning("mute tag: non-numeric target %r", seg.content)
            return
        ctx.security.mute_user(ctx.chat_id, uid)


class RestartHandler:
    """Schedule a gateway restart after the current run finishes.

    Admin-gated (batch-wide): the restart only fires if every message
    in the current batch was from an admin. This avoids a non-admin
    sneaking the tag through a shared group bucket.
    """

    type: ClassVar[str] = "restart"
    needs_admin: ClassVar[bool] = True

    async def handle(
        self, seg: ResponseSegment, ctx: SegmentContext
    ) -> None:
        ctx.schedule_restart = True
        logger.info(
            "Restart scheduled via tag (chat=%s:%d sender=%d)",
            ctx.chat_type, ctx.chat_id, ctx.sender_id,
        )


def build_registry() -> dict[str, SegmentHandler]:
    return {
        h.type: h()
        for h in (TextHandler, RenderHandler, MuteHandler, RestartHandler)
    }
