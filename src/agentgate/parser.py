"""Parser for agentgate control tags in agent output.

The tag vocabulary (``<!--SPLIT-->``, ``<!--render-->``, ``<!--mute:...-->``,
``<!--agentgate:restart-->``) is a protocol between the LLM and the gateway,
not a CC-specific concern — it lives here, above the agent adapter, so any
future agent adapter (Codex CLI, etc.) inherits it for free.

Adding a new tag:
  1. Add a regex constant below.
  2. Strip it from the text inside :func:`parse` and emit a
     ``ResponseSegment`` of the new type.
  3. Add a corresponding handler in :mod:`agentgate.segment_handlers`.
Nothing in the agent adapter should change.
"""
from __future__ import annotations

import re

from .types import ResponseSegment

_RENDER_RE = re.compile(r"<!--render-->(.*?)<!--/render-->", re.DOTALL)
_MUTE_RE = re.compile(r"<!--mute:(\d+)-->")
_RESTART_RE = re.compile(r"<!--agentgate:restart-->")


def parse(text: str) -> list[ResponseSegment]:
    """Split a raw agent text chunk into ResponseSegments.

    Out-of-band directives (mute, restart) are extracted into their own
    segments and stripped from the user-visible text. The remainder is
    split on ``<!--SPLIT-->`` boundaries and ``<!--render-->`` blocks.
    """
    if not text:
        return []

    segments: list[ResponseSegment] = []

    # Out-of-band directives. Emitted as segments so the gateway's
    # handler registry can act on them; stripped from user-visible text.
    for uid in _MUTE_RE.findall(text):
        segments.append(ResponseSegment("mute", uid))
    text = _MUTE_RE.sub("", text)

    if _RESTART_RE.search(text):
        segments.append(ResponseSegment("restart", ""))
        text = _RESTART_RE.sub("", text)

    # Remainder: message boundary + inline render block handling.
    for chunk in text.split("<!--SPLIT-->"):
        chunk = chunk.strip()
        if not chunk:
            continue

        parts = _RENDER_RE.split(chunk)
        is_render = False
        for part in parts:
            part = part.strip()
            if not part:
                is_render = not is_render
                continue
            seg_type = "render" if is_render else "text"
            segments.append(ResponseSegment(seg_type, part))
            is_render = not is_render

    return segments or [ResponseSegment("text", text)]
