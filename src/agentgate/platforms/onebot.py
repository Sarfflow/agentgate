"""OneBot V11 reverse WebSocket platform adapter.

Works with NapCat, go-cqhttp, Lagrange, and any OneBot V11 implementation.
NapCat connects *to us*; we receive events and send API calls over the same
WebSocket connection.
"""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import Any, Callable, Coroutine

from aiohttp import WSMsgType, web

from ..config import OneBotConfig
from ..types import HistoryMessage, Message

logger = logging.getLogger(__name__)


class OneBotPlatform:
    def __init__(
        self,
        config: OneBotConfig,
        on_message: Callable[[Message], Coroutine[Any, Any, None]],
    ):
        self.config = config
        self._on_message = on_message
        self._ws: web.WebSocketResponse | None = None
        self._pending: dict[str, asyncio.Future[dict]] = {}
        self.self_id: int | None = None

    def register(self, app: web.Application):
        app.router.add_get("/onebot/v11/ws", self._handle_ws)
        # Without this, the reverse-WS handler never exits on shutdown and
        # systemd ends up hitting TimeoutStopSec.
        app.on_shutdown.append(self._on_shutdown)

    async def _on_shutdown(self, _app: web.Application) -> None:
        ws = self._ws
        if ws is not None and not ws.closed:
            try:
                await ws.close(code=1001, message=b"server shutdown")
            except Exception:
                logger.exception("Error closing platform WS on shutdown")

    # ── WebSocket handler ───────────────────────────────────────

    async def _handle_ws(self, request: web.Request) -> web.WebSocketResponse:
        token = self.config.access_token
        if token:
            got = (
                request.headers.get("Authorization", "")
                .removeprefix("Bearer ")
                .removeprefix("Token ")
                .strip()
            )
            if got != token:
                logger.warning("WS auth failed from %s", request.remote)
                return web.Response(status=403, text="Forbidden")

        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self._ws = ws
        logger.info("Platform connected from %s", request.remote)

        try:
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    try:
                        await self._dispatch(json.loads(msg.data))
                    except Exception:
                        logger.exception("Error handling WS data")
                elif msg.type in (WSMsgType.ERROR, WSMsgType.CLOSE):
                    break
        finally:
            self._ws = None
            logger.warning("Platform disconnected")

        return ws

    async def _dispatch(self, data: dict):
        if "echo" in data and "status" in data:
            echo = data["echo"]
            if fut := self._pending.get(echo):
                fut.set_result(data)
            return

        post_type = data.get("post_type")

        if post_type == "meta_event":
            if sid := data.get("self_id"):
                new_id = int(sid)
                if self.self_id != new_id:
                    self.self_id = new_id
                    logger.info("Bot ID: %s", self.self_id)

        elif post_type == "message":
            asyncio.create_task(self._handle_message_event(data))

    async def _handle_message_event(self, event: dict):
        try:
            msg = await self._parse_event(event)
            if msg:
                await self._on_message(msg)
        except Exception:
            logger.exception("Error in message handler")

    # ── event parsing ───────────────────────────────────────────

    async def _parse_event(self, event: dict) -> Message | None:
        """Parse an OneBot V11 message event into a platform-agnostic Message."""
        msg_type: str = event.get("message_type", "")
        user_id = int(event["user_id"])
        group_id: int | None = event.get("group_id")
        if group_id is not None:
            group_id = int(group_id)
        self_id = event.get("self_id")
        segments: list[dict] = event.get("message", [])
        message_id = event.get("message_id", 0)

        # ignore bot's own messages
        if self_id and user_id == int(self_id):
            return None

        # Check @bot
        is_at = any(
            s["type"] == "at"
            and str(s["data"].get("qq")) == str(self_id)
            for s in segments
        )

        # Check reply-to-bot (group only)
        is_reply_to_bot = False
        if msg_type == "group":
            for s in segments:
                if s["type"] == "reply":
                    reply_id = s["data"].get("id")
                    if reply_id:
                        is_reply_to_bot = await self._is_bot_message(
                            int(reply_id)
                        )
                    break

        is_reply = (
            is_reply_to_bot
            if msg_type == "group"
            else any(s["type"] == "reply" for s in segments)
        )

        if msg_type not in ("private", "group"):
            return None

        # Extract content
        text_parts: list[str] = []
        image_urls: list[str] = []
        file_entries: list[dict] = []
        reply_context: str | None = None
        for seg in segments:
            if seg["type"] == "text":
                t = seg["data"].get("text", "").strip()
                if t:
                    text_parts.append(t)
            elif seg["type"] == "image":
                url = seg["data"].get("url")
                if url:
                    image_urls.append(url)
            elif seg["type"] == "file":
                url = seg["data"].get("url")
                name = seg["data"].get("file", seg["data"].get("name", "unknown"))
                if url:
                    file_entries.append({"name": name, "url": url})
            elif seg["type"] == "reply":
                reply_id = seg["data"].get("id")
                if reply_id:
                    reply_context = await self._get_reply_text(int(reply_id))
            elif seg["type"] == "forward":
                fwd_id = seg["data"].get("id")
                if fwd_id:
                    fwd_lines, fwd_imgs = await self._expand_forward(fwd_id)
                    text_parts.append(
                        "[转发消息]\n" + "\n".join(fwd_lines)
                    )
                    image_urls.extend(fwd_imgs)

        text = "\n".join(text_parts)
        if reply_context:
            text = f"[用户回复了以下消息]\n{reply_context}\n[回复内容]\n{text}"
        if not text and not image_urls and not file_entries:
            return None

        chat_id = group_id if msg_type == "group" else user_id
        nickname = event.get("sender", {}).get("nickname", "")

        return Message(
            text=text,
            images=image_urls,
            files=file_entries,
            sender_id=user_id,
            sender_name=nickname,
            chat_id=chat_id,
            chat_type=msg_type,
            message_id=int(message_id) if message_id else 0,
            reply_text=reply_context,
            is_bot_mentioned=is_at or is_reply,
        )

    async def _expand_forward(
        self, res_id: str, depth: int = 1, max_depth: int = 3
    ) -> tuple[list[str], list[str]]:
        """Expand a forward message into text lines and image URLs.

        Returns (text_lines, image_urls). Recurses up to max_depth levels.
        """
        text_lines: list[str] = []
        image_urls: list[str] = []

        data = await self.call_api("get_forward_msg", message_id=res_id)
        if not data:
            text_lines.append("[转发消息: 获取失败]")
            return text_lines, image_urls

        messages = data.get("messages", data.get("message", []))
        for node in messages:
            # node format varies: could be wrapped in data or direct
            content = node.get("content", node.get("message", []))
            sender = node.get("sender", {})
            nick = sender.get("nickname", "?")

            parts: list[str] = []
            for seg in (content if isinstance(content, list) else []):
                seg_type = seg.get("type", "")
                if seg_type == "text":
                    t = seg.get("data", {}).get("text", "").strip()
                    if t:
                        parts.append(t)
                elif seg_type == "image":
                    url = seg.get("data", {}).get("url")
                    if url:
                        image_urls.append(url)
                        parts.append("[图片]")
                elif seg_type == "forward":
                    nested_id = seg.get("data", {}).get("id")
                    if nested_id and depth < max_depth:
                        nested_text, nested_imgs = await self._expand_forward(
                            nested_id, depth + 1, max_depth
                        )
                        image_urls.extend(nested_imgs)
                        indent = "  " * depth
                        parts.append(
                            f"[转发消息]\n"
                            + "\n".join(indent + l for l in nested_text)
                        )
                    else:
                        parts.append("[转发消息: 嵌套过深]" if nested_id else "[转发消息]")

            line = " ".join(parts) if parts else "[空]"
            text_lines.append(f"{nick}: {line}")

        return text_lines, image_urls

    async def _is_bot_message(self, message_id: int) -> bool:
        data = await self.call_api("get_msg", message_id=message_id)
        if not data:
            return False
        sender_id = data.get("sender", {}).get("user_id")
        return sender_id is not None and int(sender_id) == self.self_id

    async def _get_reply_text(self, message_id: int) -> str | None:
        data = await self.call_api("get_msg", message_id=message_id)
        if not data:
            return None
        sender = data.get("sender", {})
        nick = sender.get("nickname", str(sender.get("user_id", "?")))
        segments = data.get("message", [])
        parts: list[str] = []
        for seg in segments:
            seg_type = seg.get("type", "")
            if seg_type == "text":
                t = seg.get("data", {}).get("text", "").strip()
                if t:
                    parts.append(t)
            elif seg_type == "image":
                parts.append("[图片]")
            elif seg_type == "forward":
                fwd_id = seg.get("data", {}).get("id")
                if fwd_id:
                    fwd_lines, _ = await self._expand_forward(fwd_id)
                    parts.append(f"[转发消息 {len(fwd_lines)}条]")
        content = " ".join(parts)
        if not content:
            return None
        return f"{nick}: {content}"

    # ── API calls ───────────────────────────────────────────────

    async def call_api(self, action: str, **params: Any) -> dict | None:
        if not self._ws or self._ws.closed:
            logger.error("No connection for API call %s", action)
            return None

        echo = uuid.uuid4().hex
        fut: asyncio.Future[dict] = asyncio.get_event_loop().create_future()
        self._pending[echo] = fut

        try:
            await self._ws.send_json(
                {"action": action, "params": params, "echo": echo}
            )
            resp = await asyncio.wait_for(fut, timeout=30)
            if resp.get("retcode") != 0:
                logger.error("API %s failed: %s", action, resp)
                return None
            return resp.get("data")
        except asyncio.TimeoutError:
            logger.error("API %s timed out", action)
            return None
        finally:
            self._pending.pop(echo, None)

    # ── ChatPlatform interface ──────────────────────────────────

    @property
    def bot_id(self) -> int | None:
        return self.self_id

    async def send_text(
        self,
        chat_id: int,
        chat_type: str,
        text: str,
        reply_to: int | None = None,
        mention: int | None = None,
    ) -> None:
        chain: list[dict] = []
        if chat_type == "group" and reply_to is not None:
            chain.append({"type": "reply", "data": {"id": str(reply_to)}})
        if mention is not None:
            chain.append({"type": "at", "data": {"qq": str(mention)}})
            chain.append({"type": "text", "data": {"text": " " + text}})
        else:
            chain.append({"type": "text", "data": {"text": text}})

        if chat_type == "group":
            await self.call_api("send_group_msg", group_id=chat_id, message=chain)
        else:
            await self.call_api(
                "send_private_msg", user_id=chat_id, message=chain
            )

    async def send_image(
        self, chat_id: int, chat_type: str, image_path: str
    ) -> None:
        chain = [
            {"type": "image", "data": {"file": f"file://{image_path}"}}
        ]
        if chat_type == "group":
            await self.call_api("send_group_msg", group_id=chat_id, message=chain)
        else:
            await self.call_api(
                "send_private_msg", user_id=chat_id, message=chain
            )

    async def send_forward(
        self,
        chat_id: int,
        chat_type: str,
        chunks: list[str],
        sender_name: str = "Bot",
    ) -> None:
        uin = str(self.self_id or 0)
        nodes = [
            {
                "type": "node",
                "data": {
                    "name": sender_name,
                    "uin": uin,
                    "content": [{"type": "text", "data": {"text": c}}],
                },
            }
            for c in chunks
        ]
        if chat_type == "group":
            await self.call_api(
                "send_group_forward_msg", group_id=chat_id, messages=nodes
            )
        else:
            await self.call_api(
                "send_private_forward_msg", user_id=chat_id, messages=nodes
            )

    async def fetch_history(
        self, chat_id: int, limit: int = 1000
    ) -> list[HistoryMessage]:
        all_msgs: list[dict] = []
        message_seq = 0
        batches = (limit + 99) // 100
        for _ in range(batches):
            params: dict[str, Any] = {"group_id": chat_id, "count": 100}
            if message_seq:
                params["message_seq"] = message_seq
            data = await self.call_api("get_group_msg_history", **params)
            if not data:
                break
            msgs = data.get("messages", [])
            if not msgs:
                break
            all_msgs.extend(msgs)
            oldest_seq = msgs[0].get("message_seq", 0)
            if oldest_seq == message_seq or oldest_seq == 0:
                break
            message_seq = oldest_seq
            if len(all_msgs) >= limit:
                break

        all_msgs.sort(key=lambda m: m.get("time", 0))

        result: list[HistoryMessage] = []
        msg_map: dict[str, dict] = {
            str(m.get("message_id")): m for m in all_msgs
        }

        for raw in all_msgs:
            sender = raw.get("sender", {})
            segments = raw.get("message", [])
            sender_id = int(sender.get("user_id", 0))

            text_parts: list[str] = []
            at_bot = False
            reply_to_bot = False

            for seg in segments:
                seg_type = seg.get("type", "")
                if seg_type == "text":
                    t = seg["data"].get("text", "").strip()
                    if t:
                        text_parts.append(t)
                elif seg_type == "image":
                    text_parts.append("[图片]")
                elif seg_type == "at":
                    qq = seg.get("data", {}).get("qq")
                    text_parts.append(f"@{qq}")
                    if str(qq) == str(self.self_id):
                        at_bot = True
                elif seg_type == "reply":
                    text_parts.append("[回复]")
                    rid = seg.get("data", {}).get("id")
                    if rid:
                        replied_raw = msg_map.get(str(rid))
                        if replied_raw:
                            ru = int(
                                replied_raw.get("sender", {}).get(
                                    "user_id", 0
                                )
                            )
                            if ru == self.self_id:
                                reply_to_bot = True
                elif seg_type == "forward":
                    fwd_id = seg.get("data", {}).get("id")
                    if fwd_id:
                        fwd_lines, _ = await self._expand_forward(fwd_id)
                        text_parts.append(
                            f"[转发消息 {len(fwd_lines)}条]"
                        )

            result.append(
                HistoryMessage(
                    text=" ".join(text_parts) or "[空]",
                    sender_id=sender_id,
                    sender_name=sender.get("nickname", "?"),
                    timestamp=raw.get("time", 0),
                    message_id=int(raw.get("message_id", 0)),
                    is_from_bot=sender_id == self.self_id,
                    mentions_bot=at_bot or reply_to_bot,
                )
            )

        return result

    async def fetch_message(self, message_id: int) -> HistoryMessage | None:
        data = await self.call_api("get_msg", message_id=message_id)
        if not data:
            return None
        sender = data.get("sender", {})
        segments = data.get("message", [])
        text_parts: list[str] = []
        for seg in segments:
            if seg.get("type") == "text":
                t = seg.get("data", {}).get("text", "").strip()
                if t:
                    text_parts.append(t)
            elif seg.get("type") == "image":
                text_parts.append("[图片]")
            elif seg.get("type") == "forward":
                fwd_id = seg.get("data", {}).get("id")
                if fwd_id:
                    fwd_lines, _ = await self._expand_forward(fwd_id)
                    text_parts.append(f"[转发消息 {len(fwd_lines)}条]")

        sender_id = int(sender.get("user_id", 0))
        return HistoryMessage(
            text=" ".join(text_parts),
            sender_id=sender_id,
            sender_name=sender.get("nickname", str(sender_id)),
            timestamp=data.get("time", 0),
            message_id=int(data.get("message_id", 0)),
            is_from_bot=sender_id == self.self_id,
        )

    def get_platform_rules(self) -> str:
        api = self.config.http_api
        auth = ""
        if self.config.access_token:
            auth = (
                f"-H 'Authorization: Bearer {self.config.access_token}' "
                "\\\n  "
            )
        return (
            "## Sending Messages Proactively\n\n"
            f"HTTP API: {api}\n\n"
            "Private message:\n"
            "```bash\n"
            f"curl -s -X POST {api}/send_private_msg \\\n"
            f"  {auth}"
            "-H 'Content-Type: application/json' \\\n"
            '  -d \'{"user_id": <ID>, "message": [{"type":"text","data":{"text":"message"}}]}\'\n'
            "```\n\n"
            "Group message:\n"
            "```bash\n"
            f"curl -s -X POST {api}/send_group_msg \\\n"
            f"  {auth}"
            "-H 'Content-Type: application/json' \\\n"
            '  -d \'{"group_id": <ID>, "message": [{"type":"text","data":{"text":"message"}}]}\'\n'
            "```\n"
        )
