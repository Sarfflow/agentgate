"""Core gateway: orchestrates messages between chat platform and CLI agent."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from pathlib import Path

import aiohttp

from .agents.base import Agent
from .commands import GATEWAY_COMMANDS, CommandHandler
from .config import Config
from .platforms.base import ChatPlatform
from .response import ResponseSender
from .security import RateLimiter, SecurityChecker
from .session import SessionManager
from .types import Message

logger = logging.getLogger(__name__)


class Gateway:
    def __init__(self, config: Config):
        self.config = config
        self.security = SecurityChecker(config)
        self.rate_limiter = RateLimiter(config)
        self.session_mgr = SessionManager(config)

        self._platform: ChatPlatform | None = None
        self._agent: Agent | None = None
        self._response: ResponseSender | None = None
        self._commands: CommandHandler | None = None

        # message queue per debounce key (session_key:user_id)
        self._queues: dict[str, list[Message]] = {}
        self._timers: dict[str, asyncio.Task] = {}

        # tracks running agent per session: session_key -> monotonic start time
        self._busy_since: dict[str, float] = {}
        self._stall_timeout = config.gateway.stall_timeout

        # results from tasks superseded by a fork
        self._pending_results: dict[str, list[str]] = {}

    def set_platform(self, platform: ChatPlatform):
        self._platform = platform
        self._response = ResponseSender(
            platform, self.session_mgr, self.config.gateway.max_message_length
        )
        self._commands = CommandHandler(
            platform, self.session_mgr, self.security
        )

    def set_agent(self, agent: Agent):
        self._agent = agent

    # ── entry point (called by platform adapter) ────────────────

    async def on_message(self, msg: Message):
        # Security gate
        if msg.chat_type == "private":
            if not self.security.check_private(msg.sender_id):
                return
        elif msg.chat_type == "group":
            if not self.security.check_group(
                msg.sender_id, msg.chat_id, msg.is_bot_mentioned
            ):
                return
        else:
            return

        msg.is_admin = self.security.is_admin(msg.sender_id)

        # Rate limit (admin exempt)
        if not msg.is_admin and not self.rate_limiter.check(msg.sender_id):
            logger.info("Rate-limited user %s", msg.sender_id)
            return

        # Gateway commands
        if msg.text.startswith("/"):
            cmd_word = msg.text.split()[0].lower()
            if cmd_word in GATEWAY_COMMANDS:
                await self._commands.handle(cmd_word, msg)
                return

        # Enqueue with debounce
        session_key = SessionManager.make_key(msg.chat_type, msg.chat_id)
        dkey = f"{session_key}:{msg.sender_id}"
        self._queues.setdefault(dkey, []).append(msg)

        if session_key in self._busy_since:
            if old := self._timers.pop(dkey, None):
                old.cancel()
            self._timers[dkey] = asyncio.create_task(
                self._debounce(dkey, short=True)
            )
            return

        if old := self._timers.pop(dkey, None):
            old.cancel()
        self._timers[dkey] = asyncio.create_task(
            self._debounce(dkey, short=True)
        )

    # ── debounce & process ──────────────────────────────────────

    async def _debounce(self, dkey: str, *, short: bool = False):
        delay = 3.0 if short else self.config.gateway.debounce_seconds
        await asyncio.sleep(delay)
        self._timers.pop(dkey, None)
        await self._flush_and_process(dkey)

    async def _flush_and_process(self, dkey: str):
        msgs = self._queues.pop(dkey, [])
        self._timers.pop(dkey, None)
        if not msgs:
            return

        session_key = SessionManager.make_key(msgs[0].chat_type, msgs[0].chat_id)
        fork = False

        if session_key in self._busy_since:
            elapsed = (
                asyncio.get_event_loop().time()
                - self._busy_since[session_key]
            )
            if elapsed < self._stall_timeout:
                self._queues.setdefault(dkey, []).extend(msgs)
                if dkey not in self._timers:
                    self._timers[dkey] = asyncio.create_task(
                        self._debounce(dkey, short=True)
                    )
                return
            fork = True
            logger.info(
                "Session %s busy for %.0fs, forking",
                session_key,
                elapsed,
            )

        if not fork:
            self._busy_since[session_key] = asyncio.get_event_loop().time()

        try:
            await self._process(msgs, fork_session=fork)
        finally:
            if not fork:
                self._busy_since.pop(session_key, None)

        if dkey in self._queues and dkey not in self._timers:
            self._timers[dkey] = asyncio.create_task(
                self._debounce(dkey, short=True)
            )

    async def _process(self, msgs: list[Message], *, fork_session: bool = False):
        first = msgs[0]
        last = msgs[-1]
        session_key = SessionManager.make_key(first.chat_type, first.chat_id)

        # ── build prompt ────────────────────────────────────────
        prompt = self._build_prompt(msgs)

        # Inject results from previously superseded tasks
        pending = self._pending_results.pop(session_key, None)
        if pending:
            ctx_parts = []
            for i, res_text in enumerate(pending, 1):
                truncated = res_text[:2000]
                if len(res_text) > 2000:
                    truncated += "\n... (truncated)"
                ctx_parts.append(f"--- Task {i} ---\n{truncated}")
            joined = "\n\n".join(ctx_parts)
            prompt = (
                "[Previously forked tasks completed and were sent to the user. "
                "Summary below for context. Do not resend.]\n"
                f"{joined}\n"
                "[End of forked task results]\n\n"
                + prompt
            )

        # Inject group chat context
        if first.chat_type == "group":
            context = await self._fetch_group_context(first.chat_id)
            if context:
                prompt = (
                    "[Group chat context (messages since last @bot)]\n"
                    f"{context}\n"
                    "[End of context]\n\n"
                    + prompt
                )

        if fork_session:
            prompt = (
                "[SYSTEM NOTE: A previous long-running task is still executing "
                "in a separate process. DO NOT continue, restart, or reference "
                "that task unless the user explicitly asks. Focus ONLY on the "
                "new message below.]\n\n"
                + prompt
            )

        # Download images
        all_urls = [u for m in msgs for u in m.images]
        img_paths: list[str] = []
        for url in all_urls:
            if p := await self._download_image(url, session_key):
                img_paths.append(p)
        if img_paths:
            note = "\n".join(
                f"[User sent image, saved to: {p} — use Read tool to view]"
                for p in img_paths
            )
            prompt = f"{prompt}\n\n{note}" if prompt else note

        if not prompt.strip():
            return

        # ── run agent ───────────────────────────────────────────
        work_dir = self.session_mgr.get_work_dir(session_key)
        cc_sid = self.session_mgr.get_cc_session_id(session_key)

        result = await self._agent.run(
            prompt=prompt,
            work_dir=work_dir,
            session_id=cc_sid,
            is_admin=first.is_admin,
            fork_session=fork_session,
        )

        # Update session
        if result.session_id and result.session_id != cc_sid:
            self.session_mgr.set_cc_session_id(session_key, result.session_id)

        # If this was the OLD task and a fork has taken over, stash result
        if not fork_session and result.text:
            current_sid = self.session_mgr.get_cc_session_id(session_key)
            if current_sid and current_sid != cc_sid:
                self._pending_results.setdefault(session_key, []).append(
                    result.text
                )
                logger.info(
                    "Session %s: stashed superseded result (%d chars)",
                    session_key,
                    len(result.text),
                )

        self.session_mgr.update_stats(
            session_key,
            cost_usd=result.cost_usd,
            input_tokens=result.input_tokens,
            output_tokens=result.output_tokens,
            cache_read=result.cache_read_tokens,
            cache_creation=result.cache_creation_tokens,
            context_window=result.context_window,
            model=result.model,
        )

        await self._response.send(
            result.text,
            last.chat_id,
            last.chat_type,
            last.message_id,
            last.sender_id,
        )

    # ── prompt building ─────────────────────────────────────────

    @staticmethod
    def _build_prompt(msgs: list[Message]) -> str:
        parts: list[str] = []
        for m in msgs:
            if not m.text:
                continue
            if msgs[0].chat_type == "group":
                nick = m.sender_name or str(m.sender_id)
                role = "admin" if m.is_admin else "user"
                parts.append(f"[{nick} ({m.sender_id}, {role})]:\n{m.text}")
            else:
                parts.append(m.text)
        return "\n\n".join(parts)

    # ── group context ───────────────────────────────────────────

    async def _fetch_group_context(self, chat_id: int) -> str:
        history = await self._platform.fetch_history(chat_id, limit=1000)
        if not history:
            return ""

        # Find last interaction point (where someone @'d or replied to the bot)
        cutoff_idx = max(0, len(history) - 100)
        for i in range(len(history) - 2, -1, -1):
            if history[i].mentions_bot:
                cutoff_idx = i + 1
                break

        context_msgs = history[cutoff_idx:]

        lines: list[str] = []
        for msg in context_msgs:
            ts = datetime.fromtimestamp(msg.timestamp).strftime("%H:%M")
            lines.append(
                f"[{ts}] {msg.sender_name}({msg.sender_id}): {msg.text}"
            )

        return "\n".join(lines)

    # ── image download ──────────────────────────────────────────

    async def _download_image(
        self, url: str, session_key: str
    ) -> str | None:
        work_dir = self.session_mgr.get_work_dir(session_key)
        img_dir = work_dir / "images"
        img_dir.mkdir(exist_ok=True)

        is_local = "://127.0.0.1" in url or "://localhost" in url
        try:
            async with aiohttp.ClientSession(
                trust_env=not is_local
            ) as sess:
                async with sess.get(
                    url, timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status != 200:
                        logger.warning(
                            "Image download %d: %s", resp.status, url
                        )
                        return None
                    ct = resp.content_type or ""
                    ext = ".jpg"
                    for t, e in [
                        ("png", ".png"),
                        ("gif", ".gif"),
                        ("webp", ".webp"),
                    ]:
                        if t in ct:
                            ext = e
                            break
                    data = await resp.read()

            ts = int(asyncio.get_event_loop().time() * 1000)
            fname = f"img_{ts}{ext}"
            path = img_dir / fname
            path.write_bytes(data)
            return str(path)
        except Exception:
            logger.exception("Image download failed: %s", url[:120])
            return None
