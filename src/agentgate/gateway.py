"""Core gateway: orchestrates messages between chat platform and CLI agent."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime

import aiohttp

from .agents.base import Agent
from .commands import GATEWAY_COMMANDS, CommandHandler, _RESUME_PICK_RE
from .config import Config
from .platforms.base import ChatPlatform
from .response import ResponseSender
from .security import RateLimiter, SecurityChecker
from .session import SessionManager
from .types import Message, PromptContext

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
        # Messages popped from _queues for processing but not yet responded to.
        # Tracked here so shutdown can persist them for replay.
        self._in_flight: dict[str, list[Message]] = {}

        # Active processing tasks, so shutdown can cancel them
        self._processing_tasks: set[asyncio.Task] = set()

        # tracks running agent per session: session_key -> monotonic start time
        self._busy_since: dict[str, float] = {}
        self._stall_timeout = config.gateway.stall_timeout

        # results from tasks superseded by a fork
        self._pending_results: dict[str, list[str]] = {}

        # Set once on_shutdown fires; used to skip new work during teardown
        self._shutting_down = False
        # Marks dkeys that are being replayed after a restart
        self._replay_dkeys: set[str] = set()

    def set_platform(self, platform: ChatPlatform):
        self._platform = platform
        self._response = ResponseSender(
            platform, self.session_mgr, self.config.gateway.max_message_length
        )
        self._commands = CommandHandler(
            platform, self.session_mgr, self.security, self._agent
        )

    def set_agent(self, agent: Agent):
        self._agent = agent
        if self._commands:
            self._commands.agent = agent

    # ── lifecycle ───────────────────────────────────────────────

    def load_inbox_and_resume(self) -> None:
        """Load any persisted messages from a previous run and re-enqueue them.

        Called once during startup, before the event loop starts accepting
        new traffic.
        """
        pending = self.session_mgr.load_inbox()
        if not pending:
            return
        for dkey, msgs in pending.items():
            self._queues.setdefault(dkey, []).extend(msgs)
            self._replay_dkeys.add(dkey)

    async def on_startup(self) -> None:
        """Kick off debounce timers for any replayed messages."""
        for dkey in list(self._queues):
            if dkey not in self._timers:
                self._timers[dkey] = asyncio.create_task(
                    self._debounce(dkey, short=True)
                )

    async def on_shutdown(self, grace: float = 3.0) -> None:
        """Called by aiohttp on_shutdown: cancel work, persist inbox."""
        if self._shutting_down:
            return
        self._shutting_down = True
        logger.info("Gateway shutdown starting")

        # Cancel debounce timers — their messages remain in _queues.
        for t in list(self._timers.values()):
            t.cancel()
        self._timers.clear()

        # Cancel in-flight processing tasks — _in_flight entries remain.
        for t in list(self._processing_tasks):
            t.cancel()

        # Ask the agent to stop its subprocesses.
        if self._agent is not None and hasattr(self._agent, "shutdown"):
            try:
                await self._agent.shutdown(grace=grace)
            except Exception:
                logger.exception("Agent shutdown error")

        # Wait briefly for processing tasks to unwind (so any successful
        # responses already sent don't get replayed).
        if self._processing_tasks:
            await asyncio.gather(
                *self._processing_tasks, return_exceptions=True
            )

        # Snapshot remaining queue + in-flight messages for replay.
        combined: dict[str, list[Message]] = {}
        for dkey, msgs in self._in_flight.items():
            if msgs:
                combined.setdefault(dkey, []).extend(msgs)
        for dkey, msgs in self._queues.items():
            if msgs:
                combined.setdefault(dkey, []).extend(msgs)
        self.session_mgr.save_inbox(combined)
        logger.info("Gateway shutdown complete")

    # ── entry point (called by platform adapter) ────────────────

    async def on_message(self, msg: Message):
        if self._shutting_down:
            # Drop cleanly; the platform may buffer/retry.
            return
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
            # /N resume pick — only intercept if there's a pending selection
            if _RESUME_PICK_RE.match(cmd_word):
                session_key = SessionManager.make_key(
                    msg.chat_type, msg.chat_id
                )
                if self._commands.has_pending_resume(session_key):
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
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            # Cancelled during sleep — messages are still in _queues, they'll
            # be persisted by shutdown.
            raise
        self._timers.pop(dkey, None)
        task = asyncio.current_task()
        if task is not None:
            self._processing_tasks.add(task)
        try:
            await self._flush_and_process(dkey)
        finally:
            if task is not None:
                self._processing_tasks.discard(task)

    async def _flush_and_process(self, dkey: str):
        msgs = self._queues.pop(dkey, [])
        self._timers.pop(dkey, None)
        if not msgs:
            return

        session_key = SessionManager.make_key(
            msgs[0].chat_type, msgs[0].chat_id
        )
        fork = False

        if session_key in self._busy_since:
            elapsed = (
                asyncio.get_event_loop().time()
                - self._busy_since[session_key]
            )
            if elapsed < self._stall_timeout:
                # Agent still within grace period — re-queue
                self._queues.setdefault(dkey, []).extend(msgs)
                if dkey not in self._timers:
                    self._timers[dkey] = asyncio.create_task(
                        self._debounce(dkey, short=True)
                    )
                return

            # Agent has been running too long
            if self._agent.supports_fork:
                fork = True
                logger.info(
                    "Session %s busy for %.0fs, forking",
                    session_key,
                    elapsed,
                )
            else:
                # Agent can't fork — keep queuing until current run finishes
                logger.info(
                    "Session %s busy for %.0fs, agent doesn't support fork, re-queuing",
                    session_key,
                    elapsed,
                )
                self._queues.setdefault(dkey, []).extend(msgs)
                if dkey not in self._timers:
                    self._timers[dkey] = asyncio.create_task(
                        self._debounce(dkey, short=True)
                    )
                return

        if not fork:
            self._busy_since[session_key] = asyncio.get_event_loop().time()

        # Mark these messages as in-flight so shutdown can snapshot them.
        self._in_flight.setdefault(dkey, []).extend(msgs)
        try:
            await self._process(msgs, dkey=dkey, fork_session=fork)
        except asyncio.CancelledError:
            # Shutdown or supersession cancelled us — leave in-flight entries
            # in place so they get persisted to inbox.
            raise
        else:
            # Processed successfully; drop these from in-flight.
            remaining = self._in_flight.get(dkey, [])
            for m in msgs:
                try:
                    remaining.remove(m)
                except ValueError:
                    pass
            if not remaining:
                self._in_flight.pop(dkey, None)
        finally:
            if not fork:
                self._busy_since.pop(session_key, None)

        if dkey in self._queues and dkey not in self._timers:
            self._timers[dkey] = asyncio.create_task(
                self._debounce(dkey, short=True)
            )

    async def _process(
        self,
        msgs: list[Message],
        *,
        dkey: str | None = None,
        fork_session: bool = False,
    ):
        first = msgs[0]
        last = msgs[-1]
        session_key = SessionManager.make_key(first.chat_type, first.chat_id)

        # ── gather context (gateway concern) ────────────────────
        user_prompt = self._build_prompt(msgs)

        pending = self._pending_results.pop(session_key, None) or []

        group_context = ""
        if first.chat_type == "group":
            group_context = await self._fetch_group_context(first.chat_id)

        all_urls = [u for m in msgs for u in m.images]
        img_paths: list[str] = []
        for url in all_urls:
            if p := await self._download_image(url, session_key):
                img_paths.append(p)

        is_replay = dkey is not None and dkey in self._replay_dkeys
        if is_replay:
            self._replay_dkeys.discard(dkey)

        # ── let agent assemble the final prompt ─────────────────
        context = PromptContext(
            pending_results=pending,
            group_context=group_context,
            is_fork=fork_session,
            image_paths=img_paths,
            is_replay=is_replay,
        )
        prompt = self._agent.prepare_prompt(user_prompt, context)

        if not prompt.strip():
            return

        # ── run agent ───────────────────────────────────────────
        work_dir = self.session_mgr.get_work_dir(session_key)
        sid = self.session_mgr.get_agent_session_id(session_key)

        result = await self._agent.run(
            prompt=prompt,
            work_dir=work_dir,
            session_id=sid,
            is_admin=first.is_admin,
            fork_session=fork_session,
        )

        # Update session ID
        if result.session_id and result.session_id != sid:
            self.session_mgr.set_agent_session_id(
                session_key, result.session_id
            )

        # If this was the OLD task and a fork has taken over, stash result
        if not fork_session and result.text:
            current_sid = self.session_mgr.get_agent_session_id(session_key)
            if current_sid and current_sid != sid:
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

        # ── let agent parse response, then send ────────────────
        segments = self._agent.parse_response(result.text)
        await self._response.send(
            segments,
            last.chat_id,
            last.chat_type,
            last.message_id,
            last.sender_id,
        )

    # ── prompt building (gateway concern: merge debounced messages) ──

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

    # ── group context (gateway concern: fetch history, find cutoff) ──

    async def _fetch_group_context(self, chat_id: int) -> str:
        history = await self._platform.fetch_history(chat_id, limit=1000)
        if not history:
            return ""

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
