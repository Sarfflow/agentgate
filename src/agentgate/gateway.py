"""Core gateway: orchestrates messages between chat platform and CLI agent."""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime

import aiohttp

from . import parser
from .agents.base import Agent
from .commands import GATEWAY_COMMANDS, CommandHandler, _RESUME_PICK_RE
from .config import Config
from .platforms.base import ChatPlatform
from .response import ResponseSender
from .security import RateLimiter, SecurityChecker
from .segment_handlers import SegmentContext, build_registry
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
        # Messages popped from _queues for processing but not yet fully
        # responded to. Tracked so shutdown can persist them for replay.
        self._in_flight: dict[str, list[Message]] = {}

        # Active processing tasks, so shutdown can cancel them
        self._processing_tasks: set[asyncio.Task] = set()

        # session_key -> monotonic start time of the current run
        self._busy_since: dict[str, float] = {}
        # session_keys actively streaming assistant text — stall interrupt
        # skips these so long replies aren't cut off by new arrivals.
        self._streaming: set[str] = set()
        self._stall_timeout = config.gateway.stall_timeout

        # Set once on_shutdown fires; used to skip new work during teardown
        self._shutting_down = False
        # Marks dkeys that are being replayed after a restart
        self._replay_dkeys: set[str] = set()
        # Emergency lockdown: when True, ignore all messages except admin /unlock
        self._locked = False
        # Segment handler registry — one class per ResponseSegment.type.
        # Adding a new side-effect tag is: add a regex to parser, add a
        # handler class in segment_handlers, register it there. Gateway
        # stays unchanged.
        self._segment_handlers = build_registry()

    def set_platform(self, platform: ChatPlatform):
        self._platform = platform
        self._response = ResponseSender(
            platform, self.session_mgr, self.config.gateway.max_message_length
        )
        self._commands = CommandHandler(
            platform,
            self.session_mgr,
            self.security,
            self._agent,
            on_stop=self._handle_stop,
            on_kill=self._handle_kill,
        )

    def set_agent(self, agent: Agent):
        self._agent = agent
        if self._commands:
            self._commands.agent = agent

    # ── lifecycle ───────────────────────────────────────────────

    def load_inbox_and_resume(self) -> None:
        pending = self.session_mgr.load_inbox()
        if not pending:
            return
        # Re-bucket under the current dkey scheme (session_key only). Legacy
        # inbox files may hold old per-user keys like "group_X:sender_Y"; we
        # derive the canonical dkey from each message's own chat metadata.
        for msgs in pending.values():
            for m in msgs:
                dkey = SessionManager.make_key(m.chat_type, m.chat_id)
                self._queues.setdefault(dkey, []).append(m)
                self._replay_dkeys.add(dkey)

    async def on_startup(self) -> None:
        for dkey in list(self._queues):
            if dkey not in self._timers:
                self._timers[dkey] = asyncio.create_task(
                    self._debounce(dkey, short=True)
                )
        # The online notification has to wait until the OneBot WS client
        # (NapCat) connects back to us, which happens shortly after
        # on_startup. Fire-and-forget a background task so startup
        # doesn't block on it.
        asyncio.create_task(self._announce_online())

    async def _announce_online(self) -> None:
        """Wait for the platform to be ready, then notify admins."""
        deadline = asyncio.get_event_loop().time() + 30
        while asyncio.get_event_loop().time() < deadline:
            if self._platform is not None and self._platform.bot_id:
                break
            await asyncio.sleep(1)
        else:
            logger.warning(
                "Platform did not come up within 30s; skipping online notify"
            )
            return
        await self._notify_admins("[agentgate] 已上线")

    async def _notify_admins(self, text: str) -> None:
        """Send a plain-text private message to every configured admin.

        Used for out-of-band lifecycle events (startup, pre-restart)
        that the admin should see even when no agent run is active.
        Failures are logged and swallowed — a failed notification must
        not block the lifecycle event itself.
        """
        if self._platform is None:
            return
        for uid in self.config.security.admin_users:
            try:
                await self._platform.send_text(
                    uid, "private", text
                )
            except Exception:
                logger.exception(
                    "Failed to notify admin %d: %s", uid, text
                )

    async def on_shutdown(self, grace: float = 3.0) -> None:
        if self._shutting_down:
            return
        self._shutting_down = True
        logger.info("Gateway shutdown starting")

        for t in list(self._timers.values()):
            t.cancel()
        self._timers.clear()

        for t in list(self._processing_tasks):
            t.cancel()

        if self._agent is not None and hasattr(self._agent, "shutdown"):
            try:
                await self._agent.shutdown(grace=grace)
            except Exception:
                logger.exception("Agent shutdown error")

        if self._processing_tasks:
            await asyncio.gather(
                *self._processing_tasks, return_exceptions=True
            )

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
            return

        # Emergency lockdown check
        if self._locked:
            is_admin = self.security.is_admin(msg.sender_id)
            if is_admin and msg.text.strip().lower() == "/unlock":
                self._locked = False
                logger.info("Gateway unlocked by admin %d", msg.sender_id)
                await self._platform.send_text(
                    msg.chat_id, msg.chat_type, "Gateway unlocked."
                )
                return
            if is_admin:
                await self._platform.send_text(
                    msg.chat_id,
                    msg.chat_type,
                    "Gateway is locked. Use /unlock to resume.",
                )
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

        if not msg.is_admin and not self.rate_limiter.check(msg.sender_id):
            logger.info("Rate-limited user %s", msg.sender_id)
            return

        # Gateway commands
        if msg.text.startswith("/"):
            cmd_word = msg.text.split()[0].lower()
            if cmd_word in GATEWAY_COMMANDS:
                handled = await self._commands.handle(cmd_word, msg)
                if handled:
                    return
                # /stop with payload returns False to signal "continue with
                # the payload as a new message" — fall through.
            elif _RESUME_PICK_RE.match(cmd_word):
                session_key = SessionManager.make_key(
                    msg.chat_type, msg.chat_id
                )
                if self._commands.has_pending_resume(session_key):
                    await self._commands.handle(cmd_word, msg)
                    return

        await self._enqueue(msg)

    async def _enqueue(self, msg: Message) -> None:
        session_key = SessionManager.make_key(msg.chat_type, msg.chat_id)
        # One bucket per chat (not per sender): concurrent messages from
        # different users in a group aggregate into one Claude run, avoiding
        # duplicate "I already answered"-style replies.
        dkey = session_key
        if not msg.received_at:
            msg.received_at = time.time()
        self._queues.setdefault(dkey, []).append(msg)

        if old := self._timers.pop(dkey, None):
            old.cancel()
        self._timers[dkey] = asyncio.create_task(
            self._debounce(dkey, short=True)
        )

    # ── /stop callback from CommandHandler ─────────────────────

    async def _handle_stop(self, msg: Message, payload: str) -> None:
        """Called by /stop. Interrupts current run (if any) and optionally
        queues the payload as a new message."""
        session_key = SessionManager.make_key(msg.chat_type, msg.chat_id)

        interrupted = False
        if self._agent is not None:
            interrupted = await self._agent.interrupt(session_key)

        # Clear busy_since so the new message starts a fresh run immediately.
        self._busy_since.pop(session_key, None)

        payload = payload.strip()
        if not payload:
            note = "Stopped." if interrupted else "Nothing to stop."
            reply_to = msg.message_id if msg.chat_type == "group" else None
            await self._platform.send_text(
                msg.chat_id, msg.chat_type, note, reply_to=reply_to
            )
            return

        # Queue the payload as a new user message from the same sender.
        from dataclasses import replace
        new_msg = replace(msg, text=payload)
        await self._enqueue(new_msg)

    # ── /kill callback from CommandHandler ───────────────────────

    async def _handle_kill(self, msg: Message) -> None:
        """Emergency stop: interrupt all running agents and lock gateway."""
        self._locked = True
        logger.warning("KILL issued by admin %d — locking gateway", msg.sender_id)

        # Cancel all debounce timers and queued messages
        for t in list(self._timers.values()):
            t.cancel()
        self._timers.clear()
        self._queues.clear()

        # Interrupt all running agents
        if self._agent is not None and hasattr(self._agent, "shutdown"):
            try:
                await self._agent.shutdown(grace=2.0)
            except Exception:
                logger.exception("Error during kill shutdown")

        self._busy_since.clear()
        self._streaming.clear()

        await self._platform.send_text(
            msg.chat_id, msg.chat_type,
            "All agents killed. Gateway locked.\nUse /unlock to resume.",
        )

    # ── restart scheduling ──────────────────────────────────────

    async def _schedule_restart(self) -> None:
        """Schedule `systemctl --user restart agentgate` via a detached
        systemd transient timer.

        The timer unit lives in its own cgroup, so when this process is
        killed by the restart, the timer keeps going and fires the
        restart. Delay is short because by the time we get here the
        current turn has already finished streaming and the CC
        subprocess has exited — there's no dangling tool_use to
        orphan.
        """
        await self._notify_admins("[agentgate] 3 秒后重启")

        unit = f"agentgate-restart-{int(time.time())}.service"
        cmd = [
            "systemd-run",
            "--user",
            "--on-active=3s",
            f"--unit={unit}",
            "--description=admin-requested agentgate restart",
            "/bin/systemctl",
            "--user",
            "restart",
            "agentgate",
        ]
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await proc.communicate()
            if proc.returncode != 0:
                logger.error(
                    "systemd-run failed (rc=%d): %s",
                    proc.returncode,
                    (stderr or b"").decode(errors="replace"),
                )
                return
            logger.warning("Restart scheduled in 3s via %s", unit)
        except Exception:
            logger.exception("Failed to schedule restart")

    # ── debounce & process ──────────────────────────────────────

    async def _debounce(self, dkey: str, *, short: bool = False):
        delay = 3.0 if short else self.config.gateway.debounce_seconds
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
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

        if session_key in self._busy_since:
            elapsed = (
                asyncio.get_event_loop().time()
                - self._busy_since[session_key]
            )
            still_streaming = session_key in self._streaming
            if elapsed < self._stall_timeout or still_streaming:
                # Either the stall window hasn't elapsed, or the current
                # run is still actively producing output — either way, let
                # it finish. Keep the messages queued and re-fire debounce.
                self._queues.setdefault(dkey, []).extend(msgs)
                if dkey not in self._timers:
                    self._timers[dkey] = asyncio.create_task(
                        self._debounce(dkey, short=True)
                    )
                return

            # Run has been alive past stall_timeout with no streaming —
            # interrupt so the new messages can take over the session.
            logger.info(
                "Session %s busy for %.0fs, interrupting current run",
                session_key,
                elapsed,
            )
            if self._agent is not None:
                await self._agent.interrupt(session_key)
            # Give the interrupted task a moment to unwind and release
            # _busy_since (its finally block pops it).
            for _ in range(20):
                if session_key not in self._busy_since:
                    break
                await asyncio.sleep(0.1)
            self._busy_since.pop(session_key, None)

        self._busy_since[session_key] = asyncio.get_event_loop().time()

        ctx: SegmentContext | None = None
        self._in_flight.setdefault(dkey, []).extend(msgs)
        try:
            ctx = await self._process(msgs, dkey=dkey)
        except asyncio.CancelledError:
            raise
        else:
            # A successful run serves whatever the user was waiting for — and
            # thereby implicitly the intent of any earlier interrupted runs
            # on this chat too. Drop the whole dkey instead of only the msgs
            # we just processed, so messages stranded by prior interrupts
            # can't accumulate across long uptimes.
            self._in_flight.pop(dkey, None)
        finally:
            self._busy_since.pop(session_key, None)
            self._streaming.discard(session_key)

        if ctx is not None and ctx.schedule_restart:
            await self._schedule_restart()

        if dkey in self._queues and dkey not in self._timers:
            self._timers[dkey] = asyncio.create_task(
                self._debounce(dkey, short=True)
            )

    async def _process(
        self,
        msgs: list[Message],
        *,
        dkey: str | None = None,
    ) -> SegmentContext | None:
        first = msgs[0]
        last = msgs[-1]
        session_key = SessionManager.make_key(first.chat_type, first.chat_id)

        user_prompt = self._build_prompt(msgs)

        group_context = ""
        if first.chat_type == "group":
            group_context = await self._fetch_group_context(first.chat_id)

        all_urls = [u for m in msgs for u in m.images]
        img_paths: list[str] = []
        for url in all_urls:
            if p := await self._download_image(url, session_key):
                img_paths.append(p)

        all_files = [f for m in msgs for f in m.files]
        file_paths: list[str] = []
        for entry in all_files:
            if p := await self._download_file(
                entry["url"], entry["name"], session_key
            ):
                file_paths.append(p)

        is_replay = dkey is not None and dkey in self._replay_dkeys
        if is_replay:
            self._replay_dkeys.discard(dkey)

        context = PromptContext(
            group_context=group_context,
            image_paths=img_paths,
            file_paths=file_paths,
            is_replay=is_replay,
        )
        prompt = self._agent.prepare_prompt(user_prompt, context)

        if not prompt.strip():
            return None

        work_dir = self.session_mgr.get_work_dir(session_key)
        sid = self.session_mgr.get_agent_session_id(session_key)

        # Admin-gated side effects (restart) require every message in the
        # batch to be from an admin. A single non-admin piggybacking on a
        # group-aggregated run must not be able to smuggle a restart tag
        # past the gate via the bot's reply text.
        batch_admin = all(m.is_admin for m in msgs)

        ctx = SegmentContext(
            chat_id=last.chat_id,
            chat_type=last.chat_type,
            sender_id=last.sender_id,
            is_admin=batch_admin,
            session_key=session_key,
            response_sender=self._response,
            security=self.security,
            reply_anchor=last.message_id,
        )
        final_result = None

        async for event in self._agent.run(
            prompt=prompt,
            work_dir=work_dir,
            session_id=sid,
            is_admin=first.is_admin,
            session_key=session_key,
        ):
            if event.kind == "text":
                await self._dispatch_segments(
                    parser.parse(event.text), ctx, session_key
                )
            elif event.kind == "result":
                final_result = event.result
            elif event.kind == "error":
                final_result = event.result
                if event.text:
                    await self._dispatch_segments(
                        parser.parse(event.text), ctx, session_key
                    )

        # Persist session_id + stats from the final event.
        if final_result is not None:
            if (
                final_result.session_id
                and final_result.session_id != sid
            ):
                self.session_mgr.set_agent_session_id(
                    session_key, final_result.session_id
                )
            self.session_mgr.update_stats(
                session_key,
                cost_usd=final_result.cost_usd,
                input_tokens=final_result.input_tokens,
                output_tokens=final_result.output_tokens,
                cache_read=final_result.cache_read_tokens,
                cache_creation=final_result.cache_creation_tokens,
                context_window=final_result.context_window,
                model=final_result.model,
            )

        return ctx

    async def _dispatch_segments(
        self,
        segments,
        ctx: SegmentContext,
        session_key: str,
    ) -> None:
        """Route each segment to its registered handler with an admin
        gate, a streaming-state marker for stall-detection, and a small
        delay between user-visible segments."""
        for seg in segments:
            handler = self._segment_handlers.get(seg.type)
            if handler is None:
                logger.warning(
                    "Unknown segment type %r — dropping", seg.type
                )
                continue
            if handler.needs_admin and not ctx.is_admin:
                logger.warning(
                    "Non-admin sender %d blocked from %s tag "
                    "(batch had mixed authority)",
                    ctx.sender_id,
                    seg.type,
                )
                continue
            if seg.type == "text":
                self._streaming.add(session_key)
            await handler.handle(seg, ctx)
            if seg.type in ("text", "render"):
                await asyncio.sleep(0.2)

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

    # ── group context ──────────────────────────────────────────

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

    async def _download_file(
        self, url: str, filename: str, session_key: str
    ) -> str | None:
        work_dir = self.session_mgr.get_work_dir(session_key)
        file_dir = work_dir / "files"
        file_dir.mkdir(exist_ok=True)

        # Deduplicate: prepend timestamp if filename already exists
        dest = file_dir / filename
        if dest.exists():
            ts = int(asyncio.get_event_loop().time() * 1000)
            dest = file_dir / f"{ts}_{filename}"

        is_local = "://127.0.0.1" in url or "://localhost" in url
        try:
            async with aiohttp.ClientSession(
                trust_env=not is_local
            ) as sess:
                async with sess.get(
                    url, timeout=aiohttp.ClientTimeout(total=120)
                ) as resp:
                    if resp.status != 200:
                        logger.warning(
                            "File download %d: %s", resp.status, url
                        )
                        return None
                    data = await resp.read()

            dest.write_bytes(data)
            logger.info("File downloaded: %s (%d bytes)", dest, len(data))
            return str(dest)
        except Exception:
            logger.exception("File download failed: %s", url[:120])
            return None
