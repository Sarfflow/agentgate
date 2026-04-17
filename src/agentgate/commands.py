"""Gateway commands (/new, /session, /resume, /stop, /help)."""
from __future__ import annotations

import logging
import re
import time
from typing import Awaitable, Callable

from .agents.base import Agent
from .platforms.base import ChatPlatform
from .security import SecurityChecker
from .session import SessionManager
from .types import Message

logger = logging.getLogger(__name__)

GATEWAY_COMMANDS = {"/new", "/session", "/resume", "/stop", "/help", "/kill", "/unlock", "/unmute"}

# Callback signature: (original message, payload after /stop) -> None
StopHandler = Callable[[Message, str], Awaitable[None]]
# Callback for /kill: (original message) -> None
KillHandler = Callable[[Message], Awaitable[None]]

# Matches /1, /2, ... /99
_RESUME_PICK_RE = re.compile(r"^/(\d{1,2})$")

# Pending resume selections expire after this many seconds
_PENDING_TTL = 120


class CommandHandler:
    def __init__(
        self,
        platform: ChatPlatform,
        session_mgr: SessionManager,
        security: SecurityChecker,
        agent: Agent | None = None,
        on_stop: StopHandler | None = None,
        on_kill: KillHandler | None = None,
    ):
        self.platform = platform
        self.session_mgr = session_mgr
        self.security = security
        self.agent = agent
        self._on_stop = on_stop
        self._on_kill = on_kill

        # session_key -> (timestamp, [session_id, ...])
        self._pending_resume: dict[str, tuple[float, list[str]]] = {}

    async def handle(self, cmd: str, msg: Message) -> bool:
        """Handle a gateway command. Returns True if fully handled."""
        session_key = SessionManager.make_key(msg.chat_type, msg.chat_id)

        # Check /N pick first (not in GATEWAY_COMMANDS but routed here)
        pick_match = _RESUME_PICK_RE.match(cmd)
        if pick_match:
            return await self._handle_resume_pick(
                int(pick_match.group(1)), session_key, msg
            )

        if cmd not in GATEWAY_COMMANDS:
            return False

        if cmd == "/stop":
            return await self._handle_stop(msg)
        if cmd == "/kill":
            return await self._handle_kill(msg)
        if cmd == "/unlock":
            # /unlock is handled directly in gateway.on_message() during lockdown.
            # If we reach here, gateway is not locked.
            reply_to = msg.message_id if msg.chat_type == "group" else None
            await self.platform.send_text(
                msg.chat_id, msg.chat_type, "Gateway is not locked.",
                reply_to=reply_to,
            )
            return True
        if cmd == "/unmute":
            return await self._handle_unmute(msg)

        if cmd == "/new":
            if not self.security.is_admin(msg.sender_id):
                text = "Permission denied: admin only."
            else:
                self.session_mgr.reset(session_key)
                text = "Session reset. Next message starts a new session."

        elif cmd == "/session":
            text = self._format_session_info(session_key)

        elif cmd == "/resume":
            text = self._handle_resume_list(session_key)

        elif cmd == "/help":
            text = (
                "/new — Reset session (admin)\n"
                "/session — Session info\n"
                "/resume — List recent sessions to switch to\n"
                "/stop — Interrupt current run. `/stop <msg>` interrupts then "
                "sends msg as the next prompt (admin)\n"
                "/kill — Emergency stop: kill all agents & lock gateway (admin)\n"
                "/unlock — Unlock gateway after /kill (admin)\n"
                "/unmute <user_id> — Unmute a user in current group (admin)\n"
                "/help — Show help\n"
                "Other /commands are passed through to the agent"
            )
        else:
            return False

        reply_to = msg.message_id if msg.chat_type == "group" else None
        await self.platform.send_text(
            msg.chat_id, msg.chat_type, text, reply_to=reply_to
        )
        return True

    async def _handle_stop(self, msg: Message) -> bool:
        if not self.security.is_admin(msg.sender_id):
            reply_to = msg.message_id if msg.chat_type == "group" else None
            await self.platform.send_text(
                msg.chat_id,
                msg.chat_type,
                "Permission denied: admin only.",
                reply_to=reply_to,
            )
            return True

        # Everything after "/stop" (including whitespace-stripped remainder)
        # becomes the follow-up prompt. Empty means "just stop".
        raw = msg.text
        payload = raw[len("/stop"):] if raw.lower().startswith("/stop") else ""
        # Drop exactly one leading whitespace character (to allow "/stop foo"
        # → "foo") but preserve any intentional leading space beyond that.
        if payload.startswith(" ") or payload.startswith("\t") or payload.startswith("\n"):
            payload = payload[1:]
        payload = payload.strip()

        if self._on_stop is None:
            reply_to = msg.message_id if msg.chat_type == "group" else None
            await self.platform.send_text(
                msg.chat_id, msg.chat_type, "/stop not wired up.", reply_to=reply_to
            )
            return True

        await self._on_stop(msg, payload)
        return True

    def has_pending_resume(self, session_key: str) -> bool:
        """Check if there's a valid pending resume selection for this session."""
        entry = self._pending_resume.get(session_key)
        if not entry:
            return False
        ts, _ = entry
        if time.time() - ts > _PENDING_TTL:
            del self._pending_resume[session_key]
            return False
        return True

    # ── /resume ─────────────────────────────────────────────────

    def _handle_resume_list(self, session_key: str) -> str:
        if not self.agent:
            return "No agent configured."

        work_dir = self.session_mgr.get_work_dir(session_key)
        # Only list sessions the gateway itself has started — hides auto-sessions
        # created by cron tasks etc. that share the same workspace.
        known = set(self.session_mgr.get_known_sessions(session_key))
        sessions: list = []
        if known:
            try:
                sessions = self.agent.list_sessions(
                    work_dir, limit=10, only_ids=known
                )
            except TypeError:
                # Agent adapter doesn't support only_ids yet
                sessions = self.agent.list_sessions(work_dir, limit=10)
        if not sessions:
            return "No session history found."

        current_sid = self.session_mgr.get_agent_session_id(session_key)

        lines = ["Recent sessions:"]
        sid_list: list[str] = []
        for i, s in enumerate(sessions, 1):
            ts = s.last_modified.strftime("%m-%d %H:%M") if s.last_modified else "?"
            summary = s.last_user_message or "(empty)"
            marker = " *" if s.session_id == current_sid else ""
            lines.append(f"{i}. [{ts}] {summary}{marker}")
            sid_list.append(s.session_id)

        lines.append("")
        lines.append("Reply /1 ~ /N to switch. * = current session.")

        self._pending_resume[session_key] = (time.time(), sid_list)
        return "\n".join(lines)

    async def _handle_resume_pick(
        self, pick: int, session_key: str, msg: Message
    ) -> bool:
        entry = self._pending_resume.get(session_key)
        if not entry:
            return False  # No pending list — not our command

        ts, sid_list = entry
        if time.time() - ts > _PENDING_TTL:
            del self._pending_resume[session_key]
            reply_to = msg.message_id if msg.chat_type == "group" else None
            await self.platform.send_text(
                msg.chat_id,
                msg.chat_type,
                "Selection expired. Use /resume again.",
                reply_to=reply_to,
            )
            return True

        if pick < 1 or pick > len(sid_list):
            reply_to = msg.message_id if msg.chat_type == "group" else None
            await self.platform.send_text(
                msg.chat_id,
                msg.chat_type,
                f"Invalid choice. Pick /1 ~ /{len(sid_list)}.",
                reply_to=reply_to,
            )
            return True

        chosen_sid = sid_list[pick - 1]
        del self._pending_resume[session_key]

        self.session_mgr.set_agent_session_id(session_key, chosen_sid)

        reply_to = msg.message_id if msg.chat_type == "group" else None
        await self.platform.send_text(
            msg.chat_id,
            msg.chat_type,
            f"Switched to session {chosen_sid[:12]}...",
            reply_to=reply_to,
        )
        return True

    # ── /session ────────────────────────────────────────────────

    def _format_session_info(self, session_key: str) -> str:
        stats = self.session_mgr.get_stats(session_key)
        sid = stats.get("agent_session_id")
        if not sid:
            return f"[{session_key}] No active session"

        model = stats.get("model", "?")
        invocations = stats.get("invocations", 0)
        cost = stats.get("total_cost_usd", 0)
        in_tok = stats.get("total_input_tokens", 0)
        out_tok = stats.get("total_output_tokens", 0)
        cache_r = stats.get("total_cache_read", 0)
        cache_w = stats.get("total_cache_creation", 0)
        ctx_win = stats.get("context_window", 0)

        last_in = stats.get("last_input_tokens", 0)
        last_cache_r = stats.get("last_cache_read", 0)
        last_cache_w = stats.get("last_cache_creation", 0)
        current_ctx = last_in + last_cache_r + last_cache_w
        ctx_pct = (current_ctx / ctx_win * 100) if ctx_win else 0

        lines = [
            f"Session: {session_key}",
            f"Model: {model}",
            f"Agent Session: {sid[:12]}...",
            f"Invocations: {invocations}",
            f"Tokens:",
            f"  In: {in_tok:,}  Out: {out_tok:,}",
            f"  Cache read: {cache_r:,}  Cache write: {cache_w:,}",
            f"Context: {current_ctx:,} / {ctx_win:,} ({ctx_pct:.1f}%)",
            f"Cost: ${cost:.4f}",
        ]
        return "\n".join(lines)

    # ── /kill ───────────────────────────────────────────────────

    async def _handle_kill(self, msg: Message) -> bool:
        reply_to = msg.message_id if msg.chat_type == "group" else None
        if not self.security.is_admin(msg.sender_id):
            await self.platform.send_text(
                msg.chat_id, msg.chat_type,
                "Permission denied: admin only.", reply_to=reply_to,
            )
            return True

        if self._on_kill:
            await self._on_kill(msg)
        return True

    # ── /unmute ─────────────────────────────────────────────────

    async def _handle_unmute(self, msg: Message) -> bool:
        reply_to = msg.message_id if msg.chat_type == "group" else None
        if not self.security.is_admin(msg.sender_id):
            await self.platform.send_text(
                msg.chat_id, msg.chat_type,
                "Permission denied: admin only.", reply_to=reply_to,
            )
            return True

        parts = msg.text.split()
        if len(parts) < 2:
            await self.platform.send_text(
                msg.chat_id, msg.chat_type,
                "Usage: /unmute <user_id>", reply_to=reply_to,
            )
            return True

        try:
            target_uid = int(parts[1])
        except ValueError:
            await self.platform.send_text(
                msg.chat_id, msg.chat_type,
                "Invalid user_id.", reply_to=reply_to,
            )
            return True

        group_id = msg.chat_id if msg.chat_type == "group" else None
        if group_id is None:
            await self.platform.send_text(
                msg.chat_id, msg.chat_type,
                "/unmute only works in group chat.", reply_to=reply_to,
            )
            return True

        if self.security.unmute_user(group_id, target_uid):
            text = f"Unmuted user {target_uid} in this group."
        else:
            text = f"User {target_uid} is not muted in this group."
        await self.platform.send_text(
            msg.chat_id, msg.chat_type, text, reply_to=reply_to,
        )
        return True
