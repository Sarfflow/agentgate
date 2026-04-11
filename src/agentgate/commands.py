"""Gateway commands (/new, /session, /help)."""
from __future__ import annotations

import logging

from .platforms.base import ChatPlatform
from .security import SecurityChecker
from .session import SessionManager
from .types import Message

logger = logging.getLogger(__name__)

GATEWAY_COMMANDS = {"/new", "/session", "/help"}


class CommandHandler:
    def __init__(
        self,
        platform: ChatPlatform,
        session_mgr: SessionManager,
        security: SecurityChecker,
    ):
        self.platform = platform
        self.session_mgr = session_mgr
        self.security = security

    async def handle(self, cmd: str, msg: Message) -> bool:
        """Handle a gateway command. Returns True if handled."""
        if cmd not in GATEWAY_COMMANDS:
            return False

        session_key = SessionManager.make_key(msg.chat_type, msg.chat_id)

        if cmd == "/new":
            if not self.security.is_admin(msg.sender_id):
                text = "Permission denied: admin only."
            else:
                self.session_mgr.reset(session_key)
                text = "Session reset. Next message starts a new session."

        elif cmd == "/session":
            text = self._format_session_info(session_key)

        elif cmd == "/help":
            text = (
                "/new — Reset session (admin)\n"
                "/session — Session info\n"
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

    def _format_session_info(self, session_key: str) -> str:
        stats = self.session_mgr.get_stats(session_key)
        sid = stats.get("cc_session_id")
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
