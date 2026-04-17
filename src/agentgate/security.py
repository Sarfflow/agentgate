from __future__ import annotations

import json
import logging
import time
from collections import defaultdict
from pathlib import Path

from .config import Config

logger = logging.getLogger(__name__)

_MUTED_FILE = "data/muted_users.json"


class SecurityChecker:
    def __init__(self, config: Config):
        self.admin_users = set(config.security.admin_users)
        self.whitelist_users = set(config.security.whitelist_users)
        self.whitelist_groups = set(config.security.whitelist_groups)
        # group_id -> set of muted user_ids
        self._muted: dict[int, set[int]] = {}
        self._load_muted()

    def _load_muted(self) -> None:
        path = Path(_MUTED_FILE)
        if not path.exists():
            return
        try:
            raw = json.loads(path.read_text())
            for gid, uids in raw.items():
                self._muted[int(gid)] = {int(u) for u in uids}
        except Exception:
            logger.exception("Failed to load muted_users.json")

    def _save_muted(self) -> None:
        path = Path(_MUTED_FILE)
        path.parent.mkdir(parents=True, exist_ok=True)
        serializable = {
            str(gid): sorted(uids) for gid, uids in self._muted.items() if uids
        }
        path.write_text(json.dumps(serializable, indent=2))

    def mute_user(self, group_id: int, user_id: int) -> None:
        self._muted.setdefault(group_id, set()).add(user_id)
        self._save_muted()
        logger.info("Muted user %d in group %d", user_id, group_id)

    def unmute_user(self, group_id: int, user_id: int) -> bool:
        """Returns True if the user was actually muted."""
        s = self._muted.get(group_id)
        if s and user_id in s:
            s.discard(user_id)
            self._save_muted()
            logger.info("Unmuted user %d in group %d", user_id, group_id)
            return True
        return False

    def is_muted(self, group_id: int, user_id: int) -> bool:
        s = self._muted.get(group_id)
        return bool(s and user_id in s)

    def is_admin(self, user_id: int) -> bool:
        return int(user_id) in self.admin_users

    def check_private(self, user_id: int) -> bool:
        """All users can use private chat (agent permissions gated separately)."""
        return True

    def check_group(
        self, user_id: int, group_id: int, is_bot_mentioned: bool
    ) -> bool:
        """Group: must @bot or reply to bot; group must be whitelisted if list non-empty."""
        if not is_bot_mentioned:
            return False
        if self.whitelist_groups and int(group_id) not in self.whitelist_groups:
            return False
        if self.is_muted(group_id, user_id):
            logger.debug("Ignoring muted user %d in group %d", user_id, group_id)
            return False
        return True


class RateLimiter:
    def __init__(self, config: Config):
        self.max_messages = config.rate_limit.max_messages
        self.window = config.rate_limit.window_seconds
        self._records: dict[int, list[float]] = defaultdict(list)

    def check(self, user_id: int) -> bool:
        """Return True if allowed, False if rate-limited."""
        now = time.monotonic()
        cutoff = now - self.window
        history = [t for t in self._records[user_id] if t > cutoff]
        if len(history) >= self.max_messages:
            self._records[user_id] = history
            return False
        history.append(now)
        self._records[user_id] = history
        return True
