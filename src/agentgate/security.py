from __future__ import annotations

import logging
import time
from collections import defaultdict

from .config import Config

logger = logging.getLogger(__name__)


class SecurityChecker:
    def __init__(self, config: Config):
        self.admin_users = set(config.security.admin_users)
        self.whitelist_users = set(config.security.whitelist_users)
        self.whitelist_groups = set(config.security.whitelist_groups)

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
