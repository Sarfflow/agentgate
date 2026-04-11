from __future__ import annotations

from pathlib import Path
from typing import Protocol, runtime_checkable

from ..types import AgentResult


@runtime_checkable
class Agent(Protocol):
    """Interface that CLI agent adapters must implement.

    To add a new agent (Codex CLI, Aider, etc.), create a class that
    implements this protocol and wire it up in main.py.
    """

    async def run(
        self,
        prompt: str,
        work_dir: Path,
        session_id: str | None = None,
        is_admin: bool = False,
        fork_session: bool = False,
    ) -> AgentResult: ...
