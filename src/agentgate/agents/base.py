from __future__ import annotations

from pathlib import Path
from typing import AsyncIterator, Protocol, runtime_checkable

from ..types import AgentEvent, PromptContext, SessionSummary


@runtime_checkable
class Agent(Protocol):
    """Interface that CLI agent adapters must implement.

    To add a new agent (Codex CLI, Aider, etc.), create a class that
    implements this protocol and wire it up in main.py.
    """

    supports_resume: bool
    """Whether the agent can resume a previous session by ID."""

    def run(
        self,
        prompt: str,
        work_dir: Path,
        session_id: str | None = None,
        is_admin: bool = False,
        session_key: str | None = None,
    ) -> AsyncIterator[AgentEvent]:
        """Run the agent and stream events as they arrive.

        Yields AgentEvents in order: zero or more kind="text" events as the
        agent produces assistant messages, followed by exactly one terminal
        event (kind="result" on success, kind="error" on failure or interrupt).

        `session_key` lets the agent register the subprocess for external
        interrupt (see `interrupt`). Callers can omit it for one-off runs.
        """
        ...

    async def interrupt(self, session_key: str) -> bool:
        """Stop the claude subprocess running for the given session_key.

        Returns True if a process was found and signalled, False otherwise.
        Safe to call even when nothing is running.
        """
        ...

    def prepare_prompt(self, user_prompt: str, context: PromptContext) -> str:
        """Assemble the final prompt by injecting system context.

        Each agent may format context differently (e.g., bracketed notes,
        XML tags, special tokens). The gateway provides raw context via
        PromptContext; the agent decides how to present it.
        """
        ...

    def list_sessions(
        self,
        work_dir: Path,
        limit: int = 10,
        only_ids: set[str] | None = None,
    ) -> list[SessionSummary]:
        """List recent sessions for a workspace, newest first.

        If `only_ids` is provided, only sessions whose id is in the set are
        returned (used to hide sessions created outside the gateway).

        Returns an empty list if the agent doesn't support session listing
        or has no history on disk.
        """
        return []
