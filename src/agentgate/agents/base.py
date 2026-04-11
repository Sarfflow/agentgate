from __future__ import annotations

from pathlib import Path
from typing import Protocol, runtime_checkable

from ..types import AgentResult, PromptContext, ResponseSegment


@runtime_checkable
class Agent(Protocol):
    """Interface that CLI agent adapters must implement.

    To add a new agent (Codex CLI, Aider, etc.), create a class that
    implements this protocol and wire it up in main.py.

    Capability properties tell the gateway what features this agent supports,
    so the gateway can adapt its orchestration logic accordingly.
    """

    supports_resume: bool
    """Whether the agent can resume a previous session by ID."""

    supports_fork: bool
    """Whether the agent can fork a session (branch off a running conversation).
    If False, the gateway will queue messages until the current run finishes
    instead of spawning a parallel instance."""

    async def run(
        self,
        prompt: str,
        work_dir: Path,
        session_id: str | None = None,
        is_admin: bool = False,
        fork_session: bool = False,
    ) -> AgentResult: ...

    def prepare_prompt(self, user_prompt: str, context: PromptContext) -> str:
        """Assemble the final prompt by injecting system context.

        Each agent may format context differently (e.g., bracketed notes,
        XML tags, special tokens). The gateway provides raw context via
        PromptContext; the agent decides how to present it.
        """
        ...

    def parse_response(self, text: str) -> list[ResponseSegment]:
        """Parse raw agent output into typed segments for sending.

        Agents may use conventions like <!--SPLIT--> or ```render blocks
        to signal how output should be delivered. This method translates
        those conventions into generic ResponseSegment objects.
        """
        ...
