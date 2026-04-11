"""Claude Code CLI agent adapter."""
from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

from ..config import ClaudeCodeConfig
from ..types import AgentResult

logger = logging.getLogger(__name__)


class ClaudeCodeAgent:
    def __init__(self, config: ClaudeCodeConfig):
        self.config = config
        self._sem = asyncio.Semaphore(config.max_concurrent)

    async def run(
        self,
        prompt: str,
        work_dir: Path,
        session_id: str | None = None,
        is_admin: bool = False,
        fork_session: bool = False,
    ) -> AgentResult:
        cfg = self.config
        cmd: list[str] = [
            "claude",
            "-p",
            "--output-format",
            "stream-json",
            "--verbose",
        ]

        if cfg.model:
            cmd += ["--model", cfg.model]

        if session_id:
            cmd += ["--resume", session_id]
            if fork_session:
                cmd += ["--fork-session"]

        if is_admin:
            cmd += ["--dangerously-skip-permissions"]
        else:
            cmd += [
                "--permission-mode",
                "dontAsk",
                "--allowedTools",
                "Read Glob Grep Write Edit",
            ]

        if cfg.max_budget:
            cmd += ["--max-budget-usd", str(cfg.max_budget)]

        if cfg.fallback_model:
            cmd += ["--fallback-model", cfg.fallback_model]

        cmd += cfg.extra_flags

        logger.info(
            "Agent run: admin=%s sid=%s cwd=%s prompt_len=%d",
            is_admin,
            session_id,
            work_dir,
            len(prompt),
        )

        async with self._sem:
            try:
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    cwd=str(work_dir),
                )
                stdout, stderr = await asyncio.wait_for(
                    proc.communicate(input=prompt.encode()),
                    timeout=cfg.timeout,
                )
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                logger.error("Agent timed out after %ds", cfg.timeout)
                return AgentResult(
                    text="[Agent timed out]", session_id=session_id
                )
            except Exception as e:
                logger.exception("Agent process error")
                return AgentResult(
                    text=f"[Agent error: {e}]", session_id=session_id
                )

        if stderr:
            logger.debug("Agent stderr: %s", stderr.decode()[:500])

        return self._parse(stdout.decode(), session_id)

    @staticmethod
    def _parse(raw: str, fallback_sid: str | None) -> AgentResult:
        r = AgentResult(session_id=fallback_sid)

        for line in raw.strip().splitlines():
            if not line.strip():
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue

            evt = data.get("type")

            if evt == "system" and data.get("subtype") == "init":
                r.session_id = data.get("session_id", r.session_id)
                r.model = data.get("model", "")

            elif evt == "system" and data.get("subtype") == "api_retry":
                logger.warning(
                    "API retry #%s: %s (delay %.0fms)",
                    data.get("attempt"),
                    data.get("error"),
                    data.get("retry_delay_ms", 0),
                )

            elif evt == "result":
                r.text = data.get("result", "")
                if data.get("is_error"):
                    r.text = f"[Agent error] {r.text}"
                r.cost_usd = data.get("total_cost_usd", 0.0)
                r.num_turns = data.get("num_turns", 0)
                r.duration_ms = data.get("duration_ms", 0)

                usage = data.get("usage", {})
                r.input_tokens = usage.get("input_tokens", 0)
                r.output_tokens = usage.get("output_tokens", 0)
                r.cache_read_tokens = usage.get(
                    "cache_read_input_tokens", 0
                )
                r.cache_creation_tokens = usage.get(
                    "cache_creation_input_tokens", 0
                )

                for model_info in data.get("modelUsage", {}).values():
                    r.context_window = model_info.get("contextWindow", 0)
                    r.model = r.model or model_info.get("model", "")
                    break

        r.text = r.text or "[Agent returned empty response]"
        return r
