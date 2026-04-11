"""Claude Code CLI agent adapter."""
from __future__ import annotations

import asyncio
import json
import logging
import re
from pathlib import Path

from ..config import ClaudeCodeConfig
from ..types import AgentResult, PromptContext, ResponseSegment

logger = logging.getLogger(__name__)

_RENDER_RE = re.compile(r"<!--render-->(.*?)<!--/render-->", re.DOTALL)


class ClaudeCodeAgent:
    supports_resume = True
    supports_fork = True

    def __init__(self, config: ClaudeCodeConfig):
        self.config = config
        self._sem = asyncio.Semaphore(config.max_concurrent)

    # ── Agent protocol: run ─────────────────────────────────────

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

        return self._parse_output(stdout.decode(), session_id)

    # ── Agent protocol: prepare_prompt ──────────────────────────

    def prepare_prompt(self, user_prompt: str, context: PromptContext) -> str:
        """Assemble the final prompt using Claude Code's bracketed note format."""
        parts: list[str] = []

        if context.pending_results:
            ctx_parts = []
            for i, res_text in enumerate(context.pending_results, 1):
                truncated = res_text[:2000]
                if len(res_text) > 2000:
                    truncated += "\n... (truncated)"
                ctx_parts.append(f"--- Task {i} ---\n{truncated}")
            joined = "\n\n".join(ctx_parts)
            parts.append(
                "[Previously forked tasks completed and were sent to the user. "
                "Summary below for context. Do not resend.]\n"
                f"{joined}\n"
                "[End of forked task results]"
            )

        if context.group_context:
            parts.append(
                "[Group chat context (messages since last @bot)]\n"
                f"{context.group_context}\n"
                "[End of context]"
            )

        if context.is_fork:
            parts.append(
                "[SYSTEM NOTE: A previous long-running task is still executing "
                "in a separate process. DO NOT continue, restart, or reference "
                "that task unless the user explicitly asks. Focus ONLY on the "
                "new message below.]"
            )

        parts.append(user_prompt)

        if context.image_paths:
            note = "\n".join(
                f"[User sent image, saved to: {p} — use Read tool to view]"
                for p in context.image_paths
            )
            parts.append(note)

        return "\n\n".join(p for p in parts if p)

    # ── Agent protocol: parse_response ──────────────────────────

    def parse_response(self, text: str) -> list[ResponseSegment]:
        """Parse CC output, splitting on <!--SPLIT--> and extracting <!--render--> blocks."""
        if not text:
            return []

        segments: list[ResponseSegment] = []
        for chunk in text.split("<!--SPLIT-->"):
            chunk = chunk.strip()
            if not chunk:
                continue

            parts = _RENDER_RE.split(chunk)
            is_render = False
            for part in parts:
                part = part.strip()
                if not part:
                    is_render = not is_render
                    continue
                seg_type = "render" if is_render else "text"
                segments.append(ResponseSegment(seg_type, part))
                is_render = not is_render

        return segments or [ResponseSegment("text", text)]

    # ── stream-json parsing ─────────────────────────────────────

    @staticmethod
    def _parse_output(raw: str, fallback_sid: str | None) -> AgentResult:
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
