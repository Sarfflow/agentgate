"""Claude Code CLI agent adapter."""
from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

from ..config import ClaudeCodeConfig
from ..types import AgentResult, PromptContext, ResponseSegment, SessionSummary

logger = logging.getLogger(__name__)

_RENDER_RE = re.compile(r"<!--render-->(.*?)<!--/render-->", re.DOTALL)


class ClaudeCodeAgent:
    supports_resume = True
    supports_fork = True

    def __init__(self, config: ClaudeCodeConfig):
        self.config = config
        self._sem = asyncio.Semaphore(config.max_concurrent)
        # Track live subprocesses so we can kill them on shutdown
        self._live_procs: set[asyncio.subprocess.Process] = set()

    # ── Agent protocol: list_sessions ─────────────────────────────

    def list_sessions(
        self,
        work_dir: Path,
        limit: int = 10,
        only_ids: set[str] | None = None,
    ) -> list[SessionSummary]:
        """List recent CC sessions by reading JSONL files from disk.

        If `only_ids` is given, filter to sessions whose id is in the set.
        """
        cc_proj_dir = self._get_cc_project_dir(work_dir)
        if not cc_proj_dir or not cc_proj_dir.is_dir():
            return []

        jsonl_files = sorted(
            cc_proj_dir.glob("*.jsonl"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )

        results: list[SessionSummary] = []
        for f in jsonl_files:
            if len(results) >= limit:
                break
            sid = f.stem
            if only_ids is not None and sid not in only_ids:
                continue
            mtime = datetime.fromtimestamp(f.stat().st_mtime)
            last_msg = self._extract_last_user_message(f)
            results.append(
                SessionSummary(
                    session_id=sid,
                    last_modified=mtime,
                    last_user_message=last_msg,
                )
            )
        return results

    @staticmethod
    def _get_cc_project_dir(work_dir: Path) -> Path | None:
        """Map a workspace dir to CC's project history directory.

        CC encodes project paths by replacing / and _ with -.
        E.g. /home/saer/workspace/private_123 -> -home-saer-workspace-private-123
        """
        projects_dir = Path.home() / ".claude" / "projects"
        if not projects_dir.is_dir():
            return None
        # Build the expected encoded name
        raw = str(work_dir.resolve())
        encoded = raw.replace("/", "-").replace("_", "-").lstrip("-")
        cc_dir = projects_dir / f"-{encoded}"
        if cc_dir.is_dir():
            return cc_dir
        # Fallback: scan for a matching directory (in case encoding changes)
        suffix = work_dir.name.replace("_", "-")
        for d in projects_dir.iterdir():
            if d.is_dir() and d.name.endswith(suffix):
                return d
        return None

    @staticmethod
    def _extract_last_user_message(jsonl_path: Path) -> str:
        """Extract the last user message from a session JSONL file."""
        last_msg = ""
        try:
            with open(jsonl_path) as f:
                for line in f:
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if entry.get("type") != "user":
                        continue
                    content = entry.get("message", {}).get("content", "")
                    if isinstance(content, list):
                        # multimodal: extract text parts
                        text_parts = [
                            p.get("text", "")
                            for p in content
                            if isinstance(p, dict) and p.get("type") == "text"
                        ]
                        content = " ".join(text_parts)
                    if isinstance(content, str) and content.strip():
                        last_msg = content.strip()
        except OSError:
            pass
        # Truncate for display
        if len(last_msg) > 50:
            last_msg = last_msg[:50] + "..."
        return last_msg

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
            proc: asyncio.subprocess.Process | None = None
            try:
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    cwd=str(work_dir),
                )
                self._live_procs.add(proc)
                stdout, stderr = await asyncio.wait_for(
                    proc.communicate(input=prompt.encode()),
                    timeout=cfg.timeout,
                )
            except asyncio.TimeoutError:
                await self._kill(proc)
                logger.error("Agent timed out after %ds", cfg.timeout)
                return AgentResult(
                    text="[Agent timed out]", session_id=session_id
                )
            except asyncio.CancelledError:
                # Shutdown in progress — kill the subprocess and re-raise
                await self._kill(proc)
                raise
            except Exception as e:
                logger.exception("Agent process error")
                return AgentResult(
                    text=f"[Agent error: {e}]", session_id=session_id
                )
            finally:
                if proc is not None:
                    self._live_procs.discard(proc)

        if stderr:
            logger.debug("Agent stderr: %s", stderr.decode()[:500])

        return self._parse_output(stdout.decode(), session_id)

    @staticmethod
    async def _kill(proc: asyncio.subprocess.Process | None) -> None:
        if proc is None or proc.returncode is not None:
            return
        try:
            proc.kill()
        except ProcessLookupError:
            return
        try:
            await asyncio.wait_for(proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            logger.warning("Agent subprocess did not exit 5s after SIGKILL")

    async def shutdown(self, grace: float = 3.0) -> None:
        """Ask running agent subprocesses to exit. Called on service shutdown.

        Waits up to `grace` seconds for natural exit, then SIGKILLs anything
        still running.
        """
        procs = list(self._live_procs)
        if not procs:
            return
        logger.info("Shutdown: %d agent subprocess(es) still running", len(procs))
        # Try SIGTERM first so CC can flush output / write history
        for p in procs:
            if p.returncode is None:
                try:
                    p.terminate()
                except ProcessLookupError:
                    pass
        try:
            await asyncio.wait_for(
                asyncio.gather(
                    *(p.wait() for p in procs), return_exceptions=True
                ),
                timeout=grace,
            )
        except asyncio.TimeoutError:
            logger.warning("Agent(s) didn't exit in %.1fs; killing", grace)
            for p in procs:
                if p.returncode is None:
                    try:
                        p.kill()
                    except ProcessLookupError:
                        pass

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

        if context.is_replay:
            parts.append(
                "[SYSTEM NOTE: agentgate restarted and this message was replayed "
                "from its persistent inbox. Your previous run (if any) was "
                "interrupted before it could reply. Check git/filesystem state "
                "before re-doing any side-effecting work; if the work was "
                "already completed, just confirm to the user.]"
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
        saw_result = False

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
                saw_result = True
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

        if not saw_result:
            # No final `result` event — subprocess was killed or crashed before
            # producing its reply. Return empty text so the gateway stays silent
            # rather than posting a confusing "[Agent returned empty response]"
            # placeholder. The message will be replayed on next startup if the
            # gateway is shutting down.
            logger.warning(
                "Agent output truncated: no 'result' event (sid=%s, %d bytes of stdout). "
                "Subprocess likely killed mid-run.",
                r.session_id,
                len(raw),
            )
            return r

        if not r.text:
            logger.warning("Agent emitted empty result text (sid=%s)", r.session_id)
            r.text = "[Agent returned empty response]"
        return r
