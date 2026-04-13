"""Claude Code CLI agent adapter."""
from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import AsyncIterator

from ..config import ClaudeCodeConfig
from ..types import (
    AgentEvent,
    AgentResult,
    PromptContext,
    ResponseSegment,
    SessionSummary,
)

logger = logging.getLogger(__name__)

_RENDER_RE = re.compile(r"<!--render-->(.*?)<!--/render-->", re.DOTALL)


class ClaudeCodeAgent:
    supports_resume = True

    def __init__(self, config: ClaudeCodeConfig):
        self.config = config
        self._sem = asyncio.Semaphore(config.max_concurrent)
        # session_key -> live subprocess (for interrupt / shutdown)
        self._procs: dict[str, asyncio.subprocess.Process] = {}

    # ── Agent protocol: list_sessions ─────────────────────────────

    def list_sessions(
        self,
        work_dir: Path,
        limit: int = 10,
        only_ids: set[str] | None = None,
    ) -> list[SessionSummary]:
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
        """Map a workspace dir to CC's project history directory."""
        projects_dir = Path.home() / ".claude" / "projects"
        if not projects_dir.is_dir():
            return None
        raw = str(work_dir.resolve())
        encoded = raw.replace("/", "-").replace("_", "-").lstrip("-")
        cc_dir = projects_dir / f"-{encoded}"
        if cc_dir.is_dir():
            return cc_dir
        suffix = work_dir.name.replace("_", "-")
        for d in projects_dir.iterdir():
            if d.is_dir() and d.name.endswith(suffix):
                return d
        return None

    @staticmethod
    def _extract_last_user_message(jsonl_path: Path) -> str:
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
        if len(last_msg) > 50:
            last_msg = last_msg[:50] + "..."
        return last_msg

    # ── Agent protocol: run (streaming) ─────────────────────────

    async def run(
        self,
        prompt: str,
        work_dir: Path,
        session_id: str | None = None,
        is_admin: bool = False,
        session_key: str | None = None,
    ) -> AsyncIterator[AgentEvent]:
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
            except Exception as e:
                logger.exception("Failed to spawn agent")
                yield AgentEvent(
                    kind="error",
                    text=f"[Agent spawn error: {e}]",
                    result=AgentResult(session_id=session_id),
                )
                return

            if session_key:
                self._procs[session_key] = proc

            # Feed prompt and close stdin so CC knows input is done.
            assert proc.stdin is not None
            try:
                proc.stdin.write(prompt.encode())
                await proc.stdin.drain()
            except (BrokenPipeError, ConnectionResetError):
                pass
            try:
                proc.stdin.close()
            except Exception:
                pass

            # Drain stderr concurrently (logged at end).
            stderr_task = asyncio.create_task(self._drain_stderr(proc))

            try:
                async for ev in self._stream_events(proc, session_id):
                    yield ev
            except asyncio.CancelledError:
                # Caller cancelled (shutdown / supersession). Kill and re-raise.
                await self._kill(proc)
                raise
            finally:
                # Ensure subprocess is gone before releasing the semaphore.
                await self._kill(proc, graceful=False)
                try:
                    await asyncio.wait_for(stderr_task, timeout=2)
                except asyncio.TimeoutError:
                    stderr_task.cancel()
                if session_key and self._procs.get(session_key) is proc:
                    self._procs.pop(session_key, None)

    async def _stream_events(
        self,
        proc: asyncio.subprocess.Process,
        fallback_sid: str | None,
    ) -> AsyncIterator[AgentEvent]:
        """Read CC's stream-json stdout line by line and yield AgentEvents.

        Emits:
          - AgentEvent(kind="text", text=...)   per assistant text chunk
          - AgentEvent(kind="result", result=...) on final result event (success)
          - AgentEvent(kind="error", text=..., result=...) on error / truncation
        """
        assert proc.stdout is not None

        r = AgentResult(session_id=fallback_sid)
        saw_result = False
        result_text = ""
        result_is_error = False
        result_error_subtype = ""
        last_assistant_text = ""
        any_text_sent = False

        while True:
            try:
                line = await proc.stdout.readline()
            except asyncio.LimitOverrunError:
                # A single JSON line exceeded the default 64KB limit. Read in
                # chunks until newline.
                buf = bytearray()
                while True:
                    try:
                        buf += await proc.stdout.read(65536)
                    except Exception:
                        break
                    if b"\n" in buf:
                        idx = buf.index(b"\n")
                        line = bytes(buf[: idx + 1])
                        break
                    if not buf:
                        line = b""
                        break
            if not line:
                break
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

            elif evt == "assistant":
                msg = data.get("message") or {}
                content = msg.get("content") or []
                if isinstance(content, list):
                    text_parts = [
                        b.get("text", "")
                        for b in content
                        if isinstance(b, dict) and b.get("type") == "text"
                    ]
                    joined = "".join(t for t in text_parts if t).strip()
                    if joined:
                        last_assistant_text = joined
                        any_text_sent = True
                        yield AgentEvent(kind="text", text=joined)

            elif evt == "result":
                saw_result = True
                result_text = data.get("result", "") or ""
                result_is_error = bool(data.get("is_error"))
                result_error_subtype = data.get("subtype", "") or ""
                r.cost_usd = data.get("total_cost_usd", 0.0)
                r.num_turns = data.get("num_turns", 0)
                r.duration_ms = data.get("duration_ms", 0)

                usage = data.get("usage", {})
                r.input_tokens = usage.get("input_tokens", 0)
                r.output_tokens = usage.get("output_tokens", 0)
                r.cache_read_tokens = usage.get("cache_read_input_tokens", 0)
                r.cache_creation_tokens = usage.get(
                    "cache_creation_input_tokens", 0
                )
                for model_info in data.get("modelUsage", {}).values():
                    r.context_window = model_info.get("contextWindow", 0)
                    r.model = r.model or model_info.get("model", "")
                    break
                # result event is terminal
                break

        # ── Final disposition ──────────────────────────────────
        if not saw_result:
            # Subprocess exited without emitting result (killed, crashed,
            # interrupted). Assistant text (if any) was already streamed.
            logger.warning(
                "Agent stream ended without 'result' event (sid=%s, "
                "text_sent=%s)",
                r.session_id,
                any_text_sent,
            )
            yield AgentEvent(kind="error", text="", result=r)
            return

        if result_is_error:
            logger.warning(
                "Agent result is_error=True subtype=%s (sid=%s, result_text_len=%d, "
                "last_assistant_len=%d)",
                result_error_subtype,
                r.session_id,
                len(result_text),
                len(last_assistant_text),
            )
            # If nothing was streamed yet, try to surface a message.
            if not any_text_sent:
                fallback = result_text or "[Agent error]"
                yield AgentEvent(kind="text", text=fallback)
            yield AgentEvent(kind="result", result=r)
            return

        # Clean exit. The result.result field mirrors the last assistant
        # text, which we already streamed — do not resend.
        if not any_text_sent:
            # Unusual: clean result with no preceding assistant events. Could
            # happen with a very short run. Fall back to result_text.
            if result_text:
                yield AgentEvent(kind="text", text=result_text)
            else:
                logger.warning(
                    "Agent emitted no text at all (sid=%s)", r.session_id
                )
                yield AgentEvent(
                    kind="text", text="[Agent returned empty response]"
                )
        yield AgentEvent(kind="result", result=r)

    @staticmethod
    async def _drain_stderr(proc: asyncio.subprocess.Process) -> None:
        """Read all stderr, log at WARN if non-empty."""
        if proc.stderr is None:
            return
        try:
            data = await proc.stderr.read()
        except Exception:
            return
        if data:
            err = data.decode(errors="replace").strip()
            if err:
                logger.warning("Agent stderr: %s", err[:1000])

    async def _kill(
        self,
        proc: asyncio.subprocess.Process | None,
        graceful: bool = True,
    ) -> None:
        if proc is None or proc.returncode is not None:
            return
        if graceful:
            try:
                proc.terminate()
            except ProcessLookupError:
                return
            try:
                await asyncio.wait_for(proc.wait(), timeout=3)
                return
            except asyncio.TimeoutError:
                logger.warning("Agent did not exit 3s after SIGTERM; killing")
        try:
            proc.kill()
        except ProcessLookupError:
            return
        try:
            await asyncio.wait_for(proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            logger.warning("Agent subprocess did not exit 5s after SIGKILL")

    # ── interrupt ───────────────────────────────────────────────

    async def interrupt(self, session_key: str) -> bool:
        """Kill the subprocess running for this session, if any."""
        proc = self._procs.get(session_key)
        if proc is None or proc.returncode is not None:
            return False
        logger.info("Interrupting agent for session %s (pid=%d)", session_key, proc.pid)
        await self._kill(proc, graceful=True)
        return True

    async def shutdown(self, grace: float = 3.0) -> None:
        """Called on service shutdown: SIGTERM all live subprocesses, then SIGKILL."""
        procs = [p for p in self._procs.values() if p.returncode is None]
        if not procs:
            return
        logger.info("Shutdown: %d agent subprocess(es) still running", len(procs))
        for p in procs:
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
        """Assemble the final prompt using CC-friendly bracketed notes."""
        parts: list[str] = []

        if context.group_context:
            parts.append(
                "[Group chat context (messages since last @bot)]\n"
                f"{context.group_context}\n"
                "[End of context]"
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
        """Parse an assistant text chunk into ResponseSegments.

        Splits on <!--SPLIT--> and extracts <!--render--> blocks.
        Assumes markers are balanced within a single chunk (they are, since
        a chunk is one assistant LLM turn).
        """
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
