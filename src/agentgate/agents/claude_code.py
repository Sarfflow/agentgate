"""Claude Code CLI agent adapter."""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterator

from ..config import ClaudeCodeConfig
from ..types import (
    AgentEvent,
    AgentResult,
    PromptContext,
    SessionSummary,
)

logger = logging.getLogger(__name__)


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
                async for ev in self._stream_events(proc, session_id, work_dir):
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
        work_dir: Path,
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
        # tool_use id -> tool name, for detecting dangling tool_uses (CC CLI
        # bug where a tool_use is emitted but the tool never runs, leaving
        # the transcript in a half-broken state on --resume).
        pending_tool_uses: dict[str, str] = {}

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
                    for b in content:
                        if (
                            isinstance(b, dict)
                            and b.get("type") == "tool_use"
                            and b.get("id")
                        ):
                            pending_tool_uses[b["id"]] = (
                                b.get("name") or "?"
                            )

            elif evt == "user":
                # tool_result echoes — clear matching pending tool_use ids.
                msg = data.get("message") or {}
                content = msg.get("content") or []
                if isinstance(content, list):
                    for b in content:
                        if (
                            isinstance(b, dict)
                            and b.get("type") == "tool_result"
                            and b.get("tool_use_id")
                        ):
                            pending_tool_uses.pop(b["tool_use_id"], None)

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

        # ── Dangling tool_use repair ───────────────────────────
        # CC sometimes emits a tool_use then exits without ever running
        # the tool (native Read on large PDFs is the known offender).
        # The transcript is left with a tool_use and no matching
        # tool_result; on --resume CC would normally inject "Continue
        # from where you left off." / "No response requested." to
        # paper over it, which eats the next real user turn. We patch
        # the jsonl with synthetic error tool_results so resume sees a
        # complete pair and processes the next user message directly.
        if pending_tool_uses and r.session_id:
            dangling_names = sorted(set(pending_tool_uses.values()))
            logger.warning(
                "Agent ended with %d unresolved tool_use(s) [%s] (sid=%s) — "
                "patching transcript with synthetic tool_results",
                len(pending_tool_uses),
                ", ".join(dangling_names),
                r.session_id,
            )
            try:
                self._patch_dangling_tool_uses(
                    work_dir, r.session_id, pending_tool_uses
                )
            except Exception:
                logger.exception(
                    "Failed to patch dangling tool_uses (sid=%s)",
                    r.session_id,
                )

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
    def _read_tail(path: Path, max_bytes: int) -> str:
        """Return the last `max_bytes` of a text file, utf-8-decoded with
        replacement for any mid-char split at the head."""
        with path.open("rb") as f:
            f.seek(0, 2)
            size = f.tell()
            if size <= max_bytes:
                f.seek(0)
            else:
                f.seek(size - max_bytes)
            data = f.read()
        return data.decode("utf-8", errors="replace")

    def _patch_dangling_tool_uses(
        self,
        work_dir: Path,
        session_id: str,
        pending: dict[str, str],
    ) -> None:
        """Append synthetic error tool_results to the CC transcript for
        tool_uses that never got executed.

        CC stores per-cwd transcripts at
        ``~/.claude/projects/<encoded-cwd>/<session_id>.jsonl`` where
        the encoding replaces every non-alphanumeric char in the
        absolute cwd with ``-``. Without this patch, the next
        ``--resume`` on this session would see an unpaired tool_use
        at the tail and auto-inject a synthetic "Continue from where
        you left off." / "No response requested." exchange, which
        silently eats the user's actual next message.
        """
        cc_dir = self._get_cc_project_dir(work_dir)
        if cc_dir is None:
            logger.warning(
                "CC project dir not found for %s", work_dir
            )
            return
        jsonl = cc_dir / f"{session_id}.jsonl"
        if not jsonl.exists():
            logger.warning(
                "Transcript not found for patching: %s", jsonl
            )
            return

        # Scan the tail of the file to recover chain state (last uuid)
        # and metadata templates (version, entrypoint) we should echo
        # into synthetic entries. Long sessions can grow these files to
        # many MB, so read at most the trailing 64KB — more than enough
        # to span the last several entries.
        tail = self._read_tail(jsonl, 65536)
        last_uuid: str | None = None
        version = "unknown"
        entrypoint = "sdk-cli"
        for line in tail.splitlines():
            try:
                d = json.loads(line)
            except json.JSONDecodeError:
                # Partial first line from the tail slice — expected; skip.
                continue
            if "uuid" in d:
                last_uuid = d["uuid"]
            if d.get("version"):
                version = d["version"]
            if d.get("entrypoint"):
                entrypoint = d["entrypoint"]

        if last_uuid is None:
            logger.warning(
                "No uuid chain anchor in transcript %s", jsonl
            )
            return

        now = (
            datetime.now(timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )
        entries: list[dict] = []
        for tool_use_id, tool_name in pending.items():
            new_uuid = str(uuid.uuid4())
            entries.append(
                {
                    "parentUuid": last_uuid,
                    "isSidechain": False,
                    "type": "user",
                    "message": {
                        "role": "user",
                        "content": [
                            {
                                "type": "tool_result",
                                "tool_use_id": tool_use_id,
                                "content": (
                                    f"[agentgate] {tool_name} tool call "
                                    "was terminated before the tool ran "
                                    "(likely silent CC failure — see "
                                    "agentgate logs). Do not retry with "
                                    "the same inputs; try a different "
                                    "approach."
                                ),
                                "is_error": True,
                            }
                        ],
                    },
                    "uuid": new_uuid,
                    "timestamp": now,
                    "userType": "external",
                    "entrypoint": entrypoint,
                    "cwd": str(work_dir),
                    "sessionId": session_id,
                    "version": version,
                }
            )
            last_uuid = new_uuid

        with jsonl.open("a", encoding="utf-8") as f:
            for e in entries:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")

        logger.info(
            "Patched %d synthetic tool_result(s) into %s",
            len(entries),
            jsonl.name,
        )

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
            parts.append("[重启前未完的消息]")

        parts.append(user_prompt)

        if context.image_paths:
            note = "\n".join(
                f"[User sent image, saved to: {p} — use Read tool to view]"
                for p in context.image_paths
            )
            parts.append(note)

        if context.file_paths:
            note = "\n".join(
                f"[User sent file, saved to: {p}]"
                for p in context.file_paths
            )
            parts.append(note)

        return "\n\n".join(p for p in parts if p)

