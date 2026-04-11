"""Markdown to PNG rendering via Playwright + server-side markdown."""
from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

# ── markdown detection ──────────────────────────────────────────

_MD_PATTERNS = [
    re.compile(r"\*\*.+?\*\*"),
    re.compile(r"(?<!\*)\*(?!\*).+?(?<!\*)\*(?!\*)"),
    re.compile(r"`.+?`"),
    re.compile(r"```"),
    re.compile(r"\$\$.+?\$\$", re.S),
    re.compile(r"\$[^$\n]+?\$"),
    re.compile(r"^\s*[-*+] \[[ x]\]", re.M),
    re.compile(r"^\|.+\|$", re.M),
    re.compile(r"^#{1,6}\s", re.M),
]


def has_markdown(text: str) -> bool:
    return any(p.search(text) for p in _MD_PATTERNS)


# ── server-side markdown -> HTML ─────────────────────────────────

_ASSETS_DIR = Path(__file__).parent / "assets"
_GITHUB_CSS = ""
_css_path = _ASSETS_DIR / "github-markdown.css"
if _css_path.exists():
    _GITHUB_CSS = _css_path.read_text()

_KATEX_CSS = "https://cdn.jsdelivr.net/npm/katex@0.16.22/dist/katex.min.css"
_KATEX_JS = "https://cdn.jsdelivr.net/npm/katex@0.16.22/dist/katex.min.js"
_KATEX_AUTO = "https://cdn.jsdelivr.net/npm/katex@0.16.22/dist/contrib/auto-render.min.js"

_HLJS_CSS = "https://cdn.jsdelivr.net/gh/highlightjs/cdn-release@11.11.1/build/styles/github.min.css"
_HLJS_JS = "https://cdn.jsdelivr.net/gh/highlightjs/cdn-release@11.11.1/build/highlight.min.js"

_EXTRA_CSS = """\
.markdown-body {
  font-family: -apple-system, "Noto Sans CJK SC", "PingFang SC",
               "Microsoft YaHei", "Helvetica Neue", sans-serif;
  font-size: 15px;
  line-height: 1.7;
  padding: 24px 28px;
  max-width: 680px;
  color: #1f2328;
  background: #fff;
}
.markdown-body table { font-size: 14px; }
.markdown-body pre { font-size: 13px; }
.markdown-body code {
  font-family: "JetBrains Mono", "Fira Code", Consolas, monospace;
}
.katex-display { margin: 12px 0; overflow-x: auto; }
"""

_MATH_RE = re.compile(r"\$\$[\s\S]+?\$\$|\$[^$\n]+?\$")
_CODE_FENCE_RE = re.compile(r"```\w+")


def _has_math(text: str) -> bool:
    return bool(_MATH_RE.search(text))


def _has_code_fences(text: str) -> bool:
    return bool(_CODE_FENCE_RE.search(text))


def md_to_html(text: str) -> str:
    """Convert markdown to a complete HTML page with optional KaTeX and syntax highlighting."""
    from markdown_it import MarkdownIt
    from mdit_py_plugins.footnote import footnote_plugin
    from mdit_py_plugins.tasklists import tasklists_plugin

    md = MarkdownIt("commonmark", {"typographer": True}).enable(
        ["table", "strikethrough"]
    )
    footnote_plugin(md)
    tasklists_plugin(md)

    body = md.render(text)

    math = _has_math(text)
    code = _has_code_fences(text)

    css_links = ""
    if math:
        css_links += f'<link rel="stylesheet" href="{_KATEX_CSS}">'
    if code:
        css_links += f'<link rel="stylesheet" href="{_HLJS_CSS}">'

    script_tags = ""
    if math:
        script_tags += f'<script src="{_KATEX_JS}"></script>'
        script_tags += f'<script src="{_KATEX_AUTO}"></script>'
    if code:
        script_tags += f'<script src="{_HLJS_JS}"></script>'

    return (
        '<!DOCTYPE html><html><head><meta charset="utf-8">'
        f"{css_links}"
        f"<style>{_GITHUB_CSS}\n{_EXTRA_CSS}</style>"
        '</head><body><article class="markdown-body">'
        f"{body}</article>{script_tags}</body></html>"
    )


# ── render to PNG ───────────────────────────────────────────────

async def render_md_to_png(
    text: str, output_path: Path, *, scale: int = 2
) -> bool:
    """Render markdown to PNG via Playwright chromium."""
    math = _has_math(text)
    code = _has_code_fences(text)
    html_content = md_to_html(text)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        from playwright.async_api import async_playwright

        async with async_playwright() as p:
            browser = await p.chromium.launch()
            page = await browser.new_page(
                viewport={"width": 680, "height": 200},
                device_scale_factor=scale,
            )

            await page.set_content(html_content, wait_until="networkidle")

            if math:
                await page.evaluate(
                    "renderMathInElement(document.body,{delimiters:["
                    '{left:"$$",right:"$$",display:true},'
                    '{left:"$",right:"$",display:false},'
                    '{left:"\\\\(",right:"\\\\)",display:false},'
                    '{left:"\\\\[",right:"\\\\]",display:true}'
                    "],throwOnError:false});"
                )

            if code:
                await page.evaluate("hljs.highlightAll()")

            if not math and not code:
                await page.wait_for_timeout(200)

            height = await page.evaluate("document.body.scrollHeight")
            await page.set_viewport_size(
                {"width": 680, "height": min(height + 40, 16000)}
            )

            await page.screenshot(path=str(output_path), full_page=True)
            await browser.close()

        size = output_path.stat().st_size if output_path.exists() else 0
        if size < 500:
            logger.warning("Rendered PNG too small: %d bytes", size)
            return False
        return True
    except Exception:
        logger.exception("Failed to render markdown to PNG")
        return False
