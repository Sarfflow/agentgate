# agentgate

A thin gateway that bridges **chat platforms** to **CLI coding agents**.

Send a message on QQ, get a response from Claude Code. That's it.

## Philosophy

CLI agents like Claude Code, Codex, and Aider are already industrial-strength tools — they handle code generation, file operations, tool use, and complex multi-step reasoning. **There's no need to reinvent any of that.**

agentgate does one thing: it connects your chat app to your CLI agent. It manages sessions, merges rapid messages, and formats responses. Everything else — the actual intelligence, tool use, workspace configuration, skills — is your agent's job.

How good it is depends entirely on how well you've set up your agent, not on agentgate. It's deliberately a thin layer of session management and chat platform wiring.

**Why not just use OpenClaw / other AI platforms?** Because if you already have a powerful CLI agent set up the way you like, you don't need a 400K-line platform to talk to it. You need a ~1500-line bridge that lets you do it from your phone while you're away from your computer.

## Architecture

```
Chat Platform          agentgate              CLI Agent
┌──────────┐     ┌─────────────────┐     ┌─────────────┐
│ QQ/NapCat│◄───►│ OneBotPlatform  │     │             │
│ Telegram │     ├─────────────────┤     │ Claude Code │
│ Discord  │     │     Gateway     │◄───►│ Codex CLI   │
│ ...      │     │  (orchestrator) │     │ Aider       │
└──────────┘     ├─────────────────┤     │ ...         │
                 │ Session | Render│     └─────────────┘
                 └─────────────────┘
```

**Extensible by design.** Platforms and agents are pluggable adapters behind clean Protocol interfaces:

- `platforms/base.py` — `ChatPlatform` protocol
- `agents/base.py` — `Agent` protocol (with capability declarations: `supports_resume`, `supports_fork`)
- Add your own by implementing the protocol and wiring it in `main.py`

## Features

- **Session persistence** — conversations resume across restarts
- **Message debounce** — rapid messages merged into a single prompt
- **Fork on stall** — if the agent is busy too long, new messages spawn a parallel instance (for agents that support it)
- **Group chat** — @bot or reply-to-bot triggers, with chat history as context
- **Markdown rendering** — tables, code, and math rendered as images via Playwright
- **Security** — admin/non-admin permission tiers, rate limiting, group whitelists
- **Cost tracking** — per-session token and cost statistics

## Supported Platforms & Agents

| Platform | Protocol | Status |
|----------|----------|--------|
| QQ (NapCat, go-cqhttp, Lagrange) | OneBot V11 | Included |
| Telegram | Bot API | Planned |
| Discord | Gateway API | Planned |

| Agent | CLI | Status |
|-------|-----|--------|
| Claude Code | `claude` | Included |
| Codex CLI | `codex` | Planned |
| Aider | `aider` | Planned |

## Quick Start

### Prerequisites

- Python 3.11+
- A CLI agent installed and in PATH (e.g., [Claude Code](https://docs.anthropic.com/en/docs/claude-code))
- A chat platform bot (e.g., [NapCat](https://github.com/NapNeko/NapCatQQ) for QQ)

### Install

```bash
git clone https://github.com/Sarfflow/agentgate.git
cd agentgate
pip install -e .

# Install Playwright browser (for markdown rendering)
playwright install chromium
```

### Configure

```bash
cp config.example.yaml config.yaml
# Edit config.yaml:
#   - onebot.access_token
#   - security.admin_users (your user ID)
#   - claude_code.model (optional)
```

### Run

```bash
agentgate -c config.yaml
```

Then configure your chat platform bot to connect its reverse WebSocket to `ws://localhost:8765/onebot/v11/ws`.

## Project Structure

```
src/agentgate/
├── main.py              # Entry point & wiring
├── config.py            # Configuration dataclasses
├── types.py             # Message, AgentResult, PromptContext, ResponseSegment
├── gateway.py           # Core orchestrator (debounce, fork, context gathering)
├── response.py          # Response delivery (text, images, forward messages)
├── commands.py          # Gateway commands (/new, /session, /help)
├── session.py           # Session persistence & workspace management
├── security.py          # Auth & rate limiting
├── render.py            # Markdown -> PNG via Playwright
├── platforms/
│   ├── base.py          # ChatPlatform protocol — implement this
│   └── onebot.py        # OneBot V11 adapter (NapCat, go-cqhttp, etc.)
└── agents/
    ├── base.py          # Agent protocol — implement this
    └── claude_code.py   # Claude Code CLI adapter
```

## Adding a New Platform

1. Create `src/agentgate/platforms/your_platform.py`
2. Implement the `ChatPlatform` protocol (see `platforms/base.py`)
3. Wire it up in `main.py`

## Adding a New Agent

1. Create `src/agentgate/agents/your_agent.py`
2. Implement the `Agent` protocol (see `agents/base.py`)
3. Wire it up in `main.py`

Each agent controls its own prompt format and response parsing via `prepare_prompt()` and `parse_response()`, so the gateway doesn't need to change.

## Gateway Commands

| Command | Description | Access |
|---------|-------------|--------|
| `/new` | Reset session | Admin |
| `/session` | Show session stats | All |
| `/help` | List commands | All |

Other `/commands` are passed through to the agent.

## License

MIT
