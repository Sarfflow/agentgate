from __future__ import annotations

import argparse
import logging

from aiohttp import web

from .agents.claude_code import ClaudeCodeAgent
from .config import Config
from .gateway import Gateway
from .platforms.onebot import OneBotPlatform


def main():
    parser = argparse.ArgumentParser(
        description="agentgate — Chat platform <-> CLI agent gateway"
    )
    parser.add_argument(
        "-c", "--config", default="config.yaml", help="Config file path"
    )
    args = parser.parse_args()

    config = Config.load(args.config)

    logging.basicConfig(
        level=getattr(logging, config.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
    log = logging.getLogger("agentgate")

    # Wire up components
    gateway = Gateway(config)
    platform = OneBotPlatform(config.onebot, on_message=gateway.on_message)
    agent = ClaudeCodeAgent(config.claude_code)

    gateway.set_platform(platform)
    gateway.set_agent(agent)

    # Initialize agent workspace with platform-specific rules
    gateway.session_mgr.init_workspace(
        platform_rules=platform.get_platform_rules(),
        admin_users=config.security.admin_users,
    )

    app = web.Application()
    platform.register(app)

    # Re-enqueue any messages persisted from a previous run
    gateway.load_inbox_and_resume()

    async def _on_startup(_app):
        await gateway.on_startup()

    async def _on_shutdown(_app):
        await gateway.on_shutdown(grace=3.0)

    app.on_startup.append(_on_startup)
    app.on_shutdown.append(_on_shutdown)

    log.info(
        "agentgate starting — WS port %d, workspace %s",
        config.onebot.ws_port,
        gateway.session_mgr.work_dir,
    )
    web.run_app(
        app,
        host="0.0.0.0",
        port=config.onebot.ws_port,
        print=None,
        shutdown_timeout=10.0,
    )


if __name__ == "__main__":
    main()
