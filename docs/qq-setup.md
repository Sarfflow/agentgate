# QQ 平台接入指南

本文档介绍如何通过 [NapCat](https://github.com/NapNeko/NapCatQQ) 将 agentgate 接入 QQ。

完成后你可以在 QQ 私聊或群聊中与 CLI agent（如 Claude Code）对话。

## 前置条件

- 一台 Linux 服务器（本地机器也行）
- Python 3.11+
- 你要接入的 CLI agent 已安装（如 [Claude Code](https://docs.anthropic.com/en/docs/claude-code)）
- Docker（推荐）或 QQ Linux 客户端

## 整体架构

```
你的 QQ ──→ 腾讯服务器 ──→ NapCat（QQ 机器人框架）
                                │
                          反向 WebSocket
                                │
                                ▼
                           agentgate ──→ Claude Code
```

NapCat 登录你的另一个 QQ 号（机器人号），通过反向 WebSocket 把收到的消息推给 agentgate，agentgate 调用 CLI agent 处理后把回复发回去。

## 第一步：部署 NapCat

### 方式一：Docker（推荐）

创建 `docker-compose.yml`：

```yaml
services:
  napcat:
    image: mlikiowa/napcat-docker:latest
    container_name: napcat
    restart: always
    network_mode: host
    environment:
      - NAPCAT_UID=1000
      - NAPCAT_GID=1000
      - ACCOUNT=<你的机器人QQ号>
      - WEBUI_TOKEN=<WebUI登录密码>
    volumes:
      - ./napcat/config:/app/napcat/config
      - ./ntqq:/app/.config/QQ
```

> 把 `NAPCAT_UID`/`NAPCAT_GID` 改成你当前用户的 UID/GID（运行 `id` 查看）。
> `network_mode: host` 让 NapCat 直接使用宿主机网络，省去端口映射。

启动：

```bash
docker compose up -d
```

### 方式二：手动安装

参考 [NapCat 官方文档](https://napneko.github.io/)。简要步骤：

```bash
# 安装依赖
sudo apt install xvfb xauth

# 安装 QQ Linux（amd64）
curl -o linuxqq.deb https://dldir1.qq.com/qqfile/qq/QQNT/94704804/linuxqq_3.2.23-44343_amd64.deb
sudo dpkg -i linuxqq.deb

# 下载 NapCat Shell 版并解压到 /opt/QQ/resources/app/napcat/
# 具体步骤见 NapCat 官方文档
```

## 第二步：登录 QQ

首次启动需要扫码登录。打开 NapCat WebUI：

```
http://<服务器IP>:6099/webui
```

> 密码是你在 docker-compose.yml 中设置的 `WEBUI_TOKEN`。
> 如果忘了或没设置，运行 `docker logs napcat` 查看日志中的 URL 和 token。

在 WebUI 中扫码登录机器人 QQ 号。登录成功后，QQ 数据会持久化在 `./ntqq` 目录，之后重启不需要再扫码。

> **提示**：Docker 部署建议在 docker-compose.yml 中加 `mac_address: 02:42:ac:11:00:02`，固化 MAC 地址避免容器重建后需要重新验证设备。

## 第三步：配置 NapCat 反向 WebSocket

NapCat 需要主动连接到 agentgate 的 WebSocket 服务端。

### 通过 WebUI 配置（推荐）

1. 打开 WebUI → 网络配置
2. 添加一个 **WebSocket 客户端**（反向 WS）
3. 填写：
   - **启用**：是
   - **URL**：`ws://127.0.0.1:8765/onebot/v11/ws`
   - **Token**：设一个密码（后面 agentgate 配置要用同一个）
   - **消息格式**：`array`

### 通过配置文件

编辑 `./napcat/config/onebot11_<QQ号>.json`：

```json
{
  "network": {
    "websocketClients": [
      {
        "enable": true,
        "name": "agentgate",
        "url": "ws://127.0.0.1:8765/onebot/v11/ws",
        "messagePostFormat": "array",
        "reportSelfMessage": false,
        "reconnectInterval": 5000,
        "token": "你的密码",
        "heartInterval": 30000
      }
    ]
  }
}
```

> 如果 NapCat 和 agentgate 不在同一台机器上，把 `127.0.0.1` 改成 agentgate 的 IP。

## 第四步：安装和配置 agentgate

```bash
git clone https://github.com/Sarfflow/agentgate.git
cd agentgate
pip install -e .

# 安装 Playwright 浏览器（用于 Markdown 渲染为图片）
playwright install chromium
```

创建配置文件：

```bash
cp config.example.yaml config.yaml
```

编辑 `config.yaml`：

```yaml
onebot:
  ws_port: 8765                        # 和 NapCat 中配置的端口一致
  http_api: "http://127.0.0.1:3000"   # NapCat HTTP API 地址
  access_token: "你的密码"             # 和 NapCat 中配置的 token 一致

security:
  admin_users: [你的QQ号]              # 你自己的 QQ 号（不是机器人号）
  whitelist_groups: []                 # 允许响应的群号，空 = 所有群

claude_code:
  model: ""                            # 留空使用 CC 默认模型
  timeout: 1800                        # 30 分钟超时
  max_budget: 5.0                      # 每次调用最大花费（美元）
```

> **admin_users** 填你自己的 QQ 号。管理员拥有完整权限（可以让 agent 执行任意命令）。非管理员用户只有只读权限。

## 第五步：启动

```bash
agentgate -c config.yaml
```

看到以下日志说明启动成功：

```
agentgate starting — WS port 8765, workspace /path/to/workspace
Platform connected from 127.0.0.1
Bot ID: 1234567890
```

`Bot ID` 是你的机器人 QQ 号。如果 NapCat 还没连上，等它自动重连即可。

## 使用

### 私聊

直接给机器人发消息即可。

### 群聊

在群里 **@机器人** 或 **回复机器人的消息** 触发。需要先把机器人拉进群。

如果配置了 `whitelist_groups`，只有在白名单中的群会响应。

### 命令

| 命令 | 说明 | 权限 |
|------|------|------|
| `/new` | 重置会话 | 管理员 |
| `/session` | 查看当前会话信息（token、费用等） | 所有人 |
| `/help` | 显示帮助 | 所有人 |

其他 `/` 开头的命令会直接透传给 agent。

## 后台运行

### 使用 systemd

```ini
# /etc/systemd/system/agentgate.service
[Unit]
Description=agentgate
After=network.target

[Service]
User=你的用户名
WorkingDirectory=/path/to/agentgate
ExecStart=/path/to/agentgate/.venv/bin/agentgate -c config.yaml
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable agentgate --now
```

### 使用 screen/tmux

```bash
screen -S agentgate
agentgate -c config.yaml
# Ctrl+A D 分离
```

## 常见问题

### NapCat 连不上 agentgate

- 检查 NapCat 配置的 URL 是否正确（`ws://127.0.0.1:8765/onebot/v11/ws`）
- 检查 token 是否一致
- 检查 agentgate 是否已启动
- 如果不在同一台机器，检查防火墙

### 机器人收到消息但不回复

- 检查 agentgate 日志是否有报错
- 确认 CLI agent 已安装且在 PATH 中（运行 `claude --version` 确认）
- 私聊测试：admin_users 中的 QQ 号直接私聊机器人
- 群聊测试：必须 @机器人 或回复机器人的消息

### 回复很慢

这取决于你的 CLI agent。Claude Code 首次调用需要初始化 session，可能需要 10-30 秒。后续调用（同一 session）会快很多，因为有缓存。

### 如何让 agent 更好用

agentgate 只是一个桥接层，agent 好不好用取决于你的 workspace 配置。对于 Claude Code：

- 编辑 `workspace/CLAUDE.md` 添加全局指令
- 在 `workspace/.claude/skills/` 下添加技能
- 在 `workspace/.claude/rules/` 下添加规则

具体参考 [Claude Code 文档](https://docs.anthropic.com/en/docs/claude-code)。
