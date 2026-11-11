# MaiBot Milky 协议适配器 (maibot-plugin-milky)

基于 [`milky-python-sdk`](https://github.com/notnotype/milky-python-sdk) 与 `maibot-plugin-sdk` 2.22.x 开发的 [Milky 协议](https://github.com/SaltifyDev/milky) QQ 适配器插件，支持连接 LagrangeV2、Acidify（Yogurt）、LuckyLilliaBot 等支持 Milky 协议的 QQ 协议端。

## 核心特性

- **原生 SDK 驱动**：直接基于 `milky-python-sdk` (`AsyncMilkyClient` + `milky.models` 强类型 Pydantic 模型)，复用 `httpx` 异步连接池调用 `/api/:api`。
- **双模式事件流**：支持 WebSocket (`ws://{host}:{port}/event`) 与 SSE (`http://{host}:{port}/event`) 两种事件接收方式，断线自动重连并支持 WebUI 状态面板与一键重连控件（`ConfigWidget`）。
- **完整消息编解码与向后兼容**：
  - 支持文本、`@`、全体 `@`、QQ 原生表情、回复引用、图片、动画表情、语音、视频、文件、合并转发的双向转换。
  - 遵循 Milky 联合类型兼容性规范：遇到未来协议新增的未知消息段子类型自动安全降级为可读文本段，不中断消息流。
  - 严格遵循 MaiBot 1.4+ 消息归属规范（顶层 `platform` / `account_id` / `scope`）。
- **宿主标准接口全覆盖**：
  - `adapter.avatar.get`（version `1`, `public=True`）：无缝支持 MaiBot 宿主与 WebUI 的用户/群头像按需加载与缓存。
  - `adapter.files.resolve`（version `1`, `public=False`）：对接 MaiBot `FileService`，为入站群文件与私聊文件提供按需下载直链解析。
  - 出站回执 `message_id_echo`：完整回传平台真实消息 ID，保障后续引用回复与撤回链路。
- **内置 LLM 群管与交互工具 (`@Tool`，对齐官方 OneBot 适配器标准命名与签名)**：
  - `open_private_chat` & `get_qq_by_msg_id`：主动发起私聊会话、按消息 ID 查询发送者 QQ 号。
  - `send_poke`：群聊 / 私聊戳一戳（Nudge）。
  - `recall_message`：撤回群聊或私聊消息。
  - `set_mute`：群成员禁言 / 解除禁言（含群主/管理员角色预检、时长约束与保护名单）。
  - `kick_member`：将成员移出群聊。
  - `react_emoji`：群消息贴表情表态（Reaction）。
  - `list_chat_streams` & `forward_messages`：查询活跃聊天流并打包发送合并转发消息。
- **公开插件 RPC 接口 (`@API`)**：提供 `adapter.milky.*` 系列接口供其他插件调用。
## 配置说明

插件首次加载时会自动生成 `config.toml`，也可直接在 MaiBot WebUI「插件配置」页面可视化配置：

```toml
[plugin]
enabled = true
config_version = "1.0.0"

[client]
host = "127.0.0.1"
port = 3000
token = ""
event_mode = "websocket" # "websocket" 或 "sse"
reconnect_delay_sec = 5.0
action_timeout_sec = 30.0
connection_id = ""       # 多链路路由 scope，单链路留空

[chat_abilities]
enable_private_chat_tool = false
enable_poke_tool = false
enable_recall_tool = false
enable_mute_tool = false
enable_kick_tool = false
enable_emoji_like_tool = false
enable_forward_tool = false
[mute]
admin_users = []
allowed_groups = []
min_duration = 60
max_duration = 2592000
```

> 群聊与私聊的准入黑白名单统一由 MaiBot 主程序的 `config/adapter_policy.toml`（或 WebUI「适配器设置」）管理。
