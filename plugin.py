"""MaiBot Milky 协议适配器插件（基于 milky-python-sdk）。"""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import suppress
from typing import Any, ClassVar, Dict, List, Optional, Set, Tuple, Type

import asyncio
import json
import re

from aiohttp import ClientSession, ClientTimeout, WSMsgType
from maibot_sdk import (
    API,
    CONFIG_RELOAD_SCOPE_SELF,
    ConfigWidget,
    ConfigWidgetState,
    MaiBotPlugin,
    MessageGateway,
    PluginConfigBase,
    Tool,
)
from maibot_sdk.types import ToolParameterInfo, ToolParamType
from milky import AsyncMilkyClient
from milky.models import MessageScene
import httpx

from .codec import PLATFORM, MilkyCodec, build_message_id, parse_milky_message_id
from .config import MilkyAdapterConfig

GATEWAY_NAME = "milky_gateway"
AVATAR_TTL_SEC = 86400
SET_MUTE_TOOL = "set_mute"
RECALL_TOOL = "recall_message"
KICK_TOOL = "kick_member"
POKE_TOOL = "send_poke"
REACT_TOOL = "react_message"
PRIVATE_CHAT_TOOL = "send_private_message"


class MilkyAdapterPlugin(MaiBotPlugin):
    """基于 milky-python-sdk 的 QQ / Milky 消息网关与能力插件。"""

    config_model: ClassVar[Optional[Type[PluginConfigBase]]] = MilkyAdapterConfig

    config_widgets: ClassVar[List[ConfigWidget]] = [
        ConfigWidget(id="connection", kind="status", label="Milky 连接状态"),
        ConfigWidget(id="reconnect", kind="action", label="重新连接"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self._codec: Optional[MilkyCodec] = None
        self._client: Optional[AsyncMilkyClient] = None
        self._http_client: Optional[httpx.AsyncClient] = None
        self._worker: Optional[asyncio.Task[None]] = None
        self._dispatch_tasks: Set[asyncio.Task[None]] = set()
        self._lifecycle_lock = asyncio.Lock()
        self._ready = False
        self._account_id = ""
        self._bot_nickname = ""
        self._impl_name = ""

    async def get_config_widget_state(self) -> Dict[str, ConfigWidgetState]:
        if self._ready:
            impl_suffix = f" ({self._impl_name})" if self._impl_name else ""
            text = f"已连接：{self._bot_nickname or self._account_id} [{self._account_id}]{impl_suffix}"
            tone = "success"
        elif self._worker is not None and not self._worker.done():
            text = f"正在连接 / 等待重连：{self.config.client.base_url}"
            tone = "warning"
        else:
            text = "未启用或未连接"
            tone = "neutral"
        return {
            "connection": ConfigWidgetState(text=text, tone=tone),
            "reconnect": ConfigWidgetState(disabled=not self.config.plugin.enabled),
        }

    async def on_config_widget_action(self, action_id: str) -> None:
        if action_id != "reconnect":
            raise ValueError(f"未知操作: {action_id}")
        async with self._lifecycle_lock:
            await self._stop()
            if self.config.plugin.enabled:
                self._start()

    async def on_load(self) -> None:
        self._codec = MilkyCodec(self.ctx.logger)
        await self._sync_tool_states()
        if self.config.plugin.enabled:
            self._start()

    async def on_unload(self) -> None:
        async with self._lifecycle_lock:
            await self._stop()

    async def on_config_update(self, scope: str, config_data: Dict[str, Any], version: str) -> None:
        del version
        if scope != CONFIG_RELOAD_SCOPE_SELF:
            return
        async with self._lifecycle_lock:
            self.set_plugin_config(config_data)
            await self._sync_tool_states()
            await self._stop()
            if self.config.plugin.enabled:
                self._start()

    def _start(self) -> None:
        if self._worker is not None and not self._worker.done():
            return
        self._worker = asyncio.create_task(self._connection_loop(), name="milky-adapter-worker")

    async def _stop(self) -> None:
        worker = self._worker
        self._worker = None
        if worker is not None:
            worker.cancel()
            with suppress(asyncio.CancelledError):
                await worker

        tasks = tuple(self._dispatch_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._dispatch_tasks.clear()

        await self._set_ready(False)
        if self._client is not None:
            await self._client.close()
            self._client = None
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None
        self._account_id = ""
        self._bot_nickname = ""
        self._impl_name = ""

    async def _sync_tool_states(self) -> None:
        abilities = self.config.chat_abilities
        tool_switches = (
            (POKE_TOOL, abilities.enable_poke_tool),
            (RECALL_TOOL, abilities.enable_recall_tool),
            (SET_MUTE_TOOL, abilities.enable_mute_tool),
            (KICK_TOOL, abilities.enable_kick_tool),
            (REACT_TOOL, abilities.enable_reaction_tool),
            (PRIVATE_CHAT_TOOL, abilities.enable_private_chat_tool),
        )
        for tool_name, enabled in tool_switches:
            comp_name = f"{self.ctx.plugin_id}.{tool_name}"
            with suppress(Exception):
                if enabled:
                    await self.ctx.component.enable_component(comp_name, "TOOL")
                else:
                    await self.ctx.component.disable_component(comp_name, "TOOL")

    async def _set_ready(self, ready: bool) -> None:
        if not ready and not self._ready:
            return
        scope = self.config.client.normalized_scope
        with suppress(Exception):
            accepted = await self.ctx.gateway.update_state(
                gateway_name=GATEWAY_NAME,
                ready=ready,
                platform=PLATFORM,
                account_id=self._account_id,
                scope=scope,
                metadata={
                    "protocol": "milky",
                    "impl_name": self._impl_name,
                    "base_url": self.config.client.base_url,
                },
            )
            self._ready = bool(ready and accepted)
            return
        self._ready = False

    def _require_client(self) -> AsyncMilkyClient:
        if not self._ready or self._client is None:
            raise RuntimeError("Milky 协议网关尚未就绪")
        return self._client

    def _require_codec(self) -> MilkyCodec:
        if self._codec is None:
            self._codec = MilkyCodec(self.ctx.logger)
        return self._codec

    async def _connection_loop(self) -> None:
        client_cfg = self.config.client
        self._client = AsyncMilkyClient(
            base_url=client_cfg.base_url,
            access_token=client_cfg.token.strip() or None,
            timeout=client_cfg.action_timeout_sec,
        )
        self._http_client = httpx.AsyncClient(timeout=client_cfg.action_timeout_sec, follow_redirects=True)

        while True:
            try:
                login_info = await self._client.get_login_info()
                self._account_id = str(login_info.uin)
                self._bot_nickname = login_info.nickname
                with suppress(Exception):
                    impl_info = await self._client.get_impl_info()
                    self._impl_name = f"{impl_info.impl_name} {impl_info.impl_version}".strip()

                await self._set_ready(True)
                self.ctx.logger.info(
                    "Milky 网关已激活: account=%s nickname=%s mode=%s",
                    self._account_id,
                    self._bot_nickname,
                    client_cfg.event_mode,
                )

                if client_cfg.event_mode == "sse":
                    await self._run_sse_loop()
                else:
                    await self._run_ws_loop()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.ctx.logger.warning(
                    "Milky 事件流连接断开或异常: %s，将在 %s 秒后重连",
                    exc,
                    client_cfg.reconnect_delay_sec,
                )
            finally:
                await self._set_ready(False)

            await asyncio.sleep(client_cfg.reconnect_delay_sec)

    async def _run_sse_loop(self) -> None:
        assert self._client is not None
        async for event in self._client.events_sse():
            if isinstance(event, Mapping):
                self._spawn_event_task(dict(event))

    async def _run_ws_loop(self) -> None:
        client_cfg = self.config.client
        headers: Dict[str, str] = {}
        if client_cfg.token.strip():
            headers["Authorization"] = f"Bearer {client_cfg.token.strip()}"
        timeout = ClientTimeout(total=None, connect=10)
        async with ClientSession(headers=headers, timeout=timeout) as session:
            async with session.ws_connect(client_cfg.ws_event_url) as ws:
                async for msg in ws:
                    if msg.type == WSMsgType.TEXT:
                        try:
                            payload = json.loads(msg.data)
                        except ValueError:
                            continue
                        if isinstance(payload, Mapping):
                            self._spawn_event_task(dict(payload))
                    elif msg.type in {WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR}:
                        break

    def _spawn_event_task(self, payload: Dict[str, Any]) -> None:
        task = asyncio.create_task(self._handle_event(payload), name="milky-event-dispatch")
        self._dispatch_tasks.add(task)
        task.add_done_callback(self._dispatch_tasks.discard)

    async def _handle_event(self, payload: Dict[str, Any]) -> None:
        if not self._ready or self._client is None or self._http_client is None:
            return
        event_type = str(payload.get("event_type") or "").strip()
        event_data = payload.get("data")
        if not isinstance(event_data, Mapping):
            return

        self_id = str(payload.get("self_id") or self._account_id).strip()
        scope = self.config.client.normalized_scope
        codec = self._require_codec()

        if event_type == "message_receive":
            sender_id = str(event_data.get("sender_id") or "").strip()
            if not sender_id:
                return
            if self.config.filters.ignore_self_message and sender_id == self_id:
                return
            if sender_id in self.config.filters.ban_user_id:
                return

            try:
                message_dict = await codec.decode_inbound_message(
                    event_data,
                    client=self._client,
                    http_client=self._http_client,
                    account_id=self_id,
                    scope=scope,
                )
            except Exception as exc:
                self.ctx.logger.warning("解析 Milky 入站消息失败: %s", exc)
                return

            if message_dict is None:
                return
            plain_text = str(message_dict.get("processed_plain_text") or "")
            if not self.config.filters.is_text_allowed(plain_text):
                return

            ext_id = message_dict["message_id"]
            await self.ctx.gateway.route_message(
                gateway_name=GATEWAY_NAME,
                message=message_dict,
                external_message_id=ext_id,
                dedupe_key=ext_id,
            )
            return

        notice_msg, dedupe_key = codec.decode_notice_event(
            event_type,
            event_data,
            notice_cfg=self.config.notice,
            account_id=self_id,
            scope=scope,
        )
        if notice_msg is not None:
            await self.ctx.gateway.route_message(
                gateway_name=GATEWAY_NAME,
                message=notice_msg,
                external_message_id="",
                dedupe_key=dedupe_key,
            )

    # =========================================================================
    # 消息网关出站处理
    # =========================================================================

    @MessageGateway(
        name=GATEWAY_NAME,
        route_type="duplex",
        platform=PLATFORM,
        protocol="milky",
        description="Milky 协议 QQ 双向消息网关",
    )
    async def handle_milky_gateway(
        self,
        message: Dict[str, Any],
        route: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        del metadata, kwargs
        try:
            client = self._require_client()
            codec = self._require_codec()
            route_dict = route or {}
            msg_info = message.get("message_info")
            msg_info = msg_info if isinstance(msg_info, Mapping) else {}
            group_info = msg_info.get("group_info")
            group_info = group_info if isinstance(group_info, Mapping) else {}
            add_cfg = msg_info.get("additional_config")
            add_cfg = add_cfg if isinstance(add_cfg, Mapping) else {}

            raw_segments = message.get("raw_message", [])
            normal_items, file_uploads = codec.extract_outbound_files(raw_segments)

            group_id_str = str(
                group_info.get("group_id") or add_cfg.get("platform_io_target_group_id") or ""
            ).strip()
            if group_id_str:
                group_id = int(group_id_str)
                ext_id = ""
                meta: Dict[str, Any] = {}
                if normal_items or not file_uploads:
                    segments = codec.encode_outbound_segments(normal_items)
                    res = await client.send_group_message(group_id, segments)
                    ext_id = build_message_id("group", group_id, res.message_seq)
                    meta = {"message_seq": res.message_seq, "time": res.time}
                for file_uri, file_name in file_uploads:
                    up_res = await client.upload_group_file(group_id, file_uri, file_name)
                    if not ext_id:
                        ext_id = f"milky-group-file-{group_id}-{up_res.file_id}"
                return {
                    "success": True,
                    "external_message_id": ext_id or None,
                    "metadata": meta,
                }

            user_id_str = str(
                add_cfg.get("platform_io_target_user_id")
                or add_cfg.get("target_user_id")
                or route_dict.get("target_user_id")
                or ""
            ).strip()
            if not user_id_str:
                return {"success": False, "error": "出站私聊消息缺少目标 user_id"}

            user_id = int(user_id_str)
            ext_id = ""
            meta = {}
            if normal_items or not file_uploads:
                segments = codec.encode_outbound_segments(normal_items)
                res = await client.send_private_message(user_id, segments)
                ext_id = build_message_id("friend", user_id, res.message_seq)
                meta = {"message_seq": res.message_seq, "time": res.time}
            for file_uri, file_name in file_uploads:
                up_res = await client.upload_private_file(user_id, file_uri, file_name)
                if not ext_id:
                    ext_id = f"milky-private-file-{user_id}-{up_res.file_id}"
            return {
                "success": True,
                "external_message_id": ext_id or None,
                "metadata": meta,
            }
        except Exception as exc:
            self.ctx.logger.warning("Milky 出站发送失败: %s", exc)
            return {"success": False, "error": str(exc)}

    # =========================================================================
    # 统一头像协议 API
    # =========================================================================

    @API("adapter.avatar.get", description="获取 QQ 用户或群头像资源", version="1", public=True)
    async def api_get_avatar(
        self,
        platform: str,
        target_id: str,
        target_type: str = "user",
        account_id: str = "",
        scope: str = "",
    ) -> Dict[str, Any]:
        del account_id, scope
        if platform != PLATFORM:
            return {"status": "unsupported"}
        if target_type not in {"user", "group"}:
            raise ValueError("target_type 必须为 user 或 group")
        normalized_id = str(target_id or "").strip()
        if not re.fullmatch(r"[1-9][0-9]*", normalized_id):
            return {"status": "unsupported"}
        url = (
            f"https://p.qlogo.cn/gh/{normalized_id}/{normalized_id}/640"
            if target_type == "group"
            else f"https://q1.qlogo.cn/g?b=qq&nk={normalized_id}&s=640"
        )
        return {"status": "available", "url": url, "expires_in": AVATAR_TTL_SEC}

    # =========================================================================
    # LLM 群管与交互工具 (@Tool)
    # =========================================================================

    async def _resolve_message_context(
        self, msg_id: str, stream_id: str
    ) -> Tuple[Optional[Mapping[str, Any]], str, str, Optional[int]]:
        """根据 MaiBot 消息 ID 解析 ``(message_info, group_id, sender_id, message_seq)``。"""
        query_res = await self.ctx.message.get_by_id(msg_id, stream_id=stream_id)
        if not isinstance(query_res, Mapping):
            return None, "", "", None
        msg_info = query_res.get("message_info")
        if not isinstance(msg_info, Mapping):
            return None, "", "", None
        group_info = msg_info.get("group_info")
        group_id = str(group_info.get("group_id") or "").strip() if isinstance(group_info, Mapping) else ""
        user_info = msg_info.get("user_info")
        sender_id = str(user_info.get("user_id") or "").strip() if isinstance(user_info, Mapping) else ""
        add_cfg = msg_info.get("additional_config")
        add_cfg = add_cfg if isinstance(add_cfg, Mapping) else {}
        if not group_id:
            group_id = str(add_cfg.get("platform_io_target_group_id") or "").strip()
        seq_raw = add_cfg.get("milky_message_seq")
        _, _, parsed_seq = parse_milky_message_id(str(query_res.get("message_id") or msg_id))
        seq = int(seq_raw) if isinstance(seq_raw, int) else parsed_seq
        return msg_info, group_id, sender_id, seq

    @Tool(
        SET_MUTE_TOOL,
        platforms=PLATFORM,
        description="设置 QQ 群成员的禁言状态：duration 为正整数时禁言对应秒数，0 表示解除禁言。msg_id 使用目标成员发过的消息 ID。",
        parameters=[
            ToolParameterInfo(name="msg_id", param_type=ToolParamType.STRING, description="目标成员发过的消息 ID", required=True),
            ToolParameterInfo(name="duration", param_type=ToolParamType.INTEGER, description="禁言秒数；0 为解除禁言", required=True),
            ToolParameterInfo(name="reason", param_type=ToolParamType.STRING, description="禁言原因（可选）", required=False, default=""),
        ],
    )
    async def tool_set_mute(self, msg_id: str, duration: int, reason: str = "", **kwargs: Any) -> Dict[str, Any]:
        if not self.config.chat_abilities.enable_mute_tool:
            return {"success": False, "error": "禁言工具未启用"}
        try:
            client = self._require_client()
            stream_id = str(kwargs.get("stream_id") or "")
            msg_info, group_id, target_uid, _ = await self._resolve_message_context(msg_id.strip(), stream_id)
            if msg_info is None or not group_id or not target_uid:
                return {"success": False, "error": f"未能从消息 {msg_id} 解析出群聊与发送者"}
            mute_cfg = self.config.mute
            if mute_cfg.allowed_groups and group_id not in mute_cfg.allowed_groups:
                return {"success": False, "error": f"群 {group_id} 不在允许群管白名单中"}
            if duration > 0 and target_uid in mute_cfg.admin_users:
                return {"success": False, "error": f"用户 {target_uid} 在保护名单中，不可禁言"}
            effective_duration = 0 if duration <= 0 else max(mute_cfg.min_duration, min(int(duration), mute_cfg.max_duration))
            await client.set_group_member_mute(int(group_id), int(target_uid), effective_duration)
            action_text = f"已解除用户 {target_uid} 的禁言" if effective_duration == 0 else f"已将用户 {target_uid} 禁言 {effective_duration} 秒"
            return {"success": True, "content": action_text, "group_id": group_id, "user_id": target_uid, "duration": effective_duration, "reason": reason}
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    @Tool(
        RECALL_TOOL,
        platforms=PLATFORM,
        description="撤回一条 QQ 群聊或私聊消息。msg_id 使用聊天记录中的消息 ID。",
        parameters=[
            ToolParameterInfo(name="msg_id", param_type=ToolParamType.STRING, description="要撤回的消息 ID", required=True),
        ],
    )
    async def tool_recall_message(self, msg_id: str, **kwargs: Any) -> Dict[str, Any]:
        if not self.config.chat_abilities.enable_recall_tool:
            return {"success": False, "error": "撤回工具未启用"}
        try:
            client = self._require_client()
            stream_id = str(kwargs.get("stream_id") or "")
            _, group_id, sender_id, seq = await self._resolve_message_context(msg_id.strip(), stream_id)
            if seq is None:
                _, parsed_peer, parsed_seq = parse_milky_message_id(msg_id.strip())
                seq = parsed_seq
                if not group_id and parsed_peer is not None:
                    group_id = str(parsed_peer)
            if seq is None:
                return {"success": False, "error": f"无法解析消息 {msg_id} 的 Milky message_seq"}
            if group_id:
                if self.config.mute.allowed_groups and group_id not in self.config.mute.allowed_groups:
                    return {"success": False, "error": f"群 {group_id} 不在允许白名单中"}
                await client.recall_group_message(int(group_id), seq)
            elif sender_id:
                await client.recall_private_message(int(sender_id), seq)
            else:
                return {"success": False, "error": "无法定位消息所属会话"}
            return {"success": True, "content": f"已撤回消息 {msg_id} (seq={seq})"}
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    @Tool(
        KICK_TOOL,
        platforms=PLATFORM,
        description="将指定成员移出 QQ 群。msg_id 使用目标成员发过的消息 ID。",
        parameters=[
            ToolParameterInfo(name="msg_id", param_type=ToolParamType.STRING, description="目标成员发过的消息 ID", required=True),
            ToolParameterInfo(name="reason", param_type=ToolParamType.STRING, description="移出原因（可选）", required=False, default=""),
        ],
    )
    async def tool_kick_member(self, msg_id: str, reason: str = "", **kwargs: Any) -> Dict[str, Any]:
        if not self.config.chat_abilities.enable_kick_tool:
            return {"success": False, "error": "踢人工具未启用"}
        try:
            client = self._require_client()
            stream_id = str(kwargs.get("stream_id") or "")
            _, group_id, target_uid, _ = await self._resolve_message_context(msg_id.strip(), stream_id)
            if not group_id or not target_uid:
                return {"success": False, "error": f"未能从消息 {msg_id} 解析出群号与用户 ID"}
            if self.config.mute.allowed_groups and group_id not in self.config.mute.allowed_groups:
                return {"success": False, "error": f"群 {group_id} 不在允许群管白名单中"}
            if target_uid in self.config.mute.admin_users or target_uid == self._account_id:
                return {"success": False, "error": f"用户 {target_uid} 在保护名单中或为机器人自身"}
            await client.kick_group_member(int(group_id), int(target_uid), reject_add_request=False)
            return {"success": True, "content": f"已将成员 {target_uid} 移出群 {group_id}", "reason": reason}
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    @Tool(
        POKE_TOOL,
        platforms=PLATFORM,
        description="发送 QQ 戳一戳（拍一拍）。可传入 msg_id 戳该消息发送者，或直接指定 user_id。",
        parameters=[
            ToolParameterInfo(name="user_id", param_type=ToolParamType.STRING, description="目标用户 QQ 号（可选，不填则从 msg_id 解析）", required=False, default=""),
            ToolParameterInfo(name="msg_id", param_type=ToolParamType.STRING, description="目标用户发过的消息 ID（可选）", required=False, default=""),
        ],
    )
    async def tool_send_poke(self, user_id: str = "", msg_id: str = "", **kwargs: Any) -> Dict[str, Any]:
        if not self.config.chat_abilities.enable_poke_tool:
            return {"success": False, "error": "戳一戳工具未启用"}
        try:
            client = self._require_client()
            group_id = str(kwargs.get("group_id") or "").strip()
            target_uid = str(user_id or "").strip()
            if msg_id.strip():
                _, resolved_group, resolved_uid, _ = await self._resolve_message_context(
                    msg_id.strip(), str(kwargs.get("stream_id") or "")
                )
                group_id = group_id or resolved_group
                target_uid = target_uid or resolved_uid
            if not target_uid:
                target_uid = str(kwargs.get("user_id") or "").strip()
            if not target_uid.isdigit():
                return {"success": False, "error": "缺少有效的目标用户 QQ 号"}
            if group_id and group_id.isdigit():
                await client.send_group_nudge(int(group_id), int(target_uid))
            else:
                await client.send_friend_nudge(int(target_uid))
            return {"success": True, "content": f"已戳一戳用户 {target_uid}"}
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    @Tool(
        REACT_TOOL,
        platforms=PLATFORM,
        description="对当前 QQ 群聊中的指定消息贴表情表态（reaction）。",
        parameters=[
            ToolParameterInfo(name="msg_id", param_type=ToolParamType.STRING, description="目标消息 ID", required=True),
            ToolParameterInfo(name="reaction", param_type=ToolParamType.STRING, description="QQ 表情 ID（如 '76' 为赞）", required=True),
            ToolParameterInfo(name="is_add", param_type=ToolParamType.BOOLEAN, description="True 添加回应，False 取消回应", required=False, default=True),
        ],
    )
    async def tool_react_message(self, msg_id: str, reaction: str, is_add: bool = True, **kwargs: Any) -> Dict[str, Any]:
        if not self.config.chat_abilities.enable_reaction_tool:
            return {"success": False, "error": "表情表态工具未启用"}
        try:
            client = self._require_client()
            stream_id = str(kwargs.get("stream_id") or "")
            _, group_id, _, seq = await self._resolve_message_context(msg_id.strip(), stream_id)
            if not group_id or seq is None:
                return {"success": False, "error": "仅支持对有效的群聊消息添加表情回应"}
            await client.send_group_message_reaction(int(group_id), seq, reaction.strip(), is_add=is_add)
            return {"success": True, "content": f"已对消息 {msg_id} {'添加' if is_add else '取消'}表情回应 {reaction}"}
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    @Tool(
        PRIVATE_CHAT_TOOL,
        platforms=PLATFORM,
        description="主动向指定 QQ 号发送私聊消息。",
        parameters=[
            ToolParameterInfo(name="user_id", param_type=ToolParamType.STRING, description="目标用户 QQ 号", required=True),
            ToolParameterInfo(name="content", param_type=ToolParamType.STRING, description="要发送的私聊文本内容", required=True),
        ],
    )
    async def tool_send_private_message(self, user_id: str, content: str, **kwargs: Any) -> Dict[str, Any]:
        del kwargs
        if not self.config.chat_abilities.enable_private_chat_tool:
            return {"success": False, "error": "主动私聊工具未启用"}
        try:
            uid = str(user_id or "").strip()
            text = str(content or "").strip()
            if not uid.isdigit() or not text:
                return {"success": False, "error": "必须提供有效的数字 QQ 号与非空消息内容"}
            opened = await self.ctx.chat.open_session(platform=PLATFORM, chat_type="private", user_id=uid)
            stream_id = str(opened.get("stream_id") or opened.get("session_id") or "").strip() if isinstance(opened, Mapping) else ""
            if stream_id:
                await self.ctx.send.text(text, stream_id)
                return {"success": True, "content": f"已向用户 {uid} 发送私聊消息"}
            client = self._require_client()
            codec = self._require_codec()
            res = await client.send_private_message(int(uid), codec.encode_outbound_segments([{"type": "text", "data": text}]))
            return {"success": True, "content": f"已向用户 {uid} 发送私聊消息 (seq={res.message_seq})"}
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    # =========================================================================
    # 公开插件 RPC API (@API)
    # =========================================================================

    @API("adapter.milky.action.call", description="调用任意 Milky HTTP API", version="1", public=True)
    async def api_call_action(self, action: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        client = self._require_client()
        data = await client._request(action.strip(), params or {})
        return {"status": "ok", "retcode": 0, "data": data}

    @API("adapter.milky.account.get_login_info", description="获取登录账号信息", version="1", public=True)
    async def api_get_login_info(self) -> Dict[str, Any]:
        info = await self._require_client().get_login_info()
        return info.model_dump()

    @API("adapter.milky.system.get_impl_info", description="获取 Milky 协议端版本信息", version="1", public=True)
    async def api_get_impl_info(self) -> Dict[str, Any]:
        info = await self._require_client().get_impl_info()
        return info.model_dump()

    @API("adapter.milky.system.get_user_profile", description="获取 QQ 用户个人资料", version="1", public=True)
    async def api_get_user_profile(self, user_id: int) -> Dict[str, Any]:
        profile = await self._require_client().get_user_profile(int(user_id))
        return profile.model_dump()

    @API("adapter.milky.account.get_friend_list", description="获取好友列表", version="1", public=True)
    async def api_get_friend_list(self, no_cache: bool = False) -> List[Dict[str, Any]]:
        friends = await self._require_client().get_friend_list(no_cache=no_cache)
        return [f.model_dump() for f in friends]

    @API("adapter.milky.group.get_group_list", description="获取群列表", version="1", public=True)
    async def api_get_group_list(self, no_cache: bool = False) -> List[Dict[str, Any]]:
        groups = await self._require_client().get_group_list(no_cache=no_cache)
        return [g.model_dump() for g in groups]

    @API("adapter.milky.group.get_group_info", description="获取群信息", version="1", public=True)
    async def api_get_group_info(self, group_id: int, no_cache: bool = False) -> Dict[str, Any]:
        group = await self._require_client().get_group_info(int(group_id), no_cache=no_cache)
        return group.model_dump()

    @API("adapter.milky.group.get_group_member_info", description="获取群成员信息", version="1", public=True)
    async def api_get_group_member_info(self, group_id: int, user_id: int, no_cache: bool = False) -> Dict[str, Any]:
        member = await self._require_client().get_group_member_info(int(group_id), int(user_id), no_cache=no_cache)
        return member.model_dump()

    @API("adapter.milky.group.get_group_member_list", description="获取群成员列表", version="1", public=True)
    async def api_get_group_member_list(self, group_id: int, no_cache: bool = False) -> List[Dict[str, Any]]:
        members = await self._require_client().get_group_member_list(int(group_id), no_cache=no_cache)
        return [m.model_dump() for m in members]

    @API("adapter.milky.group.set_group_member_mute", description="设置群成员禁言", version="1", public=True)
    async def api_set_group_member_mute(self, group_id: int, user_id: int, duration: int = 0) -> Dict[str, Any]:
        await self._require_client().set_group_member_mute(int(group_id), int(user_id), int(duration))
        return {"success": True}

    @API("adapter.milky.group.kick_group_member", description="踢出群成员", version="1", public=True)
    async def api_kick_group_member(self, group_id: int, user_id: int, reject_add_request: bool = False) -> Dict[str, Any]:
        await self._require_client().kick_group_member(int(group_id), int(user_id), reject_add_request=reject_add_request)
        return {"success": True}

    @API("adapter.milky.group.send_group_nudge", description="发送群戳一戳", version="1", public=True)
    async def api_send_group_nudge(self, group_id: int, user_id: int) -> Dict[str, Any]:
        await self._require_client().send_group_nudge(int(group_id), int(user_id))
        return {"success": True}

    @API("adapter.milky.account.send_friend_nudge", description="发送好友戳一戳", version="1", public=True)
    async def api_send_friend_nudge(self, user_id: int, is_self: bool = False) -> Dict[str, Any]:
        await self._require_client().send_friend_nudge(int(user_id), is_self=is_self)
        return {"success": True}

    @API("adapter.milky.message.recall_group_message", description="撤回群消息", version="1", public=True)
    async def api_recall_group_message(self, group_id: int, message_seq: int) -> Dict[str, Any]:
        await self._require_client().recall_group_message(int(group_id), int(message_seq))
        return {"success": True}

    @API("adapter.milky.message.recall_private_message", description="撤回私聊消息", version="1", public=True)
    async def api_recall_private_message(self, user_id: int, message_seq: int) -> Dict[str, Any]:
        await self._require_client().recall_private_message(int(user_id), int(message_seq))
        return {"success": True}

    @API("adapter.milky.message.get_resource_temp_url", description="获取资源临时下载链接", version="1", public=True)
    async def api_get_resource_temp_url(self, resource_id: str) -> Dict[str, Any]:
        res = await self._require_client().get_resource_temp_url(resource_id)
        return res.model_dump()

    @API("adapter.milky.message.mark_message_as_read", description="标记消息已读", version="1", public=True)
    async def api_mark_message_as_read(self, message_scene: str, peer_id: int, message_seq: int) -> Dict[str, Any]:
        scene = MessageScene(message_scene)
        await self._require_client().mark_message_as_read(scene, int(peer_id), int(message_seq))
        return {"success": True}


    @API("adapter.milky.account.get_friend_info", description="获取好友信息", version="1", public=True)
    async def api_get_friend_info(self, user_id: int, no_cache: bool = False) -> Dict[str, Any]:
        friend = await self._require_client().get_friend_info(int(user_id), no_cache=no_cache)
        return friend.model_dump()

    @API("adapter.milky.group.set_group_whole_mute", description="设置群全体禁言", version="1", public=True)
    async def api_set_group_whole_mute(self, group_id: int, is_mute: bool = True) -> Dict[str, Any]:
        await self._require_client().set_group_whole_mute(int(group_id), is_mute=is_mute)
        return {"success": True}

    @API("adapter.milky.group.set_group_member_card", description="设置群成员名片", version="1", public=True)
    async def api_set_group_member_card(self, group_id: int, user_id: int, card: str) -> Dict[str, Any]:
        await self._require_client().set_group_member_card(int(group_id), int(user_id), str(card))
        return {"success": True}

    @API("adapter.milky.group.set_group_name", description="设置群名称", version="1", public=True)
    async def api_set_group_name(self, group_id: int, new_group_name: str) -> Dict[str, Any]:
        await self._require_client().set_group_name(int(group_id), str(new_group_name))
        return {"success": True}

    @API("adapter.milky.group.quit_group", description="退出群聊", version="1", public=True)
    async def api_quit_group(self, group_id: int) -> Dict[str, Any]:
        await self._require_client().quit_group(int(group_id))
        return {"success": True}

    @API("adapter.milky.file.upload_group_file", description="上传群文件", version="1", public=True)
    async def api_upload_group_file(
        self, group_id: int, file_uri: str, file_name: str, parent_folder_id: str = "/"
    ) -> Dict[str, Any]:
        res = await self._require_client().upload_group_file(
            int(group_id), str(file_uri), str(file_name), parent_folder_id=str(parent_folder_id)
        )
        return res.model_dump()

    @API("adapter.milky.file.upload_private_file", description="上传私聊文件", version="1", public=True)
    async def api_upload_private_file(self, user_id: int, file_uri: str, file_name: str) -> Dict[str, Any]:
        res = await self._require_client().upload_private_file(int(user_id), str(file_uri), str(file_name))
        return res.model_dump()

def create_plugin() -> MilkyAdapterPlugin:
    return MilkyAdapterPlugin()
