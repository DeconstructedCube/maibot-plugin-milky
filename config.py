"""Milky 协议适配器配置模型。"""

from __future__ import annotations

from typing import Any, ClassVar, List, Literal

import re

from maibot_sdk import Field, PluginConfigBase
from pydantic import field_validator

SUPPORTED_CONFIG_VERSION = "1.0.0"


def _normalize_str_list(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    result: List[str] = []
    for item in value:
        text = str(item or "").strip()
        if text and text not in result:
            result.append(text)
    return result


class MilkyPluginSection(PluginConfigBase):
    """插件基础设置。"""

    __ui_label__: ClassVar[str] = "插件设置"
    __ui_icon__: ClassVar[str] = "settings"
    __ui_order__: ClassVar[int] = 0

    enabled: bool = Field(
        default=True,
        description="是否启用 Milky 协议适配器",
        json_schema_extra={"label": "启用适配器", "order": 0},
    )
    config_version: str = Field(
        default=SUPPORTED_CONFIG_VERSION,
        description="配置结构版本",
        json_schema_extra={"label": "配置版本", "disabled": True, "hidden": True, "order": 99},
    )


class MilkyClientSection(PluginConfigBase):
    """Milky 协议端连接配置。"""

    __ui_label__: ClassVar[str] = "Milky 连接"
    __ui_icon__: ClassVar[str] = "link"
    __ui_order__: ClassVar[int] = 1

    host: str = Field(
        default="127.0.0.1",
        description="Milky 协议端主机地址",
        json_schema_extra={"label": "主机地址", "placeholder": "127.0.0.1", "order": 0},
    )
    port: int = Field(
        default=3000,
        description="Milky 协议端 HTTP 服务端口",
        json_schema_extra={"label": "端口", "order": 1},
    )
    token: str = Field(
        default="",
        description="Milky 访问令牌（access_token），未启用鉴权时留空",
        json_schema_extra={"label": "访问令牌", "input_type": "password", "placeholder": "可留空", "order": 2},
    )
    event_mode: Literal["websocket", "sse"] = Field(
        default="websocket",
        description="事件接收模式：websocket（/event WS）或 sse（/event SSE）",
        json_schema_extra={"label": "事件模式", "order": 3},
    )
    reconnect_delay_sec: float = Field(
        default=5.0,
        description="断线重连等待时间（秒）",
        json_schema_extra={"label": "重连间隔（秒）", "step": 1, "order": 4},
    )
    action_timeout_sec: float = Field(
        default=30.0,
        description="调用 Milky HTTP API 的超时时间（秒）",
        json_schema_extra={"label": "API 超时（秒）", "step": 1, "order": 5},
    )
    connection_id: str = Field(
        default="",
        description="连接作用域标识（scope），多实例连接同一账号时用于区分链路",
        json_schema_extra={"label": "连接标识 (scope)", "placeholder": "例如 primary，单链路留空", "order": 6},
    )

    @property
    def base_url(self) -> str:
        return f"http://{self.host.strip() or '127.0.0.1'}:{self.port}"

    @property
    def ws_event_url(self) -> str:
        return f"ws://{self.host.strip() or '127.0.0.1'}:{self.port}/event"

    @property
    def normalized_scope(self) -> str:
        return self.connection_id.strip()


class MilkyChatAbilitiesSection(PluginConfigBase):
    """提供给大模型的聊天与群管工具开关。"""

    __ui_label__: ClassVar[str] = "LLM 工具开关"
    __ui_icon__: ClassVar[str] = "build"
    __ui_order__: ClassVar[int] = 2

    enable_poke_tool: bool = Field(
        default=False,
        description="启用戳一戳工具（send_poke）",
        json_schema_extra={"label": "启用戳一戳工具", "order": 0},
    )
    enable_recall_tool: bool = Field(
        default=False,
        description="启用撤回消息工具（recall_message）",
        json_schema_extra={"label": "启用撤回工具", "order": 1},
    )
    enable_mute_tool: bool = Field(
        default=False,
        description="启用群禁言工具（set_mute）",
        json_schema_extra={"label": "启用禁言工具", "order": 2},
    )
    enable_kick_tool: bool = Field(
        default=False,
        description="启用群踢人工具（kick_member）",
        json_schema_extra={"label": "启用踢人工具", "order": 3},
    )
    enable_reaction_tool: bool = Field(
        default=False,
        description="启用群消息表情回应工具（react_message）",
        json_schema_extra={"label": "启用表情表态工具", "order": 4},
    )
    enable_private_chat_tool: bool = Field(
        default=False,
        description="启用主动发起私聊工具（send_private_message）",
        json_schema_extra={"label": "启用主动私聊工具", "order": 5},
    )


class MilkyMuteSection(PluginConfigBase):
    """禁言与群管保护规则。"""

    __ui_label__: ClassVar[str] = "群管保护设置"
    __ui_icon__: ClassVar[str] = "security"
    __ui_order__: ClassVar[int] = 3

    admin_users: List[str] = Field(
        default_factory=list,
        description="群管保护名单（QQ 号），名单内用户不会被禁言或踢出",
        json_schema_extra={"label": "保护用户单 (QQ号)", "order": 0},
    )
    allowed_groups: List[str] = Field(
        default_factory=list,
        description="允许执行禁言/踢人/撤回的群号白名单，留空表示不限制",
        json_schema_extra={"label": "允许群白名单", "order": 1},
    )
    min_duration: int = Field(
        default=60,
        description="最短禁言时长（秒）",
        json_schema_extra={"label": "最短禁言时长（秒）", "order": 2},
    )
    max_duration: int = Field(
        default=2592000,
        description="最长禁言时长（秒），默认 30 天",
        json_schema_extra={"label": "最长禁言时长（秒）", "order": 3},
    )

    @field_validator("admin_users", "allowed_groups", mode="before")
    @classmethod
    def _validate_lists(cls, value: Any) -> List[str]:
        return _normalize_str_list(value)


class MilkyNoticeSection(PluginConfigBase):
    """平台通知事件转发设置。"""

    __ui_label__: ClassVar[str] = "通知事件"
    __ui_icon__: ClassVar[str] = "notifications"
    __ui_order__: ClassVar[int] = 4

    enabled: bool = Field(default=True, description="是否向 MaiBot 转发平台通知事件")
    enable_nudge: bool = Field(default=True, description="转发好友与群聊戳一戳事件")
    enable_recall: bool = Field(default=True, description="转发消息撤回事件")
    enable_group_mute: bool = Field(default=True, description="转发群禁言与全体禁言事件")
    enable_group_reaction: bool = Field(default=True, description="转发群消息表情回应事件")
    enable_group_file_upload: bool = Field(default=True, description="转发文件上传事件")
    enable_group_member_change: bool = Field(default=True, description="转发群成员增加/减少事件")
    enable_group_admin_change: bool = Field(default=True, description="转发群管理员变更事件")
    enable_group_essence: bool = Field(default=True, description="转发群精华消息变更事件")
    enable_group_name_change: bool = Field(default=True, description="转发群名称变更事件")


class MilkyFilterSection(PluginConfigBase):
    """入站消息过滤配置（会话放行范围请在主程序「适配器策略」中配置）。"""

    __ui_label__: ClassVar[str] = "消息过滤"
    __ui_icon__: ClassVar[str] = "filter_alt"
    __ui_order__: ClassVar[int] = 5

    ignore_self_message: bool = Field(
        default=True,
        description="是否忽略机器人自身发送的消息",
    )
    ban_user_id: List[str] = Field(
        default_factory=list,
        description="全局屏蔽的用户 QQ 号列表",
    )
    regex_filter_enabled: bool = Field(
        default=False,
        description="是否启用正则表达式消息过滤",
    )
    regex_filter_mode: Literal["blacklist", "whitelist"] = Field(
        default="blacklist",
        description="正则过滤模式：blacklist（匹配则丢弃）或 whitelist（仅放行匹配）",
    )
    regex_filter_patterns: List[str] = Field(
        default_factory=list,
        description="正则表达式列表（Python re 语法）",
    )

    @field_validator("ban_user_id", "regex_filter_patterns", mode="before")
    @classmethod
    def _validate_lists(cls, value: Any) -> List[str]:
        return _normalize_str_list(value)

    def is_text_allowed(self, text: str) -> bool:
        if not self.regex_filter_enabled or not self.regex_filter_patterns:
            return True
        matched = False
        for pattern in self.regex_filter_patterns:
            try:
                if re.search(pattern, text):
                    matched = True
                    break
            except re.error:
                continue
        return not matched if self.regex_filter_mode == "blacklist" else matched


class MilkyAdapterConfig(PluginConfigBase):
    """Milky 协议适配器完整配置。"""

    plugin: MilkyPluginSection = Field(default_factory=MilkyPluginSection)
    client: MilkyClientSection = Field(default_factory=MilkyClientSection)
    chat_abilities: MilkyChatAbilitiesSection = Field(default_factory=MilkyChatAbilitiesSection)
    mute: MilkyMuteSection = Field(default_factory=MilkyMuteSection)
    notice: MilkyNoticeSection = Field(default_factory=MilkyNoticeSection)
    filters: MilkyFilterSection = Field(default_factory=MilkyFilterSection)
