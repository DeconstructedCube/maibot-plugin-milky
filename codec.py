"""Milky 协议与 MaiBot 标准消息双向编解码器（基于 milky-python-sdk）。"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

import base64
import hashlib
import time

import httpx
from milky.async_client import AsyncMilkyClient
from milky.models import (
    EventType,
    FaceSegmentData,
    FriendMessage,
    GroupMessage,
    ImageSubType,
    IncomingMessage,
    MentionAllSegmentData,
    MentionSegmentData,
    OutgoingFaceSegment,
    OutgoingForwardedMessage,
    OutgoingForwardSegment,
    OutgoingForwardSegmentData,
    OutgoingImageSegment,
    OutgoingImageSegmentData,
    OutgoingMentionAllSegment,
    OutgoingMentionSegment,
    OutgoingRecordSegment,
    OutgoingRecordSegmentData,
    OutgoingReplySegment,
    OutgoingSegment,
    OutgoingTextSegment,
    OutgoingVideoSegment,
    OutgoingVideoSegmentData,
    ReplySegmentData,
    TempMessage,
    TextSegmentData,
    parse_incoming_message,
)

from .config import MilkyNoticeSection

PLATFORM = "qq"
MAX_MEDIA_DOWNLOAD_BYTES = 30 * 1024 * 1024

_KNOWN_INCOMING_SEGMENT_TYPES = frozenset({
    "text",
    "mention",
    "mention_all",
    "face",
    "reply",
    "image",
    "record",
    "video",
    "file",
    "forward",
    "market_face",
    "light_app",
    "xml",
})


def build_message_id(scene: str, peer_id: Any, message_seq: Any) -> str:
    """构造可逆的 MaiBot 消息 ID：``milky-{scene}-{peer_id}-{message_seq}``。"""
    if message_seq is not None and str(peer_id or "").strip():
        return f"milky-{scene}-{peer_id}-{message_seq}"
    return f"milky-{uuid4().hex}"


def parse_milky_message_id(message_id: str) -> Tuple[Optional[str], Optional[int], Optional[int]]:
    """从 ``milky-{scene}-{peer_id}-{message_seq}`` 或纯数字序号解析 ``(scene, peer_id, message_seq)``。"""
    normalized = str(message_id or "").strip()
    if not normalized:
        return None, None, None
    if normalized.startswith("milky-"):
        parts = normalized.split("-")
        if len(parts) == 4:
            scene = parts[1]
            try:
                return scene, int(parts[2]), int(parts[3])
            except ValueError:
                return None, None, None
    try:
        return None, None, int(normalized)
    except ValueError:
        return None, None, None


def sanitize_raw_incoming_message(raw_data: Mapping[str, Any]) -> Dict[str, Any]:
    """根据 Milky 1.2/1.3 兼容性规范，将 markdown 或未知消息段子类型安全降级为 text 段后再交由 SDK 解析。"""
    payload = dict(raw_data)
    raw_segments = payload.get("segments")
    if isinstance(raw_segments, list):
        sanitized: List[Dict[str, Any]] = []
        for seg in raw_segments:
            if not isinstance(seg, Mapping):
                continue
            seg_type = str(seg.get("type") or "").strip()
            seg_data = seg.get("data") if isinstance(seg.get("data"), Mapping) else {}
            if seg_type == "markdown":
                sanitized.append({
                    "type": "text",
                    "data": {"text": str(seg_data.get("content") or "")},
                })
            elif seg_type not in _KNOWN_INCOMING_SEGMENT_TYPES:
                sanitized.append({
                    "type": "text",
                    "data": {"text": f"[不支持的消息段:{seg_type or 'unknown'}]"},
                })
            else:
                sanitized.append(dict(seg))
        payload["segments"] = sanitized
    return payload


class MilkyCodec:
    """基于 ``milky.models`` 的入站/出站/通知消息编解码器。"""

    def __init__(self, logger: Any) -> None:
        self._logger = logger

    async def decode_inbound_message(
        self,
        raw_event_data: Mapping[str, Any],
        client: AsyncMilkyClient,
        http_client: httpx.AsyncClient,
        account_id: str,
        scope: str = "",
    ) -> Optional[Dict[str, Any]]:
        """将 Milky ``message_receive`` 事件的 ``data`` 解码为 MaiBot ``MessageDict``。"""
        sanitized_data = sanitize_raw_incoming_message(raw_event_data)
        incoming: IncomingMessage = parse_incoming_message(sanitized_data)

        scene = incoming.message_scene
        peer_id = str(incoming.peer_id)
        sender_id = str(incoming.sender_id)
        message_seq = incoming.message_seq

        group_id = ""
        group_name = ""
        user_nickname = sender_id
        user_cardname: Optional[str] = None

        if isinstance(incoming, GroupMessage):
            group_id = str(incoming.group.group_id)
            group_name = incoming.group.group_name or f"group_{group_id}"
            user_nickname = incoming.group_member.nickname or incoming.group_member.card or sender_id
            user_cardname = incoming.group_member.card.strip() or None
        elif isinstance(incoming, FriendMessage):
            user_nickname = incoming.friend.nickname or incoming.friend.remark or sender_id
        elif isinstance(incoming, TempMessage):
            if incoming.group is not None:
                group_id = str(incoming.group.group_id)
                group_name = incoming.group.group_name or f"group_{group_id}"

        raw_segments_dict = raw_event_data.get("segments", [])
        raw_message, is_at, is_picture, is_emoji = await self._decode_segments(
            incoming.segments,
            raw_segments_dict if isinstance(raw_segments_dict, list) else [],
            client=client,
            http_client=http_client,
            account_id=account_id,
            scene=scene,
            peer_id=peer_id,
        )
        if not raw_message:
            return None

        plain_text = self.build_plain_text(raw_message)
        additional_config: Dict[str, Any] = {
            "milky_message_scene": scene,
            "milky_message_seq": message_seq,
            "milky_peer_id": peer_id,
        }
        if group_id:
            additional_config["platform_io_target_group_id"] = group_id
        else:
            additional_config["platform_io_target_user_id"] = sender_id

        message_info: Dict[str, Any] = {
            "user_info": {
                "user_id": sender_id,
                "user_nickname": user_nickname,
                "user_cardname": user_cardname,
            },
            "additional_config": additional_config,
        }
        if group_id:
            message_info["group_info"] = {
                "group_id": group_id,
                "group_name": group_name,
            }

        message_id = build_message_id(scene, peer_id, message_seq)
        return {
            "message_id": message_id,
            "timestamp": str(float(incoming.time or time.time())),
            "platform": PLATFORM,
            "account_id": account_id,
            "scope": scope or None,
            "message_info": message_info,
            "raw_message": raw_message,
            "processed_plain_text": plain_text,
            "display_message": plain_text,
            "is_mentioned": is_at,
            "is_at": is_at,
            "is_emoji": is_emoji,
            "is_picture": is_picture,
            "is_command": plain_text.startswith("/"),
            "is_notify": False,
            "session_id": "",
        }

    async def _decode_segments(
        self,
        segments: List[Any],
        raw_segments: List[Any],
        *,
        client: AsyncMilkyClient,
        http_client: httpx.AsyncClient,
        account_id: str,
        scene: str,
        peer_id: str,
    ) -> Tuple[List[Dict[str, Any]], bool, bool, bool]:
        converted: List[Dict[str, Any]] = []
        is_at = False
        is_picture = False
        is_emoji = False

        for idx, segment in enumerate(segments):
            raw_seg = raw_segments[idx] if idx < len(raw_segments) and isinstance(raw_segments[idx], Mapping) else {}
            raw_data = raw_seg.get("data") if isinstance(raw_seg.get("data"), Mapping) else {}
            seg_type = segment.type

            if seg_type == "text":
                text_val = segment.data.text
                if text_val:
                    converted.append({"type": "text", "data": text_val})
                continue

            if seg_type == "mention":
                target_uid = str(segment.data.user_id)
                target_name = str(raw_data.get("name") or "").strip() or None
                converted.append({
                    "type": "at",
                    "data": {
                        "target_user_id": target_uid,
                        "target_user_nickname": target_name,
                        "target_user_cardname": None,
                    },
                })
                if account_id and target_uid == account_id:
                    is_at = True
                continue

            if seg_type == "mention_all":
                converted.append({
                    "type": "at",
                    "data": {
                        "target_user_id": "all",
                        "target_user_nickname": "全体成员",
                        "target_user_cardname": None,
                    },
                })
                continue

            if seg_type == "face":
                is_large = bool(raw_data.get("is_large", False))
                label = "[超级表情]" if is_large else f"[表情:{segment.data.face_id}]"
                converted.append({"type": "text", "data": label})
                continue

            if seg_type == "reply":
                reply_seq = segment.data.message_seq
                target_msg_id = build_message_id(scene, peer_id, reply_seq)
                reply_payload: Dict[str, Any] = {"target_message_id": target_msg_id}
                sender_id = str(raw_data.get("sender_id") or "").strip()
                sender_name = str(raw_data.get("sender_name") or "").strip()
                if sender_id:
                    reply_payload["target_message_sender_id"] = sender_id
                if sender_name:
                    reply_payload["target_message_sender_nickname"] = sender_name
                quoted_segments = raw_data.get("segments")
                if isinstance(quoted_segments, list) and quoted_segments:
                    sanitized_quote = sanitize_raw_incoming_message({"segments": quoted_segments})["segments"]
                    quote_texts: List[str] = []
                    for qseg in sanitized_quote:
                        qtype = qseg.get("type")
                        qdata = qseg.get("data") if isinstance(qseg.get("data"), Mapping) else {}
                        if qtype == "text":
                            quote_texts.append(str(qdata.get("text") or ""))
                        elif qtype == "mention":
                            quote_texts.append(f"@{qdata.get('name') or qdata.get('user_id') or ''} ")
                    if quote_texts:
                        reply_payload["target_message_content"] = "".join(quote_texts).strip()
                converted.append({"type": "reply", "data": reply_payload})
                continue

            if seg_type == "image":
                target_type = "emoji" if segment.data.sub_type == ImageSubType.STICKER else "image"
                media_seg = await self._download_media_segment(
                    target_type,
                    resource_id=segment.data.resource_id,
                    temp_url=segment.data.temp_url,
                    client=client,
                    http_client=http_client,
                )
                converted.append(media_seg)
                if media_seg["type"] == "emoji":
                    is_emoji = True
                elif media_seg["type"] == "image":
                    is_picture = True
                continue

            if seg_type == "record":
                media_seg = await self._download_media_segment(
                    "voice",
                    resource_id=segment.data.resource_id,
                    temp_url=segment.data.temp_url,
                    client=client,
                    http_client=http_client,
                )
                converted.append(media_seg)
                continue

            if seg_type == "video":
                media_seg = await self._download_media_segment(
                    "video",
                    resource_id=segment.data.resource_id,
                    temp_url=segment.data.temp_url,
                    client=client,
                    http_client=http_client,
                )
                converted.append(media_seg)
                continue

            if seg_type == "market_face":
                media_seg = await self._download_media_segment(
                    "emoji",
                    resource_id="",
                    temp_url=segment.data.url,
                    client=client,
                    http_client=http_client,
                )
                converted.append(media_seg)
                if media_seg["type"] == "emoji":
                    is_emoji = True
                continue

            if seg_type == "file":
                converted.append({
                    "type": "file",
                    "data": {
                        "name": segment.data.file_name,
                        "size": str(segment.data.file_size),
                        "file_id": segment.data.file_id,
                        "file_hash": segment.data.file_hash or "",
                    },
                })
                continue

            if seg_type == "forward":
                converted.append({
                    "type": "forward",
                    "data": {"forward_id": segment.data.forward_id},
                })
                continue

            if seg_type == "light_app":
                converted.append({"type": "text", "data": f"[小程序:{segment.data.app_name}]"})
                continue

            if seg_type == "xml":
                converted.append({"type": "text", "data": "[XML消息]"})
                continue

        return converted, is_at, is_picture, is_emoji

    async def _download_media_segment(
        self,
        seg_type: str,
        *,
        resource_id: str,
        temp_url: str,
        client: AsyncMilkyClient,
        http_client: httpx.AsyncClient,
    ) -> Dict[str, Any]:
        url = temp_url.strip()
        if not url and resource_id.strip():
            try:
                res = await client.get_resource_temp_url(resource_id.strip())
                url = res.url.strip()
            except Exception as exc:
                self._logger.debug("获取 Milky 资源临时链接失败: resource_id=%s error=%s", resource_id, exc)

        if url:
            try:
                response = await http_client.get(url, follow_redirects=True)
                response.raise_for_status()
                binary = response.content
                if binary and len(binary) <= MAX_MEDIA_DOWNLOAD_BYTES:
                    return {
                        "type": seg_type,
                        "data": "",
                        "hash": hashlib.sha256(binary).hexdigest(),
                        "binary_data_base64": base64.b64encode(binary).decode("ascii"),
                    }
            except Exception as exc:
                self._logger.warning("下载 Milky 媒体资源失败: type=%s error=%s", seg_type, exc)

        return {"type": "text", "data": f"[{seg_type}]"}

    def extract_outbound_files(self, raw_message: Any) -> Tuple[List[Dict[str, Any]], List[Tuple[str, str]]]:
        """从出站 ``raw_message`` 中分离普通消息段与需通过文件上传接口发送的 ``(file_uri, file_name)``。"""
        if not isinstance(raw_message, list):
            return [{"type": "text", "data": str(raw_message or "")}], []
        normal_items: List[Dict[str, Any]] = []
        file_uploads: List[Tuple[str, str]] = []
        for item in raw_message:
            if not isinstance(item, Mapping):
                continue
            if str(item.get("type") or "").strip() == "file":
                uri = self._extract_media_uri(item)
                data = item.get("data")
                file_name = str(data.get("name") or "file").strip() if isinstance(data, Mapping) else "file"
                if uri:
                    file_uploads.append((uri, file_name or "file"))
            else:
                normal_items.append(dict(item))
        return normal_items, file_uploads

    def encode_outbound_segments(self, raw_message: Any) -> List[OutgoingSegment]:
        """将 MaiBot 出站 ``raw_message`` 转换为 ``milky.models.OutgoingSegment`` 列表。"""
        if not isinstance(raw_message, list):
            return [OutgoingTextSegment(data=TextSegmentData(text=str(raw_message or "")))]

        segments: List[OutgoingSegment] = []
        for item in raw_message:
            if not isinstance(item, Mapping):
                continue
            seg_type = str(item.get("type") or "").strip()
            if seg_type == "file":
                continue
            built = self._encode_single_segment(seg_type, item)
            if built is not None:
                segments.append(built)

        if not segments:
            segments.append(OutgoingTextSegment(data=TextSegmentData(text="")))
        return segments
    def _encode_single_segment(self, seg_type: str, item: Mapping[str, Any]) -> Optional[OutgoingSegment]:
        data = item.get("data")

        if seg_type == "text":
            text_val = str(data if data is not None else "")
            return OutgoingTextSegment(data=TextSegmentData(text=text_val)) if text_val else None

        if seg_type == "at":
            target_uid = ""
            if isinstance(data, Mapping):
                target_uid = str(data.get("target_user_id") or "").strip()
            else:
                target_uid = str(data or "").strip()
            if not target_uid:
                return None
            if target_uid.lower() == "all":
                return OutgoingMentionAllSegment(data=MentionAllSegmentData())
            if target_uid.isdigit():
                return OutgoingMentionSegment(data=MentionSegmentData(user_id=int(target_uid)))
            return None

        if seg_type == "reply":
            target_mid = ""
            if isinstance(data, Mapping):
                target_mid = str(data.get("target_message_id") or data.get("message_id") or "").strip()
            else:
                target_mid = str(data or "").strip()
            _, _, seq = parse_milky_message_id(target_mid)
            if seq is not None:
                return OutgoingReplySegment(data=ReplySegmentData(message_seq=seq))
            return None

        if seg_type == "face":
            face_id = str(data.get("id") if isinstance(data, Mapping) else data or "").strip()
            return OutgoingFaceSegment(data=FaceSegmentData(face_id=face_id)) if face_id else None

        if seg_type in {"image", "emoji"}:
            uri = self._extract_media_uri(item)
            if not uri:
                return None
            sub_type = ImageSubType.STICKER if seg_type == "emoji" else ImageSubType.NORMAL
            return OutgoingImageSegment(data=OutgoingImageSegmentData(uri=uri, sub_type=sub_type))

        if seg_type == "voice":
            uri = self._extract_media_uri(item)
            return OutgoingRecordSegment(data=OutgoingRecordSegmentData(uri=uri)) if uri else None

        if seg_type == "video":
            uri = self._extract_media_uri(item)
            return OutgoingVideoSegment(data=OutgoingVideoSegmentData(uri=uri)) if uri else None

        if seg_type == "forward" and isinstance(data, list):
            nodes: List[OutgoingForwardedMessage] = []
            for node in data:
                if not isinstance(node, Mapping):
                    continue
                uid_raw = str(node.get("user_id") or "10000").strip()
                uid = int(uid_raw) if uid_raw.isdigit() else 10000
                sender_name = str(node.get("user_nickname") or node.get("user_cardname") or "QQ用户")
                child_segments = self.encode_outbound_segments(node.get("content", []))
                nodes.append(OutgoingForwardedMessage(user_id=uid, sender_name=sender_name, segments=child_segments))
            if nodes:
                return OutgoingForwardSegment(data=OutgoingForwardSegmentData(messages=nodes))
            return None

        return OutgoingTextSegment(data=TextSegmentData(text=f"[{seg_type or 'unknown'}]"))

    @staticmethod
    def _extract_media_uri(item: Mapping[str, Any]) -> str:
        b64 = str(item.get("binary_data_base64") or "").strip()
        if b64:
            return f"base64://{b64}"
        data = item.get("data")
        if isinstance(data, Mapping):
            b64_inner = str(data.get("base64") or data.get("content") or "").strip()
            if b64_inner and not b64_inner.startswith(("http://", "https://", "file://", "base64://")):
                return f"base64://{b64_inner}"
            raw_uri = str(data.get("uri") or data.get("url") or data.get("file") or data.get("path") or "").strip()
        else:
            raw_uri = str(data or "").strip()
        if not raw_uri:
            return ""
        if raw_uri.startswith(("http://", "https://", "file://", "base64://")):
            return raw_uri
        return f"file://{raw_uri}"

    def decode_notice_event(
        self,
        event_type: str,
        event_data: Mapping[str, Any],
        notice_cfg: MilkyNoticeSection,
        account_id: str,
        scope: str = "",
    ) -> Tuple[Optional[Dict[str, Any]], str]:
        """将 Milky 通知事件转换为 MaiBot 通知 ``MessageDict`` 与去重键。"""
        if not notice_cfg.enabled:
            return None, ""

        group_id = str(event_data.get("group_id") or "").strip()
        scene = str(event_data.get("message_scene") or "").strip()
        peer_id = str(event_data.get("peer_id") or "").strip()
        if not group_id and scene == "group":
            group_id = peer_id

        sender_id = str(
            event_data.get("sender_id")
            or event_data.get("user_id")
            or event_data.get("operator_id")
            or event_data.get("initiator_id")
            or account_id
        ).strip()

        text = ""
        dedupe = f"{event_type}:{group_id or peer_id}:{sender_id}:{int(time.time())}"

        if event_type in {EventType.FRIEND_NUDGE.value, EventType.GROUP_NUDGE.value}:
            if not notice_cfg.enable_nudge:
                return None, ""
            target_id = str(event_data.get("receiver_id") or event_data.get("target_user_id") or account_id).strip()
            action_str = str(event_data.get("display_action") or "戳了戳").strip()
            suffix_str = str(event_data.get("display_suffix") or "").strip()
            text = f"[戳一戳] {sender_id} {action_str} {target_id} {suffix_str}".strip()
        elif event_type == EventType.MESSAGE_RECALL.value:
            if not notice_cfg.enable_recall:
                return None, ""
            seq = event_data.get("message_seq")
            text = f"[消息撤回] 用户 {sender_id} 撤回了消息 (seq={seq})"
            dedupe = f"recall:{scene}:{peer_id}:{seq}"
        elif event_type in {EventType.GROUP_MUTE.value, EventType.GROUP_WHOLE_MUTE.value}:
            if not notice_cfg.enable_group_mute:
                return None, ""
            duration = event_data.get("duration")
            text = f"[群禁言通知] 群 {group_id} 成员 {sender_id} 禁言变更 (duration={duration})"
        elif event_type == EventType.GROUP_MESSAGE_REACTION.value:
            if not notice_cfg.enable_group_reaction:
                return None, ""
            face_id = event_data.get("face_id")
            seq = event_data.get("message_seq")
            text = f"[表情回应] 用户 {sender_id} 对消息 {seq} 回应了表情 {face_id}"
            dedupe = f"reaction:{group_id}:{seq}:{sender_id}:{face_id}"
        elif event_type in {EventType.GROUP_FILE_UPLOAD.value, EventType.FRIEND_FILE_UPLOAD.value}:
            if not notice_cfg.enable_group_file_upload:
                return None, ""
            file_name = event_data.get("file_name") or "文件"
            text = f"[文件上传] 用户 {sender_id} 上传了文件 {file_name}"
        elif event_type in {EventType.GROUP_MEMBER_INCREASE.value, EventType.GROUP_MEMBER_DECREASE.value}:
            if not notice_cfg.enable_group_member_change:
                return None, ""
            label = "加入" if event_type == EventType.GROUP_MEMBER_INCREASE.value else "离开"
            text = f"[群成员变动] 用户 {sender_id} {label}了群 {group_id}"
        elif event_type == EventType.GROUP_ADMIN_CHANGE.value:
            if not notice_cfg.enable_group_admin_change:
                return None, ""
            is_set = bool(event_data.get("is_set", True))
            text = f"[管理员变动] 用户 {sender_id} 被{'设为' if is_set else '取消'}管理员"
        elif event_type == EventType.GROUP_ESSENCE_MESSAGE_CHANGE.value:
            if not notice_cfg.enable_group_essence:
                return None, ""
            seq = event_data.get("message_seq")
            text = f"[群精华变动] 群 {group_id} 消息 {seq} 精华状态变更"
        elif event_type == EventType.GROUP_NAME_CHANGE.value:
            if not notice_cfg.enable_group_name_change:
                return None, ""
            new_name = event_data.get("new_group_name") or event_data.get("group_name") or ""
            text = f"[群名变更] 群 {group_id} 名称变更为 {new_name}"
        elif event_type == "group_disband":
            if not notice_cfg.enable_group_member_change:
                return None, ""
            text = f"[群解散] 群 {group_id} 已被 {sender_id} 解散"
        else:
            return None, ""
        additional_config: Dict[str, Any] = {"milky_notice_event": event_type}
        if group_id:
            additional_config["platform_io_target_group_id"] = group_id
        else:
            additional_config["platform_io_target_user_id"] = sender_id

        message_info: Dict[str, Any] = {
            "user_info": {
                "user_id": sender_id,
                "user_nickname": sender_id,
                "user_cardname": None,
            },
            "additional_config": additional_config,
        }
        if group_id:
            message_info["group_info"] = {"group_id": group_id, "group_name": f"group_{group_id}"}

        msg_id = f"milky-notice-{uuid4().hex[:12]}"
        return {
            "message_id": msg_id,
            "timestamp": str(time.time()),
            "platform": PLATFORM,
            "account_id": account_id,
            "scope": scope or None,
            "message_info": message_info,
            "raw_message": [{"type": "text", "data": text}],
            "processed_plain_text": text,
            "display_message": text,
            "is_mentioned": False,
            "is_at": False,
            "is_emoji": False,
            "is_picture": False,
            "is_command": False,
            "is_notify": True,
            "session_id": "",
        }, dedupe

    @staticmethod
    def build_plain_text(raw_message: List[Dict[str, Any]]) -> str:
        parts: List[str] = []
        for seg in raw_message:
            seg_type = seg.get("type")
            if seg_type == "text":
                parts.append(str(seg.get("data") or ""))
            elif seg_type == "at":
                at_data = seg.get("data")
                if isinstance(at_data, Mapping):
                    name = str(at_data.get("target_user_nickname") or at_data.get("target_user_id") or "")
                    if name:
                        parts.append(f"@{name} ")
        return "".join(parts).strip()
