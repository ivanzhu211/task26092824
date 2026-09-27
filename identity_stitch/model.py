"""标识符与事件的数据模型：解析、全序、校验、内容哈希。"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from .errors import ValidationError

CHANNELS = ("web", "mobile", "kiosk", "inbox")
KINDS = ("view", "purchase")
IDENT_TYPES = ("cust", "email", "inbox", "device")
_TYPE_RANK = {t: i for i, t in enumerate(IDENT_TYPES)}
MAX_IDENTIFIERS = 8


@dataclass(frozen=True)
class Event:
    event_id: str
    channel: str
    ts: int
    kind: str
    amount_cents: int
    identifiers: tuple[str, ...]


@dataclass(frozen=True)
class Profile:
    key: str
    members: tuple[str, ...]
    channel_counts: tuple[tuple[str, int], ...]
    purchase_count: int
    amount_cents: int
    last_ts: int


@dataclass(frozen=True)
class SharedDevice:
    identifier: str


@dataclass(frozen=True)
class Stats:
    profiles: int
    unattributed_events: int


@lru_cache(maxsize=131072)
def parse_identifier(ident: str) -> tuple[int, str]:
    """解析「类型:值」，返回 (类型序号, 值)。不合法则抛 ValidationError。"""
    if not isinstance(ident, str):
        raise ValidationError(f"标识符必须是字符串: {ident!r}")
    type_part, sep, value = ident.partition(":")
    if not sep or type_part not in _TYPE_RANK or not value:
        raise ValidationError(f"非法标识符: {ident!r}")
    return _TYPE_RANK[type_part], value


@lru_cache(maxsize=131072)
def sort_key(ident: str) -> tuple[int, str]:
    """标识符全序的比较键：先类型序号，再值（str 序与 UTF-8 字节序一致）。"""
    return parse_identifier(ident)


def normalize_event(obj: Any) -> Event:
    """校验并规范化输入（dict 或 Event），返回 Event。不合法则抛 ValidationError。"""
    if isinstance(obj, Event):
        fields = {
            "event_id": obj.event_id,
            "channel": obj.channel,
            "ts": obj.ts,
            "kind": obj.kind,
            "amount_cents": obj.amount_cents,
            "identifiers": list(obj.identifiers),
        }
    elif isinstance(obj, dict):
        fields = {k: obj.get(k) for k in (
            "event_id", "channel", "ts", "kind", "amount_cents", "identifiers")}
    else:
        raise ValidationError(f"不支持的事件类型: {type(obj)!r}")

    event_id = fields["event_id"]
    if not isinstance(event_id, str) or not event_id:
        raise ValidationError(f"非法 event_id: {event_id!r}")

    channel = fields["channel"]
    if channel not in CHANNELS:
        raise ValidationError(f"非法 channel: {channel!r}")

    ts = fields["ts"]
    if not isinstance(ts, int) or isinstance(ts, bool):
        raise ValidationError(f"非法 ts: {ts!r}")

    kind = fields["kind"]
    if kind not in KINDS:
        raise ValidationError(f"非法 kind: {kind!r}")

    amount = fields["amount_cents"]
    if not isinstance(amount, int) or isinstance(amount, bool):
        raise ValidationError(f"非法 amount_cents: {amount!r}")
    if kind == "purchase" and amount <= 0:
        raise ValidationError("purchase 事件的 amount_cents 必须为正整数")
    if kind == "view" and amount != 0:
        raise ValidationError("view 事件的 amount_cents 必须为 0")

    raw_idents = fields["identifiers"]
    if not isinstance(raw_idents, (list, tuple)):
        raise ValidationError("identifiers 必须是列表")
    unique = sorted({str(i) for i in raw_idents}, key=sort_key)
    if not 1 <= len(unique) <= MAX_IDENTIFIERS:
        raise ValidationError(f"标识符数量必须在 1..{MAX_IDENTIFIERS} 之间")
    for ident in unique:
        parse_identifier(ident)

    return Event(
        event_id=event_id,
        channel=channel,
        ts=ts,
        kind=kind,
        amount_cents=amount,
        identifiers=tuple(unique),
    )


def content_hash(event: Event) -> str:
    """事件内容的规范哈希，用于重复写入检测。"""
    canonical = "|".join([
        event.channel,
        str(event.ts),
        event.kind,
        str(event.amount_cents),
        *event.identifiers,
    ])
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
