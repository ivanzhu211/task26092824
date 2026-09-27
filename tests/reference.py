"""参照实现：不做任何增量维护，每次查询都从当前存活事件集从零计算。"""
from __future__ import annotations

from typing import Any, Iterator

from identity_stitch.errors import (
    DuplicateEventError,
    RetractedEventError,
    UnknownEventError,
)
from identity_stitch.model import (
    CHANNELS,
    Event,
    Profile,
    SharedDevice,
    Stats,
    content_hash,
    normalize_event,
    sort_key,
)


class Reference:
    def __init__(self) -> None:
        self._events: dict[str, tuple[Event, bool]] = {}  # event_id -> (事件, 已撤回)
        self._hashes: dict[str, str] = {}

    def ingest(self, event: Any) -> None:
        ev = normalize_event(event)
        digest = content_hash(ev)
        if ev.event_id in self._events:
            _, retracted = self._events[ev.event_id]
            if retracted:
                raise RetractedEventError(ev.event_id)
            if self._hashes[ev.event_id] == digest:
                return
            raise DuplicateEventError(ev.event_id)
        self._events[ev.event_id] = (ev, False)
        self._hashes[ev.event_id] = digest

    def retract(self, event_id: str) -> None:
        if event_id not in self._events:
            raise UnknownEventError(event_id)
        ev, retracted = self._events[event_id]
        if retracted:
            return
        self._events[event_id] = (ev, True)

    def _live(self) -> list[Event]:
        return [ev for ev, r in self._events.values() if not r]

    def _compute(self) -> tuple[dict[str, list[str]], dict[str, list[Event]], set[str], int]:
        """返回 (分量成员, 分量key->事件, 共享设备, 未归属事件数)。"""
        live = self._live()
        peers: dict[str, set[str]] = {}
        for ev in live:
            devices = [i for i in ev.identifiers if i.startswith("device:")]
            non_dev = [i for i in ev.identifiers if not i.startswith("device:")]
            for d in devices:
                peers.setdefault(d, set()).update(non_dev)
        shared = {d for d, p in peers.items() if len(p) > 3}

        parent: dict[str, str] = {}

        def find(x: str) -> str:
            parent.setdefault(x, x)
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(x: str, y: str) -> None:
            rx, ry = find(x), find(y)
            if rx != ry:
                parent[rx] = ry

        for ev in live:
            rest = [i for i in ev.identifiers if i not in shared]
            if rest:
                find(rest[0])  # 注册单标识符分量
            for other in rest[1:]:
                union(rest[0], other)

        comps: dict[str, list[str]] = {}
        for ident in parent:
            comps.setdefault(find(ident), []).append(ident)

        comp_of: dict[str, str] = {}
        for root, idents in comps.items():
            for ident in idents:
                comp_of[ident] = root

        comp_events: dict[str, list[Event]] = {root: [] for root in comps}
        unattributed = 0
        for ev in live:
            rest = [i for i in ev.identifiers if i not in shared]
            if not rest:
                unattributed += 1
            else:
                comp_events[comp_of[rest[0]]].append(ev)

        members_by_key: dict[str, list[str]] = {}
        events_by_key: dict[str, list[Event]] = {}
        for root, idents in comps.items():
            key = min(idents, key=sort_key)
            members_by_key[key] = sorted(idents, key=sort_key)
            events_by_key[key] = comp_events[root]
        return members_by_key, events_by_key, shared, unattributed

    def export_profiles(self) -> list[Profile]:
        members_by_key, events_by_key, _, _ = self._compute()
        profiles = []
        for key in sorted(members_by_key, key=sort_key):
            counts = {c: 0 for c in CHANNELS}
            purchase_cnt = 0
            amount_sum = 0
            max_ts = 0
            for ev in events_by_key[key]:
                counts[ev.channel] += 1
                if ev.kind == "purchase":
                    purchase_cnt += 1
                    amount_sum += ev.amount_cents
                max_ts = max(max_ts, ev.ts)
            profiles.append(Profile(
                key=key,
                members=tuple(members_by_key[key]),
                channel_counts=tuple((c, counts[c]) for c in CHANNELS),
                purchase_count=purchase_cnt,
                amount_cents=amount_sum,
                last_ts=max_ts,
            ))
        return profiles

    def stats(self) -> Stats:
        members_by_key, _, _, unattributed = self._compute()
        return Stats(profiles=len(members_by_key), unattributed_events=unattributed)

    def resolve(self, identifier: str) -> Profile | SharedDevice | None:
        members_by_key, _, shared, _ = self._compute()
        if identifier in shared:
            return SharedDevice(identifier)
        for profile in self.export_profiles():
            if identifier in profile.members:
                return profile
        return None
