"""参照实现：从当前存活事件集合从零计算全部画像（纯内存、不考虑性能）。

语义与 stitch.IdentityStitcher 完全一致，用于随机测试逐步比对。
"""

from typing import Any, Dict, List, Optional

TYPE_ORDER = {"cust": 0, "email": 1, "inbox": 2, "device": 3}
CHANNELS = ("web", "mobile", "kiosk", "inbox")
SHARED_THRESHOLD = 3


def sort_key_of(identifier: str) -> tuple:
    prefix = identifier.split(":", 1)[0]
    return (TYPE_ORDER[prefix], identifier)


def is_device(identifier: str) -> bool:
    return identifier.startswith("device:")


def _shared_devices(events: List[Dict[str, Any]]) -> set:
    peers: Dict[str, set] = {}
    for ev in events:
        idents = set(ev["identifiers"])
        nondev = {i for i in idents if not is_device(i)}
        for dev in (i for i in idents if is_device(i)):
            peers.setdefault(dev, set()).update(nondev)
    return {d for d, p in peers.items() if len(p) > SHARED_THRESHOLD}


def compute(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    """返回 {'profiles': [...], 'unassigned_events': n}，profiles 按 key 全序。"""
    shared = _shared_devices(events)
    adj: Dict[str, set] = {}
    all_idents = set()
    remainders: List[tuple] = []  # (event, remaining idents)
    unassigned = 0
    for ev in events:
        remaining = set(ev["identifiers"]) - shared
        remainders.append((ev, remaining))
        if not remaining:
            unassigned += 1
            continue
        all_idents.update(remaining)
        for a in remaining:
            adj.setdefault(a, set())
            for b in remaining:
                if a != b:
                    adj[a].add(b)
    # 连通分量
    seen = set()
    comps: List[set] = []
    for start in sorted(all_idents):
        if start in seen:
            continue
        stack = [start]
        seen.add(start)
        part = set()
        while stack:
            node = stack.pop()
            part.add(node)
            for nxt in adj.get(node, ()):
                if nxt not in seen:
                    seen.add(nxt)
                    stack.append(nxt)
        comps.append(part)
    # 事件归属 + 聚合
    ident_comp: Dict[str, int] = {}
    for idx, part in enumerate(comps):
        for ident in part:
            ident_comp[ident] = idx
    buckets: List[list] = [[] for _ in comps]
    for ev, remaining in remainders:
        if remaining:
            buckets[ident_comp[next(iter(remaining))]].append(ev)
    profiles = []
    for part, evs in zip(comps, buckets):
        channels = {c: 0 for c in CHANNELS}
        purchases = 0
        amount = 0
        last_ts: Optional[int] = None
        for ev in evs:
            channels[ev["channel"]] += 1
            if ev["kind"] == "purchase":
                purchases += 1
            amount += ev["amount_cents"]
            last_ts = ev["ts"] if last_ts is None else max(last_ts, ev["ts"])
        profiles.append(
            {
                "key": min(part, key=sort_key_of),
                "identifiers": sorted(part, key=sort_key_of),
                "channels": channels,
                "purchases": purchases,
                "amount_cents": amount,
                "last_ts": last_ts,
            }
        )
    profiles.sort(key=lambda p: sort_key_of(p["key"]))
    return {"profiles": profiles, "unassigned_events": unassigned}


def resolve(events: List[Dict[str, Any]], identifier: str) -> Optional[Dict[str, Any]]:
    """参照 resolve：画像 dict / {'shared': True} / None。"""
    if not any(identifier in ev["identifiers"] for ev in events):
        return None
    if identifier in _shared_devices(events):
        return {"shared": True, "identifier": identifier}
    result = compute(events)
    for profile in result["profiles"]:
        if identifier in profile["identifiers"]:
            return profile
    raise AssertionError("有存活事件的非共享标识符必须属于某个画像")
