"""身份拼接主类：增量维护连通分量与画像聚合。

设计要点：
- 全部状态存 sqlite，任何操作只触碰受影响画像的行。
- pairs 表记录存活事件支撑的标识符共现次数（含共享设备的端点）。
- edges 表是 pairs 的子集：两端当前都不是共享设备时才存在，cnt 与 pairs 同步。
- idents 表记录每个有存活事件的标识符所属分量 comp_id；共享设备 comp_id 为 NULL。
- comps 表维护每个连通分量的 key、成员数与聚合值。
- 撤回 / 共享状态翻转只重算受影响分量（BFS 限于该分量成员）。
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterator, List, Optional, Tuple

from .errors import StitchError

# 标识符类型全序：cust < email < inbox < device
TYPE_ORDER = {"cust": 0, "email": 1, "inbox": 2, "device": 3}
CHANNELS = ("web", "mobile", "kiosk", "inbox")
KINDS = ("view", "purchase")
# 设备与超过该数的不同非设备标识符共现即为共享设备
SHARED_THRESHOLD = 3

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events(
    event_id     TEXT PRIMARY KEY,
    channel      TEXT NOT NULL,
    ts           INTEGER NOT NULL,
    kind         TEXT NOT NULL,
    amount_cents INTEGER NOT NULL,
    state        TEXT NOT NULL,           -- 'live' | 'retracted'
    comp_id      INTEGER                  -- 归属分量；未归属/已撤回为 NULL
);
CREATE INDEX IF NOT EXISTS idx_events_comp ON events(comp_id);
CREATE TABLE IF NOT EXISTS event_idents(
    event_id   TEXT NOT NULL,
    identifier TEXT NOT NULL,
    PRIMARY KEY(event_id, identifier)
);
CREATE INDEX IF NOT EXISTS idx_eident_ident ON event_idents(identifier);
CREATE TABLE IF NOT EXISTS pairs(
    a   TEXT NOT NULL,                    -- 全序较小的一端
    b   TEXT NOT NULL,
    cnt INTEGER NOT NULL,                 -- 支撑该共现的存活事件数
    PRIMARY KEY(a, b)
);
CREATE INDEX IF NOT EXISTS idx_pairs_b ON pairs(b);
CREATE TABLE IF NOT EXISTS edges(
    a   TEXT NOT NULL,
    b   TEXT NOT NULL,
    cnt INTEGER NOT NULL,
    PRIMARY KEY(a, b)
);
CREATE INDEX IF NOT EXISTS idx_edges_b ON edges(b);
CREATE TABLE IF NOT EXISTS device_peer(
    dev  TEXT NOT NULL,
    peer TEXT NOT NULL,                   -- 非设备标识符
    cnt  INTEGER NOT NULL,
    PRIMARY KEY(dev, peer)
);
CREATE TABLE IF NOT EXISTS idents(
    identifier TEXT PRIMARY KEY,
    comp_id    INTEGER,                   -- 共享设备为 NULL
    is_shared  INTEGER NOT NULL DEFAULT 0,
    ev_cnt     INTEGER NOT NULL,          -- 含该标识符的存活事件数
    peer_cnt   INTEGER NOT NULL DEFAULT 0 -- 仅设备用：不同非设备共现标识符数
);
CREATE INDEX IF NOT EXISTS idx_idents_comp ON idents(comp_id);
CREATE TABLE IF NOT EXISTS comps(
    comp_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    sort_key  TEXT NOT NULL,              -- 'rank:identifier'，用于按全序流式输出
    key_ident TEXT NOT NULL,
    size      INTEGER NOT NULL,
    web       INTEGER NOT NULL DEFAULT 0,
    mobile    INTEGER NOT NULL DEFAULT 0,
    kiosk     INTEGER NOT NULL DEFAULT 0,
    inbox     INTEGER NOT NULL DEFAULT 0,
    purchases INTEGER NOT NULL DEFAULT 0,
    amount    INTEGER NOT NULL DEFAULT 0,
    last_ts   INTEGER
);
CREATE INDEX IF NOT EXISTS idx_comps_sort ON comps(sort_key);
CREATE TABLE IF NOT EXISTS meta(
    k TEXT PRIMARY KEY,
    v INTEGER NOT NULL
);
"""


def split_ident(identifier: str) -> Tuple[str, str]:
    """把 'type:value' 拆成 (type, value) 并校验类型。"""
    prefix, sep, value = identifier.partition(":")
    if not sep or prefix not in TYPE_ORDER or not value:
        raise StitchError(f"非法标识符: {identifier!r}")
    return prefix, value


def sort_key_of(identifier: str) -> str:
    """全序比较键：先类型序，再值字节序（UTF-8 memcmp 与码点序一致）。"""
    prefix, _ = split_ident(identifier)
    return f"{TYPE_ORDER[prefix]}:{identifier}"


def is_device(identifier: str) -> bool:
    return identifier.startswith("device:")


class IdentityStitcher:
    """基于 sqlite3 的增量身份拼接引擎。"""

    def __init__(self, path: str) -> None:
        self._path = path
        self._conn = sqlite3.connect(path, isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(_SCHEMA)
        self._conn.execute(
            "INSERT OR IGNORE INTO meta(k, v) VALUES('unassigned', 0)"
        )

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "IdentityStitcher":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ---- 事务与校验 ----

    def _begin(self) -> sqlite3.Cursor:
        assert self._conn is not None, "库已关闭"
        cur = self._conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        return cur

    def _commit(self) -> None:
        assert self._conn is not None
        self._conn.execute("COMMIT")

    def _rollback(self) -> None:
        assert self._conn is not None
        self._conn.execute("ROLLBACK")

    @staticmethod
    def _validate_event(event: Dict[str, Any]) -> List[str]:
        """校验事件字段，返回去重并按全序排好的标识符列表。"""
        try:
            event_id = event["event_id"]
            channel = event["channel"]
            ts = event["ts"]
            kind = event["kind"]
            amount = event["amount_cents"]
            identifiers = event["identifiers"]
        except (KeyError, TypeError) as exc:
            raise StitchError(f"事件缺少字段: {exc}") from exc
        if not isinstance(event_id, str) or not event_id:
            raise StitchError("event_id 必须是非空字符串")
        if channel not in CHANNELS:
            raise StitchError(f"非法渠道: {channel!r}")
        if not isinstance(ts, int) or isinstance(ts, bool):
            raise StitchError("ts 必须是整数毫秒")
        if kind not in KINDS:
            raise StitchError(f"非法 kind: {kind!r}")
        if not isinstance(amount, int) or isinstance(amount, bool):
            raise StitchError("amount_cents 必须是整数")
        if kind == "purchase" and amount <= 0:
            raise StitchError("purchase 的 amount_cents 必须为正整数")
        if kind == "view" and amount != 0:
            raise StitchError("view 的 amount_cents 必须为 0")
        if not isinstance(identifiers, (list, tuple, set, frozenset)):
            raise StitchError("identifiers 必须是标识符集合")
        idents = {str(i) for i in identifiers}
        if not 1 <= len(idents) <= 8:
            raise StitchError("identifiers 去重后必须在 1 到 8 个之间")
        for ident in idents:
            split_ident(ident)
        return sorted(idents, key=sort_key_of)


    # ---- 内部：分量维护 ----

    def _new_comp(self, cur: sqlite3.Cursor, key_ident: str) -> int:
        cur.execute(
            "INSERT INTO comps(sort_key, key_ident, size) VALUES(?,?,1)",
            (sort_key_of(key_ident), key_ident),
        )
        return int(cur.lastrowid)

    def _touch_ident(self, cur: sqlite3.Cursor, ident: str) -> None:
        """确保标识符存在（不存在则建单例分量），并把存活事件计数 +1。"""
        row = cur.execute(
            "SELECT ev_cnt FROM idents WHERE identifier=?", (ident,)
        ).fetchone()
        if row:
            cur.execute(
                "UPDATE idents SET ev_cnt=ev_cnt+1 WHERE identifier=?", (ident,)
            )
        else:
            comp_id = self._new_comp(cur, ident)
            cur.execute(
                "INSERT INTO idents(identifier, comp_id, is_shared, ev_cnt, peer_cnt)"
                " VALUES(?,?,0,1,0)",
                (ident, comp_id),
            )

    def _comp_of(self, cur: sqlite3.Cursor, ident: str) -> Optional[int]:
        row = cur.execute(
            "SELECT comp_id FROM idents WHERE identifier=?", (ident,)
        ).fetchone()
        return row[0] if row else None

    def _is_shared(self, cur: sqlite3.Cursor, ident: str) -> bool:
        row = cur.execute(
            "SELECT is_shared FROM idents WHERE identifier=?", (ident,)
        ).fetchone()
        return bool(row and row[0])

    def _union(self, cur: sqlite3.Cursor, ca: int, cb: int) -> int:
        """合并两个分量，小的并入大的，返回存活 comp_id。"""
        if ca == cb:
            return ca
        ra = cur.execute(
            "SELECT size, sort_key, key_ident, web, mobile, kiosk, inbox,"
            " purchases, amount, last_ts FROM comps WHERE comp_id=?",
            (ca,),
        ).fetchone()
        rb = cur.execute(
            "SELECT size, sort_key, key_ident, web, mobile, kiosk, inbox,"
            " purchases, amount, last_ts FROM comps WHERE comp_id=?",
            (cb,),
        ).fetchone()
        big_id, small_id = (ca, cb) if ra[0] >= rb[0] else (cb, ca)
        big, small = (ra, rb) if ra[0] >= rb[0] else (rb, ra)
        cur.execute("UPDATE idents SET comp_id=? WHERE comp_id=?", (big_id, small_id))
        cur.execute("UPDATE events SET comp_id=? WHERE comp_id=?", (big_id, small_id))
        if small[1] < big[1]:
            sort_key, key_ident = small[1], small[2]
        else:
            sort_key, key_ident = big[1], big[2]
        lasts = [t for t in (big[9], small[9]) if t is not None]
        cur.execute(
            "UPDATE comps SET size=?, sort_key=?, key_ident=?, web=?, mobile=?,"
            " kiosk=?, inbox=?, purchases=?, amount=?, last_ts=? WHERE comp_id=?",
            (
                big[0] + small[0],
                sort_key,
                key_ident,
                big[3] + small[3],
                big[4] + small[4],
                big[5] + small[5],
                big[6] + small[6],
                big[7] + small[7],
                big[8] + small[8],
                max(lasts) if lasts else None,
                big_id,
            ),
        )
        cur.execute("DELETE FROM comps WHERE comp_id=?", (small_id,))
        return big_id

    def _resplit(self, cur: sqlite3.Cursor, comp_id: int) -> None:
        """重新分解一个分量：边删除或设备退出后，可能裂成多个分量。"""
        members = [
            r[0]
            for r in cur.execute(
                "SELECT identifier FROM idents WHERE comp_id=?", (comp_id,)
            )
        ]
        if len(members) <= 1:
            return
        adj: Dict[str, List[str]] = {m: [] for m in members}
        for a, b in cur.execute(
            "SELECT a, b FROM edges WHERE a IN"
            " (SELECT identifier FROM idents WHERE comp_id=?)",
            (comp_id,),
        ):
            adj[a].append(b)
            adj[b].append(a)
        seen = set()
        parts: List[List[str]] = []
        for m in members:
            if m in seen:
                continue
            stack = [m]
            seen.add(m)
            part = []
            while stack:
                node = stack.pop()
                part.append(node)
                for nxt in adj[node]:
                    if nxt not in seen:
                        seen.add(nxt)
                        stack.append(nxt)
            parts.append(part)
        if len(parts) == 1:
            return
        parts.sort(key=len, reverse=True)
        keep = parts[0]
        affected = [comp_id]
        for part in parts[1:]:
            key_ident = min(part, key=sort_key_of)
            nid = self._new_comp(cur, key_ident)
            cur.execute(
                "UPDATE comps SET size=? WHERE comp_id=?", (len(part), nid)
            )
            for chunk_start in range(0, len(part), 500):
                chunk = part[chunk_start : chunk_start + 500]
                cur.execute(
                    f"UPDATE idents SET comp_id=? WHERE identifier IN"
                    f" ({','.join('?' * len(chunk))})",
                    [nid, *chunk],
                )
            affected.append(nid)
        keep_key = min(keep, key=sort_key_of)
        cur.execute(
            "UPDATE comps SET size=?, sort_key=?, key_ident=? WHERE comp_id=?",
            (len(keep), sort_key_of(keep_key), keep_key, comp_id),
        )
        # 重新分配事件并精确重建各分量的聚合
        aggs: Dict[int, List[Any]] = {}
        for eid, channel, ts, kind, amount in cur.execute(
            "SELECT event_id, channel, ts, kind, amount_cents FROM events"
            " WHERE comp_id=?",
            (comp_id,),
        ).fetchall():
            target = cur.execute(
                "SELECT idents.comp_id FROM event_idents"
                " JOIN idents ON idents.identifier = event_idents.identifier"
                " WHERE event_idents.event_id=? AND idents.is_shared=0 LIMIT 1",
                (eid,),
            ).fetchone()[0]
            if target != comp_id:
                cur.execute(
                    "UPDATE events SET comp_id=? WHERE event_id=?", (target, eid)
                )
            slot = aggs.setdefault(
                target, [0, 0, 0, 0, 0, 0, None]
            )
            slot[CHANNELS.index(channel)] += 1
            if kind == "purchase":
                slot[4] += 1
            slot[5] += amount
            slot[6] = ts if slot[6] is None else max(slot[6], ts)
        for cid in affected:
            slot = aggs.get(cid, [0, 0, 0, 0, 0, 0, None])
            cur.execute(
                "UPDATE comps SET web=?, mobile=?, kiosk=?, inbox=?, purchases=?,"
                " amount=?, last_ts=? WHERE comp_id=?",
                (*slot, cid),
            )

    def _agg_apply(
        self,
        cur: sqlite3.Cursor,
        comp_id: Optional[int],
        channel: str,
        kind: str,
        amount: int,
        ts: int,
        sign: int,
    ) -> None:
        """把一条事件的聚合贡献加到/移出分量；comp_id 为 None 表示未归属。"""
        if comp_id is None:
            cur.execute(
                "UPDATE meta SET v=v+? WHERE k='unassigned'", (sign,)
            )
            return
        col = channel  # channel 已经过校验，必是 CHANNELS 之一
        purch = 1 if kind == "purchase" else 0
        if sign > 0:
            cur.execute(
                f"UPDATE comps SET {col}={col}+1, purchases=purchases+?,"
                f" amount=amount+?,"
                f" last_ts=MAX(COALESCE(last_ts, ?), ?) WHERE comp_id=?",
                (purch, amount, ts, ts, comp_id),
            )
        else:
            cur.execute(
                f"UPDATE comps SET {col}={col}-1, purchases=purchases-?,"
                f" amount=amount-? WHERE comp_id=?",
                (purch, amount, comp_id),
            )
            last = cur.execute(
                "SELECT last_ts FROM comps WHERE comp_id=?", (comp_id,)
            ).fetchone()[0]
            if last == ts:
                new_last = cur.execute(
                    "SELECT MAX(ts) FROM events WHERE comp_id=?", (comp_id,)
                ).fetchone()[0]
                cur.execute(
                    "UPDATE comps SET last_ts=? WHERE comp_id=?",
                    (new_last, comp_id),
                )

    def _reassign_event(self, cur: sqlite3.Cursor, event_id: str) -> None:
        """按当前共享状态重算一条存活事件的归属分量并修正聚合。"""
        row = cur.execute(
            "SELECT comp_id, channel, ts, kind, amount_cents FROM events"
            " WHERE event_id=?",
            (event_id,),
        ).fetchone()
        old = row[0]
        new = cur.execute(
            "SELECT idents.comp_id FROM event_idents"
            " JOIN idents ON idents.identifier = event_idents.identifier"
            " WHERE event_idents.event_id=? AND idents.is_shared=0 LIMIT 1",
            (event_id,),
        ).fetchone()
        new = new[0] if new else None
        if new == old:
            return
        cur.execute(
            "UPDATE events SET comp_id=? WHERE event_id=?", (new, event_id)
        )
        self._agg_apply(cur, old, row[1], row[3], row[4], row[2], -1)
        self._agg_apply(cur, new, row[1], row[3], row[4], row[2], +1)

    def _live_events_with(self, cur: sqlite3.Cursor, ident: str) -> List[str]:
        return [
            r[0]
            for r in cur.execute(
                "SELECT event_idents.event_id FROM event_idents"
                " JOIN events ON events.event_id = event_idents.event_id"
                " WHERE event_idents.identifier=? AND events.state='live'",
                (ident,),
            )
        ]

    def _deactivate_device(self, cur: sqlite3.Cursor, dev: str) -> None:
        """设备变成共享：摘除其全部活跃边，拆分分量，重排含它的事件。"""
        old_comp = self._comp_of(cur, dev)
        cur.execute("DELETE FROM edges WHERE a=? OR b=?", (dev, dev))
        cur.execute(
            "UPDATE idents SET is_shared=1, comp_id=NULL WHERE identifier=?",
            (dev,),
        )
        if old_comp is not None:
            self._resplit(cur, old_comp)
        for eid in self._live_events_with(cur, dev):
            self._reassign_event(cur, eid)
        if old_comp is not None:
            # 设备原是单例分量时，摘除后分量不再有任何成员与事件，删除空壳
            still = cur.execute(
                "SELECT EXISTS(SELECT 1 FROM idents WHERE comp_id=?)",
                (old_comp,),
            ).fetchone()[0]
            if not still:
                cur.execute("DELETE FROM comps WHERE comp_id=?", (old_comp,))

    def _activate_device(self, cur: sqlite3.Cursor, dev: str) -> None:
        """设备回落到阈值内：恢复其活跃边并合并分量，重排含它的事件。"""
        comp_id = self._new_comp(cur, dev)
        cur.execute(
            "UPDATE idents SET is_shared=0, comp_id=? WHERE identifier=?",
            (comp_id, dev),
        )
        for a, b, cnt in cur.execute(
            "SELECT a, b, cnt FROM pairs WHERE a=? OR b=?", (dev, dev)
        ).fetchall():
            other = b if a == dev else a
            row = cur.execute(
                "SELECT is_shared, comp_id FROM idents WHERE identifier=?",
                (other,),
            ).fetchone()
            if row is None or row[0]:
                continue
            cur.execute(
                "INSERT INTO edges(a, b, cnt) VALUES(?,?,?)", (a, b, cnt)
            )
            comp_id = self._union(cur, self._comp_of(cur, dev), row[1])
        for eid in self._live_events_with(cur, dev):
            self._reassign_event(cur, eid)

    # ---- 公开操作 ----

    def ingest(self, event: Dict[str, Any]) -> None:
        idents = self._validate_event(event)
        event_id = event["event_id"]
        cur = self._begin()
        try:
            row = cur.execute(
                "SELECT state, channel, ts, kind, amount_cents FROM events"
                " WHERE event_id=?",
                (event_id,),
            ).fetchone()
            if row is not None:
                if row[0] == "retracted":
                    raise StitchError(f"event_id 已撤回，不得复用: {event_id!r}")
                stored = {
                    r[0]
                    for r in cur.execute(
                        "SELECT identifier FROM event_idents WHERE event_id=?",
                        (event_id,),
                    )
                }
                same = (
                    row[1] == event["channel"]
                    and row[2] == event["ts"]
                    and row[3] == event["kind"]
                    and row[4] == event["amount_cents"]
                    and stored == set(idents)
                )
                if same:
                    self._commit()
                    return
                raise StitchError(f"event_id 内容冲突: {event_id!r}")
            cur.execute(
                "INSERT INTO events(event_id, channel, ts, kind, amount_cents,"
                " state, comp_id) VALUES(?,?,?,?,?,'live',NULL)",
                (
                    event_id,
                    event["channel"],
                    event["ts"],
                    event["kind"],
                    event["amount_cents"],
                ),
            )
            for ident in idents:
                cur.execute(
                    "INSERT INTO event_idents(event_id, identifier) VALUES(?,?)",
                    (event_id, ident),
                )
            # 不变式：comp_id 为 NULL 的存活事件计入未归属；先计入，再由
            # _reassign_event 搬到正确分量
            self._agg_apply(
                cur, None, event["channel"], event["kind"],
                event["amount_cents"], event["ts"], +1,
            )
            for ident in idents:
                self._touch_ident(cur, ident)
            # 设备共享阈值检查（只可能因写入而越过阈值）
            devs = [i for i in idents if is_device(i)]
            nondevs = [i for i in idents if not is_device(i)]
            for dev in devs:
                for peer in nondevs:
                    cur.execute(
                        "INSERT INTO device_peer(dev, peer, cnt) VALUES(?,?,1)"
                        " ON CONFLICT(dev, peer) DO UPDATE SET cnt=cnt+1",
                        (dev, peer),
                    )
                    if cur.execute(
                        "SELECT cnt FROM device_peer WHERE dev=? AND peer=?",
                        (dev, peer),
                    ).fetchone()[0] == 1:
                        cur.execute(
                            "UPDATE idents SET peer_cnt=peer_cnt+1"
                            " WHERE identifier=?",
                            (dev,),
                        )
                shared_row = cur.execute(
                    "SELECT peer_cnt, is_shared FROM idents WHERE identifier=?",
                    (dev,),
                ).fetchone()
                if shared_row[0] > SHARED_THRESHOLD and not shared_row[1]:
                    self._deactivate_device(cur, dev)
            # 共现对：pairs 全量计数，edges 仅非共享端点
            for idx in range(len(idents)):
                for jdx in range(idx + 1, len(idents)):
                    x, y = idents[idx], idents[jdx]
                    a, b = (x, y) if sort_key_of(x) < sort_key_of(y) else (y, x)
                    cur.execute(
                        "INSERT INTO pairs(a, b, cnt) VALUES(?,?,1)"
                        " ON CONFLICT(a, b) DO UPDATE SET cnt=cnt+1",
                        (a, b),
                    )
                    if self._is_shared(cur, a) or self._is_shared(cur, b):
                        continue
                    erow = cur.execute(
                        "SELECT cnt FROM edges WHERE a=? AND b=?", (a, b)
                    ).fetchone()
                    if erow:
                        cur.execute(
                            "UPDATE edges SET cnt=cnt+1 WHERE a=? AND b=?", (a, b)
                        )
                    else:
                        cur.execute(
                            "INSERT INTO edges(a, b, cnt) VALUES(?,?,1)", (a, b)
                        )
                        self._union(
                            cur, self._comp_of(cur, a), self._comp_of(cur, b)
                        )
            self._reassign_event(cur, event_id)
            self._commit()
        except Exception:
            self._rollback()
            raise

    def retract(self, event_id: str) -> None:
        if not isinstance(event_id, str) or not event_id:
            raise StitchError("event_id 必须是非空字符串")
        cur = self._begin()
        try:
            row = cur.execute(
                "SELECT state, comp_id, channel, ts, kind, amount_cents"
                " FROM events WHERE event_id=?",
                (event_id,),
            ).fetchone()
            if row is None:
                raise StitchError(f"未知 event_id: {event_id!r}")
            if row[0] == "retracted":
                self._commit()
                return
            _, comp_id, channel, ts, kind, amount = row
            idents = [
                r[0]
                for r in cur.execute(
                    "SELECT identifier FROM event_idents WHERE event_id=?",
                    (event_id,),
                )
            ]
            # 1. 摘除事件本身及其聚合贡献
            cur.execute(
                "UPDATE events SET state='retracted', comp_id=NULL"
                " WHERE event_id=?",
                (event_id,),
            )
            self._agg_apply(cur, comp_id, channel, kind, amount, ts, -1)
            # 2. 共现对计数 -1；支撑归零的活跃边删除并检查拆分
            ordered = sorted(idents, key=sort_key_of)
            for idx in range(len(ordered)):
                for jdx in range(idx + 1, len(ordered)):
                    a, b = ordered[idx], ordered[jdx]
                    cur.execute(
                        "UPDATE pairs SET cnt=cnt-1 WHERE a=? AND b=?", (a, b)
                    )
                    cur.execute("DELETE FROM pairs WHERE a=? AND b=? AND cnt=0",
                                (a, b))
                    erow = cur.execute(
                        "SELECT cnt FROM edges WHERE a=? AND b=?", (a, b)
                    ).fetchone()
                    if erow is None:
                        continue
                    if erow[0] > 1:
                        cur.execute(
                            "UPDATE edges SET cnt=cnt-1 WHERE a=? AND b=?",
                            (a, b),
                        )
                    else:
                        cur.execute(
                            "DELETE FROM edges WHERE a=? AND b=?", (a, b)
                        )
                        comp = self._comp_of(cur, a)
                        if comp is not None:
                            self._resplit(cur, comp)
            # 3. 设备共现计数 -1；回落到阈值内则解除共享
            devs = [i for i in idents if is_device(i)]
            nondevs = [i for i in idents if not is_device(i)]
            for dev in devs:
                for peer in nondevs:
                    cur.execute(
                        "UPDATE device_peer SET cnt=cnt-1"
                        " WHERE dev=? AND peer=?",
                        (dev, peer),
                    )
                    cur.execute(
                        "DELETE FROM device_peer WHERE dev=? AND peer=?"
                        " AND cnt=0",
                        (dev, peer),
                    )
                    if cur.rowcount:
                        cur.execute(
                            "UPDATE idents SET peer_cnt=peer_cnt-1"
                            " WHERE identifier=?",
                            (dev,),
                        )
                shared_row = cur.execute(
                    "SELECT peer_cnt, is_shared FROM idents WHERE identifier=?",
                    (dev,),
                ).fetchone()
                if shared_row and shared_row[1] and shared_row[0] <= SHARED_THRESHOLD:
                    self._activate_device(cur, dev)
            # 4. 标识符存活计数 -1；归零的标识符连同其单例分量删除
            for ident in idents:
                cur.execute(
                    "UPDATE idents SET ev_cnt=ev_cnt-1 WHERE identifier=?",
                    (ident,),
                )
                irow = cur.execute(
                    "SELECT ev_cnt, comp_id FROM idents WHERE identifier=?",
                    (ident,),
                ).fetchone()
                if irow and irow[0] == 0:
                    cur.execute(
                        "DELETE FROM idents WHERE identifier=?", (ident,)
                    )
                    if irow[1] is not None:
                        cur.execute(
                            "DELETE FROM comps WHERE comp_id=?", (irow[1],)
                        )
            self._commit()
        except Exception:
            self._rollback()
            raise

    def _profile_of(self, cur: sqlite3.Cursor, comp_id: int) -> Dict[str, Any]:
        row = cur.execute(
            "SELECT key_ident, web, mobile, kiosk, inbox, purchases, amount,"
            " last_ts FROM comps WHERE comp_id=?",
            (comp_id,),
        ).fetchone()
        members = sorted(
            (
                r[0]
                for r in cur.execute(
                    "SELECT identifier FROM idents WHERE comp_id=?", (comp_id,)
                )
            ),
            key=sort_key_of,
        )
        return {
            "key": row[0],
            "identifiers": members,
            "channels": {
                "web": row[1],
                "mobile": row[2],
                "kiosk": row[3],
                "inbox": row[4],
            },
            "purchases": row[5],
            "amount_cents": row[6],
            "last_ts": row[7],
        }

    def resolve(self, identifier: str) -> Optional[Dict[str, Any]]:
        split_ident(identifier)
        cur = self._conn.cursor()
        row = cur.execute(
            "SELECT comp_id, is_shared FROM idents WHERE identifier=?",
            (identifier,),
        ).fetchone()
        if row is None:
            return None
        if row[1]:
            return {"shared": True, "identifier": identifier}
        return self._profile_of(cur, row[0])

    def export_profiles(self) -> Iterator[Dict[str, Any]]:
        outer = self._conn.cursor()
        inner = self._conn.cursor()  # 外层流式遍历期间内层查询必须用独立游标
        for (comp_id,) in outer.execute(
            "SELECT comp_id FROM comps ORDER BY sort_key"
        ):
            yield self._profile_of(inner, comp_id)

    def stats(self) -> Dict[str, int]:
        cur = self._conn.cursor()
        profiles = cur.execute("SELECT COUNT(*) FROM comps").fetchone()[0]
        unassigned = cur.execute(
            "SELECT v FROM meta WHERE k='unassigned'"
        ).fetchone()[0]
        return {"profiles": profiles, "unassigned_events": unassigned}
