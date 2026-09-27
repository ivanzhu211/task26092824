"""基于 sqlite3 的增量身份拼接存储。

不变式（任何操作返回后都成立，且与从存活事件集从零计算一致）：
- edges(a, b, cnt)：a<b，cnt = 当前均非共享的标识符对在所有存活事件中的共现次数。
- device_peers(device, peer, cnt)：device 与非 device 标识符 peer 在存活事件中的共现次数。
- device_counts(device, cnt)：device 的不同非 device 共现标识符数量；>3 即共享设备。
- members / profiles：受影响画像在每次操作后按当前边集重算，其余画像不动。
- events.attributed：该事件当前是否归属于某个画像；meta.unattributed 为存活且未归属事件数。
  插入时先记 attributed=1，由 _reattribute 校正；撤回未归属事件时直接减计数。
"""
from __future__ import annotations

import sqlite3
from typing import Any, Iterable, Iterator

from .errors import (
    DuplicateEventError,
    RetractedEventError,
    UnknownEventError,
)
from .model import (
    CHANNELS,
    Event,
    Profile,
    SharedDevice,
    Stats,
    content_hash,
    normalize_event,
    parse_identifier,
    sort_key,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id     TEXT PRIMARY KEY,
    content_hash TEXT NOT NULL,
    channel      TEXT NOT NULL,
    ts           INTEGER NOT NULL,
    kind         TEXT NOT NULL,
    amount_cents INTEGER NOT NULL,
    retracted    INTEGER NOT NULL DEFAULT 0,
    attributed   INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS event_ids (
    ident    TEXT NOT NULL,
    event_id TEXT NOT NULL REFERENCES events(event_id),
    PRIMARY KEY (ident, event_id)
);
CREATE INDEX IF NOT EXISTS idx_event_ids_event ON event_ids(event_id);
CREATE TABLE IF NOT EXISTS edges (
    a   TEXT NOT NULL,
    b   TEXT NOT NULL,
    cnt INTEGER NOT NULL,
    PRIMARY KEY (a, b)
);
CREATE INDEX IF NOT EXISTS idx_edges_b ON edges(b);
CREATE TABLE IF NOT EXISTS device_peers (
    device TEXT NOT NULL,
    peer   TEXT NOT NULL,
    cnt    INTEGER NOT NULL,
    PRIMARY KEY (device, peer)
);
CREATE TABLE IF NOT EXISTS device_counts (
    device TEXT PRIMARY KEY,
    cnt    INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS shared_devices (
    device TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS members (
    ident       TEXT PRIMARY KEY,
    profile_key TEXT NOT NULL,
    sort_key    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_members_profile ON members(profile_key);
CREATE TABLE IF NOT EXISTS profiles (
    key          TEXT PRIMARY KEY,
    sort_key     TEXT NOT NULL,
    cnt_web      INTEGER NOT NULL,
    cnt_mobile   INTEGER NOT NULL,
    cnt_kiosk    INTEGER NOT NULL,
    cnt_inbox    INTEGER NOT NULL,
    purchase_cnt INTEGER NOT NULL,
    amount_sum   INTEGER NOT NULL,
    max_ts       INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
"""

_SHARED_THRESHOLD = 3  # 不同非 device 共现标识符数 > 3 即共享设备
_BATCH = 400


def _chunks(items: Iterable[str], size: int = _BATCH) -> Iterator[list[str]]:
    batch: list[str] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def _placeholders(n: int) -> str:
    return ",".join("?" * n)


class IdentityStitcher:
    def __init__(self, db_path: str) -> None:
        self._conn = sqlite3.connect(db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        # 题目明确无需崩溃恢复，关闭同步刷盘以避免每次提交都 fsync
        self._conn.execute("PRAGMA synchronous=OFF")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.executescript(_SCHEMA)
        self._conn.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES ('unattributed', 0)")
        self._conn.commit()

    # ------------------------------------------------------------------
    # 底层小工具
    # ------------------------------------------------------------------
    def _sort_key_str(self, ident: str) -> str:
        rank, value = parse_identifier(ident)
        return f"{rank}:{value}"

    def _shared_set(self, idents: Iterable[str]) -> set[str]:
        """返回给定标识符中当前是共享设备的子集。"""
        found: set[str] = set()
        for batch in _chunks(list(idents)):
            q = f"SELECT device FROM shared_devices WHERE device IN ({_placeholders(len(batch))})"
            found.update(row[0] for row in self._conn.execute(q, batch))
        return found

    def _add_edge(self, x: str, y: str, delta: int) -> bool:
        """调整边计数，返回该边是否因此消失（计数降到 0）。"""
        a, b = (x, y) if sort_key(x) <= sort_key(y) else (y, x)
        self._conn.execute(
            "INSERT INTO edges(a, b, cnt) VALUES (?, ?, ?) "
            "ON CONFLICT(a, b) DO UPDATE SET cnt = cnt + ?",
            (a, b, delta, delta))
        cur = self._conn.execute(
            "DELETE FROM edges WHERE a = ? AND b = ? AND cnt <= 0", (a, b))
        return cur.rowcount > 0

    def _live_events_with(self, ident: str) -> list[str]:
        q = ("SELECT ei.event_id FROM event_ids ei "
             "JOIN events e ON e.event_id = ei.event_id "
             "WHERE ei.ident = ? AND e.retracted = 0")
        return [row[0] for row in self._conn.execute(q, (ident,))]

    def _event_idents(self, event_id: str) -> list[str]:
        q = "SELECT ident FROM event_ids WHERE event_id = ?"
        return [row[0] for row in self._conn.execute(q, (event_id,))]

    # ------------------------------------------------------------------
    # ingest
    # ------------------------------------------------------------------
    def ingest(self, event: Any) -> None:
        ev = normalize_event(event)  # 校验失败时状态不变
        digest = content_hash(ev)
        row = self._conn.execute(
            "SELECT content_hash, retracted FROM events WHERE event_id = ?",
            (ev.event_id,)).fetchone()
        if row is not None:
            if row[1]:
                raise RetractedEventError(f"event_id 已被撤回: {ev.event_id}")
            if row[0] == digest:
                return  # 内容相同的重复写入：无操作
            raise DuplicateEventError(f"event_id 已存在且内容不同: {ev.event_id}")

        try:
            self._ingest_txn(ev, digest)
        except Exception:
            self._conn.rollback()
            raise
        self._conn.commit()

    def _ingest_txn(self, ev: Event, digest: str) -> None:
        cur = self._conn
        cur.execute(
            "INSERT INTO events(event_id, content_hash, channel, ts, kind, "
            "amount_cents, retracted, attributed) VALUES (?,?,?,?,?,?,0,1)",
            (ev.event_id, digest, ev.channel, ev.ts, ev.kind, ev.amount_cents))
        cur.executemany(
            "INSERT INTO event_ids(ident, event_id) VALUES (?, ?)",
            [(ident, ev.event_id) for ident in ev.identifiers])

        newly_shared = self._update_device_peers(ev.identifiers, +1)
        shared = self._shared_set(ev.identifiers)
        self._add_event_edges(ev.identifiers, shared, +1)
        if newly_shared:
            # 有设备越过阈值：可能拆分画像，重算受影响区域
            affected = set(ev.identifiers)
            for device in newly_shared:
                affected |= self._become_shared(device)
            self._recompute(affected)
        else:
            # 纯新增：只可能合并画像，走增量快速路径
            self._merge_for_event(ev, shared)

    def _update_device_peers(self, idents: tuple[str, ...], delta: int) -> list[str]:
        """维护 device_peers / device_counts，返回共享状态发生变化的设备列表。"""
        devices = [i for i in idents if i.startswith("device:")]
        peers = [i for i in idents if not i.startswith("device:")]
        changed: list[str] = []
        for device in devices:
            for peer in peers:
                row = self._conn.execute(
                    "SELECT cnt FROM device_peers WHERE device = ? AND peer = ?",
                    (device, peer)).fetchone()
                if row is None and delta > 0:
                    self._conn.execute(
                        "INSERT INTO device_peers(device, peer, cnt) VALUES (?,?,1)",
                        (device, peer))
                    self._bump_device_count(device, +1, changed)
                elif row is not None:
                    new_cnt = row[0] + delta
                    if new_cnt <= 0:
                        self._conn.execute(
                            "DELETE FROM device_peers WHERE device = ? AND peer = ?",
                            (device, peer))
                        self._bump_device_count(device, -1, changed)
                    else:
                        self._conn.execute(
                            "UPDATE device_peers SET cnt = ? WHERE device = ? AND peer = ?",
                            (new_cnt, device, peer))
        return changed

    def _bump_device_count(self, device: str, delta: int, changed: list[str]) -> None:
        row = self._conn.execute(
            "SELECT cnt FROM device_counts WHERE device = ?", (device,)).fetchone()
        old = row[0] if row else 0
        new = old + delta
        if new <= 0:
            self._conn.execute("DELETE FROM device_counts WHERE device = ?", (device,))
        elif row is None:
            self._conn.execute(
                "INSERT INTO device_counts(device, cnt) VALUES (?, ?)", (device, new))
        else:
            self._conn.execute(
                "UPDATE device_counts SET cnt = ? WHERE device = ?", (new, device))
        was_shared = old > _SHARED_THRESHOLD
        is_shared = new > _SHARED_THRESHOLD
        if is_shared and not was_shared:
            self._conn.execute(
                "INSERT OR IGNORE INTO shared_devices(device) VALUES (?)", (device,))
            changed.append(device)
        elif was_shared and not is_shared:
            self._conn.execute(
                "DELETE FROM shared_devices WHERE device = ?", (device,))
            changed.append(device)

    def _become_shared(self, device: str) -> set[str]:
        """设备变为共享：删除其全部边，返回其共现标识符集合。"""
        co_occurring: set[str] = set()
        for event_id in self._live_events_with(device):
            co_occurring.update(self._event_idents(event_id))
        co_occurring.discard(device)
        self._conn.execute("DELETE FROM edges WHERE a = ? OR b = ?", (device, device))
        return co_occurring

    def _add_event_edges(self, idents, shared: set[str], delta: int) -> bool:
        """调整事件内非共享标识符两两之间的边，返回是否有边消失。"""
        live = [i for i in idents if i not in shared]
        vanished = False
        for i in range(len(live)):
            for j in range(i + 1, len(live)):
                if self._add_edge(live[i], live[j], delta):
                    vanished = True
        return vanished

    # ------------------------------------------------------------------
    # 纯新增的增量合并（不拆分，只连接）
    # ------------------------------------------------------------------
    def _merge_for_event(self, ev: Event, shared: set[str]) -> None:
        cur = self._conn
        rest = [i for i in ev.identifiers if i not in shared]
        if not rest:
            # 事件标识符全是共享设备：事件未归属
            cur.execute("UPDATE events SET attributed = 0 WHERE event_id = ?",
                        (ev.event_id,))
            cur.execute(
                "UPDATE meta SET value = value + 1 WHERE key = 'unattributed'")
            return

        # 事件涉及的已有画像
        key_of: dict[str, str] = {}
        for batch in _chunks(sorted(rest)):
            q = f"SELECT ident, profile_key FROM members WHERE ident IN ({_placeholders(len(batch))})"
            for ident, key in cur.execute(q, batch):
                key_of[ident] = key
        existing_keys = set(key_of.values())

        if not existing_keys:
            # 全新标识符组成的画像
            key = min(rest, key=sort_key)
            cur.executemany(
                "INSERT INTO members(ident, profile_key, sort_key) VALUES (?,?,?)",
                [(i, key, self._sort_key_str(i)) for i in rest])
            cur.execute(
                "INSERT INTO profiles(key, sort_key, cnt_web, cnt_mobile, cnt_kiosk, "
                "cnt_inbox, purchase_cnt, amount_sum, max_ts) VALUES (?,?,?,?,?,?,?,?,?)",
                (key, self._sort_key_str(key),
                 *(1 if ev.channel == c else 0 for c in CHANNELS),
                 1 if ev.kind == "purchase" else 0,
                 ev.amount_cents, ev.ts))
            return

        # 保留成员数最多的画像（并列时按 key 确定，仅影响性能不影响结果）
        sizes: dict[str, int] = {}
        for batch in _chunks(sorted(existing_keys)):
            q = (f"SELECT profile_key, COUNT(*) FROM members "
                 f"WHERE profile_key IN ({_placeholders(len(batch))}) GROUP BY profile_key")
            sizes.update(cur.execute(q, batch).fetchall())
        keeper = min(existing_keys, key=lambda k: (-sizes[k], k))

        agg_rows: dict[str, tuple] = {}
        for batch in _chunks(sorted(existing_keys)):
            q = (f"SELECT key, cnt_web, cnt_mobile, cnt_kiosk, cnt_inbox, "
                 f"purchase_cnt, amount_sum, max_ts FROM profiles "
                 f"WHERE key IN ({_placeholders(len(batch))})")
            for row in cur.execute(q, batch):
                agg_rows[row[0]] = row[1:]

        # 被吸收画像的成员并入 keeper
        for absorbed in existing_keys - {keeper}:
            cur.execute("UPDATE members SET profile_key = ? WHERE profile_key = ?",
                        (keeper, absorbed))
            cur.execute("DELETE FROM profiles WHERE key = ?", (absorbed,))

        # 新标识符插入 keeper
        new_idents = [i for i in rest if i not in key_of]
        cur.executemany(
            "INSERT INTO members(ident, profile_key, sort_key) VALUES (?,?,?)",
            [(i, keeper, self._sort_key_str(i)) for i in new_idents])

        # 合并后的 key 是全体标识符的全序最小者
        merged_key = min([keeper] + [k for k in existing_keys if k != keeper] + new_idents,
                         key=sort_key)
        if merged_key != keeper:
            cur.execute("UPDATE members SET profile_key = ? WHERE profile_key = ?",
                        (merged_key, keeper))
            cur.execute("UPDATE profiles SET key = ?, sort_key = ? WHERE key = ?",
                        (merged_key, self._sort_key_str(merged_key), keeper))

        # 聚合值：计数与金额相加，最近事件 ts 取最大，再加上本事件
        totals = [0, 0, 0, 0, 0, 0, 0]
        for vals in agg_rows.values():
            for i in range(6):
                totals[i] += vals[i]
            totals[6] = max(totals[6], vals[6])
        totals[CHANNELS.index(ev.channel)] += 1
        if ev.kind == "purchase":
            totals[4] += 1
            totals[5] += ev.amount_cents
        totals[6] = max(totals[6], ev.ts)
        cur.execute(
            "UPDATE profiles SET cnt_web=?, cnt_mobile=?, cnt_kiosk=?, cnt_inbox=?, "
            "purchase_cnt=?, amount_sum=?, max_ts=? WHERE key=?",
            (*totals, merged_key))

    # ------------------------------------------------------------------
    # 以下方法后续回合填充
    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    # retract
    # ------------------------------------------------------------------
    def retract(self, event_id: str) -> None:
        row = self._conn.execute(
            "SELECT retracted, attributed, channel, ts, kind, amount_cents "
            "FROM events WHERE event_id = ?",
            (event_id,)).fetchone()
        if row is None:
            raise UnknownEventError(f"未知 event_id: {event_id}")
        if row[0]:
            return  # 重复撤回：无操作
        fields = {"attributed": bool(row[1]), "channel": row[2], "ts": row[3],
                  "kind": row[4], "amount_cents": row[5]}
        try:
            self._retract_txn(event_id, fields)
        except Exception:
            self._conn.rollback()
            raise
        self._conn.commit()

    def _retract_txn(self, event_id: str, fields: dict) -> None:
        cur = self._conn
        cur.execute("UPDATE events SET retracted = 1 WHERE event_id = ?", (event_id,))
        if not fields["attributed"]:
            cur.execute(
                "UPDATE meta SET value = value - 1 WHERE key = 'unattributed'")

        idents = self._event_idents(event_id)
        shared = self._shared_set(idents)

        # 先按当前共享集合拆除本事件贡献的边
        vanished = self._add_event_edges(tuple(idents), shared, -1)

        # 再更新设备共现计数；可能让某些设备回落到阈值以内
        newly_unshared = self._update_device_peers(tuple(idents), -1)
        rest = [i for i in idents if i not in shared]
        if newly_unshared:
            # 有设备回落到阈值内：可能重新连接画像，重算受影响区域
            affected = set(idents)
            affected |= self._restore_device_edges(set(newly_unshared))
            self._recompute(affected)
        elif vanished and rest and not self._rest_connected(rest):
            # 消失的边真的切断了连通性：拆分画像，重算受影响区域
            self._recompute(set(idents))
        else:
            # 连通性不变：只更新归属画像的聚合值
            self._shrink_for_event(event_id, idents, shared, fields)

    def _rest_connected(self, idents: list[str]) -> bool:
        """判断 idents 在删边后的当前边图里是否仍全部连通（BFS，尽快命中目标）。

        不变式保证边不会跨画像、不会触及共享设备，因此 BFS 只需沿边表进行。
        """
        targets = set(idents)
        seen = {idents[0]}
        frontier = [idents[0]]
        while frontier and not targets <= seen:
            nxt: set[str] = set()
            for batch in _chunks(frontier):
                ph = _placeholders(len(batch))
                q = (f"SELECT b FROM edges WHERE a IN ({ph}) "
                     f"UNION SELECT a FROM edges WHERE b IN ({ph})")
                nxt.update(r[0] for r in self._conn.execute(q, batch + batch))
            frontier = [i for i in nxt if i not in seen]
            seen.update(frontier)
        return targets <= seen

    def _shrink_for_event(self, event_id: str, idents: list[str],
                          shared: set[str], fields: dict) -> None:
        rest = [i for i in idents if i not in shared]
        if not rest:
            return  # 未归属事件，计数已在调用处修正
        cur = self._conn
        row = cur.execute(
            "SELECT profile_key FROM members WHERE ident = ?", (rest[0],)).fetchone()
        if row is None:
            return  # 防御：理论上不会发生
        key = row[0]
        agg = cur.execute(
            "SELECT cnt_web, cnt_mobile, cnt_kiosk, cnt_inbox, purchase_cnt, "
            "amount_sum, max_ts FROM profiles WHERE key = ?", (key,)).fetchone()
        totals = list(agg)
        totals[CHANNELS.index(fields["channel"])] -= 1
        if fields["kind"] == "purchase":
            totals[4] -= 1
            totals[5] -= fields["amount_cents"]
        total_events = sum(totals[:4])
        if total_events == 0:
            # 画像已无任何事件：删除画像及其成员
            cur.execute("DELETE FROM profiles WHERE key = ?", (key,))
            cur.execute("DELETE FROM members WHERE profile_key = ?", (key,))
            return
        if fields["ts"] == totals[6]:
            # 撤回的可能是最近事件，沿成员索引重算精确 max ts
            new_max = 0
            for batch in _chunks(sorted(
                    r[0] for r in cur.execute(
                        "SELECT ident FROM members WHERE profile_key = ?", (key,)))):
                q = (f"SELECT MAX(e.ts) FROM events e JOIN event_ids ei "
                     f"ON ei.event_id = e.event_id "
                     f"WHERE ei.ident IN ({_placeholders(len(batch))}) AND e.retracted = 0")
                value = cur.execute(q, batch).fetchone()[0]
                if value is not None and value > new_max:
                    new_max = value
            totals[6] = new_max
        cur.execute(
            "UPDATE profiles SET cnt_web=?, cnt_mobile=?, cnt_kiosk=?, cnt_inbox=?, "
            "purchase_cnt=?, amount_sum=?, max_ts=? WHERE key=?",
            (*totals, key))

    def _restore_device_edges(self, devices: set[str]) -> set[str]:
        """一批设备同时回落为非共享：按存活事件恢复边，返回共现标识符集合。

        必须按批处理：同一事件里两个同时回落的设备之间的边只能恢复一次。
        """
        event_ids: set[str] = set()
        for device in devices:
            event_ids.update(self._live_events_with(device))
        co_occurring: set[str] = set()
        for event_id in event_ids:
            idents = self._event_idents(event_id)
            shared = self._shared_set(idents)
            targets = [i for i in idents if i not in shared]
            co_occurring.update(targets)
            restored = [d for d in targets if d in devices]
            for idx, device in enumerate(restored):
                for other in targets:
                    if other != device and other not in restored[idx + 1:]:
                        self._add_edge(device, other, +1)
        return co_occurring

    # ------------------------------------------------------------------
    # 受影响画像重算
    # ------------------------------------------------------------------
    def _recompute(self, affected: set[str]) -> None:
        if not affected:
            return
        cur = self._conn

        old_keys: set[str] = set()
        for batch in _chunks(sorted(affected)):
            q = f"SELECT DISTINCT profile_key FROM members WHERE ident IN ({_placeholders(len(batch))})"
            old_keys.update(row[0] for row in cur.execute(q, batch))

        # 记录旧归属；画像行总是重建，members 行只做最小差异更新
        universe = set(affected)
        old_assign: dict[str, str] = {}
        for batch in _chunks(sorted(old_keys)):
            q = (f"SELECT ident, profile_key FROM members "
                 f"WHERE profile_key IN ({_placeholders(len(batch))})")
            for ident, key in cur.execute(q, batch):
                universe.add(ident)
                old_assign[ident] = key
            cur.execute(
                f"DELETE FROM profiles WHERE key IN ({_placeholders(len(batch))})",
                batch)

        shared = self._shared_set(universe)
        candidates = universe - shared

        parent = {i: i for i in candidates}

        def find(x: str) -> str:
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(x: str, y: str) -> None:
            rx, ry = find(x), find(y)
            if rx != ry:
                parent[rx] = ry

        # 分批拉取候选集内部的全部边。不变式：边的两端必同属一个画像，
        # 因此 a 在 candidates 中当且仅当 b 也在，按 a 单点取即可覆盖全部内部边。
        for batch in _chunks(sorted(candidates)):
            q = f"SELECT a, b FROM edges WHERE a IN ({_placeholders(len(batch))})"
            for a, b in cur.execute(q, batch):
                if b in parent:
                    union(a, b)

        components: dict[str, list[str]] = {}
        for ident in candidates:
            components.setdefault(find(ident), []).append(ident)

        new_assign: dict[str, str] = {}
        for members_list in components.values():
            key, events = self._collect_profile_events(members_list)
            if key is None:
                continue  # 无存活事件的分量：不形成画像
            for ident in members_list:
                new_assign[ident] = key
            self._write_profile(key, members_list, events)

        # members 差异更新：删除（死分量）、改挂（拆分/合并换 key）、新增
        to_delete = [i for i, k in old_assign.items() if new_assign.get(i) != k and i not in new_assign]
        to_update = [(new_assign[i], i) for i, k in old_assign.items()
                     if i in new_assign and new_assign[i] != k]
        to_insert = [(i, new_assign[i]) for i in new_assign if i not in old_assign]
        for batch in _chunks(sorted(to_delete)):
            cur.execute(
                f"DELETE FROM members WHERE ident IN ({_placeholders(len(batch))})", batch)
        cur.executemany("UPDATE members SET profile_key = ? WHERE ident = ?", to_update)
        cur.executemany(
            "INSERT INTO members(ident, profile_key, sort_key) VALUES (?,?,?)",
            [(i, k, self._sort_key_str(i)) for i, k in to_insert])

        self._reattribute(affected)

    def _collect_profile_events(self, members_list: list[str]):
        """收集分量触及的存活事件；无事件返回 (None, {})。返回 (key, {event_id: row})。"""
        cur = self._conn
        # 先沿 event_ids 索引收集分量触及的事件 id（索引扫描，无需回表），
        # 再批量取存活事件行；两阶段避免按标识符分块重复扫描事件表
        event_ids: set[str] = set()
        for batch in _chunks(sorted(members_list)):
            q = (f"SELECT event_id FROM event_ids "
                 f"WHERE ident IN ({_placeholders(len(batch))})")
            event_ids.update(row[0] for row in cur.execute(q, batch))
        events: dict[str, tuple] = {}
        for batch in _chunks(sorted(event_ids)):
            q = (f"SELECT event_id, channel, ts, kind, amount_cents FROM events "
                 f"WHERE event_id IN ({_placeholders(len(batch))}) AND retracted = 0")
            for row in cur.execute(q, batch):
                events[row[0]] = row
        if not events:
            return None, {}
        return min(members_list, key=sort_key), events

    def _write_profile(self, key: str, members_list: list[str],
                       events: dict[str, tuple]) -> None:
        """写入聚合画像行（members 行由 _recompute 统一做差异更新）。"""
        cur = self._conn
        counts = {c: 0 for c in CHANNELS}
        purchase_cnt = 0
        amount_sum = 0
        max_ts = 0
        for _, channel, ts, kind, amount in events.values():
            counts[channel] += 1
            if kind == "purchase":
                purchase_cnt += 1
                amount_sum += amount
            if ts > max_ts:
                max_ts = ts
        cur.execute(
            "INSERT INTO profiles(key, sort_key, cnt_web, cnt_mobile, cnt_kiosk, "
            "cnt_inbox, purchase_cnt, amount_sum, max_ts) VALUES (?,?,?,?,?,?,?,?,?)",
            (key, self._sort_key_str(key), counts["web"], counts["mobile"],
             counts["kiosk"], counts["inbox"], purchase_cnt, amount_sum, max_ts))

    def _reattribute(self, affected: set[str]) -> None:
        """重算触及 affected 的存活事件的归属标记，并维护未归属计数。"""
        cur = self._conn
        event_ids: set[str] = set()
        for batch in _chunks(sorted(affected)):
            q = (f"SELECT ei.event_id FROM event_ids ei "
                 f"JOIN events e ON e.event_id = ei.event_id "
                 f"WHERE ei.ident IN ({_placeholders(len(batch))}) AND e.retracted = 0")
            event_ids.update(row[0] for row in cur.execute(q, batch))

        # 批量取回这些事件的标识符、旧归属标记与共享状态，避免逐事件查询
        idents_by_event: dict[str, list[str]] = {e: [] for e in event_ids}
        old_flags: dict[str, int] = {}
        all_idents: set[str] = set()
        for batch in _chunks(sorted(event_ids)):
            ph = _placeholders(len(batch))
            q = f"SELECT event_id, ident FROM event_ids WHERE event_id IN ({ph})"
            for event_id, ident in cur.execute(q, batch):
                idents_by_event[event_id].append(ident)
                all_idents.add(ident)
            q = f"SELECT event_id, attributed FROM events WHERE event_id IN ({ph})"
            for event_id, attributed in cur.execute(q, batch):
                old_flags[event_id] = attributed
        shared = self._shared_set(all_idents)

        delta = 0
        rows = []
        for event_id in sorted(event_ids):
            attributed = 1 if any(
                i not in shared for i in idents_by_event[event_id]) else 0
            if old_flags[event_id] != attributed:
                rows.append((attributed, event_id))
                delta += 1 if attributed else -1
        cur.executemany(
            "UPDATE events SET attributed = ? WHERE event_id = ?", rows)
        if delta:
            cur.execute(
                "UPDATE meta SET value = value + ? WHERE key = 'unattributed'", (-delta,))

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def resolve(self, identifier: str) -> Profile | SharedDevice | None:
        parse_identifier(identifier)  # 非法格式抛 ValidationError
        if self._conn.execute(
                "SELECT 1 FROM shared_devices WHERE device = ?",
                (identifier,)).fetchone():
            return SharedDevice(identifier)
        row = self._conn.execute(
            "SELECT profile_key FROM members WHERE ident = ?",
            (identifier,)).fetchone()
        if row is None:
            return None
        return self._load_profile(row[0])

    def _load_profile(self, key: str) -> Profile:
        row = self._conn.execute(
            "SELECT cnt_web, cnt_mobile, cnt_kiosk, cnt_inbox, purchase_cnt, "
            "amount_sum, max_ts FROM profiles WHERE key = ?", (key,)).fetchone()
        members = tuple(r[0] for r in self._conn.execute(
            "SELECT ident FROM members WHERE profile_key = ? ORDER BY sort_key",
            (key,)))
        return Profile(
            key=key,
            members=members,
            channel_counts=tuple(zip(CHANNELS, row[:4])),
            purchase_count=row[4],
            amount_cents=row[5],
            last_ts=row[6],
        )

    def export_profiles(self) -> Iterator[Profile]:
        cur = self._conn.execute("SELECT key FROM profiles ORDER BY sort_key")
        for (key,) in cur:
            yield self._load_profile(key)

    def stats(self) -> Stats:
        profiles = self._conn.execute("SELECT COUNT(*) FROM profiles").fetchone()[0]
        unattributed = self._conn.execute(
            "SELECT value FROM meta WHERE key = 'unattributed'").fetchone()[0]
        return Stats(profiles=profiles, unattributed_events=unattributed)

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "IdentityStitcher":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
