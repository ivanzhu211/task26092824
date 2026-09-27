"""一致性测试：随机操作序列对照参照实现 + 顺序无关/重开持久化。"""

import os
import random
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import reference
from stitch import IdentityStitcher, StitchError

CHANNELS = ("web", "mobile", "kiosk", "inbox")


def make_event(event_id, rng, pool, ts_lo=1, ts_hi=5000):
    k = rng.randint(1, 5)
    kind = "purchase" if rng.random() < 0.3 else "view"
    return {
        "event_id": event_id,
        "channel": rng.choice(CHANNELS),
        "ts": rng.randint(ts_lo, ts_hi),
        "kind": kind,
        "amount_cents": rng.randint(1, 10000) if kind == "purchase" else 0,
        "identifiers": rng.sample(pool, k),
    }


def shared_set(events):
    return {
        d
        for d in {i for ev in events for i in ev["identifiers"]}
        if reference.resolve(events, d) == {"shared": True, "identifier": d}
    }


class RandomSequenceTest(unittest.TestCase):
    """随机操作序列：每一步后 export_profiles / stats 必须与参照一致。"""

    def run_sequence(self, seed, n_ops):
        rng = random.Random(seed)
        pool = (
            [f"cust:c{i}" for i in range(4)]
            + [f"email:e{i}@x.com" for i in range(8)]
            + [f"inbox:i{i}" for i in range(3)]
            + [f"device:d{i}" for i in range(3)]
        )
        live = {}
        retracted = []
        seq = 0
        crossings = {"up": 0, "down": 0}
        prev_shared = set()
        pair_support_seen = 0

        with IdentityStitcher(":memory:") as lib:
            for step in range(n_ops):
                r = rng.random()
                if r < 0.45 or not live:
                    seq += 1
                    ev = make_event(f"ev{seq}", rng, pool)
                    lib.ingest(ev)
                    live[ev["event_id"]] = ev
                elif r < 0.62:
                    eid = rng.choice(list(live))
                    lib.retract(eid)
                    retracted.append(eid)
                    del live[eid]
                elif r < 0.70:
                    # 专挑含设备的存活事件撤回，驱动设备回落阈值
                    with_dev = [
                        eid
                        for eid, ev in live.items()
                        if any(i.startswith("device:") for i in ev["identifiers"])
                    ]
                    if with_dev:
                        eid = rng.choice(with_dev)
                        lib.retract(eid)
                        retracted.append(eid)
                        del live[eid]
                elif r < 0.76:
                    ev = rng.choice(list(live.values()))
                    lib.ingest(dict(ev, identifiers=list(ev["identifiers"])))
                elif r < 0.80 and retracted:
                    lib.retract(rng.choice(retracted))  # 重复撤回：无操作
                elif r < 0.84:
                    ev = dict(rng.choice(list(live.values())))
                    ev["ts"] = ev["ts"] + 1  # 同 id 不同内容：报错
                    with self.assertRaises(StitchError):
                        lib.ingest(ev)
                elif r < 0.87 and retracted:
                    bad = make_event(rng.choice(retracted), rng, pool)
                    with self.assertRaises(StitchError):
                        lib.ingest(bad)  # 已撤回的 id：报错
                elif r < 0.89:
                    with self.assertRaises(StitchError):
                        lib.retract(f"ghost-{step}")  # 未知 id：报错
                else:
                    ident = rng.choice(pool)
                    got = lib.resolve(ident)
                    want = reference.resolve(list(live.values()), ident)
                    self.assertEqual(got, want, f"resolve({ident}) @step{step}")

                events = list(live.values())
                ref = reference.compute(events)
                got_profiles = list(lib.export_profiles())
                self.assertEqual(
                    got_profiles, ref["profiles"], f"profiles diverge @step{step}"
                )
                st = lib.stats()
                self.assertEqual(st["profiles"], len(ref["profiles"]))
                self.assertEqual(st["unassigned_events"], ref["unassigned_events"])

                now_shared = shared_set(events)
                for dev in now_shared - prev_shared:
                    crossings["up"] += 1
                for dev in prev_shared - now_shared:
                    crossings["down"] += 1
                prev_shared = now_shared
                pair_support_seen = max(
                    pair_support_seen, self._max_pair_support(events)
                )

        self.assertGreater(crossings["up"], 0, "序列未覆盖设备越过阈值")
        self.assertGreater(crossings["down"], 0, "序列未覆盖设备回落阈值")
        self.assertGreater(pair_support_seen, 1, "序列未覆盖同对多次共现")

    @staticmethod
    def _max_pair_support(events):
        counts = {}
        for ev in events:
            ids = sorted(set(ev["identifiers"]))
            for i in range(len(ids)):
                for j in range(i + 1, len(ids)):
                    key = (ids[i], ids[j])
                    counts[key] = counts.get(key, 0) + 1
        return max(counts.values(), default=0)

    def test_seed_1(self):
        self.run_sequence(seed=1, n_ops=500)

    def test_seed_2(self):
        self.run_sequence(seed=2, n_ops=500)

    def test_seed_3(self):
        self.run_sequence(seed=3, n_ops=500)


class TargetedScenarioTest(unittest.TestCase):
    """确定性场景：阈值双向跨越、同对多次共现逐条撤回、重复写/撤。"""

    def test_threshold_crossing_both_directions(self):
        with IdentityStitcher(":memory:") as lib:
            for i in range(4):
                lib.ingest(
                    {
                        "event_id": f"e{i}",
                        "channel": "kiosk",
                        "ts": 100 + i,
                        "kind": "view",
                        "amount_cents": 0,
                        "identifiers": ["device:d1", f"email:e{i}@x.com"],
                    }
                )
            # 4 个不同非设备共现标识符 -> d1 共享，原画像拆成 4 个单例
            self.assertEqual(
                lib.resolve("device:d1"),
                {"shared": True, "identifier": "device:d1"},
            )
            keys = [p["key"] for p in lib.export_profiles()]
            self.assertEqual(keys, [f"email:e{i}@x.com" for i in range(4)])
            # 撤回一条 -> 回落到 3 个 -> 重新连成单一画像
            lib.retract("e3")
            self.assertNotEqual(
                lib.resolve("device:d1"), {"shared": True, "identifier": "device:d1"}
            )
            profiles = list(lib.export_profiles())
            self.assertEqual(len(profiles), 1)
            self.assertEqual(
                profiles[0]["identifiers"],
                ["email:e0@x.com", "email:e1@x.com", "email:e2@x.com", "device:d1"],
            )

    def test_multi_support_pair_retracted_one_by_one(self):
        with IdentityStitcher(":memory:") as lib:
            for i in range(3):
                lib.ingest(
                    {
                        "event_id": f"p{i}",
                        "channel": "web",
                        "ts": 10 + i,
                        "kind": "purchase",
                        "amount_cents": 100 * (i + 1),
                        "identifiers": ["cust:c1", "email:e1@x.com"],
                    }
                )
            lib.retract("p0")
            lib.retract("p1")
            profiles = list(lib.export_profiles())  # 仍有 p2 支撑，不得切断
            self.assertEqual(len(profiles), 1)
            self.assertEqual(profiles[0]["key"], "cust:c1")
            self.assertEqual(profiles[0]["purchases"], 1)
            self.assertEqual(profiles[0]["amount_cents"], 300)
            self.assertEqual(profiles[0]["last_ts"], 12)
            lib.retract("p2")
            self.assertEqual(list(lib.export_profiles()), [])
            self.assertEqual(lib.stats(), {"profiles": 0, "unassigned_events": 0})

    def test_duplicate_ingest_and_retract(self):
        ev = {
            "event_id": "x1",
            "channel": "mobile",
            "ts": 7,
            "kind": "purchase",
            "amount_cents": 42,
            "identifiers": ["cust:c9", "device:d9"],
        }
        with IdentityStitcher(":memory:") as lib:
            lib.ingest(ev)
            before = list(lib.export_profiles())
            lib.ingest(dict(ev, identifiers=list(ev["identifiers"])))  # 幂等
            self.assertEqual(list(lib.export_profiles()), before)
            with self.assertRaises(StitchError):
                lib.ingest(dict(ev, ts=8))  # 同 id 不同内容
            self.assertEqual(list(lib.export_profiles()), before)  # 报错不改状态
            lib.retract("x1")
            after = list(lib.export_profiles())
            lib.retract("x1")  # 重复撤回：无操作
            self.assertEqual(list(lib.export_profiles()), after)
            with self.assertRaises(StitchError):
                lib.ingest(ev)  # 已撤回 id 复用
            with self.assertRaises(StitchError):
                lib.retract("never-existed")

    def test_shared_device_event_unassigned(self):
        with IdentityStitcher(":memory:") as lib:
            for i in range(4):
                lib.ingest(
                    {
                        "event_id": f"s{i}",
                        "channel": "web",
                        "ts": i,
                        "kind": "view",
                        "amount_cents": 0,
                        "identifiers": ["device:d1", f"email:e{i}@x.com"],
                    }
                )
            lib.ingest(
                {
                    "event_id": "solo",
                    "channel": "inbox",
                    "ts": 9,
                    "kind": "view",
                    "amount_cents": 0,
                    "identifiers": ["device:d1"],
                }
            )
            # solo 去掉共享设备后为空 -> 未归属
            self.assertEqual(lib.stats()["unassigned_events"], 1)
            lib.retract("solo")
            self.assertEqual(lib.stats()["unassigned_events"], 0)


class OrderIndependenceTest(unittest.TestCase):
    """同一批存活事件不同写入顺序 -> 导出逐项相同；关闭重开后状态不变。"""

    def test_order_and_reopen(self):
        rng = random.Random(99)
        pool = (
            [f"cust:c{i}" for i in range(5)]
            + [f"email:e{i}@x.com" for i in range(10)]
            + [f"inbox:i{i}" for i in range(4)]
            + [f"device:d{i}" for i in range(4)]
        )
        events = [make_event(f"ev{i}", rng, pool) for i in range(80)]
        order_a = list(events)
        order_b = list(events)
        rng.shuffle(order_b)
        self.assertNotEqual(
            [e["event_id"] for e in order_a], [e["event_id"] for e in order_b]
        )

        with tempfile.TemporaryDirectory() as tmp:
            path_a = os.path.join(tmp, "a.db")
            path_b = os.path.join(tmp, "b.db")
            lib_a = IdentityStitcher(path_a)
            lib_b = IdentityStitcher(path_b)
            for ev in order_a:
                lib_a.ingest(ev)
            for ev in order_b:
                lib_b.ingest(ev)
            export_a = list(lib_a.export_profiles())
            export_b = list(lib_b.export_profiles())
            self.assertEqual(export_a, export_b)
            self.assertEqual(lib_a.stats(), lib_b.stats())
            lib_a.close()
            lib_b.close()

            # 重新打开：状态不变
            lib_a2 = IdentityStitcher(path_a)
            self.assertEqual(list(lib_a2.export_profiles()), export_a)
            ref = reference.compute(events)
            self.assertEqual(list(lib_a2.export_profiles()), ref["profiles"])
            self.assertEqual(
                lib_a2.stats(),
                {"profiles": len(ref["profiles"]),
                 "unassigned_events": ref["unassigned_events"]},
            )
            # 重开后仍可继续增量操作
            victim = order_a[0]["event_id"]
            lib_a2.retract(victim)
            remaining = [e for e in events if e["event_id"] != victim]
            ref2 = reference.compute(remaining)
            self.assertEqual(list(lib_a2.export_profiles()), ref2["profiles"])
            lib_a2.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
