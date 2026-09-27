"""确定性场景测试：阈值双向跨越、多重共现逐条撤回、重复写入与撤回、未归属事件。"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from identity_stitch import IdentityStitcher, SharedDevice
from identity_stitch.errors import (
    DuplicateEventError,
    RetractedEventError,
    UnknownEventError,
)
from reference import Reference


def ev(eid, idents, ts=1, kind="view", amount=0, channel="web"):
    return {"event_id": eid, "channel": channel, "ts": ts, "kind": kind,
            "amount_cents": amount, "identifiers": idents}


class ScenarioTest(unittest.TestCase):
    def setUp(self) -> None:
        self.lib = IdentityStitcher(":memory:")
        self.ref = Reference()

    def tearDown(self) -> None:
        self.lib.close()

    def apply(self, op, *args):
        getattr(self.lib, op)(*args)
        getattr(self.ref, op)(*args)
        self.assertEqual(list(self.lib.export_profiles()), self.ref.export_profiles())
        self.assertEqual(self.lib.stats(), self.ref.stats())

    def test_device_threshold_both_directions(self):
        # d1 依次与 4 个不同 cust 共现，第 4 条时越过阈值成为共享设备
        for i in range(1, 5):
            self.apply("ingest", ev(f"e{i}", ["device:d1", f"cust:c{i}"], ts=i))
        self.assertEqual(self.lib.resolve("device:d1"), SharedDevice("device:d1"))
        self.assertEqual(self.lib.stats().profiles, 4)  # 4 个互不相连的 cust 画像
        # 逐条撤回，回落到 3 个共现标识符时恢复非共享，画像重新合并
        self.apply("retract", "e4")
        self.assertEqual(self.lib.stats().profiles, 1)
        p = self.lib.resolve("cust:c1")
        self.assertEqual(p.key, "cust:c1")
        self.assertEqual(set(p.members),
                         {"cust:c1", "cust:c2", "cust:c3", "device:d1"})

    def test_shared_device_makes_event_unattributed(self):
        for i in range(1, 5):
            self.apply("ingest", ev(f"e{i}", ["device:d1", f"cust:c{i}"], ts=i))
        self.apply("ingest", ev("solo", ["device:d1"], ts=9))
        self.assertEqual(self.lib.stats().unattributed_events, 1)
        # 撤回使 d1 回落为非共享后，solo 重新归属
        self.apply("retract", "e4")
        self.assertEqual(self.lib.stats().unattributed_events, 0)
        self.assertIn("device:d1", self.lib.resolve("cust:c1").members)

    def test_multi_cooccurrence_then_retract_one_by_one(self):
        for i in range(3):
            self.apply("ingest", ev(f"e{i}", ["cust:c1", "email:a@x"], ts=i + 1))
        self.apply("retract", "e0")
        self.assertEqual(self.lib.stats().profiles, 1)  # 仍有 e1、e2 支撑
        self.apply("retract", "e1")
        self.assertEqual(self.lib.stats().profiles, 1)  # 仍有 e2 支撑
        self.apply("retract", "e2")
        self.assertEqual(self.lib.stats().profiles, 0)  # 全部撤回，画像消失

    def test_duplicate_ingest_and_retract(self):
        e = ev("e1", ["cust:c1"], ts=1, kind="purchase", amount=100)
        self.apply("ingest", e)
        self.apply("ingest", dict(e))  # 内容相同：无操作
        self.assertEqual(self.lib.stats().profiles, 1)
        with self.assertRaises(DuplicateEventError):
            self.lib.ingest(ev("e1", ["cust:c1"], ts=2))  # 内容不同：报错
        with self.assertRaises(DuplicateEventError):
            self.ref.ingest(ev("e1", ["cust:c1"], ts=2))
        self.apply("retract", "e1")
        self.apply("retract", "e1")  # 重复撤回：无操作
        with self.assertRaises(RetractedEventError):
            self.lib.ingest(dict(e))  # 已撤回的 id：报错
        with self.assertRaises(UnknownEventError):
            self.lib.retract("ghost")

    def test_error_leaves_state_unchanged(self):
        self.apply("ingest", ev("e1", ["cust:c1", "email:a@x"], ts=1))
        before = list(self.lib.export_profiles())
        for bad_op in (
            lambda: self.lib.ingest(ev("e1", ["cust:c2"])),
            lambda: self.lib.retract("ghost"),
            lambda: self.lib.ingest(ev("e2", [], ts=1)),
            lambda: self.lib.ingest(ev("e3", ["cust:c1"], kind="view", amount=5)),
        ):
            with self.assertRaises(Exception):
                bad_op()
        self.assertEqual(list(self.lib.export_profiles()), before)
        self.assertEqual(self.lib.stats(), self.ref.stats())

    def test_last_ts_exact_after_retract(self):
        self.apply("ingest", ev("e1", ["cust:c1"], ts=100))
        self.apply("ingest", ev("e2", ["cust:c1"], ts=500))
        self.apply("ingest", ev("e3", ["cust:c1"], ts=300))
        self.apply("retract", "e2")  # 撤回最大值
        self.assertEqual(self.lib.resolve("cust:c1").last_ts, 300)
        self.apply("retract", "e3")
        self.assertEqual(self.lib.resolve("cust:c1").last_ts, 100)


if __name__ == "__main__":
    unittest.main()
