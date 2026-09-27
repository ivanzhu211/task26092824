"""随机操作序列差分测试：每步之后比对 export_profiles / stats / resolve。"""
import random
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from identity_stitch import IdentityStitcher
from identity_stitch.errors import StitchError
from reference import Reference

CUSTS = [f"cust:c{i}" for i in range(6)]
EMAILS = [f"email:e{i}@x.com" for i in range(6)]
INBOXES = [f"inbox:i{i}" for i in range(4)]
DEVICES = [f"device:d{i}" for i in range(4)]
ALL_IDENTS = CUSTS + EMAILS + INBOXES + DEVICES
CHANNELS = ["web", "mobile", "kiosk", "inbox"]


def make_event(rng: random.Random, seq: int) -> dict:
    n = rng.randint(1, 4)
    idents = rng.sample(ALL_IDENTS, n)
    if rng.random() < 0.6:  # 提高设备出现率，促使阈值双向跨越
        idents.append(rng.choice(DEVICES))
        idents = list(dict.fromkeys(idents))[:8]
    kind = rng.choice(["view", "purchase"])
    return {
        "event_id": f"ev{seq}",
        "channel": rng.choice(CHANNELS),
        "ts": rng.randint(1, 10_000),
        "kind": kind,
        "amount_cents": rng.randint(1, 100_000) if kind == "purchase" else 0,
        "identifiers": idents,
    }


def run_ops(seed: int, steps: int) -> None:
    rng = random.Random(seed)
    lib = IdentityStitcher(":memory:")
    ref = Reference()
    live_ids: list[str] = []
    retracted_ids: list[str] = []
    seq = 0

    def compare(step: int) -> None:
        got = list(lib.export_profiles())
        want = ref.export_profiles()
        assert got == want, f"seed={seed} step={step}\nlib={got}\nref={want}"
        assert lib.stats() == ref.stats(), f"seed={seed} step={step} stats"
        for ident in rng.sample(ALL_IDENTS, 4):
            assert lib.resolve(ident) == ref.resolve(ident), \
                f"seed={seed} step={step} resolve({ident})"

    for step in range(steps):
        roll = rng.random()
        try:
            if roll < 0.50 or not live_ids:
                ev = make_event(rng, seq)
                seq += 1
                lib.ingest(ev)
                ref.ingest(ev)
                live_ids.append(ev["event_id"])
            elif roll < 0.60 and live_ids:  # 内容相同的重复写入：无操作
                eid = rng.choice(live_ids)
                dup = dict(ref._events[eid][0].__dict__)
                lib.ingest(dup)
                ref.ingest(dup)
            elif roll < 0.65 and live_ids:  # 内容不同的重复写入：双方都应报错
                eid = rng.choice(live_ids)
                bad = dict(ref._events[eid][0].__dict__)
                bad["amount_cents"] = 999 if bad["kind"] == "purchase" else 0
                bad["kind"] = "purchase"
                bad["amount_cents"] = 999
                for impl in (lib, ref):
                    with unittest.TestCase().assertRaises(StitchError):
                        impl.ingest(bad)
            elif roll < 0.85 and live_ids:  # 撤回一条存活事件
                eid = live_ids.pop(rng.randrange(len(live_ids)))
                lib.retract(eid)
                ref.retract(eid)
                retracted_ids.append(eid)
            elif roll < 0.90 and retracted_ids:  # 重复撤回：无操作
                eid = rng.choice(retracted_ids)
                lib.retract(eid)
                ref.retract(eid)
            elif roll < 0.93 and retracted_ids:  # 写已撤回的 id：双方都应报错
                eid = rng.choice(retracted_ids)
                ev = make_event(rng, seq)
                ev["event_id"] = eid
                seq += 1
                for impl in (lib, ref):
                    with unittest.TestCase().assertRaises(StitchError):
                        impl.ingest(ev)
            else:  # 撤回未知 id：双方都应报错
                for impl in (lib, ref):
                    with unittest.TestCase().assertRaises(StitchError):
                        impl.retract(f"ghost{step}")
        except AssertionError:
            raise
        compare(step)
    lib.close()


class DifferentialTest(unittest.TestCase):
    def test_random_sequences(self) -> None:
        for seed in range(8):
            with self.subTest(seed=seed):
                run_ops(seed, steps=300)


if __name__ == "__main__":
    unittest.main()
