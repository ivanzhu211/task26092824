"""顺序无关性与持久化测试：不同写入顺序导出逐项相同；关闭重开后状态不变。"""
import random
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from identity_stitch import IdentityStitcher
from test_differential import make_event


class OrderPersistenceTest(unittest.TestCase):
    def test_order_independence_and_reopen(self) -> None:
        rng = random.Random(42)
        events = [make_event(rng, i) for i in range(120)]
        retract_ids = {e["event_id"] for e in rng.sample(events, 30)}

        tmp = Path(tempfile.mkdtemp())
        paths = [tmp / "a.db", tmp / "b.db"]
        orders = [events.copy(), events.copy()]
        rng.shuffle(orders[0])
        rng.shuffle(orders[1])

        exports = []
        for path, order in zip(paths, orders):
            lib = IdentityStitcher(str(path))
            for e in order:
                lib.ingest(e)
            for eid in sorted(retract_ids, reverse=path.name == "a.db"):
                lib.retract(eid)
            exports.append(list(lib.export_profiles()))
            stats_before = lib.stats()
            lib.close()
            # 重新打开后状态不变
            lib2 = IdentityStitcher(str(path))
            self.assertEqual(list(lib2.export_profiles()), exports[-1])
            self.assertEqual(lib2.stats(), stats_before)
            lib2.close()

        self.assertEqual(exports[0], exports[1])
        self.assertGreater(len(exports[0]), 0)


if __name__ == "__main__":
    unittest.main()
