# 零售客户身份拼接库

纯 Python 3 标准库实现（持久化仅用 `sqlite3`），把 web / mobile / kiosk / inbox
四个渠道的行为事件增量归并到统一客户画像。

## 快速使用

```python
from identity_stitch import IdentityStitcher, SharedDevice

db = IdentityStitcher("/tmp/identity.db")

db.ingest({
    "event_id": "e1",
    "channel": "web",                 # web | mobile | kiosk | inbox
    "ts": 1710000000000,              # 整数毫秒
    "kind": "purchase",               # view | purchase
    "amount_cents": 4999,             # purchase 为正整数；view 为 0
    "identifiers": ["cust:c1", "device:d1"],
})

db.retract("e1")                      # 撤回
result = db.resolve("cust:c1")        # Profile / SharedDevice / None
for profile in db.export_profiles():  # 按 key 全序流式输出
    ...
db.stats()                            # Stats(profiles=..., unattributed_events=...)
db.close()
```

## 标识符与全序

标识符写成 `类型:值`，类型只有 `cust < email < inbox < device`；
同类型按值的字节序比较。画像 key 是连通分量中全序最小的标识符。

## 语义要点

- 画像完全由当前存活（已写入且未撤回）事件集合决定，与到达顺序无关。
- 某 device 若在存活事件中与超过 3 个不同非 device 标识符共现，即为**共享设备**：
  不参与连接、不属于任何画像；去掉共享设备后无标识符剩余的事件记为未归属。
- `ingest`：同 id 同内容重复写入无操作；同 id 不同内容、或 id 已撤回 → 报错。
- `retract`：未知 id 报错；重复撤回无操作。
- 任何报错都不会改变状态。

## 增量维护设计

任何操作返回后，状态与“从当前存活事件集从零计算”完全一致；不做后台收敛、
不全表扫描，读写量只与受影响画像的规模相关。

- `edges(a,b,cnt)`：当前均非共享的标识符对在存活事件中的共现次数（多条事件
  共现同一对时计数，撤回最后一条支撑才真正断边）。
- `device_peers/device_counts/shared_devices`：设备共现去重计数与阈值跨越
  （3→4 成为共享，4→3 回落为非共享）。
- 写入纯新增时走增量合并（只更新被吸收的小画像）；撤回且连通性不变时只递减
  聚合值；只有设备共享状态变化或边真正切断连通性时，才对受影响区域重算
  连通分量与聚合。重算只触碰：本事件标识符 ∪ 共享状态变化设备的共现标识符
  所在画像，并复用 BFS 判定“断边后是否仍连通”，避免不必要的拆分重算。
- 最近事件 `ts`、渠道计数、purchase 笔数、金额合计均按存活事件重算/递减，
  撤回后不保留已撤回事件的值。

## 测试

- `tests/test_differential.py`：随机操作序列（多随机种子、数百步），每步与
  `tests/reference.py` 中的从零计算参照实现比对 `export_profiles` / `stats` /
  `resolve`；序列覆盖阈值双向跨越、同对标识符多次共现逐条撤回、重复写入、
  重复撤回、错误后状态不变等场景。
- `tests/test_scenarios.py`：上述场景的确定性测试。
- `tests/test_order_persistence.py`：同一批存活事件以不同顺序写入两个新库，
  导出逐项相同；关闭重开后状态不变。

运行：`python3 -m unittest discover -s tests`
