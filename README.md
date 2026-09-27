# task26092824 — 零售客户身份拼接库

仅用 Python 3 标准库实现，持久化使用 sqlite3。把 web / mobile / kiosk / inbox
四个渠道的行为事件增量归并到统一客户画像，支持合并与拆分双向维护。

## 用法

```python
from stitch import IdentityStitcher, StitchError

with IdentityStitcher("profiles.db") as lib:
    lib.ingest({
        "event_id": "e1", "channel": "web", "ts": 1727000000000,
        "kind": "purchase", "amount_cents": 1299,
        "identifiers": ["cust:c1", "email:a@x.com", "device:d1"],
    })
    lib.retract("e1")
    profile = lib.resolve("email:a@x.com")   # 画像 dict / {"shared": True,...} / None
    for p in lib.export_profiles():          # 按 key 全序流式输出
        ...
    lib.stats()  # {"profiles": n, "unassigned_events": m}
```

错误（重复 id 内容冲突、复用已撤回 id、未知 id 撤回、非法事件）抛出
`StitchError`，报错时状态不变（单事务回滚）。

## 设计

- 标识符全序 `cust < email < inbox < device`，同类型按值字节序；SQL 侧用
  `sort_key = "rank:identifier"` 文本列支持按全序流式输出。
- `pairs` 记录存活事件支撑的共现次数（含共享设备端点）；`edges` 是其子集，
  仅两端均非共享设备时存在。同一对标识符多条事件共现时靠计数支撑，逐条撤回
  不会误切连接。
- `device_peer` + `idents.peer_cnt` 跟踪设备的不同非设备共现标识符数，越过 3
  即共享（摘除全部活跃边、拆分分量、重排含它的事件），回落到 3 即恢复
  （重建边、合并分量、重排事件）。
- 连通分量增量维护：新边做按大小合并；边删除/设备退出时只对该分量成员 BFS
  重分解；分量聚合（各渠道事件数、purchase 笔数、金额、最近 ts）随事件增删
  精确增减，`last_ts` 在撤回最大值时按分量索引重算，拆分整分量重建。
- 读写量只与受影响画像规模相关：无全表扫描、无后台任务、无定期重建。

## 测试

```
python3 -m unittest discover -s tests -v
```

- `tests/reference.py`：从存活事件集合从零计算的参照实现。
- `tests/test_stitch.py`：随机操作序列逐步比对（覆盖阈值双向跨越、同对多次
  共现逐条撤回、重复写入/撤回、各类报错）；确定性场景；同一批事件不同写入
  顺序导出一致 + 关闭重开状态不变。
