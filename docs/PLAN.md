# MiniLake 实施计划

一个从零实现的 Delta-Lake 风格 ACID 表格式：Java 核心 + Scala Spark connector。

## 模块

| 模块 | 语言 | 职责 |
|---|---|---|
| `minilake-core` | Java 21 | 事务日志、快照重放、checkpoint、OCC 冲突检测、data skipping 谓词 |
| `minilake-spark` | Scala 2.13 / Spark 4.1 | `format("minilake")` 读写；FileIndex 做分区裁剪 + 文件跳过；Parquet footer 采集 min/max |
| `minilake-raft` | Java 21 | Raft 复制的提交协调器，替代"存储层 put-if-absent"；确定性模拟测试 |

## 表在磁盘上的样子

```
table/
  _log/
    00000000000000000000.json        # 每行一个 action：protocol / metaData / add / remove / txn / commitInfo
    00000000000000000001.json
    00000000000000000010.checkpoint.json   # 重放后的完整状态（每 10 个版本）
  _data/<batch-uuid>/part=a/part-*.parquet   # 每次写入一个批次目录；只有进了 log 的文件才可见
```

提交 = 原子地创建 `_log/<version>.json`，已存在则失败。本地实现：先写临时文件，再 `Files.createLink`（POSIX link 不覆盖，读者永远看不到半个文件）。

## 冲突检测规则（WriteSerializable）

赢家提交 = `readVersion+1 .. latest` 之间别人写进去的版本。对每个赢家：

| 赢家做了什么 | 我做了什么 | 结果 |
|---|---|---|
| 改了 metadata / protocol | 任何 | 失败 |
| 新增了文件，命中我的读谓词 | 读过表（非盲追加） | `ConcurrentAppendException` |
| 删了我读过的文件 | 读过 | `ConcurrentDeleteReadException` |
| 删了我也要删的文件 | 删除 | `ConcurrentDeleteDeleteException` |
| 写了同一个 appId 的 txn | 写 txn | `ConcurrentTransactionException` |
| 其它 | | 无冲突，改到下一个版本号重试 |

## 阶段

- [x] **P1 核心层 MVP**：actions JSON 编解码、LocalLogStore、Snapshot 重放、time travel（按版本/时间戳）、checkpoint、OptimisticTransaction + 冲突检测、并发 writer 测试
- [x] **P1 Spark 读写**：`df.write.format("minilake")` append/overwrite + partitionBy；`spark.read.format("minilake")` + `versionAsOf`；端到端测试
- [x] **P2 索引**：Parquet footer → min/max/nullCount stats；FileIndex 用 dataFilters 跳文件；`OPTIMIZE` 合并小文件（dataChange=false，只按精确文件记读集，不和并发追加冲突）；Z-order（分位数分桶 + 位交错 + range 分区）；benchmark：跳过文件比例（见 BENCHMARKS.md）
- [x] **P2 Spark 实验**：倾斜 join → baseline / AQE skew join / salting 对比，用 SparkListener 采每个 task 的耗时和 shuffle 读记录数（代替 Spark UI 截图）
- [x] **P2 顺手修的 bug**：并发提交时 readdir 可能漏掉刚创建的文件，导致"log gap"误报；改为按版本号直接读
- [x] **P3 Raft 协调器**：3/5 节点，leader 选举 + 日志复制 + 快照/InstallSnapshot；客户端会话去重；`CoordinatedLogStore`；确定性模拟（丢包/重复/乱序/崩溃/分区）+ 每步安全性检查 + WGL 线性一致性检查；变异测试（Figure 8、去重）；真线程 kill leader 集成测试（见 RAFT.md）
- [x] **P4**：Structured Streaming sink，每个微批和 `txn(queryId, batchId)` 同一次原子提交，重放的批次跳过（Kafka 容器端到端测试）；多表按 CRC32 分片到多个 Raft group（故障隔离 + 吞吐基准）；S3 LogStore 用条件写入 `If-None-Match: *`（LocalStack 测试；MinIO 社区镜像已下架所以没用）

## 构建

```
export JAVA_HOME=/opt/homebrew/opt/openjdk@21
mvn -q test
```
