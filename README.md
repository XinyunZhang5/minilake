# MiniLake

A from-scratch, Delta-Lake-style ACID table format: a Java core that turns a directory of Parquet
files into a transactional table, a Scala connector so Spark can read and write it, and a
Raft-replicated commit coordinator tested by deterministic simulation.

```scala
df.write.format("minilake").partitionBy("kind").save("/tmp/people")
df2.write.format("minilake").mode("append").save("/tmp/people")
kafkaStream.writeStream.format("minilake").option("checkpointLocation", ckpt).start("/tmp/events")

spark.read.format("minilake").load("/tmp/people").filter($"id" < 50)   // skips files by min/max
spark.read.format("minilake").option("versionAsOf", 0).load("/tmp/people")  // time travel

Optimize.run(spark, "/tmp/people")                          // compact small files
Optimize.run(spark, "/tmp/points", zOrderBy = Seq("x", "y"))  // cluster for multi-column skipping
```

## How it works

**The log is the table.** Every commit is one file, `_log/<version>.json`, holding the actions it
applied (`add` / `remove` a data file, `metaData`, `txn`, `commitInfo`). A snapshot at version N is
the result of replaying commits 0..N; every 10 commits a checkpoint stores the replayed state so a
reader replays at most 9 files. Data files not referenced by the log, such as those from crashed
writers, are invisible.

**Commit = atomic put-if-absent.** Writers race to create `_log/N.json`, and exactly one wins. Locally
that's a temp file plus `link(2)`, which fails instead of overwriting, and readers never see a half
written commit.

**Optimistic concurrency.** A transaction records *what it read* (predicates, files, app ids). When
it loses the race for version N, it reads every winning commit and fails only if one of them changed
something it depended on. Otherwise it retries at the next version:

| winner did | I did | result |
|---|---|---|
| changed metadata / protocol | anything | conflict |
| added files matching my read predicate | read | `ConcurrentAppendException` |
| removed a file I read | read | `ConcurrentDeleteReadException` |
| removed a file I also remove | delete | `ConcurrentDeleteDeleteException` |
| committed the same streaming app id | streaming write | `ConcurrentTransactionException` |
| anything else | | retry at next version |

So concurrent blind appends all succeed. Conflict checks reuse the data-skipping predicate
evaluation: a concurrent file only conflicts if its stats say it *could* contain rows I read.

**Data skipping.** On write, per-file `min`/`max`/`nullCount` are taken from the Parquet footer (one
footer read per file, no data scan). On read, Spark's partition and data filters are translated
into predicates that are evaluated conservatively against those stats. Partition values are just
exact stats (min = max). `NOT` is pushed down to the leaves, and under `NOT` translation is strict,
because dropping an untranslatable conjunct would make the file set too narrow.

**OPTIMIZE and Z-order.** OPTIMIZE rewrites files without changing what the table contains, so
every add and remove in its commit is `dataChange = false`, and it records only its exact input
files as read, not a predicate. Concurrent appends therefore never conflict with it, while a
concurrent delete of one of its inputs does, because otherwise the rewrite would resurrect deleted
rows. Without Z-order it bin-packs small files per partition. With `zOrderBy`, each column is mapped
to rank buckets via approximate quantiles, so skewed or wide-ranged columns get equal resolution.
The bucket bits are then interleaved into a Morton key, and the data is range-partitioned and
sorted by that key. Each output file then covers a small box in every Z-order column, and
min/max skipping works on all of them, not just the first sort column.

**Exactly-once streaming.** MiniLake is also a Structured Streaming sink. Spark delivers each
micro-batch at least once: after a crash it re-runs the last batch it didn't record as done,
even if that batch's output was already committed. So each batch commits its files together with
`txn(appId = query id, version = batchId)` in one atomic log commit, and a batch at or below the
recorded version is skipped. The test runs Kafka in a container and streams 1000 records in
batches of 100. It then deletes the checkpoint's last commit marker, which is exactly the state a
crash between "sink committed" and "Spark recorded it" leaves behind, and restarts with 500 more
records. Spark replays the batch, the sink skips it, and all 1500 records land once.

**Commits on S3 without a lock service.** Delta Lake needed DynamoDB to coordinate writers on S3
because S3 had no atomic create-if-absent. Since 2024, `PutObject` with `If-None-Match: *` is
that primitive, and `S3LogStore` (module `minilake-s3`) builds on it. A losing writer gets 412,
and a 409 means the race is still being decided, so the write is retried. Tested against
LocalStack's S3: 8 threads racing for 30 keys produce exactly one winner per key, and the
read-modify-write counter loses no updates.

**Raft commit coordinator.** `minilake-raft` can take over the put-if-absent decision from the
file system: `CoordinatedLogStore` keeps the `_log/` files in a Raft-replicated map, so commits
survive losing a minority of coordinator nodes. The Raft node is a pure state machine with no
clock and no I/O. That lets a seeded simulator drive it through message loss, duplication,
reordering, crashes, leader kills and partitions. After every event the simulator checks Raft's
safety properties, and at the end it checks the clients' history for linearizability. Details,
results and simplifications: [docs/RAFT.md](docs/RAFT.md). `ShardedLogStore` spreads tables over
several Raft groups by a hash of the table root. One table's log stays in one group, so its commits
are still totally ordered, but tables in different groups commit in parallel and fail
independently. Killing a group's majority stalls only its own tables.

**Spark integration.** Reads return a stock `HadoopFsRelation` over Parquet whose `FileIndex` comes
from a snapshot, so Spark's vectorized reader, column pruning and partition pruning all still apply.
Writes use Spark's Parquet writer into a fresh directory, then commit the produced files.

## Tested behaviour

- 8 threads × 25 concurrent blind appends: contiguous versions, every file present exactly once
- 8 threads doing read-modify-write increments of a counter stored in the table: final value equals
  the number of successful commits (no lost updates), with hundreds of conflicts retried
- Spark: range-partitioned 10-file table, `id < 50` reads 1 file; filter results identical to plain
  Spark for `NOT`/`OR`/`IN`/`BETWEEN` and partially translatable filters; null partitions; dates;
  time travel; orphan files ignored; 6 concurrent Spark appends
- OPTIMIZE: contents and time travel unchanged, commit is all `dataChange=false`, per-partition
  bin-packing, Z-order never mixes partitions; core tests interleave a compaction with concurrent
  appends (both commit) and with a concurrent delete of an input (compaction fails)
- A directory listing that misses a commit created mid-listing still builds the right snapshot
- Raft: 300 simulated seeds (10k+ elections, 6k crashes, 14k snapshot installs) with no safety or
  linearizability violation; deliberately reintroducing the Raft paper's Figure 8 bug or removing
  retry deduplication is caught; 4 MiniLake writers over a real-threaded 3-node cluster complete
  400 read-modify-write increments with no lost updates while the leader is killed every 150 ms

## Benchmarks

From [docs/BENCHMARKS.md](docs/BENCHMARKS.md), 4M rows in 64 files, 1% selective filters:

| query | random layout | sorted by x | Z-order (x, y) |
|---|---|---|---|
| `x < 10k` | 64/64 files | 1/64 | 8/65 |
| `y < 10k` | 64/64 | 64/64 | 13/65 |
| `x < 100k or y < 100k` | 64/64 | 64/64 | 23/65 |

Also there: commit throughput rising 1.71x from 1 to 4 Raft groups (limited by one laptop's 8
cores), compacting 200 small files into 1 (query 34% faster), and a skewed-join experiment where
AQE's skew join cuts the straggler from 18.3x the median task to 1.5x, while salting balances tasks
but runs 2x slower because it replicates the dimension table 32 times.

## Build

Requires JDK 21 and Maven. The Kafka and S3 tests need Docker and are skipped without it.

```
export JAVA_HOME=/opt/homebrew/opt/openjdk@21
mvn test
```

Benchmarks: `./bench.sh` (excluded from `mvn test`).

Roadmap: [docs/PLAN.md](docs/PLAN.md).
