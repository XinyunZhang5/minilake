# MiniLake

A from-scratch, Delta-Lake-style ACID table format: a Java core that turns a directory of Parquet
files into a transactional table, and a Scala connector so Spark can read and write it.

```scala
df.write.format("minilake").partitionBy("kind").save("/tmp/people")
df2.write.format("minilake").mode("append").save("/tmp/people")

spark.read.format("minilake").load("/tmp/people").filter($"id" < 50)   // skips files by min/max
spark.read.format("minilake").option("versionAsOf", 0).load("/tmp/people")  // time travel
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

## Build

Requires JDK 21 and Maven.

```
export JAVA_HOME=/opt/homebrew/opt/openjdk@21
mvn test
```

Roadmap: [docs/PLAN.md](docs/PLAN.md). Next: OPTIMIZE/Z-order and skipping benchmarks, a Spark skew
experiment, then a Raft-replicated commit coordinator.
