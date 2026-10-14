# Benchmarks

All numbers are from one run of `./bench.sh` on an Apple M2 laptop (8 cores, 24 GB), Spark 4.1.3 in
`local[8]`, JDK 21, tables on the local SSD. Timings on a laptop are noisy; the file and record
counts are the point, and they are deterministic (fixed random seeds).

## Data skipping: random vs linear sort vs Z-order

4M rows with two independent uniform columns `x`, `y` in [0, 1e6), 64 files per layout. Each cell
is files read / total files, then median query time over 5 runs (`filter(q).agg(sum(y))`).

| query | random | linear (x) | z-order (x, y) |
|---|---|---|---|
| x < 10k (1%) | 64/64 files, 98 ms | 1/64 files, 39 ms | 8/65 files, 43 ms |
| y < 10k (1%) | 64/64 files, 72 ms | 64/64 files, 69 ms | 13/65 files, 43 ms |
| x, y in [450k, 550k) (1%) | 64/64 files, 81 ms | 8/64 files, 60 ms | 9/65 files, 41 ms |
| x = 123456 | 64/64 files, 80 ms | 1/64 files, 35 ms | 12/65 files, 50 ms |
| x < 100k or y < 100k (19%) | 64/64 files, 85 ms | 64/64 files, 100 ms | 23/65 files, 50 ms |

`OPTIMIZE ZORDER BY (x, y)` rewrote 64 files into 65 in 5.4 s.

- **Random layout skips nothing**: every file spans the whole range of both columns, so min/max
  stats can't rule any file out.
- **Sorting by x is perfect for x and useless for y.** A 1% range or point lookup on x reads 1 file;
  the same query on y reads all 64.
- **Z-order trades some x precision for y.** With 64 files over two columns the best possible is an
  8 x 8 grid, so a 1% slice of either column should touch about 8 files. Z-order gets 8 for x and 13
  for y; y is worse because the sampled range boundaries don't fall exactly on grid lines. The OR
  query, which no single-column sort can help, drops from 64 files to 23.

## Compaction

200 appends of 20k rows each (one file per append), then `OPTIMIZE` with the default 128 MB target.

|  | files | group-by query (median ms) | load latest snapshot (median ms) |
|---|---|---|---|
| before | 200 | 142 | 0.9 |
| after | 1 | 94 | 0.3 |

Same rows, 34% faster query: 200 files means 200 file opens and footer reads. Snapshot loading is
cheap either way because a checkpoint every 10 commits bounds how much log gets replayed.

## Skewed join: baseline vs AQE skew join vs salting

8M fact rows where 50% share key 0, joined to a 1M-key dimension, then grouped. Broadcast joins
are off so every variant shuffles; 64 shuffle partitions. The AQE skew threshold is lowered from
256 MB to 4 MB, since a laptop-sized shuffle never reaches the default. "Straggler" is the slowest
join task's time over the median task's time.

| variant | wall ms | join tasks | records read max / median | task ms max / median | straggler |
|---|---|---|---|---|---|
| baseline (AQE off) | 1312 | 64 | 4,075,349 / 78,120 | 586 / 32 | 18.3x |
| AQE skew join | 838 | 17 | 1,032,044 / 391,206 | 218 / 141 | 1.5x |
| salting x32 (AQE off) | 2781 | 64 | 938,306 / 563,203 | 327 / 166 | 2.0x |

- **Baseline**: one reduce task reads 4.1M records, 52 times the median, and the stage waits for it.
- **AQE** splits the hot partition at runtime and also coalesces the 64 small partitions into 17
  tasks: 36% faster, straggler gone.
- **Salting** balances tasks almost as well, but it's the slowest variant overall: to cover 32
  salts the 1M-row dimension is replicated into 32M rows, and that extra shuffle costs more than
  the skew did. Salting pays off when the replicated side is small relative to the skew. Here AQE
  gets most of the benefit without copying anything.

All three variants return identical results, which the benchmark asserts.

## Commit throughput vs Raft groups

8 writers, one table each, blind appends for 5 s per row, 3-node groups, 5 ms ticks, all in one
JVM (`ShardingBenchmark`).

| groups | commits | commits/s | vs 1 group |
|---|---|---|---|
| 1 | 4388 | 878 | 1.00x |
| 2 | 6223 | 1245 | 1.42x |
| 4 | 7524 | 1505 | 1.71x |

More groups add more leaders, so commits to different tables stop queuing behind one Raft log.
The scaling is sublinear because everything shares one 8-core laptop: 4 groups means 12 node
threads plus 8 writers. Each MiniLake commit is also several Raft operations: listing the log,
reading new commits, then the put-if-absent. In a real deployment each group would run on its own
machines.

## Reproduce

```
./bench.sh                                            # all suites
./bench.sh io.minilake.spark.bench.SkewJoinBenchmark  # one suite
```

Results land in `minilake-spark/target/benchmarks/`.
