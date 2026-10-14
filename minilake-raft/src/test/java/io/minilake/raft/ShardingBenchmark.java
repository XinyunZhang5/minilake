package io.minilake.raft;

import io.minilake.MiniLakeTable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Commit throughput as tables spread over more Raft groups: 8 writer threads, each appending to
 * its own table, against 1, 2 and 4 groups of 3 nodes. One group serializes every table's
 * commits through one leader thread; more groups add leaders.
 */
@Tag("benchmark")
class ShardingBenchmark {
  static final int WRITERS = 8;
  static final long SECONDS = 5;

  @Test
  void commitThroughputByGroupCount() throws Exception {
    StringBuilder md = new StringBuilder("### Commit throughput vs Raft groups\n\n")
        .append(WRITERS).append(" writers, one table each, blind appends for ").append(SECONDS)
        .append(" s per row, 3-node groups, 5 ms ticks, all in one JVM.\n\n")
        .append("| groups | commits | commits/s | vs 1 group |\n|---|---|---|---|\n");
    double base = 0;
    for (int groups : new int[] {1, 2, 4}) {
      List<LocalCluster> gs = ShardedLogStoreTest.groups(groups);
      try {
        List<MiniLakeTable> tables = new ArrayList<>();
        for (int w = 0; w < WRITERS; w++) {
          MiniLakeTable t = MiniLakeTable.forPath("/bench/" + groups + "/t" + w, ShardedLogStoreTest.client(gs, 30_000));
          ShardedLogStoreTest.create(t);
          tables.add(t);
        }
        AtomicBoolean stop = new AtomicBoolean();
        AtomicLong commits = new AtomicLong();
        List<Thread> threads = new ArrayList<>();
        for (MiniLakeTable t : tables) {
          threads.add(new Thread(() -> {
            while (!stop.get()) {
              ShardedLogStoreTest.append(t);
              commits.incrementAndGet();
            }
          }));
        }
        threads.forEach(Thread::start);
        Thread.sleep(SECONDS * 1000);
        stop.set(true);
        for (Thread th : threads) th.join();
        double rate = commits.get() / (double) SECONDS;
        if (groups == 1) base = rate;
        md.append(String.format("| %d | %d | %.0f | %.2fx |%n", groups, commits.get(), rate, rate / base));
      } finally {
        gs.forEach(LocalCluster::close);
      }
    }
    System.out.println(md);
    Path out = Path.of("target", "benchmarks", "sharding.md");
    Files.createDirectories(out.getParent());
    Files.writeString(out, md + "\n", StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
  }
}
