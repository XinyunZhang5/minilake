package io.minilake.raft;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minilake.CommitConflictException;
import io.minilake.MiniLakeTable;
import io.minilake.OptimisticTransaction;
import io.minilake.Snapshot;
import io.minilake.actions.AddFile;
import io.minilake.actions.Metadata;
import io.minilake.expr.FileStats;
import io.minilake.schema.DataType;
import io.minilake.schema.Field;
import io.minilake.schema.Schema;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Real threads and wall-clock timeouts: the coordinator under MiniLake, with leaders being killed. */
class CoordinatedLogStoreTest {
  static final RaftConfig CONFIG = RaftConfig.defaults().withSnapshotEvery(30);
  static final Schema SCHEMA = Schema.of(new Field("id", DataType.LONG, false));

  @Test
  void logStoreContract() throws Exception {
    try (LocalCluster cluster = new LocalCluster(3, CONFIG, 5)) {
      CoordinatedLogStore store = cluster.newClient();
      store.writeIfAbsent("t/_log/0.json", new byte[] {1});
      assertThrows(FileAlreadyExistsException.class, () -> store.writeIfAbsent("t/_log/0.json", new byte[] {2}));
      assertArrayEquals(new byte[] {1}, store.read("t/_log/0.json"));
      assertThrows(NoSuchFileException.class, () -> store.read("t/_log/1.json"));
      store.overwrite("t/_log/9.checkpoint.json", new byte[] {3});
      store.writeIfAbsent("t/_log/sub/x", new byte[] {4});
      assertEquals(List.of("0.json", "9.checkpoint.json"), store.list("t/_log"));
      assertTrue(store.exists("t/_log/0.json"));
      assertFalse(store.exists("t/other"));
    }
  }

  /** One file holds the counter as its id; an increment replaces it. Lost updates would show up. */
  private static AddFile counterFile(long value) {
    String stats = FileStats.toJson(value + 1, Map.of("id", 0L), Map.of("id", value), Map.of("id", 0L));
    return new AddFile(UUID.randomUUID() + ".parquet", Map.of(), 1, 0, true, stats);
  }

  private static long counterValue(AddFile f) {
    return FileStats.parse(f.stats(), SCHEMA).numRecords() - 1;
  }

  @Test
  void miniLakeCommitsSurviveLeaderKills() throws Exception {
    int writers = 4, increments = 100;
    try (LocalCluster cluster = new LocalCluster(3, CONFIG, 5)) {
      String path = "/tables/counter";
      MiniLakeTable setup = MiniLakeTable.forPath(path, cluster.newClient());
      OptimisticTransaction create = setup.startTransaction();
      create.updateMetadata(new Metadata(UUID.randomUUID().toString(), SCHEMA.toJson(), List.of(), Map.of(), 0L));
      create.commit(List.of(counterFile(0)), "CREATE");

      AtomicBoolean done = new AtomicBoolean();
      AtomicInteger kills = new AtomicInteger(), conflicts = new AtomicInteger();
      Thread chaos = new Thread(() -> {
        while (!done.get()) {
          try {
            Thread.sleep(150);
            var leader = cluster.leader();
            if (leader.isEmpty()) continue;
            cluster.kill(leader.get());
            kills.incrementAndGet();
            Thread.sleep(100);
            cluster.restart(leader.get());
          } catch (InterruptedException e) {
            return;
          }
        }
      });
      chaos.start();

      ExecutorService pool = Executors.newFixedThreadPool(writers);
      List<Future<?>> futures = new ArrayList<>();
      for (int w = 0; w < writers; w++) {
        MiniLakeTable table = MiniLakeTable.forPath(path, cluster.newClient());
        futures.add(pool.submit(() -> {
          for (int ok = 0; ok < increments; ) {
            OptimisticTransaction txn = table.startTransaction();
            AddFile current = txn.scanAll().get(0);
            try {
              txn.commit(List.of(current.remove(0, true), counterFile(counterValue(current) + 1)), "INCREMENT");
              ok++;
            } catch (CommitConflictException e) {
              conflicts.incrementAndGet();
            }
          }
          return null;
        }));
      }
      for (Future<?> f : futures) f.get();
      done.set(true);
      chaos.interrupt();
      chaos.join();
      pool.shutdown();

      Snapshot s = MiniLakeTable.forPath(path, cluster.newClient()).latest();
      assertEquals(1, s.numFiles());
      assertEquals(writers * increments, counterValue(s.allFiles().iterator().next()), "no lost updates");
      assertEquals(writers * increments, s.version(), "version 0 created the table, then one commit per increment");
      System.out.printf("raft-backed counter: %d increments, %d conflicts retried, %d leaders killed%n",
          writers * increments, conflicts.get(), kills.get());
      assertTrue(kills.get() >= 3, "the test should actually have killed leaders");
    }
  }
}
