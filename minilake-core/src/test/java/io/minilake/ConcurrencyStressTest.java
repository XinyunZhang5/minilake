package io.minilake;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.minilake.actions.AddFile;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Real threads, real file system races. */
class ConcurrencyStressTest {
  static final int THREADS = 8, OPS = 25;
  @TempDir Path dir;

  @Test
  void concurrentBlindAppendsAllLandExactlyOnce() throws Exception {
    MiniLakeTable t = Fixtures.create(dir.toString());
    List<String> written = java.util.Collections.synchronizedList(new ArrayList<>());
    runConcurrently(() -> {
      for (int i = 0; i < OPS; i++) {
        AddFile f = Fixtures.file("p" + (i % 3), i, i);
        t.startTransaction().commit(List.of(f), "WRITE");
        written.add(f.path());
      }
      return null;
    });
    Snapshot s = t.latest();
    assertEquals(THREADS * OPS, s.version(), "versions are contiguous: one commit file per append");
    assertEquals(new HashSet<>(written), TableTest.paths(s));
  }

  /**
   * A counter stored as "the single file's id": each increment reads the whole table, removes the
   * current file and adds one with id+1. Any lost update would leave the final value below the
   * number of successful increments, so this checks the commits are serializable.
   */
  @Test
  void readModifyWriteCounterLosesNoUpdates() throws Exception {
    MiniLakeTable t = Fixtures.create(dir.toString());
    t.startTransaction().commit(List.of(Fixtures.file("c", 0, 0)), "INIT");
    AtomicInteger conflicts = new AtomicInteger();
    runConcurrently(() -> {
      for (int done = 0; done < OPS; ) {
        OptimisticTransaction txn = t.startTransaction();
        AddFile current = txn.scanAll().get(0);
        long value = io.minilake.expr.FileStats.parse(current.stats(), Fixtures.SCHEMA).numRecords() - 1;
        AddFile next = Fixtures.file("c", 0, value + 1);
        try {
          txn.commit(List.of(current.remove(0, true), next), "INCREMENT");
          done++;
        } catch (CommitConflictException e) {
          conflicts.incrementAndGet();
        }
      }
      return null;
    });
    Snapshot s = t.latest();
    assertEquals(1, s.numFiles());
    long finalValue = io.minilake.expr.FileStats.parse(s.allFiles().iterator().next().stats(), Fixtures.SCHEMA)
        .numRecords() - 1;
    assertEquals(THREADS * OPS, finalValue);
    System.out.printf("counter: %d increments, %d conflicts retried%n", finalValue, conflicts.get());
  }

  private static void runConcurrently(Callable<Void> body) throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(THREADS);
    CountDownLatch go = new CountDownLatch(1);
    List<Future<Void>> fs = new ArrayList<>();
    for (int i = 0; i < THREADS; i++) fs.add(pool.submit(() -> { go.await(); return body.call(); }));
    go.countDown();
    for (Future<Void> f : fs) f.get();
    pool.shutdown();
  }

}
