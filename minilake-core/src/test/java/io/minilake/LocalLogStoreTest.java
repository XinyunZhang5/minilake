package io.minilake;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.minilake.storage.LocalLogStore;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LocalLogStoreTest {
  @TempDir Path dir;
  final LocalLogStore store = new LocalLogStore();

  @Test
  void secondWriteToSamePathFailsAndKeepsFirst() throws Exception {
    String p = dir.resolve("a/0.json").toString();
    store.writeIfAbsent(p, new byte[] {1});
    assertThrows(FileAlreadyExistsException.class, () -> store.writeIfAbsent(p, new byte[] {2}));
    assertArrayEquals(new byte[] {1}, store.read(p));
    assertEquals(List.of("0.json"), store.list(dir.resolve("a").toString()), "temp files must be cleaned up");
  }

  @Test
  void exactlyOneOfManyRacingWritersWins() throws Exception {
    String p = dir.resolve("race.json").toString();
    int n = 32;
    ExecutorService pool = Executors.newFixedThreadPool(n);
    CountDownLatch go = new CountDownLatch(1);
    List<Future<Boolean>> results = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      byte id = (byte) i;
      results.add(pool.submit(() -> {
        go.await();
        try {
          store.writeIfAbsent(p, new byte[] {id});
          return true;
        } catch (FileAlreadyExistsException e) {
          return false;
        }
      }));
    }
    go.countDown();
    int winners = 0;
    for (Future<Boolean> f : results) if (f.get()) winners++;
    pool.shutdown();
    assertEquals(1, winners);
  }

  @Test
  void listingMissingDirectoryIsEmpty() throws Exception {
    assertEquals(List.of(), store.list(dir.resolve("nope").toString()));
  }
}
