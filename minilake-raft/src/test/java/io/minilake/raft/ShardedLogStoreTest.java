package io.minilake.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minilake.MiniLakeTable;
import io.minilake.OptimisticTransaction;
import io.minilake.actions.AddFile;
import io.minilake.actions.Metadata;
import io.minilake.schema.DataType;
import io.minilake.schema.Field;
import io.minilake.schema.Schema;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class ShardedLogStoreTest {
  static final RaftConfig CONFIG = RaftConfig.defaults().withSnapshotEvery(30);
  static final Schema SCHEMA = Schema.of(new Field("id", DataType.LONG, false));

  static List<LocalCluster> groups(int n) {
    return IntStream.range(0, n).mapToObj(i -> new LocalCluster(3, CONFIG, 5)).toList();
  }

  static ShardedLogStore client(List<LocalCluster> groups, long deadlineMs) {
    return new ShardedLogStore(groups.stream().map(g -> g.newClient(deadlineMs)).toList());
  }

  /** First table name (t0, t1, ...) that hashes to {@code group}. */
  static String tableIn(int group, int groupCount) {
    for (int i = 0; ; i++) if (ShardedLogStore.groupOf("/tables/t" + i, groupCount) == group) return "/tables/t" + i;
  }

  static void create(MiniLakeTable t) {
    OptimisticTransaction txn = t.startTransaction();
    txn.updateMetadata(new Metadata(UUID.randomUUID().toString(), SCHEMA.toJson(), List.of(), Map.of(), 0L));
    txn.commit(List.of(), "CREATE");
  }

  static long append(MiniLakeTable t) {
    return t.startTransaction().commit(List.of(new AddFile(UUID.randomUUID() + ".parquet", Map.of(), 1, 0, true, null)),
        "WRITE");
  }

  @Test
  void aTablesLogStaysInOneGroup() {
    assertEquals(ShardedLogStore.groupOf("/tables/a/_log/00000000000000000000.json", 4),
        ShardedLogStore.groupOf("/tables/a/_log", 4));
    assertEquals(ShardedLogStore.groupOf("/tables/a/_log/00000000000000000007.checkpoint.json", 4),
        ShardedLogStore.groupOf("/tables/a", 4));
    long distinct = IntStream.range(0, 40).map(i -> ShardedLogStore.groupOf("/tables/t" + i, 4)).distinct().count();
    assertEquals(4, distinct, "40 tables use all 4 groups");
  }

  @Test
  void losingOneGroupStallsOnlyItsTables() throws Exception {
    List<LocalCluster> gs = groups(2);
    try {
      String a = tableIn(0, 2), b = tableIn(1, 2);
      MiniLakeTable ta = MiniLakeTable.forPath(a, client(gs, 1500));
      MiniLakeTable tb = MiniLakeTable.forPath(b, client(gs, 1500));
      create(ta);
      create(tb);
      append(ta);

      gs.get(0).kill(0);
      gs.get(0).kill(1); // group 0 has lost its majority

      for (int i = 0; i < 20; i++) append(tb);
      assertEquals(20, tb.latest().version(), "group 1 keeps committing");
      assertThrows(UncheckedIOException.class, () -> append(ta), "group 0 cannot decide anything");

      gs.get(0).restart(0);
      gs.get(0).restart(1);
      assertEquals(2, append(ta), "and recovers with its log intact");
    } finally {
      gs.forEach(LocalCluster::close);
    }
  }

  @Test
  void writesToDifferentGroupsDoNotInterleaveLogs() throws Exception {
    List<LocalCluster> gs = groups(2);
    try {
      String a = tableIn(0, 2), b = tableIn(1, 2);
      assertNotEquals(ShardedLogStore.groupOf(a, 2), ShardedLogStore.groupOf(b, 2));
      List<Thread> ts = new ArrayList<>();
      for (String path : List.of(a, b)) {
        MiniLakeTable t = MiniLakeTable.forPath(path, client(gs, 10_000));
        create(t);
        ts.add(new Thread(() -> { for (int i = 0; i < 50; i++) append(t); }));
      }
      ts.forEach(Thread::start);
      for (Thread t : ts) t.join();
      for (String path : List.of(a, b)) {
        assertEquals(50, MiniLakeTable.forPath(path, client(gs, 10_000)).latest().version());
      }
      // Group 1 holds nothing of table a.
      assertTrue(gs.get(1).newClient().list(a + "/_log").isEmpty());
    } finally {
      gs.forEach(LocalCluster::close);
    }
  }
}
