package io.minilake;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minilake.actions.AddFile;
import io.minilake.actions.SetTransaction;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TableTest {
  @TempDir Path dir;

  @Test
  void emptyPathHasNoTable() {
    MiniLakeTable t = MiniLakeTable.forPath(dir.toString());
    assertEquals(-1, t.latestVersion());
    assertFalse(t.latest().exists());
  }

  @Test
  void firstCommitMustDefineTable() {
    OptimisticTransaction txn = MiniLakeTable.forPath(dir.toString()).startTransaction();
    assertThrows(IllegalStateException.class, () -> txn.commit(List.of(), "WRITE"));
  }

  @Test
  void addsAndRemovesReplay() {
    MiniLakeTable t = Fixtures.create(dir.toString());
    AddFile a = Fixtures.file("a", 0, 9), b = Fixtures.file("b", 0, 9), c = Fixtures.file("a", 10, 19);
    assertEquals(1, t.startTransaction().commit(List.of(a, b), "WRITE"));
    assertEquals(2, t.startTransaction().commit(List.of(a.remove(1, true), c), "UPDATE"));

    Snapshot s = t.latest();
    assertEquals(2, s.version());
    assertEquals(Set.of(b.path(), c.path()), paths(s));
    assertEquals("part", s.metadata().partitionColumns().get(0));
  }

  @Test
  void rejectsFilesWithWrongPartitionColumns() {
    MiniLakeTable t = Fixtures.create(dir.toString());
    AddFile bad = new AddFile("x.parquet", Map.of(), 1, 0, true, null);
    assertThrows(IllegalArgumentException.class, () -> t.startTransaction().commit(List.of(bad), "WRITE"));
  }

  @Test
  void timeTravelByVersionAndTimestamp() throws Exception {
    MiniLakeTable t = Fixtures.create(dir.toString());
    AddFile a = Fixtures.file("a", 0, 9), b = Fixtures.file("a", 10, 19);
    t.startTransaction().commit(List.of(a), "WRITE");
    t.startTransaction().commit(List.of(b), "WRITE");

    assertEquals(Set.of(), paths(t.snapshotAt(0)));
    assertEquals(Set.of(a.path()), paths(t.snapshotAt(1)));
    assertEquals(Set.of(a.path(), b.path()), paths(t.snapshotAt(2)));
    assertThrows(IllegalArgumentException.class, () -> t.snapshotAt(3));

    long ts1 = t.commitTimestamp(1), ts2 = t.commitTimestamp(2);
    assertTrue(t.commitTimestamp(0) < ts1 && ts1 < ts2, "commit timestamps strictly increase");
    assertEquals(1, t.snapshotAsOf(ts1).version());
    assertEquals(1, t.snapshotAsOf(ts2 - 1).version());
    assertEquals(2, t.snapshotAsOf(Long.MAX_VALUE).version());
    assertThrows(IllegalArgumentException.class, () -> t.snapshotAsOf(t.commitTimestamp(0) - 1));
  }

  @Test
  void checkpointsBoundReplayAndMatchFullReplay() throws Exception {
    MiniLakeTable t = Fixtures.create(dir.toString(), Map.of(MiniLakeTable.CHECKPOINT_INTERVAL, "3"));
    List<AddFile> all = new java.util.ArrayList<>();
    for (int i = 0; i < 7; i++) {
      AddFile f = Fixtures.file(i % 2 == 0 ? "a" : "b", i * 10, i * 10 + 9);
      List<io.minilake.actions.Action> actions = new java.util.ArrayList<>(List.of(f, new SetTransaction("app", i, 0)));
      if (i == 4) actions.add(all.remove(0).remove(0, true));
      t.startTransaction().commit(actions, "WRITE");
      all.add(f);
    }
    Path log = dir.resolve("_log");
    assertTrue(Files.exists(log.resolve(String.format("%020d.checkpoint.json", 3))));
    assertTrue(Files.exists(log.resolve(String.format("%020d.checkpoint.json", 6))));

    Snapshot fromCheckpoint = t.latest();
    // Deleting commits covered by the checkpoint proves the snapshot really came from it.
    for (int v = 0; v <= 6; v++) Files.delete(log.resolve(String.format("%020d.json", v)));
    Snapshot again = t.snapshotAt(7);
    assertEquals(all.stream().map(AddFile::path).collect(Collectors.toSet()), paths(fromCheckpoint));
    assertEquals(paths(fromCheckpoint), paths(again));
    assertEquals(6L, again.txnVersion("app").orElseThrow());
    assertEquals(fromCheckpoint.metadata(), again.metadata());
  }

  @Test
  void detectsLogGaps() throws Exception {
    MiniLakeTable t = Fixtures.create(dir.toString());
    t.startTransaction().commit(List.of(Fixtures.file("a", 0, 1)), "WRITE");
    t.startTransaction().commit(List.of(Fixtures.file("a", 0, 1)), "WRITE");
    Files.delete(dir.resolve("_log").resolve(String.format("%020d.json", 1)));
    assertThrows(IllegalStateException.class, t::latest);
  }

  static Set<String> paths(Snapshot s) {
    return s.allFiles().stream().map(AddFile::path).collect(Collectors.toCollection(HashSet::new));
  }
}
