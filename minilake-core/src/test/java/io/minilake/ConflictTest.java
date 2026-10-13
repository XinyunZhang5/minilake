package io.minilake;

import static io.minilake.expr.Predicate.eq;
import static io.minilake.expr.Predicate.gt;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.minilake.actions.AddFile;
import io.minilake.actions.SetTransaction;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Each test interleaves two transactions by hand: both read, then "winner" commits, then "loser" tries. */
class ConflictTest {
  @TempDir Path dir;
  MiniLakeTable t;
  AddFile a1, b1;

  @BeforeEach
  void setUp() {
    t = Fixtures.create(dir.toString());
    a1 = Fixtures.file("a", 0, 99);
    b1 = Fixtures.file("b", 0, 99);
    t.startTransaction().commit(List.of(a1, b1), "WRITE");
  }

  @Test
  void blindAppendsNeverConflict() {
    OptimisticTransaction x = t.startTransaction(), y = t.startTransaction();
    assertEquals(2, x.commit(List.of(Fixtures.file("a", 0, 1)), "WRITE"));
    assertEquals(3, y.commit(List.of(Fixtures.file("a", 0, 1)), "WRITE"), "retried at the next version");
    assertEquals(4, t.latest().numFiles());
  }

  @Test
  void appendOutsideMyReadIsFine() {
    OptimisticTransaction reader = t.startTransaction();
    reader.scan(eq("part", "a"));
    t.startTransaction().commit(List.of(Fixtures.file("b", 0, 1)), "WRITE");
    assertEquals(3, reader.commit(List.of(Fixtures.file("a", 0, 1)), "WRITE"));
  }

  @Test
  void appendIntoMyReadConflicts() {
    OptimisticTransaction reader = t.startTransaction();
    reader.scan(eq("part", "a"));
    t.startTransaction().commit(List.of(Fixtures.file("a", 500, 600)), "WRITE");
    assertThrows(ConcurrentAppendException.class, () -> reader.commit(List.of(a1.remove(0, true)), "DELETE"));
  }

  @Test
  void statsNarrowTheConflictSurface() {
    OptimisticTransaction reader = t.startTransaction();
    reader.scan(gt("id", 1000L));
    t.startTransaction().commit(List.of(Fixtures.file("a", 0, 50)), "WRITE");
    assertEquals(3, reader.commit(List.of(Fixtures.file("a", 2000, 2001)), "WRITE"),
        "the concurrent file's ids cannot exceed 1000, so my read is unaffected");
  }

  @Test
  void readWholeTableConflictsWithAnyAppend() {
    OptimisticTransaction reader = t.startTransaction();
    reader.scanAll();
    t.startTransaction().commit(List.of(Fixtures.file("z", 0, 0)), "WRITE");
    assertThrows(ConcurrentAppendException.class, () -> reader.commit(List.of(), "OVERWRITE"));
  }

  @Test
  void compactionAddsDoNotLookLikeNewData() {
    OptimisticTransaction reader = t.startTransaction();
    reader.scan(eq("part", "b"));
    AddFile compacted = new AddFile("part=a/c.parquet", Map.of("part", "a"), 1, 0, false, null);
    t.startTransaction().commit(List.of(a1.remove(0, false), compacted), "OPTIMIZE");
    assertEquals(3, reader.commit(List.of(b1.remove(0, true)), "DELETE"));
  }

  @Test
  void removingAFileIReadConflicts() {
    OptimisticTransaction reader = t.startTransaction();
    reader.scan(eq("part", "a"));
    t.startTransaction().commit(List.of(a1.remove(0, true)), "DELETE");
    assertThrows(ConcurrentDeleteReadException.class, () -> reader.commit(List.of(), "UPDATE"));
  }

  @Test
  void doubleDeleteConflicts() {
    OptimisticTransaction x = t.startTransaction(), y = t.startTransaction();
    x.commit(List.of(b1.remove(0, true)), "DELETE");
    assertThrows(ConcurrentDeleteDeleteException.class, () -> y.commit(List.of(b1.remove(0, true)), "DELETE"));
  }

  @Test
  void metadataChangeConflictsWithEveryone() {
    OptimisticTransaction appender = t.startTransaction();
    OptimisticTransaction alter = t.startTransaction();
    alter.updateMetadata(Fixtures.metadata(Map.of("k", "v")));
    alter.commit(List.of(), "SET TBLPROPERTIES");
    assertThrows(MetadataChangedException.class,
        () -> appender.commit(List.of(Fixtures.file("a", 0, 1)), "WRITE"));
  }

  @Test
  void concurrentCreatesHaveOneWinner(@TempDir Path other) {
    MiniLakeTable fresh = MiniLakeTable.forPath(other.toString());
    OptimisticTransaction x = fresh.startTransaction(), y = fresh.startTransaction();
    x.updateMetadata(Fixtures.metadata(Map.of()));
    y.updateMetadata(Fixtures.metadata(Map.of()));
    assertEquals(0, x.commit(List.of(), "CREATE TABLE"));
    assertThrows(CommitConflictException.class, () -> y.commit(List.of(), "CREATE TABLE"));
  }

  @Test
  void sameStreamingBatchCommitsOnce() {
    OptimisticTransaction x = t.startTransaction(), y = t.startTransaction();
    for (OptimisticTransaction txn : List.of(x, y)) assertEquals(java.util.Optional.empty(), txn.txnVersion("stream-1"));
    x.commit(List.of(Fixtures.file("a", 0, 1), new SetTransaction("stream-1", 0, 0)), "STREAMING UPDATE");
    assertThrows(ConcurrentTransactionException.class,
        () -> y.commit(List.of(Fixtures.file("a", 0, 1), new SetTransaction("stream-1", 0, 0)), "STREAMING UPDATE"));
    assertEquals(0L, t.latest().txnVersion("stream-1").orElseThrow());
  }

  @Test
  void conflictLooksAtEveryWinnerNotJustTheLatest() {
    OptimisticTransaction reader = t.startTransaction();
    reader.scan(eq("part", "a"));
    t.startTransaction().commit(List.of(Fixtures.file("a", 0, 1)), "WRITE"); // conflicting, v2
    t.startTransaction().commit(List.of(Fixtures.file("b", 0, 1)), "WRITE"); // harmless, v3
    ConcurrentAppendException e = assertThrows(ConcurrentAppendException.class,
        () -> reader.commit(List.of(), "UPDATE"));
    assertEquals(2, e.winningVersion());
  }
}
