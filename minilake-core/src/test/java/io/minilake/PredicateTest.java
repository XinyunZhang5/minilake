package io.minilake;

import static io.minilake.expr.Predicate.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minilake.actions.AddFile;
import io.minilake.expr.ColumnStats;
import io.minilake.expr.FileView;
import io.minilake.expr.Predicate;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PredicateTest {
  /** id in [10, 20], 100 rows, 5 of them null. */
  static final FileView FILE = c -> c.equals("id") ? new ColumnStats(10L, 20L, 5L, 100L) : ColumnStats.UNKNOWN;
  static final FileView ALL_NULL = c -> new ColumnStats(null, null, 100L, 100L);

  @Test
  void rangeComparisons() {
    assertTrue(eq("id", 10L).mightMatch(FILE));
    assertTrue(eq("id", 20L).mightMatch(FILE));
    assertFalse(eq("id", 21L).mightMatch(FILE));
    assertFalse(lt("id", 10L).mightMatch(FILE));
    assertTrue(le("id", 10L).mightMatch(FILE));
    assertFalse(gt("id", 20L).mightMatch(FILE));
    assertTrue(ge("id", 20L).mightMatch(FILE));
    assertTrue(neq("id", 15L).mightMatch(FILE));
  }

  @Test
  void neqSkipsOnlyConstantFiles() {
    FileView constant = c -> new ColumnStats(7L, 7L, 0L, 3L);
    assertFalse(neq("id", 7L).mightMatch(constant));
    assertTrue(neq("id", 8L).mightMatch(constant));
  }

  @Test
  void unknownStatsNeverSkip() {
    for (Predicate p : List.of(eq("x", 1L), lt("x", 1L), gt("x", 1L), isNull("x"), isNotNull("x"))) {
      assertTrue(p.mightMatch(c -> ColumnStats.UNKNOWN), p.toString());
    }
  }

  @Test
  void nulls() {
    assertTrue(isNull("id").mightMatch(FILE));
    assertTrue(isNotNull("id").mightMatch(FILE));
    assertFalse(isNotNull("id").mightMatch(ALL_NULL));
    assertFalse(eq("id", 1L).mightMatch(ALL_NULL), "comparisons on an all-null column are never true");
    assertFalse(isNull("id").mightMatch(c -> new ColumnStats(1L, 2L, 0L, 10L)));
  }

  @Test
  void booleanCombinatorsAndNegationPushdown() {
    assertFalse(and(gt("id", 5L), lt("id", 9L)).mightMatch(FILE));
    assertTrue(or(eq("id", 1L), eq("id", 15L)).mightMatch(FILE));
    assertFalse(in("id", 1L, 2L, 30L).mightMatch(FILE));
    // NOT (id >= 10) == id < 10: skippable, although NOT(mightMatch(id >= 10)) would be false too.
    assertFalse(ge("id", 10L).negate().mightMatch(FILE));
    // NOT (id < 15 AND id > 12) == id >= 15 OR id <= 12: must not skip.
    assertTrue(and(lt("id", 15L), gt("id", 12L)).negate().mightMatch(FILE));
  }

  @Test
  void snapshotSkipsByPartitionAndStats(@TempDir Path dir) {
    MiniLakeTable t = Fixtures.create(dir.toString());
    AddFile a1 = Fixtures.file("a", 0, 99), a2 = Fixtures.file("a", 100, 199), b1 = Fixtures.file("b", 0, 99);
    t.startTransaction().commit(List.of(a1, a2, b1), "WRITE");
    Snapshot s = t.latest();
    assertEquals(List.of(a1, a2), s.files(eq("part", "a")));
    assertEquals(List.of(a2), s.files(and(eq("part", "a"), ge("id", 150))), "int literal is normalized to long");
    assertEquals(List.of(a1, b1), s.files(lt("id", 50L)));
    assertEquals(List.of(), s.files(isNull("part")));
    assertEquals(3, s.files(Predicate.TRUE).size());
    assertEquals(List.of(b1), s.files(eq("part", "a").negate()));
  }

  @Test
  void nullPartitionValue(@TempDir Path dir) {
    MiniLakeTable t = Fixtures.create(dir.toString());
    java.util.HashMap<String, String> pv = new java.util.HashMap<>();
    pv.put("part", null);
    AddFile nullPart = new AddFile("part=__NULL__/x.parquet", pv, 1, 0, true, null);
    t.startTransaction().commit(List.of(nullPart, Fixtures.file("a", 0, 1)), "WRITE");
    Snapshot s = t.latest();
    assertEquals(List.of(nullPart), s.files(isNull("part")));
    assertEquals(1, s.files(isNotNull("part")).size());
    assertEquals(1, s.files(neq("part", "zzz")).size(), "NULL <> 'zzz' is not true");
  }

  @Test
  void bindRejectsUnknownColumns() {
    org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
        () -> eq("nope", 1).bind(Fixtures.SCHEMA));
  }
}
