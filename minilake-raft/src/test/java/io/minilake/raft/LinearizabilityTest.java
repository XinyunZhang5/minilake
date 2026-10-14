package io.minilake.raft;

import static io.minilake.raft.Linearizability.EXISTS;
import static io.minilake.raft.Linearizability.OK;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minilake.raft.Linearizability.Kind;
import io.minilake.raft.Linearizability.Op;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The checker itself must accept real concurrency and reject the anomalies it exists to catch. */
class LinearizabilityTest {
  static final long NEVER = Long.MAX_VALUE;

  static Op put(int c, String v, long call, long ret, String result) {
    return new Op(c, "k", Kind.PUT, v, call, ret, result);
  }

  static Op read(int c, long call, long ret, String result) {
    return new Op(c, "k", Kind.READ, null, call, ret, result);
  }

  @Test
  void overlappingPutsMayResolveEitherWay() {
    assertTrue(Linearizability.check(List.of(put(1, "a", 0, 10, EXISTS), put(2, "b", 1, 9, OK), read(3, 11, 12, "b")))
        .isEmpty());
  }

  @Test
  void twoWinnersAreRejected() {
    assertFalse(Linearizability.check(List.of(put(1, "a", 0, 10, OK), put(2, "b", 1, 9, OK))).isEmpty());
  }

  @Test
  void staleReadAfterACompletedPutIsRejected() {
    assertFalse(Linearizability.check(List.of(put(1, "a", 0, 1, OK), read(2, 2, 3, "null"))).isEmpty());
  }

  @Test
  void aWriterToldItLostWhileItsValueWonIsRejected() {
    // The bug session dedup prevents: the put applied, the reply was lost, the retry said "exists".
    assertFalse(Linearizability.check(List.of(put(1, "a", 0, 5, EXISTS), read(2, 6, 7, "a"))).isEmpty());
  }

  @Test
  void anUnansweredPutMayOrMayNotHaveHappened() {
    assertTrue(Linearizability.check(List.of(put(1, "a", 0, NEVER, null), read(2, 1, 2, "null"), read(2, 3, 4, "a")))
        .isEmpty());
    assertTrue(Linearizability.check(List.of(put(1, "a", 0, NEVER, null), read(2, 1, 2, "null"))).isEmpty());
    assertFalse(Linearizability.check(List.of(put(1, "a", 5, NEVER, null), read(2, 1, 2, "a"))).isEmpty(),
        "but not before it was invoked");
  }
}
