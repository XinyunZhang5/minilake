package io.minilake.raft;

/** An operation on the replicated file map. Reads go through the log too, which makes them linearizable. */
public sealed interface Op {
  /** Creates {@code path} unless it exists: the commit primitive MiniLake needs. */
  record PutIfAbsent(String path, byte[] data) implements Op {}

  /** Replaces {@code path}; only for derived files such as checkpoints. */
  record Overwrite(String path, byte[] data) implements Op {}

  record Read(String path) implements Op {}

  /** Names directly under {@code dir}. */
  record List(String dir) implements Op {}

  /** Appended by every new leader so entries from earlier terms can commit (Raft §5.4.2). */
  record Noop() implements Op {}
}
