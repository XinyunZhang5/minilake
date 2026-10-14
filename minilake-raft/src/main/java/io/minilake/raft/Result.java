package io.minilake.raft;

/** What applying an {@link Op} produced. */
public sealed interface Result {
  record Ok() implements Result {}

  record Exists() implements Result {}

  /** {@code data} is null when the path does not exist. */
  record Value(byte[] data) implements Result {}

  record Names(java.util.List<String> names) implements Result {}
}
