package io.minilake;

/** The table metadata (schema, partitioning or config) was changed by a concurrent commit. */
public final class MetadataChangedException extends CommitConflictException {
  public MetadataChangedException(String message, long winningVersion) {
    super(message, winningVersion);
  }
}
