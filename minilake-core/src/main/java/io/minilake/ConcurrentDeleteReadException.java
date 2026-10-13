package io.minilake;

/** A concurrent commit removed a file this transaction read. */
public final class ConcurrentDeleteReadException extends CommitConflictException {
  public ConcurrentDeleteReadException(String message, long winningVersion) {
    super(message, winningVersion);
  }
}
