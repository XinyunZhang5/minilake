package io.minilake;

/** A concurrent commit wrote the same application transaction id. */
public final class ConcurrentTransactionException extends CommitConflictException {
  public ConcurrentTransactionException(String message, long winningVersion) {
    super(message, winningVersion);
  }
}
