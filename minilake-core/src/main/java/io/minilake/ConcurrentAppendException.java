package io.minilake;

/** Files were added by a concurrent commit that match data this transaction read. */
public final class ConcurrentAppendException extends CommitConflictException {
  public ConcurrentAppendException(String message, long winningVersion) {
    super(message, winningVersion);
  }
}
