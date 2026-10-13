package io.minilake;

/** A concurrent commit removed a file this transaction also removes. */
public final class ConcurrentDeleteDeleteException extends CommitConflictException {
  public ConcurrentDeleteDeleteException(String message, long winningVersion) {
    super(message, winningVersion);
  }
}
