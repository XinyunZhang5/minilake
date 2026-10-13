package io.minilake;

/**
 * A concurrent commit made this transaction's view of the table stale in a way that matters.
 * The caller should start a new transaction (re-read) and retry the whole operation.
 */
public abstract class CommitConflictException extends RuntimeException {
  private final long winningVersion;

  protected CommitConflictException(String message, long winningVersion) {
    super(message + " (conflicting commit: version " + winningVersion + ")");
    this.winningVersion = winningVersion;
  }

  public long winningVersion() {
    return winningVersion;
  }
}
