package io.minilake;

/** The table protocol was changed by a concurrent commit. */
public final class ProtocolChangedException extends CommitConflictException {
  public ProtocolChangedException(String message, long winningVersion) {
    super(message, winningVersion);
  }
}
