package io.minilake.raft;

/**
 * A client operation with its session identity. {@code (clientId, seq)} lets the state machine
 * recognize a retry of a command that was already applied and return the original result instead
 * of applying it again: without that, a commit whose reply was lost would be retried and answered
 * "already exists", and the writer would wrongly think it lost the race.
 */
public record Command(long clientId, long seq, Op op) {
  public static final Command NOOP = new Command(0, 0, new Op.Noop());
}
