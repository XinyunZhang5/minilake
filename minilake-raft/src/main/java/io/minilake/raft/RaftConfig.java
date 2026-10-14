package io.minilake.raft;

/**
 * Timing is in ticks; the driver decides how long a tick is.
 *
 * @param snapshotEvery take a snapshot once this many applied entries sit after the last one
 * @param unsafeCommitByCount deliberately broken, for tests only: commit an entry from an earlier
 *     term as soon as a majority stores it, and skip the new leader's no-op. That is the mistake
 *     Figure 8 of the Raft paper warns about. (With the no-op in place the wrong rule is hard to
 *     trigger: the no-op travels with the old entries, so a majority holding them also holds a
 *     current-term entry.)
 */
public record RaftConfig(
    int electionTimeoutMin,
    int electionTimeoutMax,
    int heartbeatInterval,
    int maxEntriesPerAppend,
    int snapshotEvery,
    boolean unsafeCommitByCount) {

  public static RaftConfig defaults() {
    return new RaftConfig(10, 20, 3, 64, 50, false);
  }

  public RaftConfig withSnapshotEvery(int n) {
    return new RaftConfig(electionTimeoutMin, electionTimeoutMax, heartbeatInterval, maxEntriesPerAppend, n,
        unsafeCommitByCount);
  }

  public RaftConfig withUnsafeCommitByCount() {
    return new RaftConfig(electionTimeoutMin, electionTimeoutMax, heartbeatInterval, maxEntriesPerAppend,
        snapshotEvery, true);
  }
}
