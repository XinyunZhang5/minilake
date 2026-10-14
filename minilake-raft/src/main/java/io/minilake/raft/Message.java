package io.minilake.raft;

import java.util.List;

/** Everything nodes and clients send each other. Node ids are small ints; client ids are separate. */
public sealed interface Message {
  int from();

  int to();

  record RequestVote(int from, int to, long term, long lastLogIndex, long lastLogTerm) implements Message {}

  record VoteReply(int from, int to, long term, boolean granted) implements Message {}

  record AppendEntries(int from, int to, long term, long prevIndex, long prevTerm, List<Entry> entries,
      long leaderCommit) implements Message {}

  /**
   * On success {@code matchIndex} is the last index the follower now shares with the leader. On
   * failure {@code conflictIndex} is where the leader should retry from.
   */
  record AppendReply(int from, int to, long term, boolean success, long matchIndex, long conflictIndex)
      implements Message {}

  record InstallSnapshot(int from, int to, long term, long lastIndex, long lastTerm, byte[] data)
      implements Message {}

  record SnapshotReply(int from, int to, long term, long lastIndex) implements Message {}

  /** {@code from} is the client's mailbox id; {@code requestId} matches the reply to the request. */
  record ClientRequest(int from, int to, long requestId, Command command) implements Message {}

  /** Exactly one of {@code result} and a not-leader answer: result is null when not leader. */
  record ClientReply(int from, int to, long requestId, Result result, int leaderHint) implements Message {
    public boolean notLeader() { return result == null; }
  }
}
