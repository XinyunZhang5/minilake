package io.minilake.raft;

import io.minilake.raft.Message.AppendEntries;
import io.minilake.raft.Message.AppendReply;
import io.minilake.raft.Message.ClientReply;
import io.minilake.raft.Message.ClientRequest;
import io.minilake.raft.Message.InstallSnapshot;
import io.minilake.raft.Message.RequestVote;
import io.minilake.raft.Message.SnapshotReply;
import io.minilake.raft.Message.VoteReply;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

/**
 * One Raft server as a pure state machine: it never reads a clock, sleeps or does I/O. A driver
 * feeds it {@link #tick()} and {@link #handle(Message)} and delivers whatever messages they return.
 * That is what lets the same code run under a deterministic simulator and under real threads.
 */
public final class RaftNode {
  public enum Role { FOLLOWER, CANDIDATE, LEADER }

  /** Hook for the simulator's invariant checks; called synchronously, must not mutate the node. */
  public interface Observer {
    default void becameLeader(RaftNode node) {}

    default void applied(RaftNode node, Entry entry) {}
  }

  private record Pending(int client, long requestId, long term) {}

  private final int id;
  private final List<Integer> peers;
  private final RaftConfig config;
  private final RaftStorage storage;
  private final FileMapStateMachine stateMachine;
  private final Random random;
  private final Observer observer;

  private Role role = Role.FOLLOWER;
  private int leaderId = -1;
  private long commitIndex;
  private long lastApplied;
  private int electionElapsed;
  private int electionTimeout;
  private int heartbeatElapsed;
  private final Set<Integer> votes = new HashSet<>();
  private final Map<Integer, Long> nextIndex = new HashMap<>();
  private final Map<Integer, Long> matchIndex = new HashMap<>();
  private final TreeMap<Long, Pending> pending = new TreeMap<>();
  private final List<Message> outbox = new ArrayList<>();

  /**
   * Starts (or restarts) a node over {@code storage}. Volatile state, including the state machine,
   * is rebuilt from the snapshot; committed entries after it are re-applied once a leader says so.
   */
  public RaftNode(int id, List<Integer> cluster, RaftConfig config, RaftStorage storage,
      FileMapStateMachine stateMachine, long seed, Observer observer) {
    this.id = id;
    this.peers = cluster.stream().filter(p -> p != id).toList();
    this.config = config;
    this.storage = storage;
    this.stateMachine = stateMachine;
    this.random = new Random(seed);
    this.observer = observer == null ? new Observer() {} : observer;
    stateMachine.restore(storage.snapshotData);
    commitIndex = lastApplied = storage.snapshotIndex;
    resetElectionTimer();
  }

  public int id() { return id; }
  public Role role() { return role; }
  public long term() { return storage.currentTerm; }
  public int leaderId() { return leaderId; }
  public long commitIndex() { return commitIndex; }
  public long lastApplied() { return lastApplied; }
  public RaftStorage storage() { return storage; }

  // ---------------------------------------------------------------- driver API

  public List<Message> tick() {
    if (role == Role.LEADER) {
      if (++heartbeatElapsed >= config.heartbeatInterval()) {
        heartbeatElapsed = 0;
        broadcastAppend();
      }
    } else if (++electionElapsed >= electionTimeout) {
      startElection();
    }
    return drain();
  }

  public List<Message> handle(Message m) {
    switch (m) {
      case RequestVote rv -> onRequestVote(rv);
      case VoteReply vr -> onVoteReply(vr);
      case AppendEntries ae -> onAppendEntries(ae);
      case AppendReply ar -> onAppendReply(ar);
      case InstallSnapshot is -> onInstallSnapshot(is);
      case SnapshotReply sr -> onSnapshotReply(sr);
      case ClientRequest cr -> onClientRequest(cr);
      case ClientReply cr -> { }
    }
    return drain();
  }

  private List<Message> drain() {
    List<Message> out = List.copyOf(outbox);
    outbox.clear();
    return out;
  }

  // ---------------------------------------------------------------- elections

  private void resetElectionTimer() {
    electionElapsed = 0;
    electionTimeout = config.electionTimeoutMin()
        + random.nextInt(config.electionTimeoutMax() - config.electionTimeoutMin() + 1);
  }

  private int majority() { return (peers.size() + 1) / 2 + 1; }

  private void startElection() {
    storage.currentTerm++;
    storage.votedFor = id;
    role = Role.CANDIDATE;
    leaderId = -1;
    votes.clear();
    votes.add(id);
    resetElectionTimer();
    failPending();
    if (votes.size() >= majority()) {
      becomeLeader();
      return;
    }
    for (int p : peers) {
      outbox.add(new RequestVote(id, p, storage.currentTerm, storage.lastIndex(), storage.lastTerm()));
    }
  }

  /** Any message carrying a newer term means our term is over: adopt it and follow. */
  private void observeTerm(long term) {
    if (term > storage.currentTerm) {
      storage.currentTerm = term;
      storage.votedFor = null;
      if (role != Role.FOLLOWER) failPending();
      role = Role.FOLLOWER;
      leaderId = -1;
    }
  }

  private void onRequestVote(RequestVote rv) {
    observeTerm(rv.term());
    boolean upToDate = rv.lastLogTerm() > storage.lastTerm()
        || (rv.lastLogTerm() == storage.lastTerm() && rv.lastLogIndex() >= storage.lastIndex());
    boolean grant = rv.term() == storage.currentTerm
        && (storage.votedFor == null || storage.votedFor == rv.from())
        && upToDate;
    if (grant) {
      storage.votedFor = rv.from();
      resetElectionTimer();
    }
    outbox.add(new VoteReply(id, rv.from(), storage.currentTerm, grant));
  }

  private void onVoteReply(VoteReply vr) {
    observeTerm(vr.term());
    if (role != Role.CANDIDATE || vr.term() != storage.currentTerm || !vr.granted()) return;
    votes.add(vr.from());
    if (votes.size() >= majority()) becomeLeader();
  }

  private void becomeLeader() {
    role = Role.LEADER;
    leaderId = id;
    heartbeatElapsed = 0;
    for (int p : peers) {
      nextIndex.put(p, storage.lastIndex() + 1);
      matchIndex.put(p, 0L);
    }
    observer.becameLeader(this);
    // Entries from earlier terms can't be committed by counting replicas; committing this
    // current-term no-op commits everything before it.
    if (!config.unsafeCommitByCount()) appendLocal(Command.NOOP);
    broadcastAppend();
    maybeAdvanceCommit();
  }

  // ---------------------------------------------------------------- replication (leader)

  private void appendLocal(Command c) {
    storage.append(new Entry(storage.currentTerm, storage.lastIndex() + 1, c));
  }

  private void broadcastAppend() {
    for (int p : peers) sendAppend(p);
  }

  private void sendAppend(int peer) {
    long next = nextIndex.get(peer);
    if (next <= storage.snapshotIndex) {
      outbox.add(new InstallSnapshot(id, peer, storage.currentTerm, storage.snapshotIndex, storage.snapshotTerm,
          storage.snapshotData));
      return;
    }
    long prev = next - 1;
    List<Entry> entries = storage.slice(next, next + config.maxEntriesPerAppend() - 1);
    outbox.add(new AppendEntries(id, peer, storage.currentTerm, prev, storage.termAt(prev), entries, commitIndex));
  }

  private void onAppendReply(AppendReply r) {
    observeTerm(r.term());
    if (role != Role.LEADER || r.term() != storage.currentTerm) return;
    int p = r.from();
    if (r.success()) {
      // Replies can arrive out of order; never move matchIndex backwards.
      if (r.matchIndex() > matchIndex.get(p)) matchIndex.put(p, r.matchIndex());
      nextIndex.put(p, Math.max(nextIndex.get(p), matchIndex.get(p) + 1));
      maybeAdvanceCommit();
      if (nextIndex.get(p) <= storage.lastIndex()) sendAppend(p);
    } else {
      nextIndex.put(p, Math.max(matchIndex.get(p) + 1, Math.min(nextIndex.get(p), r.conflictIndex())));
      sendAppend(p);
    }
  }

  private void onSnapshotReply(SnapshotReply r) {
    observeTerm(r.term());
    if (role != Role.LEADER || r.term() != storage.currentTerm) return;
    int p = r.from();
    if (r.lastIndex() > matchIndex.get(p)) matchIndex.put(p, r.lastIndex());
    nextIndex.put(p, Math.max(nextIndex.get(p), matchIndex.get(p) + 1));
    maybeAdvanceCommit();
  }

  private void maybeAdvanceCommit() {
    for (long n = storage.lastIndex(); n > commitIndex; n--) {
      if (n <= storage.snapshotIndex) break;
      // Raft §5.4.2: only entries from the current term are committed by counting replicas.
      if (!config.unsafeCommitByCount() && storage.termAt(n) != storage.currentTerm) break;
      int count = 1;
      for (int p : peers) if (matchIndex.get(p) >= n) count++;
      if (count >= majority()) {
        commitIndex = n;
        applyCommitted();
        return;
      }
    }
  }

  // ---------------------------------------------------------------- replication (follower)

  private void followLeader(int leader) {
    if (role != Role.FOLLOWER) failPending();
    role = Role.FOLLOWER;
    leaderId = leader;
    resetElectionTimer();
  }

  private void onAppendEntries(AppendEntries ae) {
    observeTerm(ae.term());
    if (ae.term() < storage.currentTerm) {
      outbox.add(new AppendReply(id, ae.from(), storage.currentTerm, false, 0, 0));
      return;
    }
    followLeader(ae.from());

    long prev = ae.prevIndex();
    List<Entry> entries = ae.entries();
    if (prev < storage.snapshotIndex) {
      // Everything up to our snapshot is committed and therefore identical; skip that part.
      int skip = (int) Math.min(entries.size(), storage.snapshotIndex - prev);
      entries = entries.subList(skip, entries.size());
      prev = storage.snapshotIndex;
      if (!entries.isEmpty() && entries.get(0).index() != prev + 1) {
        outbox.add(new AppendReply(id, ae.from(), storage.currentTerm, false, 0, storage.lastIndex() + 1));
        return;
      }
    } else if (prev > storage.lastIndex()) {
      outbox.add(new AppendReply(id, ae.from(), storage.currentTerm, false, 0, storage.lastIndex() + 1));
      return;
    } else if (storage.termAt(prev) != ae.prevTerm()) {
      // Back up over the whole conflicting term in one round trip instead of one entry at a time.
      long conflictTerm = storage.termAt(prev);
      long first = prev;
      while (first - 1 > storage.snapshotIndex && storage.termAt(first - 1) == conflictTerm) first--;
      outbox.add(new AppendReply(id, ae.from(), storage.currentTerm, false, 0, first));
      return;
    }

    for (Entry e : entries) {
      if (e.index() <= storage.lastIndex()) {
        if (storage.termAt(e.index()) == e.term()) continue;
        // Only truncate on a real conflict: a stale, shorter AppendEntries must not erase entries
        // a newer one already added.
        storage.truncateFrom(e.index());
      }
      storage.append(e);
    }
    long lastNew = prev + entries.size();
    if (ae.leaderCommit() > commitIndex) {
      commitIndex = Math.min(ae.leaderCommit(), lastNew);
      applyCommitted();
    }
    outbox.add(new AppendReply(id, ae.from(), storage.currentTerm, true, lastNew, 0));
  }

  private void onInstallSnapshot(InstallSnapshot is) {
    observeTerm(is.term());
    if (is.term() < storage.currentTerm) {
      outbox.add(new SnapshotReply(id, is.from(), storage.currentTerm, 0));
      return;
    }
    followLeader(is.from());
    if (is.lastIndex() > commitIndex) {
      storage.installSnapshot(is.lastIndex(), is.lastTerm(), is.data());
      stateMachine.restore(is.data());
      commitIndex = lastApplied = is.lastIndex();
    }
    outbox.add(new SnapshotReply(id, is.from(), storage.currentTerm, is.lastIndex()));
  }

  // ---------------------------------------------------------------- applying and clients

  private void applyCommitted() {
    while (lastApplied < commitIndex) {
      Entry e = storage.entry(++lastApplied);
      Result r = stateMachine.apply(e.command());
      observer.applied(this, e);
      Pending p = pending.remove(e.index());
      if (p != null && p.term() == e.term()) outbox.add(new ClientReply(id, p.client(), p.requestId(), r, id));
    }
    if (lastApplied - storage.snapshotIndex >= config.snapshotEvery()) {
      storage.installSnapshot(lastApplied, storage.termAt(lastApplied), stateMachine.snapshot());
    }
  }

  private void onClientRequest(ClientRequest cr) {
    if (role != Role.LEADER) {
      outbox.add(new ClientReply(id, cr.from(), cr.requestId(), null, leaderId));
      return;
    }
    appendLocal(cr.command());
    pending.put(storage.lastIndex(), new Pending(cr.from(), cr.requestId(), storage.currentTerm));
    broadcastAppend();
    maybeAdvanceCommit(); // single-node cluster
  }

  /**
   * Lost leadership: these requests may or may not commit. Tell the clients to retry elsewhere;
   * the retry carries the same (clientId, seq), so if the entry did commit it isn't applied twice.
   */
  private void failPending() {
    for (Pending p : pending.values()) outbox.add(new ClientReply(id, p.client(), p.requestId(), null, -1));
    pending.clear();
  }
}
