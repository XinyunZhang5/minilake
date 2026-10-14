package io.minilake.raft;

import io.minilake.raft.Linearizability.Kind;
import io.minilake.raft.Message.ClientReply;
import io.minilake.raft.Message.ClientRequest;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.stream.IntStream;

/**
 * Deterministic simulation of a Raft cluster plus clients. Everything (message delay, loss,
 * duplication, reordering, crashes, partitions, election timeouts, client choices) is drawn from
 * one seeded generator, so any failing seed replays exactly.
 *
 * <p>Clients act like MiniLake committers: each tracks the version it believes is next and races
 * the others to put-if-absent that log entry, and sometimes reads entries back. After every event
 * the Raft safety properties are checked; at the end the clients' history is checked for
 * linearizability.
 */
final class Simulation {
  /** Probabilities are per message or per tick. */
  record Faults(double drop, double duplicate, int maxDelay, double crash, double killLeader, double partition) {
    static final Faults NONE = new Faults(0, 0, 1, 0, 0, 0);
    static final Faults HARSH = new Faults(0.10, 0.05, 8, 0.002, 0.003, 0.003);
  }

  static final int CLIENT_BASE = 100;
  private static final int CLIENT_TIMEOUT = 40;

  private final long seed;
  private final SplittableRandom rnd;
  private final RaftConfig config;
  private final boolean dedup;
  private final int n;
  private final List<Integer> ids;
  private final RaftStorage[] storage;
  private final RaftNode[] nodes; // null while crashed
  private final long[] restartAt;
  private final int[] group; // partition group per node
  private long healAt = -1;
  private final TreeMap<Long, List<Message>> inFlight = new TreeMap<>();
  private long now;
  private Faults faults;

  // Global truth for the safety checks.
  private final Map<Long, Integer> leaderOfTerm = new HashMap<>();
  private final Map<Long, Entry> committed = new HashMap<>();
  private String violation;

  private final List<Client> clients = new ArrayList<>();
  final List<Linearizability.Op> history = new ArrayList<>();
  int leaderChanges;
  int crashes;
  int snapshotsInstalled;
  long maxCommitted;

  Simulation(long seed, int nodeCount, int clientCount, int opsPerClient, RaftConfig config, Faults faults, boolean dedup) {
    this.seed = seed;
    this.rnd = new SplittableRandom(seed);
    this.config = config;
    this.faults = faults;
    this.dedup = dedup;
    this.n = nodeCount;
    this.ids = IntStream.range(0, n).boxed().toList();
    this.storage = new RaftStorage[n];
    this.nodes = new RaftNode[n];
    this.restartAt = new long[n];
    this.group = new int[n];
    for (int i = 0; i < n; i++) {
      storage[i] = new RaftStorage();
      start(i);
    }
    for (int c = 0; c < clientCount; c++) clients.add(new Client(CLIENT_BASE + c, opsPerClient));
  }

  Simulation(long seed, RaftConfig config, Faults faults) {
    this(seed, 3, 4, 40, config, faults, true);
  }

  private void start(int i) {
    nodes[i] = new RaftNode(i, ids, config, storage[i], new FileMapStateMachine(dedup), rnd.nextLong(), observer);
  }

  private final RaftNode.Observer observer = new RaftNode.Observer() {
    @Override public void becameLeader(RaftNode node) {
      leaderChanges++;
      Integer prev = leaderOfTerm.putIfAbsent(node.term(), node.id());
      if (prev != null && prev != node.id()) fail("election safety: nodes " + prev + " and " + node.id()
          + " both leader of term " + node.term());
      // Leader completeness: every committed entry must be in the new leader's log.
      RaftStorage s = node.storage();
      for (Entry e : committed.values()) {
        if (e.index() <= s.snapshotIndex()) continue;
        if (e.index() > s.lastIndex() || s.termAt(e.index()) != e.term()) {
          fail("leader completeness: leader " + node.id() + " of term " + node.term() + " lacks committed " + e);
        }
      }
    }

    @Override public void applied(RaftNode node, Entry e) {
      Entry prev = committed.putIfAbsent(e.index(), e);
      if (prev != null && !prev.equals(e)) {
        fail("state machine safety: node " + node.id() + " applied " + e + " at an index where " + prev + " was applied");
      }
      maxCommitted = Math.max(maxCommitted, e.index());
    }
  };

  private void fail(String msg) {
    if (violation == null) violation = "seed " + seed + ", t=" + now + ": " + msg;
  }

  // ------------------------------------------------------------------ network

  private void send(Message m) {
    if (rnd.nextDouble() < faults.drop()) return;
    int copies = rnd.nextDouble() < faults.duplicate() ? 2 : 1;
    for (int k = 0; k < copies; k++) {
      long at = now + 1 + rnd.nextInt(faults.maxDelay());
      inFlight.computeIfAbsent(at, t -> new ArrayList<>()).add(m);
    }
  }

  private boolean connected(int a, int b) {
    boolean aNode = a < CLIENT_BASE, bNode = b < CLIENT_BASE;
    return !aNode || !bNode || group[a] == group[b];
  }

  private void deliver(Message m) {
    if (!connected(m.from(), m.to())) return;
    if (m.to() >= CLIENT_BASE) {
      clients.get(m.to() - CLIENT_BASE).receive((ClientReply) m);
      return;
    }
    RaftNode node = nodes[m.to()];
    if (node == null) return;
    if (m instanceof Message.InstallSnapshot) snapshotsInstalled++;
    node.handle(m).forEach(this::send);
  }

  // ------------------------------------------------------------------ main loop

  /** Advances {@code ticks} ticks; returns the first safety violation, if any. */
  Optional<String> run(long ticks) {
    for (long end = now + ticks; now < end && violation == null; now++) step();
    return Optional.ofNullable(violation);
  }

  private void step() {
    List<Message> due = inFlight.remove(now);
    if (due != null) {
      for (Message m : due) deliver(m);
    }
    injectFaults();
    for (int i = 0; i < n; i++) if (nodes[i] != null) nodes[i].tick().forEach(this::send);
    for (Client c : clients) c.step();
  }

  private int down() {
    int d = 0;
    for (RaftNode node : nodes) if (node == null) d++;
    return d;
  }

  private void crash(int i) {
    nodes[i] = null;
    crashes++;
    restartAt[i] = now + 20 + rnd.nextInt(200);
  }

  private void injectFaults() {
    for (int i = 0; i < n; i++) if (nodes[i] == null && now >= restartAt[i]) start(i);
    if (healAt >= 0 && now >= healAt) {
      java.util.Arrays.fill(group, 0);
      healAt = -1;
    }
    boolean canCrash = down() < n / 2; // keep a majority up so the run makes progress
    if (canCrash && rnd.nextDouble() < faults.crash()) {
      int i = rnd.nextInt(n);
      if (nodes[i] != null) crash(i);
    }
    if (canCrash && rnd.nextDouble() < faults.killLeader()) {
      for (int i = 0; i < n; i++) if (nodes[i] != null && nodes[i].role() == RaftNode.Role.LEADER) { crash(i); break; }
    }
    if (healAt < 0 && rnd.nextDouble() < faults.partition()) {
      for (int i = 0; i < n; i++) group[i] = rnd.nextInt(2);
      healAt = now + 20 + rnd.nextInt(200);
    }
  }

  /**
   * Stops injecting faults, heals the network, restarts every node and lets clients finish. Then
   * reads every key once more so the final state is part of the checked history.
   */
  Optional<String> settle(long maxTicks) {
    faults = Faults.NONE;
    java.util.Arrays.fill(group, 0);
    healAt = -1;
    for (int i = 0; i < n; i++) if (nodes[i] == null) start(i);
    long deadline = now + maxTicks;
    while (now < deadline && violation == null && !clients.stream().allMatch(Client::finished)) run(1);
    if (violation != null) return Optional.of(violation);
    if (!clients.stream().allMatch(Client::finished)) return Optional.of("seed " + seed + ": clients did not finish");

    Client auditor = new Client(CLIENT_BASE + clients.size(), 0);
    clients.add(auditor);
    int keys = clients.stream().mapToInt(c -> c.guess).max().orElse(0) + 1;
    for (int v = 0; v < keys; v++) auditor.queue.add(new Op.Read(key(v)));
    while (now < deadline + maxTicks && violation == null && !auditor.finished()) run(1);
    if (violation != null) return Optional.of(violation);
    if (!auditor.finished()) return Optional.of("seed " + seed + ": final reads did not finish");
    return Linearizability.check(history).map(s -> "seed " + seed + ": " + s);
  }

  static String key(int version) { return String.format("t/_log/%020d.json", version); }

  // ------------------------------------------------------------------ clients

  private final class Client {
    final int id;
    int remaining;
    final List<Op> queue = new ArrayList<>(); // scripted ops (the auditor's final reads)
    int guess; // the version this client will try to commit next
    long seq;
    long requestId;
    Command current;
    long callTime;
    long sentAt;
    int target;

    Client(int id, int ops) {
      this.id = id;
      this.remaining = ops;
      this.target = rnd.nextInt(n);
    }

    boolean finished() { return current == null && remaining == 0 && queue.isEmpty(); }

    void step() {
      if (current == null) {
        Op op;
        if (!queue.isEmpty()) op = queue.remove(0);
        else if (remaining > 0 && rnd.nextDouble() < 0.2) {
          remaining--;
          op = rnd.nextDouble() < 0.75
              ? new Op.PutIfAbsent(key(guess), ("c" + id + "-" + (seq + 1)).getBytes(StandardCharsets.UTF_8))
              : new Op.Read(key(Math.max(0, guess - rnd.nextInt(3))));
        } else return;
        current = new Command(id, ++seq, op);
        callTime = 2 * now + 1; // after this tick's deliveries (2 * now)
        transmit();
      } else if (now - sentAt > CLIENT_TIMEOUT) {
        target = (target + 1) % n;
        transmit();
      }
    }

    void transmit() {
      sentAt = now;
      send(new ClientRequest(id, target, ++requestId, current));
    }

    void receive(ClientReply r) {
      if (current == null || r.requestId() != requestId) return; // stale or duplicate reply
      if (r.notLeader()) {
        target = r.leaderHint() >= 0 ? r.leaderHint() : (target + 1) % n;
        transmit();
        return;
      }
      long ret = 2 * now;
      switch (current.op()) {
        case Op.PutIfAbsent p -> {
          history.add(new Linearizability.Op(id, p.path(), Kind.PUT, new String(p.data(), StandardCharsets.UTF_8),
              callTime, ret, r.result() instanceof Result.Ok ? Linearizability.OK : Linearizability.EXISTS));
          guess++; // either way this version is taken now
        }
        case Op.Read rd -> {
          byte[] data = ((Result.Value) r.result()).data();
          history.add(new Linearizability.Op(id, rd.path(), Kind.READ, null, callTime, ret,
              data == null ? "null" : new String(data, StandardCharsets.UTF_8)));
        }
        default -> throw new IllegalStateException();
      }
      current = null;
    }
  }
}
