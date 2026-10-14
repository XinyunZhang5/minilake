package io.minilake.raft;

import io.minilake.raft.Message.ClientReply;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.IntStream;

/**
 * Runs {@link RaftNode}s on real threads in one process, one thread per node, with an in-memory
 * network. Nodes can be killed (thread stopped, inbox and volatile state lost) and restarted over
 * their surviving {@link RaftStorage}, which is what a process crash with a durable disk looks like.
 */
public final class LocalCluster implements AutoCloseable {
  static final int CLIENT_BASE = 1000;

  private final RaftConfig config;
  private final long tickMillis;
  private final List<Integer> ids;
  private final RaftStorage[] storage;
  // Read lock-free by the message path. kill/restart are synchronized among themselves and may
  // join a node thread while holding the lock, so node threads must never take it.
  private final AtomicReferenceArray<Runner> runners;
  private final Map<Integer, BlockingQueue<ClientReply>> mailboxes = new ConcurrentHashMap<>();
  private final AtomicInteger nextClient = new AtomicInteger(CLIENT_BASE);
  private volatile double dropRate;

  public LocalCluster(int nodes, RaftConfig config, long tickMillis) {
    this.config = config;
    this.tickMillis = tickMillis;
    this.ids = IntStream.range(0, nodes).boxed().toList();
    this.storage = new RaftStorage[nodes];
    this.runners = new AtomicReferenceArray<>(nodes);
    for (int i = 0; i < nodes; i++) {
      storage[i] = new RaftStorage();
      start(i);
    }
  }

  public int size() { return ids.size(); }

  /** Drops this fraction of node-to-node messages. */
  public void setDropRate(double rate) { this.dropRate = rate; }

  public synchronized void kill(int i) {
    Runner r = runners.getAndSet(i, null);
    if (r != null) r.stop(); // joined before returning, so a restart never shares storage with it
  }

  public synchronized void restart(int i) {
    if (runners.get(i) == null) start(i);
  }

  public boolean alive(int i) { return runners.get(i) != null; }

  /** The live node that currently believes it leads the highest term. */
  public Optional<Integer> leader() {
    Integer best = null;
    long bestTerm = -1;
    for (int i = 0; i < runners.length(); i++) {
      Runner r = runners.get(i);
      if (r != null && r.role == RaftNode.Role.LEADER && r.term > bestTerm) {
        best = i;
        bestTerm = r.term;
      }
    }
    return Optional.ofNullable(best);
  }

  public CoordinatedLogStore newClient() {
    int id = nextClient.getAndIncrement();
    mailboxes.put(id, new LinkedBlockingQueue<>());
    return new CoordinatedLogStore(this, id);
  }

  void send(Message m) {
    if (m.to() >= CLIENT_BASE) {
      BlockingQueue<ClientReply> box = mailboxes.get(m.to());
      if (box != null) box.add((ClientReply) m);
      return;
    }
    if (m.from() < CLIENT_BASE && dropRate > 0 && ThreadLocalRandom.current().nextDouble() < dropRate) return;
    Runner r = runners.get(m.to());
    if (r != null) r.inbox.add(m);
  }

  ClientReply awaitReply(int client, long timeoutMillis) throws InterruptedException {
    return mailboxes.get(client).poll(timeoutMillis, TimeUnit.MILLISECONDS);
  }

  private void start(int i) {
    RaftNode node = new RaftNode(i, ids, config, storage[i], new FileMapStateMachine(),
        ThreadLocalRandom.current().nextLong(), null);
    Runner r = new Runner(node);
    runners.set(i, r);
    r.thread.start();
  }

  @Override
  public synchronized void close() {
    for (int i = 0; i < runners.length(); i++) kill(i);
  }

  private final class Runner {
    final RaftNode node;
    final BlockingQueue<Message> inbox = new LinkedBlockingQueue<>();
    final Thread thread;
    volatile boolean running = true;
    volatile RaftNode.Role role = RaftNode.Role.FOLLOWER;
    volatile long term;

    Runner(RaftNode node) {
      this.node = node;
      this.thread = new Thread(this::loop, "raft-node-" + node.id());
      this.thread.setDaemon(true);
    }

    private void loop() {
      long nextTick = System.nanoTime() + tickMillis * 1_000_000;
      try {
        while (running) {
          long wait = nextTick - System.nanoTime();
          Message m = wait > 0 ? inbox.poll(wait, TimeUnit.NANOSECONDS) : null;
          List<Message> out = new ArrayList<>();
          if (m != null) out.addAll(node.handle(m));
          if (System.nanoTime() >= nextTick) {
            out.addAll(node.tick());
            nextTick += tickMillis * 1_000_000;
          }
          role = node.role();
          term = node.term();
          if (running) out.forEach(LocalCluster.this::send);
        }
      } catch (InterruptedException e) {
        // killed
      }
    }

    void stop() {
      running = false;
      thread.interrupt();
      try {
        thread.join(); // after this the node's storage is safely visible to its successor
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
