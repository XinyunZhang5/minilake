package io.minilake.raft;

import io.minilake.raft.Message.ClientReply;
import io.minilake.raft.Message.ClientRequest;
import io.minilake.storage.LogStore;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.List;

/**
 * A {@link LogStore} whose files live in a Raft-replicated map instead of a file system, so
 * MiniLake's commit primitive (create the next log file unless it exists) is decided by a
 * replicated majority rather than by one machine's link(2). Data files are unaffected; only the
 * transaction log moves.
 *
 * <p>Every call is a Raft command, reads included, so all of them are linearizable. One request
 * is in flight per client; a request that times out or reaches a non-leader is retried with the
 * same sequence number, which the state machine deduplicates.
 */
public final class CoordinatedLogStore implements LogStore {
  private static final long ATTEMPT_TIMEOUT_MS = 300;
  private static final long DEADLINE_MS = 30_000;

  private final LocalCluster cluster;
  private final int clientId;
  private long seq;
  private long requestId;
  private int target;

  CoordinatedLogStore(LocalCluster cluster, int clientId) {
    this.cluster = cluster;
    this.clientId = clientId;
  }

  private synchronized Result execute(Op op) throws IOException {
    Command cmd = new Command(clientId, ++seq, op);
    long deadline = System.currentTimeMillis() + DEADLINE_MS;
    cluster.leader().ifPresent(l -> target = l);
    while (System.currentTimeMillis() < deadline) {
      long rid = ++requestId;
      cluster.send(new ClientRequest(clientId, target, rid, cmd));
      long attemptEnd = System.currentTimeMillis() + ATTEMPT_TIMEOUT_MS;
      ClientReply reply = null;
      try {
        while (reply == null && System.currentTimeMillis() < attemptEnd) {
          ClientReply r = cluster.awaitReply(clientId, attemptEnd - System.currentTimeMillis());
          if (r != null && r.requestId() == rid) reply = r; // drop replies to earlier attempts
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new InterruptedIOException("interrupted waiting for the commit coordinator");
      }
      if (reply != null && !reply.notLeader()) return reply.result();
      target = reply != null && reply.leaderHint() >= 0 ? reply.leaderHint() : (target + 1) % cluster.size();
      if (reply != null) {
        try {
          Thread.sleep(10); // no leader yet; give the election a moment
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new InterruptedIOException();
        }
      }
    }
    throw new IOException("commit coordinator unavailable for " + DEADLINE_MS + " ms");
  }

  @Override
  public byte[] read(String path) throws IOException {
    byte[] data = ((Result.Value) execute(new Op.Read(path))).data();
    if (data == null) throw new NoSuchFileException(path);
    return data;
  }

  @Override
  public void writeIfAbsent(String path, byte[] data) throws IOException {
    if (execute(new Op.PutIfAbsent(path, data)) instanceof Result.Exists) throw new FileAlreadyExistsException(path);
  }

  @Override
  public void overwrite(String path, byte[] data) throws IOException {
    execute(new Op.Overwrite(path, data));
  }

  @Override
  public List<String> list(String dir) throws IOException {
    return ((Result.Names) execute(new Op.List(dir))).names();
  }

  @Override
  public boolean exists(String path) throws IOException {
    return ((Result.Value) execute(new Op.Read(path))).data() != null;
  }
}
