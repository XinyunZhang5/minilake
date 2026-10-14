package io.minilake;

import io.minilake.actions.Action;
import io.minilake.actions.ActionCodec;
import io.minilake.actions.CommitInfo;
import io.minilake.storage.LocalLogStore;
import io.minilake.storage.LogStore;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * Entry point for one table: reads the log into snapshots and hands out transactions.
 *
 * <p>Log layout under {@code <table>/_log/}: {@code %020d.json} commits (one action per line) and
 * {@code %020d.checkpoint.json} full-state checkpoints written every N commits so that rebuilding a
 * snapshot replays at most N-1 commit files.
 */
public final class MiniLakeTable {
  public static final String LOG_DIR = "_log";
  public static final String CHECKPOINT_INTERVAL = "minilake.checkpointInterval";
  static final int DEFAULT_CHECKPOINT_INTERVAL = 10;

  private final String path;
  private final LogStore store;

  private MiniLakeTable(String path, LogStore store) {
    this.path = path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
    this.store = store;
  }

  public static MiniLakeTable forPath(String path) {
    return new MiniLakeTable(path, new LocalLogStore());
  }

  public static MiniLakeTable forPath(String path, LogStore store) {
    return new MiniLakeTable(path, store);
  }

  public String path() { return path; }

  LogStore store() { return store; }

  String logDir() { return path + "/" + LOG_DIR; }

  String commitPath(long version) { return logDir() + "/" + String.format("%020d.json", version); }

  String checkpointPath(long version) {
    return logDir() + "/" + String.format("%020d.checkpoint.json", version);
  }

  /** Commit and checkpoint versions currently in the log. */
  private record Listing(TreeSet<Long> commits, TreeSet<Long> checkpoints) {}

  private Listing list() {
    TreeSet<Long> commits = new TreeSet<>(), checkpoints = new TreeSet<>();
    try {
      for (String name : store.list(logDir())) {
        if (name.length() < 20 || !Character.isDigit(name.charAt(0))) continue;
        String suffix = name.substring(20);
        long v = Long.parseLong(name.substring(0, 20));
        if (suffix.equals(".json")) commits.add(v);
        else if (suffix.equals(".checkpoint.json")) checkpoints.add(v);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return new Listing(commits, checkpoints);
  }

  /** @return the newest committed version, or -1 if the table has no commits */
  public long latestVersion() {
    TreeSet<Long> commits = list().commits();
    return commits.isEmpty() ? -1 : commits.last();
  }

  public Snapshot latest() {
    Listing l = list();
    return l.commits().isEmpty() ? new Snapshot.Builder().build(-1) : build(l, l.commits().last());
  }

  public Snapshot snapshotAt(long version) {
    Listing l = list();
    if (!l.commits().contains(version) && !l.checkpoints().contains(version) && !commitExists(version)) {
      throw new IllegalArgumentException("version " + version + " does not exist; latest is "
          + (l.commits().isEmpty() ? -1 : l.commits().last()));
    }
    return build(l, version);
  }

  /** The newest snapshot whose commit timestamp is {@code <= timestampMillis}. */
  public Snapshot snapshotAsOf(long timestampMillis) {
    List<Long> versions = new ArrayList<>(list().commits());
    if (versions.isEmpty() || commitTimestamp(versions.get(0)) > timestampMillis) {
      throw new IllegalArgumentException("no version exists at or before timestamp " + timestampMillis);
    }
    // Commit timestamps are strictly increasing (enforced at commit), so binary search is valid.
    int lo = 0, hi = versions.size() - 1;
    while (lo < hi) {
      int mid = (lo + hi + 1) >>> 1;
      if (commitTimestamp(versions.get(mid)) <= timestampMillis) lo = mid;
      else hi = mid - 1;
    }
    return snapshotAt(versions.get(lo));
  }

  public long commitTimestamp(long version) {
    for (Action a : readCommit(version)) {
      if (a instanceof CommitInfo c) return c.timestamp();
    }
    throw new IllegalStateException("commit " + version + " has no commitInfo");
  }

  private boolean commitExists(long version) {
    try {
      return store.exists(commitPath(version));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  List<Action> readCommit(long version) {
    try {
      return ActionCodec.decode(store.read(commitPath(version)));
    } catch (NoSuchFileException e) {
      throw new IllegalStateException("log gap: missing commit file for version " + version, e);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Snapshot build(Listing l, long version) {
    Snapshot.Builder b = new Snapshot.Builder();
    Long cp = l.checkpoints().floor(version);
    long from = 0;
    if (cp != null) {
      try {
        b.apply(ActionCodec.decode(store.read(checkpointPath(cp))));
        from = cp + 1;
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    // Don't trust the listing for the versions in between: readdir(3) may omit entries created
    // while it runs, so a listing can contain N without N-1 even though N-1 was committed first.
    // A commit exists only after its predecessor did, so any version below a listed one is real,
    // and only a failed read means the log actually has a hole.
    for (long v = from; v <= version; v++) {
      b.apply(readCommit(v));
    }
    return b.build(version);
  }

  public OptimisticTransaction startTransaction() {
    return new OptimisticTransaction(this, latest());
  }

  /** Writes a full-state checkpoint for {@code snapshot}. Idempotent; safe to race. */
  public void checkpoint(Snapshot snapshot) {
    List<Action> state = new ArrayList<>();
    state.add(snapshot.protocol());
    state.add(snapshot.metadata());
    state.addAll(snapshot.appTxns());
    state.addAll(snapshot.allFiles());
    try {
      store.overwrite(checkpointPath(snapshot.version()), ActionCodec.encode(state));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
