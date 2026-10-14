package io.minilake;

import io.minilake.actions.Action;
import io.minilake.actions.ActionCodec;
import io.minilake.actions.AddFile;
import io.minilake.actions.CommitInfo;
import io.minilake.actions.Metadata;
import io.minilake.actions.Protocol;
import io.minilake.actions.RemoveFile;
import io.minilake.actions.SetTransaction;
import io.minilake.expr.Predicate;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Optimistic concurrency control over the log.
 *
 * <p>A transaction reads one snapshot, records <em>what</em> it read (predicates, files, app ids),
 * and at commit tries to create the next commit file. If another writer got there first, the
 * transaction does not blindly fail: it reads every winning commit and fails only if one of them
 * touched something this transaction depended on. Otherwise its actions are still valid on top of
 * the winners, so it retries at the next version. This is what lets concurrent blind appends to
 * one table all succeed.
 */
public final class OptimisticTransaction {
  private static final int MAX_ATTEMPTS = 1000;

  private final MiniLakeTable table;
  private final Snapshot readSnapshot;

  private final List<Predicate> readPredicates = new ArrayList<>();
  private final Set<String> readFiles = new HashSet<>();
  private final Set<String> readAppIds = new HashSet<>();
  private boolean readWholeTable;
  private Metadata newMetadata;
  private boolean committed;

  OptimisticTransaction(MiniLakeTable table, Snapshot readSnapshot) {
    this.table = table;
    this.readSnapshot = readSnapshot;
  }

  public Snapshot snapshot() { return readSnapshot; }

  public long readVersion() { return readSnapshot.version(); }

  /** Reads files that may match {@code filter} and remembers both the filter and the files. */
  public List<AddFile> scan(Predicate filter) {
    if (!readSnapshot.exists()) return List.of();
    List<AddFile> files = readSnapshot.files(filter);
    if (filter == Predicate.TRUE) readWholeTable = true;
    else readPredicates.add(filter.bind(readSnapshot.schema()));
    files.forEach(f -> readFiles.add(f.path()));
    return files;
  }

  public List<AddFile> scanAll() {
    return scan(Predicate.TRUE);
  }

  /**
   * Records a dependency on exactly these files, without a predicate. A rewrite such as OPTIMIZE
   * uses this instead of {@link #scan}: it must fail if one of its inputs is removed concurrently,
   * but new files appended meanwhile are none of its business.
   */
  public void readFiles(java.util.Collection<AddFile> files) {
    for (AddFile f : files) {
      if (!readSnapshot.contains(f.path())) {
        throw new IllegalArgumentException(f.path() + " is not in the snapshot at version " + readSnapshot.version());
      }
      readFiles.add(f.path());
    }
  }

  /** Last committed version for {@code appId}; recorded so a concurrent duplicate batch conflicts. */
  public Optional<Long> txnVersion(String appId) {
    readAppIds.add(appId);
    return readSnapshot.txnVersion(appId);
  }

  /** Creates the table (on a fresh path) or replaces its schema/partitioning/config. */
  public void updateMetadata(Metadata metadata) {
    if (readSnapshot.exists() && !readSnapshot.metadata().partitionColumns().equals(metadata.partitionColumns())) {
      throw new IllegalArgumentException("changing partition columns is not supported");
    }
    this.newMetadata = metadata;
  }

  public long commit(List<? extends Action> actions, String operation) {
    return commit(actions, operation, Map.of());
  }

  /** @return the version this transaction committed as */
  public long commit(List<? extends Action> actions, String operation, Map<String, String> params) {
    if (committed) throw new IllegalStateException("transaction already committed");
    List<Action> body = prepare(actions);
    boolean blindAppend = isBlindAppend(body);

    long attempt = readSnapshot.version() + 1;
    long prevTs = attempt > 0 ? table.commitTimestamp(attempt - 1) : 0;
    for (int i = 0; i < MAX_ATTEMPTS; i++) {
      // Strictly increasing commit timestamps keep timestamp time travel well defined even
      // when the wall clock steps backwards between writers.
      long ts = Math.max(System.currentTimeMillis(), prevTs + 1);
      List<Action> full = new ArrayList<>(body);
      full.add(new CommitInfo(ts, operation, params, readSnapshot.version(), blindAppend));
      try {
        table.store().writeIfAbsent(table.commitPath(attempt), ActionCodec.encode(full));
        committed = true;
        maybeCheckpoint(attempt);
        return attempt;
      } catch (FileAlreadyExistsException lost) {
        long latest = table.latestVersion();
        for (long v = attempt; v <= latest; v++) {
          checkConflicts(v, table.readCommit(v), body);
        }
        attempt = latest + 1;
        prevTs = table.commitTimestamp(latest);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    throw new IllegalStateException("gave up after " + MAX_ATTEMPTS + " commit attempts");
  }

  private List<Action> prepare(List<? extends Action> actions) {
    List<Action> body = new ArrayList<>();
    if (!readSnapshot.exists()) {
      if (newMetadata == null) throw new IllegalStateException("first commit must set table metadata");
      body.add(Protocol.CURRENT);
    }
    if (newMetadata != null) body.add(newMetadata);
    Metadata md = newMetadata != null ? newMetadata : readSnapshot.metadata();
    for (Action a : actions) {
      if (a instanceof Metadata || a instanceof Protocol || a instanceof CommitInfo) {
        throw new IllegalArgumentException("use updateMetadata(); commit() adds protocol and commitInfo itself");
      }
      if (a instanceof AddFile f && !f.partitionValues().keySet().equals(new HashSet<>(md.partitionColumns()))) {
        throw new IllegalArgumentException("file " + f.path() + " has partition values " + f.partitionValues()
            + " but the table is partitioned by " + md.partitionColumns());
      }
      body.add(a);
    }
    return body;
  }

  /** Read nothing, delete nothing, change no metadata: such a commit can never invalidate itself. */
  private boolean isBlindAppend(List<Action> body) {
    return !readWholeTable && readPredicates.isEmpty() && readFiles.isEmpty() && newMetadata == null
        && body.stream().allMatch(a -> a instanceof AddFile f && f.dataChange());
  }

  private void checkConflicts(long version, List<Action> winner, List<Action> mine) {
    Set<String> myRemoves = new HashSet<>(), myAppIds = new HashSet<>(readAppIds);
    for (Action a : mine) {
      if (a instanceof RemoveFile r) myRemoves.add(r.path());
      if (a instanceof SetTransaction t) myAppIds.add(t.appId());
    }
    for (Action a : winner) {
      switch (a) {
        case Metadata m -> throw new MetadataChangedException("metadata changed", version);
        case Protocol p -> throw new ProtocolChangedException("protocol changed", version);
        case AddFile f -> {
          if (f.dataChange() && (readWholeTable || readPredicates.stream()
              .anyMatch(p -> p.mightMatch(readSnapshot.view(f))))) {
            throw new ConcurrentAppendException("concurrent append of " + f.path()
                + " matches data read by this transaction", version);
          }
        }
        case RemoveFile r -> {
          if (readFiles.contains(r.path())) {
            throw new ConcurrentDeleteReadException("read file " + r.path() + " was removed", version);
          }
          if (myRemoves.contains(r.path())) {
            throw new ConcurrentDeleteDeleteException("file " + r.path() + " removed twice", version);
          }
        }
        case SetTransaction t -> {
          if (myAppIds.contains(t.appId())) {
            throw new ConcurrentTransactionException("app " + t.appId() + " committed concurrently", version);
          }
        }
        case CommitInfo c -> { }
      }
    }
  }

  private void maybeCheckpoint(long version) {
    Metadata md = newMetadata != null ? newMetadata : readSnapshot.metadata();
    int interval = Integer.parseInt(md.configuration().getOrDefault(
        MiniLakeTable.CHECKPOINT_INTERVAL, String.valueOf(MiniLakeTable.DEFAULT_CHECKPOINT_INTERVAL)));
    if (version == 0 || version % interval != 0) return;
    try {
      table.checkpoint(table.snapshotAt(version));
    } catch (RuntimeException e) {
      // The commit is already durable; a missing checkpoint only costs replay time.
      System.getLogger(OptimisticTransaction.class.getName())
          .log(System.Logger.Level.WARNING, "checkpoint at version " + version + " failed", e);
    }
  }
}
