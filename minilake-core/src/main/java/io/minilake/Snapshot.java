package io.minilake;

import io.minilake.actions.Action;
import io.minilake.actions.AddFile;
import io.minilake.actions.CommitInfo;
import io.minilake.actions.Metadata;
import io.minilake.actions.Protocol;
import io.minilake.actions.RemoveFile;
import io.minilake.actions.SetTransaction;
import io.minilake.expr.ColumnStats;
import io.minilake.expr.FileStats;
import io.minilake.expr.FileView;
import io.minilake.expr.Predicate;
import io.minilake.schema.Schema;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** The immutable state of a table at one version: the result of replaying the log up to it. */
public final class Snapshot {
  private final long version;
  private final Protocol protocol;
  private final Metadata metadata;
  private final Schema schema;
  private final Map<String, AddFile> files;
  private final Map<String, SetTransaction> appTxns;

  private Snapshot(long version, Protocol protocol, Metadata metadata,
      Map<String, AddFile> files, Map<String, SetTransaction> appTxns) {
    this.version = version;
    this.protocol = protocol;
    this.metadata = metadata;
    this.schema = metadata == null ? null : Schema.fromJson(metadata.schemaString());
    this.files = Collections.unmodifiableMap(files);
    this.appTxns = Collections.unmodifiableMap(appTxns);
  }

  public long version() { return version; }

  /** False for the "version -1" snapshot of a path that has no commits yet. */
  public boolean exists() { return metadata != null; }

  public Protocol protocol() { return protocol; }
  public Metadata metadata() { return metadata; }
  public Schema schema() { return schema; }

  public Collection<AddFile> allFiles() { return files.values(); }

  public int numFiles() { return files.size(); }

  public boolean contains(String path) { return files.containsKey(path); }

  /** Files that may contain rows matching {@code filter}; all others are skipped without reading. */
  public List<AddFile> files(Predicate filter) {
    if (filter == Predicate.TRUE) return List.copyOf(files.values());
    Predicate bound = filter.bind(schema);
    List<AddFile> out = new ArrayList<>();
    for (AddFile f : files.values()) {
      if (bound.mightMatch(view(f))) out.add(f);
    }
    return out;
  }

  public Optional<Long> txnVersion(String appId) {
    return Optional.ofNullable(appTxns.get(appId)).map(SetTransaction::version);
  }

  Collection<SetTransaction> appTxns() { return appTxns.values(); }

  /**
   * Stats view of one file. Partition columns are exact (min = max = the partition value) and
   * win over whatever the file's own stats say.
   */
  FileView view(AddFile f) {
    FileStats stats = FileStats.parse(f.stats(), schema);
    return column -> {
      if (metadata.partitionColumns().contains(column)) {
        Object v = schema.typeOf(column).normalize(f.partitionValues().get(column));
        Long n = stats.numRecords();
        return v == null
            ? new ColumnStats(null, null, n == null ? 1L : n, n == null ? 1L : n)
            : new ColumnStats((Comparable<?>) v, (Comparable<?>) v, 0L, n);
      }
      return stats.column(column);
    };
  }

  /** Replays actions into table state. Package-private: only the log builds snapshots. */
  static final class Builder {
    private Protocol protocol;
    private Metadata metadata;
    private final Map<String, AddFile> files = new LinkedHashMap<>();
    private final Map<String, SetTransaction> appTxns = new HashMap<>();

    Builder apply(List<Action> actions) {
      for (Action a : actions) {
        switch (a) {
          case Protocol p -> protocol = p;
          case Metadata m -> metadata = m;
          case AddFile f -> files.put(f.path(), f);
          case RemoveFile r -> files.remove(r.path());
          case SetTransaction t -> appTxns.put(t.appId(), t);
          case CommitInfo c -> { }
        }
      }
      return this;
    }

    Snapshot build(long version) {
      return new Snapshot(version, protocol, metadata, new LinkedHashMap<>(files), new HashMap<>(appTxns));
    }
  }
}
