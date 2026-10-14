package io.minilake.raft;

import io.minilake.storage.LogStore;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Spreads tables over several independent Raft groups. Every file of one table's log goes to
 * the same group (chosen by a hash of the table root), so a table's commits are still ordered by
 * a single Raft log, while different tables commit in parallel and fail independently: losing
 * the majority of one group stalls only the tables mapped to it.
 *
 * <p>The mapping is a plain hash modulo the group count, so changing the number of groups would
 * move tables and needs a migration this project does not implement.
 */
public final class ShardedLogStore implements LogStore {
  private final List<CoordinatedLogStore> groups;

  /** One client per group; like {@link CoordinatedLogStore}, one instance per writer thread. */
  public ShardedLogStore(List<CoordinatedLogStore> groups) {
    if (groups.isEmpty()) throw new IllegalArgumentException("need at least one group");
    this.groups = List.copyOf(groups);
  }

  /** The table a log path belongs to: everything before {@code /_log}. */
  static String tableRoot(String path) {
    int i = path.indexOf("/_log/");
    if (i >= 0) return path.substring(0, i);
    return path.endsWith("/_log") ? path.substring(0, path.length() - "/_log".length()) : path;
  }

  /** Stable across processes and JVM versions, unlike relying on an object's hash code. */
  public static int groupOf(String path, int groupCount) {
    CRC32 crc = new CRC32();
    crc.update(tableRoot(path).getBytes(StandardCharsets.UTF_8));
    return (int) (crc.getValue() % groupCount);
  }

  private CoordinatedLogStore route(String path) {
    return groups.get(groupOf(path, groups.size()));
  }

  @Override public byte[] read(String path) throws IOException { return route(path).read(path); }

  @Override public void writeIfAbsent(String path, byte[] data) throws IOException {
    route(path).writeIfAbsent(path, data);
  }

  @Override public void overwrite(String path, byte[] data) throws IOException { route(path).overwrite(path, data); }

  @Override public List<String> list(String dir) throws IOException { return route(dir).list(dir); }

  @Override public boolean exists(String path) throws IOException { return route(path).exists(path); }
}
