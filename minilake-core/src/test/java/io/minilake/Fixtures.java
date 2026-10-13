package io.minilake;

import io.minilake.actions.AddFile;
import io.minilake.actions.Metadata;
import io.minilake.expr.FileStats;
import io.minilake.schema.DataType;
import io.minilake.schema.Field;
import io.minilake.schema.Schema;
import java.util.List;
import java.util.Map;
import java.util.UUID;

final class Fixtures {
  static final Schema SCHEMA = Schema.of(
      new Field("id", DataType.LONG, false),
      new Field("name", DataType.STRING, true),
      new Field("part", DataType.STRING, true));

  static Metadata metadata(Map<String, String> conf) {
    return new Metadata(UUID.randomUUID().toString(), SCHEMA.toJson(), List.of("part"), conf, 0L);
  }

  static MiniLakeTable create(String path) {
    return create(path, Map.of());
  }

  static MiniLakeTable create(String path, Map<String, String> conf) {
    MiniLakeTable t = MiniLakeTable.forPath(path);
    OptimisticTransaction txn = t.startTransaction();
    txn.updateMetadata(metadata(conf));
    txn.commit(List.of(), "CREATE TABLE");
    return t;
  }

  /** A file in partition {@code part} whose id column spans [minId, maxId]. */
  static AddFile file(String part, long minId, long maxId) {
    String stats = FileStats.toJson(maxId - minId + 1, Map.of("id", minId), Map.of("id", maxId), Map.of("id", 0L));
    return new AddFile("part=" + part + "/" + UUID.randomUUID() + ".parquet",
        Map.of("part", part), 100, 0, true, stats);
  }

  private Fixtures() {}
}
