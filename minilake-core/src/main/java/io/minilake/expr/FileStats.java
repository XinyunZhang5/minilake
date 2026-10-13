package io.minilake.expr;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.minilake.actions.ActionCodec;
import io.minilake.schema.Field;
import io.minilake.schema.Schema;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

/** Parsed form of {@code AddFile.stats}. Columns missing from the JSON are {@link ColumnStats#UNKNOWN}. */
public record FileStats(Long numRecords, Map<String, ColumnStats> columns) {
  public static final FileStats EMPTY = new FileStats(null, Map.of());

  public ColumnStats column(String name) {
    return columns.getOrDefault(name, new ColumnStats(null, null, null, numRecords));
  }

  public static FileStats parse(String json, Schema schema) {
    if (json == null || json.isBlank()) return EMPTY;
    try {
      JsonNode root = ActionCodec.MAPPER.readTree(json);
      Long n = root.hasNonNull("numRecords") ? root.get("numRecords").asLong() : null;
      JsonNode mins = root.path("minValues"), maxs = root.path("maxValues"), nulls = root.path("nullCount");
      Map<String, ColumnStats> cols = new HashMap<>();
      for (Field f : schema.fields()) {
        String c = f.name();
        if (!mins.has(c) && !maxs.has(c) && !nulls.has(c)) continue;
        cols.put(c, new ColumnStats(
            f.type().fromJson(mins.get(c)),
            f.type().fromJson(maxs.get(c)),
            nulls.hasNonNull(c) ? nulls.get(c).asLong() : null,
            n));
      }
      return new FileStats(n, cols);
    } catch (IOException e) {
      throw new UncheckedIOException("bad stats: " + json, e);
    }
  }

  /** Serializes stats; values must be JSON-friendly (numbers, strings, booleans, ISO dates as strings). */
  public static String toJson(long numRecords, Map<String, ?> min, Map<String, ?> max, Map<String, Long> nullCount) {
    ObjectNode root = ActionCodec.MAPPER.createObjectNode();
    root.put("numRecords", numRecords);
    root.set("minValues", ActionCodec.MAPPER.valueToTree(stringifyDates(min)));
    root.set("maxValues", ActionCodec.MAPPER.valueToTree(stringifyDates(max)));
    root.set("nullCount", ActionCodec.MAPPER.valueToTree(nullCount));
    return root.toString();
  }

  private static Map<String, Object> stringifyDates(Map<String, ?> m) {
    Map<String, Object> out = new HashMap<>();
    m.forEach((k, v) -> out.put(k, v instanceof java.time.temporal.TemporalAccessor ? v.toString() : v));
    return out;
  }
}
