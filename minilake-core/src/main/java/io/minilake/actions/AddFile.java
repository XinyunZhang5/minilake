package io.minilake.actions;

import java.util.Map;

/**
 * A data file that is part of the table from this version on.
 *
 * @param path relative to the table root
 * @param stats JSON: {"numRecords":N,"minValues":{..},"maxValues":{..},"nullCount":{..}}, may be null
 */
public record AddFile(
    String path,
    Map<String, String> partitionValues,
    long size,
    long modificationTime,
    boolean dataChange,
    String stats) implements Action {

  public AddFile {
    partitionValues = partitionValues == null ? Map.of() : java.util.Collections.unmodifiableMap(
        new java.util.LinkedHashMap<>(partitionValues));
  }

  public RemoveFile remove(long timestamp, boolean dataChange) {
    return new RemoveFile(path, timestamp, dataChange, partitionValues);
  }
}
