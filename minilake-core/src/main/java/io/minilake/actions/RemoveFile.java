package io.minilake.actions;

import java.util.Map;

/** Logically deletes a file. The bytes stay on disk until a vacuum, so time travel keeps working. */
public record RemoveFile(
    String path,
    long deletionTimestamp,
    boolean dataChange,
    Map<String, String> partitionValues) implements Action {

  public RemoveFile {
    partitionValues = partitionValues == null ? Map.of() : java.util.Collections.unmodifiableMap(
        new java.util.LinkedHashMap<>(partitionValues));
  }
}
