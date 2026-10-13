package io.minilake.actions;

import java.util.List;
import java.util.Map;

/** Table identity, schema and partitioning. Changing any of it conflicts with every concurrent writer. */
public record Metadata(
    String id,
    String schemaString,
    List<String> partitionColumns,
    Map<String, String> configuration,
    long createdTime) implements Action {

  public Metadata {
    partitionColumns = partitionColumns == null ? List.of() : List.copyOf(partitionColumns);
    configuration = configuration == null ? Map.of() : Map.copyOf(configuration);
  }
}
