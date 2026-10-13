package io.minilake.schema;

import io.minilake.actions.ActionCodec;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;

public record Schema(List<Field> fields) {

  public Schema {
    fields = List.copyOf(fields);
  }

  public static Schema of(Field... fields) {
    return new Schema(List.of(fields));
  }

  public Optional<Field> field(String name) {
    return fields.stream().filter(f -> f.name().equals(name)).findFirst();
  }

  public DataType typeOf(String column) {
    return field(column)
        .orElseThrow(() -> new IllegalArgumentException("unknown column: " + column))
        .type();
  }

  public String toJson() {
    try {
      return ActionCodec.MAPPER.writeValueAsString(this);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static Schema fromJson(String json) {
    try {
      return ActionCodec.MAPPER.readValue(json, Schema.class);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
