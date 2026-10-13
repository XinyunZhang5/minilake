package io.minilake.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.LocalDate;
import java.util.Locale;

/**
 * Column types. Values are normalized to one comparable Java type per family (all integers to Long,
 * all floats to Double) so stats from Parquet, literals from Spark and partition strings compare
 * without per-pair casting rules.
 */
public enum DataType {
  INT, LONG, DOUBLE, STRING, BOOLEAN, DATE;

  @JsonValue
  public String jsonName() {
    return name().toLowerCase(Locale.ROOT);
  }

  @JsonCreator
  public static DataType fromJson(String s) {
    return valueOf(s.toUpperCase(Locale.ROOT));
  }

  /** @return a Comparable in this type's normalized form, or null */
  @SuppressWarnings("rawtypes")
  public Comparable normalize(Object v) {
    if (v == null) return null;
    return switch (this) {
      case INT, LONG -> v instanceof Number n ? (Comparable) n.longValue() : Long.parseLong(v.toString());
      case DOUBLE -> v instanceof Number n ? (Comparable) n.doubleValue() : Double.parseDouble(v.toString());
      case STRING -> v.toString();
      case BOOLEAN -> v instanceof Boolean b ? b : Boolean.parseBoolean(v.toString());
      case DATE -> v instanceof LocalDate d ? d
          : v instanceof java.sql.Date d ? d.toLocalDate()
          : LocalDate.parse(v.toString());
    };
  }

  @SuppressWarnings("rawtypes")
  public Comparable fromJson(JsonNode node) {
    if (node == null || node.isNull()) return null;
    return switch (this) {
      case INT, LONG -> node.asLong();
      case DOUBLE -> node.asDouble();
      case BOOLEAN -> node.asBoolean();
      case STRING, DATE -> normalize(node.asText());
    };
  }
}
