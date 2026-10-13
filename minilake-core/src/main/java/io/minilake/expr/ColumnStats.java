package io.minilake.expr;

/**
 * What is known about one column inside one file. Any field may be null = unknown.
 * A partition column is the degenerate case min == max == its partition value.
 */
@SuppressWarnings("rawtypes")
public record ColumnStats(Comparable min, Comparable max, Long nullCount, Long numRecords) {
  public static final ColumnStats UNKNOWN = new ColumnStats(null, null, null, null);
}
