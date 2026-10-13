package io.minilake.expr;

/** A file as seen by predicate evaluation: per-column stats only, never row data. */
@FunctionalInterface
public interface FileView {
  ColumnStats column(String name);
}
