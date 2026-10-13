package io.minilake.expr;

import io.minilake.schema.Schema;
import java.util.Arrays;
import java.util.List;

/**
 * A row filter evaluated conservatively against a whole file: {@link #mightMatch} returns false
 * only when stats prove no row can satisfy it. The same check powers data skipping (reads) and
 * conflict detection (did a concurrent writer add rows my read would have seen?).
 *
 * <p>There is deliberately no Not node: "might match" does not compose under negation
 * (not mightMatch(p) is not mightMatch(not p)), so {@link #negate} pushes negation down to leaves
 * instead.
 */
public sealed interface Predicate {

  boolean mightMatch(FileView file);

  Predicate negate();

  /** Coerces literals to the column types of {@code schema}; unknown columns fail fast. */
  Predicate bind(Schema schema);

  Predicate TRUE = new Const(true);
  Predicate FALSE = new Const(false);

  static Predicate eq(String c, Object v) { return new Compare(c, Op.EQ, v); }
  static Predicate neq(String c, Object v) { return new Compare(c, Op.NEQ, v); }
  static Predicate lt(String c, Object v) { return new Compare(c, Op.LT, v); }
  static Predicate le(String c, Object v) { return new Compare(c, Op.LE, v); }
  static Predicate gt(String c, Object v) { return new Compare(c, Op.GT, v); }
  static Predicate ge(String c, Object v) { return new Compare(c, Op.GE, v); }
  static Predicate isNull(String c) { return new NullCheck(c, true); }
  static Predicate isNotNull(String c) { return new NullCheck(c, false); }

  static Predicate and(Predicate... ps) { return new And(List.of(ps)); }
  static Predicate or(Predicate... ps) { return new Or(List.of(ps)); }

  static Predicate in(String c, Object... values) {
    return new Or(Arrays.stream(values).map(v -> (Predicate) eq(c, v)).toList());
  }

  enum Op {
    EQ, NEQ, LT, LE, GT, GE;

    Op negate() {
      return switch (this) {
        case EQ -> NEQ; case NEQ -> EQ;
        case LT -> GE;  case GE -> LT;
        case LE -> GT;  case GT -> LE;
      };
    }
  }

  record Const(boolean value) implements Predicate {
    public boolean mightMatch(FileView f) { return value; }
    public Predicate negate() { return value ? FALSE : TRUE; }
    public Predicate bind(Schema schema) { return this; }
  }

  /**
   * column op literal. The literal must already be normalized to the column's type
   * (see {@link io.minilake.schema.DataType#normalize}); {@code Snapshot.normalize} does that.
   */
  record Compare(String column, Op op, Object value) implements Predicate {
    @SuppressWarnings({"rawtypes", "unchecked"})
    public boolean mightMatch(FileView f) {
      ColumnStats s = f.column(column);
      if (value == null) return false; // SQL: x op NULL is never true
      if (s.numRecords() != null && s.nullCount() != null
          && s.nullCount().equals(s.numRecords())) {
        return false; // all-null column: every comparison is NULL
      }
      Comparable lo = s.min(), hi = s.max();
      Comparable v = (Comparable) value;
      return switch (op) {
        case EQ -> (lo == null || lo.compareTo(v) <= 0) && (hi == null || hi.compareTo(v) >= 0);
        case NEQ -> lo == null || hi == null || !(lo.compareTo(v) == 0 && hi.compareTo(v) == 0);
        case LT -> lo == null || lo.compareTo(v) < 0;
        case LE -> lo == null || lo.compareTo(v) <= 0;
        case GT -> hi == null || hi.compareTo(v) > 0;
        case GE -> hi == null || hi.compareTo(v) >= 0;
      };
    }

    public Predicate negate() {
      // Under SQL three-valued logic NOT (c < v) drops NULL rows exactly like c >= v does,
      // so flipping the operator is exact.
      return new Compare(column, op.negate(), value);
    }

    public Predicate bind(Schema schema) {
      return new Compare(column, op, schema.typeOf(column).normalize(value));
    }
  }

  record NullCheck(String column, boolean isNull) implements Predicate {
    public boolean mightMatch(FileView f) {
      ColumnStats s = f.column(column);
      if (s.nullCount() == null) return true;
      if (isNull) return s.nullCount() > 0;
      return s.numRecords() == null || s.nullCount() < s.numRecords();
    }

    public Predicate negate() { return new NullCheck(column, !isNull); }
    public Predicate bind(Schema schema) { schema.typeOf(column); return this; }
  }

  record And(List<Predicate> children) implements Predicate {
    public And { children = List.copyOf(children); }
    public boolean mightMatch(FileView f) { return children.stream().allMatch(c -> c.mightMatch(f)); }
    public Predicate negate() { return new Or(children.stream().map(Predicate::negate).toList()); }
    public Predicate bind(Schema s) { return new And(children.stream().map(c -> c.bind(s)).toList()); }
  }

  record Or(List<Predicate> children) implements Predicate {
    public Or { children = List.copyOf(children); }
    public boolean mightMatch(FileView f) { return children.stream().anyMatch(c -> c.mightMatch(f)); }
    public Predicate negate() { return new And(children.stream().map(Predicate::negate).toList()); }
    public Predicate bind(Schema s) { return new Or(children.stream().map(c -> c.bind(s)).toList()); }
  }
}
