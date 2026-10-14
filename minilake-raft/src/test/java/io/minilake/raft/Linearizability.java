package io.minilake.raft;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * Wing &amp; Gong's linearizability check with Lowe's memoization, for a put-if-absent register per
 * key. Linearizability is compositional, so each key is checked on its own, which keeps the
 * search tiny even for long histories.
 *
 * <p>An operation whose reply never arrived may have taken effect at any point after it was
 * invoked, or not at all: the search may place it anywhere after its call, accepting any result,
 * or leave it out.
 */
final class Linearizability {
  enum Kind { PUT, READ }

  /** {@code ret == Long.MAX_VALUE} and {@code result == null} for an operation that never completed. */
  record Op(int client, String key, Kind kind, String value, long call, long ret, String result) {
    boolean completed() { return ret != Long.MAX_VALUE; }
  }

  static final String OK = "ok", EXISTS = "exists";

  static Optional<String> check(List<Op> history) {
    Map<String, List<Op>> byKey = new TreeMap<>();
    for (Op op : history) byKey.computeIfAbsent(op.key(), k -> new ArrayList<>()).add(op);
    for (var e : byKey.entrySet()) {
      if (!new Search(e.getValue()).run()) {
        StringBuilder sb = new StringBuilder("not linearizable on key " + e.getKey() + ":\n");
        e.getValue().stream().sorted(java.util.Comparator.comparingLong(Op::call))
            .forEach(op -> sb.append("  ").append(op).append('\n'));
        return Optional.of(sb.toString());
      }
    }
    return Optional.empty();
  }

  private static final class Search {
    private final List<Op> ops;
    private final int completed;
    private final Set<Object> seen = new HashSet<>();

    Search(List<Op> ops) {
      this.ops = ops;
      this.completed = (int) ops.stream().filter(Op::completed).count();
    }

    boolean run() { return dfs(new BitSet(ops.size()), null, 0); }

    private boolean dfs(BitSet done, String state, int completedDone) {
      if (completedDone == completed) return true;
      if (!seen.add(List.of(done.clone(), Objects.toString(state, "\0")))) return false;
      long minRet = Long.MAX_VALUE;
      for (int i = 0; i < ops.size(); i++) if (!done.get(i)) minRet = Math.min(minRet, ops.get(i).ret());
      for (int i = 0; i < ops.size(); i++) {
        Op op = ops.get(i);
        // Only an op invoked before every remaining op has returned can be the next to take effect.
        if (done.get(i) || op.call() > minRet) continue;
        String next = state;
        String result;
        if (op.kind() == Kind.PUT) {
          result = state == null ? OK : EXISTS;
          if (state == null) next = op.value();
        } else {
          result = Objects.toString(state, "null");
        }
        if (op.completed() && !op.result().equals(result)) continue;
        done.set(i);
        if (dfs(done, next, completedDone + (op.completed() ? 1 : 0))) return true;
        done.clear(i);
      }
      return false;
    }
  }

  private Linearizability() {}
}
