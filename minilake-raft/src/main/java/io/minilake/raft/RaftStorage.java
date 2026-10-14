package io.minilake.raft;

import java.util.ArrayList;
import java.util.List;

/**
 * A node's persistent state: term, vote, log and latest snapshot. Raft is only safe if these
 * survive a crash, so a restarted node is rebuilt from the same storage object. This one keeps
 * everything in memory and stands in for a disk that is fsynced before any message goes out.
 *
 * <p>The log is stored after the snapshot: entries cover {@code snapshotIndex + 1 .. lastIndex()}.
 * Index 0 is the empty prefix, with term 0.
 */
public final class RaftStorage {
  long currentTerm;
  Integer votedFor;
  long snapshotIndex;
  long snapshotTerm;
  byte[] snapshotData;
  private final List<Entry> entries = new ArrayList<>();

  public long lastIndex() { return snapshotIndex + entries.size(); }

  public long lastTerm() { return entries.isEmpty() ? snapshotTerm : entries.get(entries.size() - 1).term(); }

  public long currentTerm() { return currentTerm; }

  public long snapshotIndex() { return snapshotIndex; }

  /** Term of entry {@code i}; only valid for {@code snapshotIndex <= i <= lastIndex()}. */
  public long termAt(long i) {
    if (i == snapshotIndex) return snapshotTerm;
    return entry(i).term();
  }

  public Entry entry(long i) {
    if (i <= snapshotIndex || i > lastIndex()) {
      throw new IndexOutOfBoundsException("entry " + i + " not in log (" + snapshotIndex + ", " + lastIndex() + "]");
    }
    return entries.get((int) (i - snapshotIndex - 1));
  }

  /** Entries {@code from .. min(to, lastIndex)}, inclusive. */
  public List<Entry> slice(long from, long to) {
    long end = Math.min(to, lastIndex());
    if (from > end) return List.of();
    return List.copyOf(entries.subList((int) (from - snapshotIndex - 1), (int) (end - snapshotIndex)));
  }

  void append(Entry e) {
    if (e.index() != lastIndex() + 1) throw new IllegalStateException("append " + e.index() + " after " + lastIndex());
    entries.add(e);
  }

  /** Drops entry {@code i} and everything after it. */
  void truncateFrom(long i) {
    if (i <= snapshotIndex) throw new IllegalStateException("cannot truncate committed entry " + i);
    while (lastIndex() >= i) entries.remove(entries.size() - 1);
  }

  /** Replaces the prefix up to {@code index} with a snapshot, keeping later entries that still match. */
  void installSnapshot(long index, long term, byte[] data) {
    if (index <= snapshotIndex) return;
    List<Entry> keep = new ArrayList<>();
    if (index < lastIndex() && termAt(index) == term) keep.addAll(slice(index + 1, lastIndex()));
    entries.clear();
    entries.addAll(keep);
    snapshotIndex = index;
    snapshotTerm = term;
    snapshotData = data;
  }
}
