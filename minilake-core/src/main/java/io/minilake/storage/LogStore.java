package io.minilake.storage;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.List;

/**
 * The only storage primitive the protocol needs for correctness: an atomic, mutually exclusive
 * "create this file unless it exists" ({@link #writeIfAbsent}). Two writers racing for version N
 * must see exactly one winner, and readers must never observe a partially written file.
 */
public interface LogStore {

  byte[] read(String path) throws IOException;

  /** @throws FileAlreadyExistsException if {@code path} already exists; nothing is written then */
  void writeIfAbsent(String path, byte[] data) throws IOException;

  /** Best-effort replace; only used for derived, idempotent files (checkpoints, hints). */
  void overwrite(String path, byte[] data) throws IOException;

  /** File names (not paths) directly under {@code dir}, sorted; empty if the directory is missing. */
  List<String> list(String dir) throws IOException;

  boolean exists(String path) throws IOException;
}
