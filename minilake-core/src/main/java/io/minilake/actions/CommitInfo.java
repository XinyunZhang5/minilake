package io.minilake.actions;

import java.util.Map;

/** Provenance of a commit. Its timestamp drives timestamp-based time travel. */
public record CommitInfo(
    long timestamp,
    String operation,
    Map<String, String> operationParameters,
    long readVersion,
    boolean isBlindAppend) implements Action {

  public CommitInfo {
    operationParameters = operationParameters == null ? Map.of() : Map.copyOf(operationParameters);
  }
}
