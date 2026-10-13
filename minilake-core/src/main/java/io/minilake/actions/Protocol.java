package io.minilake.actions;

public record Protocol(int minReaderVersion, int minWriterVersion) implements Action {
  public static final Protocol CURRENT = new Protocol(1, 1);
}
