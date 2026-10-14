package io.minilake.raft;

public record Entry(long term, long index, Command command) {}
