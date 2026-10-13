package io.minilake.actions;

/** Idempotency marker: "application appId has committed up to version". Used for exactly-once streaming. */
public record SetTransaction(String appId, long version, long lastUpdated) implements Action {}
