#!/usr/bin/env bash
# Runs the benchmark suites (excluded from `mvn test`) and collects their tables.
set -euo pipefail
cd "$(dirname "$0")"
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@21}"
rm -rf minilake-spark/target/benchmarks minilake-raft/target/benchmarks
mvn -q install -DskipTests
mvn -q -pl minilake-spark test \
  -Dminilake.excludeTags=none \
  -Dsuites="${1:-io.minilake.spark.bench.SkippingBenchmark,io.minilake.spark.bench.SkewJoinBenchmark}"
if [[ -z "${1:-}" ]]; then
  mvn -q -pl minilake-raft surefire:test -Dtest=ShardingBenchmark -DexcludedGroups=none
fi
cat minilake-spark/target/benchmarks/*.md minilake-raft/target/benchmarks/*.md 2>/dev/null
