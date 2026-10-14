package io.minilake.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.minilake.raft.Simulation.Faults;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class SimulationTest {
  static final RaftConfig CONFIG = RaftConfig.defaults().withSnapshotEvery(20);

  static Optional<String> runSeed(long seed, int nodes, RaftConfig config, Faults faults, boolean dedup) {
    Simulation sim = new Simulation(seed, nodes, 4, 40, config, faults, dedup);
    return sim.run(6000).or(() -> sim.settle(6000));
  }

  @Test
  void healthyClusterElectsOneLeaderAndServesEveryone() {
    Simulation sim = new Simulation(1, CONFIG, Faults.NONE);
    assertEquals(Optional.empty(), sim.run(3000).or(() -> sim.settle(3000)));
    assertEquals(1, sim.leaderChanges, "no faults, no re-elections");
    assertEquals(4 * 40 + countKeys(sim), sim.history.size());
  }

  private static long countKeys(Simulation sim) {
    return sim.history.stream().map(Linearizability.Op::key).distinct().count();
  }

  @Test
  void survivesHarshFaultsAcross300Seeds() {
    int leaderChanges = 0, crashes = 0, snapshots = 0;
    for (long seed = 0; seed < 300; seed++) {
      int nodes = seed % 3 == 0 ? 5 : 3;
      Simulation sim = new Simulation(seed, nodes, 4, 40, CONFIG, Faults.HARSH, true);
      Optional<String> bad = sim.run(6000).or(() -> sim.settle(6000));
      if (bad.isPresent()) fail(bad.get());
      leaderChanges += sim.leaderChanges;
      crashes += sim.crashes;
      snapshots += sim.snapshotsInstalled;
    }
    System.out.printf("300 seeds: %d leader elections, %d crashes, %d snapshot installs, all safe and linearizable%n",
        leaderChanges, crashes, snapshots);
    assertTrue(leaderChanges > 600, "the faults actually forced re-elections");
    assertTrue(snapshots > 0, "lagging followers were caught up by InstallSnapshot");
  }

  @Test
  void sameSeedSameRun() {
    Simulation a = new Simulation(42, CONFIG, Faults.HARSH), b = new Simulation(42, CONFIG, Faults.HARSH);
    a.run(4000);
    b.run(4000);
    assertEquals(a.history, b.history);
    assertEquals(a.leaderChanges, b.leaderChanges);
  }

  /** Mutation test: the simulator must catch the commit rule Figure 8 of the Raft paper warns about. */
  @Test
  void catchesCommittingOldTermEntriesByCount() {
    RaftConfig broken = CONFIG.withUnsafeCommitByCount();
    for (long seed = 0; seed < 3000; seed++) {
      Optional<String> bad = runSeed(seed, 5, broken, Faults.HARSH, true);
      if (bad.isPresent()) {
        System.out.println("unsafe commit rule caught at " + bad.get().lines().findFirst().orElse(""));
        return;
      }
    }
    fail("no seed exposed the unsafe commit rule");
  }

  /** Mutation test: without client sessions a retried commit can be told it lost after it won. */
  @Test
  void catchesMissingRetryDeduplication() {
    for (long seed = 0; seed < 300; seed++) {
      Optional<String> bad = runSeed(seed, 3, CONFIG, Faults.HARSH, false);
      if (bad.isPresent()) {
        System.out.println("missing dedup caught at " + bad.get().lines().findFirst().orElse(""));
        return;
      }
    }
    fail("no seed exposed missing deduplication");
  }
}
