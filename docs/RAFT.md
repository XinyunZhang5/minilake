# Raft commit coordinator

`minilake-raft` replaces "the file system's link(2) decides who commits version N" with "a
Raft-replicated majority decides". MiniLake's protocol is unchanged: `CoordinatedLogStore`
implements the same `LogStore` interface, so `MiniLakeTable.forPath(path, store)` gets a
transaction log that survives losing any minority of coordinator nodes. Data files stay where
they are; only the `_log/` files move into the replicated state.

## Design

**The node is a pure state machine.** `RaftNode` never reads a clock, sleeps or does I/O. A driver
calls `tick()` and `handle(message)` and delivers the messages they return. Two drivers use the
same node code:

- `Simulation` (tests): a deterministic, seeded simulation with a virtual clock.
- `LocalCluster`: one thread per node and an in-memory network, used by `CoordinatedLogStore` and
  the integration tests.

**Replicated state** (`FileMapStateMachine`) is a map from path to bytes, with operations
`PutIfAbsent`, `Overwrite`, `Read` and `List`. Reads go through the log too, so every operation
is linearizable.

**Client sessions make retries safe.** Every command carries `(clientId, seq)`, and the state
machine remembers each client's last sequence number and result. This matters most for a commit:
if the put-if-absent commits but the reply is lost, the retry is answered "ok" from the session.
A plain re-apply would answer "already exists", and the writer would think it lost the race for a
version it actually won. A mutation test shows the checker catches this when dedup is off.

**Implemented:**
- elections with randomized timeouts
- log replication with conflict-term backtracking
- a no-op entry appended by each new leader, so entries from earlier terms get committed (§5.4.2)
- out-of-order and duplicate message handling
- snapshots every N applied entries, and `InstallSnapshot` for followers that fell behind the
  leader's snapshot
- crash/restart that rebuilds volatile state from the snapshot

**Simplified on purpose:**
- the network is in-process, with no TCP
- `RaftStorage` is in memory and stands in for a disk that is fsynced before replying
- reads go through the log, with no ReadIndex or leases
- membership is fixed, with no joint consensus or PreVote
- snapshots are sent in one chunk

## Testing

**Deterministic simulation.** Each seed runs 3 or 5 nodes and 4 clients for 6000 ticks under these
faults:
- 10% of messages dropped and 5% duplicated
- random delays of 1 to 8 ticks, which reorders messages
- random crashes and targeted leader kills (a majority always stays up)
- random two-way network partitions

Every node restarts over its surviving storage. After every event the simulator checks:

| property | check |
|---|---|
| Election safety | at most one leader per term |
| Leader completeness | a new leader's log contains every entry applied anywhere so far |
| State machine safety | every node applies the same entry at a given index |
| Linearizability | Wing & Gong search with Lowe's memoization, per key (linearizability is compositional), over the full client history plus a final read of every key |

The clients act like MiniLake committers: each one races the others to put-if-absent the next
log version, and sometimes reads earlier versions back.

300 seeds, about 4 s in total: 10,346 elections, 6,104 crashes, 14,297 snapshot installs, and no
violations. A failing seed replays exactly; `sameSeedSameRun` checks that.

**Mutation tests** show the checks have teeth:

| deliberate bug | caught by | first failing seed |
|---|---|---|
| commit earlier-term entries by counting replicas, with no leader no-op (Raft paper, Figure 8) | leader completeness: new leader lacks a committed entry | 1108 |
| no client sessions, so retries re-apply | linearizability: a put answered "exists" whose value is what the key holds | 0 |

Turning the Figure 8 bug into a test took one extra step. With the no-op in place, the wrong
commit rule almost never matters: the no-op is replicated together with the old entries, so a
majority that holds them also holds a current-term entry. 3000 seeds found nothing until the
mutation also dropped the no-op.

**Real threads.** `CoordinatedLogStoreTest` runs a 3-node `LocalCluster` with 5 ms ticks under 4
MiniLake writers. Each writer does read-modify-write increments of a counter through
`OptimisticTransaction`, while a chaos thread kills the current leader every 150 ms and restarts
it 100 ms later. Typical run: 400 increments, about 900 OCC conflicts retried, 11 to 20 leaders
killed. The final value is exactly 400 at version 400: no lost updates and no version gaps.

Bug found on the way: `kill()` joined the node thread while holding the cluster lock that the
node's send path also took, a lock-order deadlock that the first leader kill triggered. Sends are
now lock-free (`AtomicReferenceArray`). Kill and restart still serialize, so a restarted node never
shares storage with a thread that hasn't exited yet.
