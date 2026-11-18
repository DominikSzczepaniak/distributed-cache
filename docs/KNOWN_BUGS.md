# Known production bugs

Found while overhauling the test suite. Each entry is written to become one GitHub issue.
Confidence: VERIFIED = reproduced by a test or by running it; INFERRED = reasoned from reading the code.
Severity: Critical / High / Medium / Low.

Not covered here: key TTL, reaper-for-keys, CAS and eviction. They are not implemented yet.

---

## 1. Raft node cannot restart after its second save
- **Severity:** Critical. **Confidence:** VERIFIED.
- **Area:** `pkg/raft` persistence (`persistance.go`).
- **Root cause:** every save appends a new gob stream to the log file. A file written by two saves fails to load with `gob: duplicate type received`, so the node cannot restart.
- **Failing test:** `TestKnownBug_Restart_AfterTwoIncrementalSaves` (`pkg/raft`).
- **Fix needs a decision:** rewrite the whole file on each save, or keep one long-lived encoder. Both change the persisted WAL format, which is a breaking change that needs a migration note.

## 2. Vote or term change without a new log entry is never persisted
- **Severity:** High. **Confidence:** VERIFIED.
- **Area:** `pkg/raft` persistence (`doSaveValues`).
- **Root cause:** `doSaveValues` returns early when no entry was appended, so `currentTerm` and `votedFor` are not written.
- **Impact:** a restarted node can vote twice in the same term, which breaks Election Safety.
- **Failing test:** `TestKnownBug_Restart_VoteWithoutNewLogEntriesIsLost`.

## 3. Raft state is persisted after the RPC reply, and not atomically
- **Severity:** High. **Confidence:** INFERRED.
- **Area:** `pkg/raft` (`go logSaver.SaveValues()`).
- **Root cause:** saving runs in a goroutine after `RequestVote` and `AppendEntries` have replied. The metadata file and the log file are also not written atomically with each other.
- **Impact:** a crash after the reply loses a granted vote or an acknowledged entry. This violates the rule to persist before responding.
- **Related:** issues 1 and 2. They should probably be fixed together.

## 4. Delayed duplicate of snapshot chunk 0 corrupts the installed snapshot
- **Severity:** Medium. **Confidence:** VERIFIED.
- **Area:** `pkg/raft` `InstallSnapshot` (`grpc.go`).
- **Root cause:** a late duplicate of the first chunk truncates the partially written snapshot file. The follower then installs a corrupted snapshot.
- **Failing test:** `TestKnownBug_InstallSnapshot_DelayedDuplicateOfFirstChunkCorruptsState`.
- **Also (INFERRED):** a `Done=true` chunk that arrives before earlier chunks cannot be detected.

## 5. Controller applies `time.Now()` inside the Raft apply path
- **Severity:** High. **Confidence:** VERIFIED (reading `applyRegisterNode`).
- **Area:** `pkg/controller` state machine.
- **Root cause:** `applyRegisterNode` stamps `LastHeartbeat` with the local clock, so replicas diverge.
- **Fix direction:** the leader generates the timestamp at proposal time and replicates it in the log entry.

## 6. Registration can be lost after a rebalance
- **Severity:** High. **Confidence:** VERIFIED (intermittent).
- **Area:** `pkg/controller` (`Rebalance`, `applyUpdateTopology`).
- **Root cause:** the leader broadcasts a full `UpdateTopology` snapshot. If a `RegisterNode` is applied between taking the snapshot and applying it, `applyUpdateTopology` overwrites `c.config`, and the acknowledged registration disappears on every controller. Datanodes do not re-register after a 200, so the node stays missing.
- **Reproduce:** register 5 nodes back-to-back with no waiting.
- **Related:** `applyUpdateTopology` has no epoch monotonicity guard.

## 7. Node failure handling bypasses Raft and is per-controller
- **Severity:** High. **Confidence:** VERIFIED.
- **Area:** `pkg/controller` (`HandleNodeFailure`, `Reaper`).
- **Root cause:** heartbeats go to one controller. When they stop, that controller edits its local config directly, without a Raft proposal.
- **Impact:** which controller declares a node dead depends on where heartbeats landed. Topology can diverge between controllers.

## 8. Shard reassigned to a node with no data when the primary dies without replicas
- **Severity:** High. **Confidence:** VERIFIED.
- **Area:** `pkg/controller` and `pkg/metadata` rebalance.
- **Impact:** silent data loss. The shard is served by a node that holds nothing.

## 9. Datanode GET does not check primary status or epoch
- **Severity:** High. **Confidence:** VERIFIED.
- **Area:** `pkg/datanode` `handleGet`.
- **Impact:** a node that is no longer primary can serve stale reads while its lease is still valid. This violates linearizable reads.

## 10. Restarted datanode comes back empty
- **Severity:** Medium. **Confidence:** VERIFIED.
- **Area:** `pkg/datanode`.
- **Impact:** a restarted primary does not pull from its replica. Data is lost if the restart is quick enough that the epoch does not change.
- **Test:** `TestE2E_DataNodeRestartBehavior` asserts the current behavior.
- **Not tested:** a long outage that triggers the reaper and a rebalance.

## 11. Empty values are treated as "not found"
- **Severity:** Medium. **Confidence:** VERIFIED.
- **Area:** `pkg/cache`, `pkg/datanode`.
- **Root cause:** `cache.Get` returns `""` for a missing key, so a key stored with an empty value returns 404.
- **Fix direction:** return `(value, ok)`.

## 12. Fenced primary replicates before returning 503
- **Severity:** Low. **Confidence:** VERIFIED.
- **Area:** `pkg/datanode` `HandlePut` / `HandleDelete`.
- **Root cause:** the lease is re-checked after replication, so a primary that lost its lease may already have pushed the write to replicas.
- **Note:** the client retries, so this is mostly benign. Replicas can briefly see a write that the client was told failed.

## 13. Concurrent duplicate requests with the same Idempotency-Key can each reach Raft
- **Severity:** Medium. **Confidence:** INFERRED.
- **Area:** `pkg/api`.
- **Root cause:** there is no in-flight deduplication, only a cache of finished responses.
- **Related design point:** without an `Idempotency-Key` header, a content-derived token is no longer generated (that was a lost-update bug). A client that retries without a key can apply a write twice. That is harmless for SET and DELETE, but it matters for CAS or INCR. Clients should send a key.

## 14. Raft proposals are not tied to a client ID and sequence number
- **Severity:** Medium. **Confidence:** INFERRED.
- **Area:** `pkg/api` idempotency vs `pkg/raft`.
- **Impact:** deduplication is an in-memory cache with a TTL. It is lost on leader change or restart, so a retry after failover can apply twice. Deduplication belongs in the replicated state machine.

## 15. Goroutines that never stop
- **Severity:** Low. **Confidence:** VERIFIED.
- **Area:** `Reaper.Run` in `pkg/controller` and `IdempotencyCache.cleanupExpired` in `pkg/api`.
- **Impact:** they leak on shutdown and in tests.

## 16. Controller `/debug/*` endpoints are unauthenticated
- **Severity:** Medium. **Confidence:** VERIFIED.
- **Area:** `cmd/controller`.
- **Impact:** `/debug/config` and `/debug/raft_noop` let anyone overwrite the topology.

## 17. Unverified: does a controller group restart rebuild its topology?
- **Severity:** Unknown. **Confidence:** ASSUMED.
- **Area:** `pkg/controller`, `pkg/raft` recovery.
- **Task:** no test exists for a full controller-group restart. First find out whether the persisted Raft log is replayed on start. This is likely blocked by issues 1 to 3.
