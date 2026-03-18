# Analysis of PR #17610: RFC-102 — RLI & Secondary Index Support for Flink Streaming

**Author:** danny0405 (Danny Chan)
**State:** OPEN, changes requested (vinothchandar, Jan 28 2026)
**11 commits** (Dec 16 2025 — Jan 29 2026)
**Branch:** danny0405:rfc-102-design → apache:master

---

## Critical Points of the Proposed Changes

### 1. Index Access in BucketAssigner — Record Routing via MDT

The `BucketAssigner` operator is extended to query the Metadata Table (MDT) for record locations. Records are shuffled using `hash(record_key) % num_index_shards`, matching the MDT partitioner logic. This is the central design decision — it determines how records find their existing locations for upserts.

**Why it matters:** This is where correctness lives. If records with the same key land on different `BucketAssigner` subtasks, they can be mistagged (insert vs update). The design requires a `keyBy(record_key)` before BucketAssigner to guarantee same-key routing — this was raised by geserdugarov and confirmed as necessary by cshuo and vinothchandar.

### 2. Two-Tier Caching Strategy

- **Record-level cache** (in-memory): Holds mappings for the current checkpoint's records that haven't been committed to Hudi yet. Essential because these mappings aren't persisted anywhere until the Hudi commit completes.
- **MDT file-level cache**: Native file cache for base/log files of RLI shards already committed.

**Why it matters:** vinothchandar initially pushed for file-level-only caching, but danny0405 convinced that record-level cache is necessary for in-flight checkpoint data. The "hotspot" long-lived record cache was removed (commit cb2ed40, Jan 29) after vinothchandar raised concurrent writer consistency concerns.

### 3. Inflight Instant Support for MDT Reads

Flink checkpoint can succeed while the Hudi commit is still in-flight. On job restart, the BucketAssigner must be able to read RLI entries from these uncommitted-but-checkpointed instants. The design proposes reading inflight instants from MDT — only those whose corresponding Flink checkpoint succeeded.

**Why it matters:** This is a correctness-critical gap. Without it, records processed between the last committed instant and the checkpoint would be reprocessed with stale index state, leading to duplicate or misrouted records. vinothchandar was initially concerned this required "special instant types" but danny0405 clarified it's just a new RLI read API leveraging existing FileGroup reader inflight instant support.

### 4. Two-Phase Commit Alignment (Flink Checkpoint ↔ Hudi Commit)

The IndexWrite operator flushes index records during Flink checkpoints. Write status metadata goes to the coordinator. The MDT is committed **before** the Data Table. On recovery, uncommitted checkpoints are recommitted.

**Why it matters:** The ordering (MDT commit → DT commit) is critical. If DT committed first without MDT, subsequent reads would have stale index data.

### 5. Cross-Partition Upsert Support (Global RLI)

Enables records to move across partitions. The Spark engine already supports this; the RFC brings parity to Flink. vinothchandar explicitly requested support for both partitioned and global RLI, consistent with Spark.

### 6. Secondary Index (SI) Support Added to Scope

Originally RFC-102 covered RLI only. vinothchandar expanded the scope (commit bd1ed67, Jan 13) to include Secondary Index support, making the RFC cover both RLI and SI for Flink.

### 7. No Flink-Specific Table Configs

vinothchandar was emphatic: "strictly no flink specific table configs." All configuration must reuse existing `HoodieMetadataConfig` properties to maintain engine parity.

---

## Unanswered Reviewer Questions

### Still unresolved (no clear answer or acknowledged as open):

1. **xushiyan (Dec 18):** "so you meant a buffer of current write's index mapping entries to be flushed later upon Hudi commit? let's clarify a bit."
   - This thread was never formally resolved. The distinction between "buffer" and "cache" for in-flight index mappings remains unclear in the RFC text.

2. **vinothchandar (Jan 28):** Ordering value handling in RLI
   - "Can't we read the RLI shard via FG reader and have that handle all this... idk why any special handling is necessary here. What is the ordering value being proposed - same as the one configured for table?"
   - danny0405's response was truncated and the thread appears unresolved. The question of whether RLI payloads need to store ordering values or whether the FileGroup reader can handle merge semantics is still open.

3. **vinothchandar (Jan 28):** Long-lived cache consistency with concurrent writers
   - While danny0405 removed the hotspot cache (commit cb2ed40), the broader question of how concurrent writers affect the remaining in-checkpoint record cache was not explicitly answered. cshuo noted the invalidation issue exists "not only across commits but also inside one single checkpoint interval."

4. **xushiyan (Dec 18):** "can you define such special inflight instants a bit more clearly? being able to read such inflight instants would be a general capability or a special case for flink MDT use case?"
   - danny0405 added details in later commits, but the question of whether this is Flink-specific or a general MDT capability was not directly answered.

---

## Critical Questions That Were NOT Asked

### 1. What happens with RLI under schema evolution?

Nobody asked how the RLI/SI indexes behave when the table schema evolves (e.g., partition field changes, record key field changes). Cross-partition upserts combined with schema evolution could lead to orphaned index entries.

### 2. What is the behavior when num_rli_shards changes?

The discussion touched on how `num_rli_shards` is configured (existing min/max file group configs), but nobody asked: **what happens to existing RLI data when the number of shards changes?** Since `hash(record_key) % num_rli_shards` determines shard assignment, changing this value would invalidate all existing index mappings. Is this a table-altering operation? Does it require reindexing?

### 3. What are the performance characteristics and memory bounds?

- How much memory does the in-checkpoint record-level cache consume for large volumes?
- What happens when the cache exceeds available memory? Is there backpressure?
- What is the expected latency overhead of RLI lookup per record compared to flink_state index?
- danny0405 mentioned RLI has "relatively worse performance than simple hash index" — where's the quantification?

### 4. How does this interact with multiple concurrent Flink jobs writing to the same table?

The concurrent writer question was raised for cache consistency, but the broader coordination protocol is missing. If two Flink streaming jobs write to the same Hudi table with RLI enabled:
- How do they coordinate MDT commits?
- How do they avoid conflicting RLI updates?
- Is there a lock/conflict resolution mechanism?

### 5. What is the Secondary Index (SI) design?

vinothchandar expanded the scope to include SI (Jan 13), but the RFC appears to focus almost entirely on RLI. The SI design details — how secondary indexes are built, updated, and queried in the Flink streaming path — seem largely absent. This was requested but may not have been delivered.

### 6. What is the failure blast radius?

If the IndexWrite operator fails mid-checkpoint:
- Are partial RLI writes to MDT rolled back?
- What guarantees exist against index corruption?
- The appendix covers some corner cases, but the full failure taxonomy isn't documented.

### 7. How does MDT compaction interact with active streaming writes?

The RFC mentions MDT compaction reuses existing file pipelines, but:
- Does compaction block reads from BucketAssigner?
- Is there a read-write contention issue during compaction of RLI shards?
- How is the compaction schedule coordinated with the streaming write pipeline?

### 8. What is the testing and validation strategy?

No discussion about how to test RLI correctness at scale — integration tests, chaos testing for the recovery scenarios, performance benchmarks against flink_state index. For a feature this complex, a testing plan is critical.

---

## Summary

The RFC is in a **partially-resolved state**. The core architecture (BucketAssigner → MDT lookup → IndexWrite → two-phase commit) is sound and agreed upon. The most contentious design questions (record-level vs file-level caching, inflight instant semantics) have been largely resolved through iterative review. However:

- **3-4 review threads remain unresolved**, primarily around ordering value handling and the buffer/cache distinction
- **The SI scope expansion hasn't been designed** with the same rigor as RLI
- **Critical operational questions** (shard count changes, concurrent writers, failure blast radius, memory bounds) were never raised
- **vinothchandar's overall assessment** (Jan 13): "the RFC/design doc is still very high level. I'd like specifics to be clearly written out" — it's unclear whether the subsequent updates fully addressed this
