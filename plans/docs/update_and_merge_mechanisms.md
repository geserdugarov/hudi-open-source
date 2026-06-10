<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Hudi UPDATE, UPSERT, and MERGE Mechanisms

This document provides a comprehensive description of how Apache Hudi implements record-level updates: the upsert write pipeline, precombine/deduplication, payload and record merger concepts, index tagging, small-file handling, Copy-on-Write rewrites, Merge-on-Read log appends with later compaction, and Spark SQL MERGE INTO support.

**Companion docs** (planned in this folder; not all landed yet): `architecture_overview.md` | `write_path_callstack.md` | `read_path_callstack.md` | `delete_mechanisms.md` | `compaction_callstack.md`

---

## 1. Overview

### Upsert

Hudi's native update primitive is the **upsert**: every incoming record carries a `HoodieKey` (record key + partition path), and Hudi decides per record whether it is an UPDATE (key already exists in some file group) or an INSERT (new key). Engine APIs expose it directly:

```scala
// Spark DataSource API
df.write.format("hudi")
  .option("hoodie.datasource.write.operation", "upsert")
  .option("hoodie.datasource.write.recordkey.field", "id")
  .option("hoodie.table.ordering.fields", "ts")
  .mode(Append)
  .save(basePath)
```

```sql
-- Spark SQL
UPDATE hudi_table SET value = 'new_value' WHERE id = 42;

MERGE INTO hudi_table t
USING source s
ON t.id = s.id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED AND s.op != 'D' THEN UPDATE SET t.value = s.value, t.ts = s.ts
WHEN NOT MATCHED THEN INSERT (id, value, ts) VALUES (s.id, s.value, s.ts);
```

(The update condition explicitly excludes delete rows: Hudi evaluates matched **update** clauses before the delete condition — see section 11.3 — so an unconditional `WHEN MATCHED THEN UPDATE` would swallow the rows meant for `DELETE`.)

### Execution Strategies

The table type determines how an update is physically applied to the file group that holds the old record version:

- **Copy-on-Write (CoW):** the base (parquet) file of the file group is rewritten — old records are merged with incoming records and a new base file version (same `fileId`, new commit timestamp) is produced. Readers always see merged data.
- **Merge-on-Read (MoR):** incoming updates are appended as data blocks to a **log file** of the file group (a `deltacommit`). Readers merge base file + log blocks at query time; a later **compaction** folds the log files into a new base file version.

### CoW vs MoR Comparison

```
┌──────────────────────┬───────────────────────────┬───────────────────────────┐
│                      │  Copy-on-Write            │  Merge-on-Read            │
├──────────────────────┼───────────────────────────┼───────────────────────────┤
│ Timeline action      │ commit                    │ deltacommit               │
├──────────────────────┼───────────────────────────┼───────────────────────────┤
│ Update I/O           │ Rewrite whole base file   │ Append log block          │
├──────────────────────┼───────────────────────────┼───────────────────────────┤
│ Write amplification  │ High (full file rewrite)  │ Low (only changed rows)   │
├──────────────────────┼───────────────────────────┼───────────────────────────┤
│ Read cost            │ Plain columnar scan       │ Base + log merge          │
│                      │                           │ (snapshot query)          │
├──────────────────────┼───────────────────────────┼───────────────────────────┤
│ Update write handle  │ HoodieWriteMergeHandle /  │ HoodieAppendHandle        │
│                      │ FileGroupReaderBased-     │                           │
│                      │ MergeHandle               │                           │
├──────────────────────┼───────────────────────────┼───────────────────────────┤
│ Insert write handle  │ HoodieCreateHandle        │ HoodieCreateHandle (base) │
│                      │                           │ or HoodieAppendHandle if  │
│                      │                           │ index canIndexLogFiles()  │
├──────────────────────┼───────────────────────────┼───────────────────────────┤
│ Deferred merge       │ none                      │ Compaction (log → base)   │
├──────────────────────┼───────────────────────────┼───────────────────────────┤
│ Executor (Spark)     │ SparkUpsertCommit-        │ SparkUpsertDeltaCommit-   │
│                      │ ActionExecutor            │ ActionExecutor            │
└──────────────────────┴───────────────────────────┴───────────────────────────┘
```

Both strategies share the same upsert front-end: deduplication (precombine), index tagging, workload profiling, and bucket assignment. They diverge only at the write-handle layer.

---

## 2. High-Level Flow

```
┌──────────────────────────────────────────────────────────────────────────┐
│  df.write.format("hudi").option("operation","upsert")  /  SQL MERGE INTO │
└────────────────────────────────────┬─────────────────────────────────────┘
                                     │
                       ┌─────────────▼──────────────┐
                       │  HoodieSparkSqlWriter      │  DataFrame → RDD of
                       │  (hudi-spark-common)       │  HoodieRecord via
                       │                            │  KeyGenerator
                       └─────────────┬──────────────┘
                                     │
                       ┌─────────────▼──────────────┐
                       │  SparkRDDWriteClient       │  initTable, preWrite,
                       │  .upsert(records, instant) │  table.upsert(...)
                       └─────────────┬──────────────┘
                                     │
                  ┌──────────────────┴────────────────────┐
                  │ COPY_ON_WRITE                         │ MERGE_ON_READ
                  ▼                                       ▼
   ┌──────────────────────────────┐       ┌───────────────────────────────┐
   │ SparkUpsertCommitAction-     │       │ SparkUpsertDeltaCommitAction- │
   │ Executor (action: commit)    │       │ Executor (action: deltacommit)│
   └──────────────┬───────────────┘       └───────────────┬───────────────┘
                  └──────────────────┬────────────────────┘
                                     │
                       ┌─────────────▼──────────────┐
                       │  HoodieWriteHelper.write() │
                       │                            │
                       │  1. deduplicateRecords()   │  precombine by ordering
                       │  2. index.tagLocation()    │  UPDATE vs INSERT
                       │  3. executor.execute()     │
                       └─────────────┬──────────────┘
                                     │
                       ┌─────────────▼──────────────┐
                       │  WorkloadProfile +         │  updates → 1 bucket per
                       │  UpsertPartitioner         │  fileId; inserts → small
                       │                            │  files or new file groups
                       └─────────────┬──────────────┘
                                     │
                  ┌──────────────────┴────────────────────┐
                  │ BucketType.UPDATE                     │ BucketType.INSERT
                  ▼                                       ▼
   ┌──────────────────────────────┐       ┌───────────────────────────────┐
   │ handleUpdate()               │       │ handleInsert()                │
   │                              │       │                               │
   │ CoW: merge handle rewrites   │       │ HoodieCreateHandle writes new │
   │      base file               │       │ base file (or AppendHandle    │
   │ MoR: HoodieAppendHandle      │       │ for log-indexable MoR)        │
   │      appends log block       │       │                               │
   └──────────────┬───────────────┘       └───────────────┬───────────────┘
                  └──────────────────┬────────────────────┘
                                     │
                       ┌─────────────▼──────────────┐
                       │  commit / deltacommit on   │  requested → inflight →
                       │  timeline (atomic)         │  completed
                       └────────────────────────────┘
```

---

## 3. The Upsert Write Pipeline

### 3.1 Entry Point and Executors

```
SparkRDDWriteClient.upsert(JavaRDD<HoodieRecord>, instantTime)
       │            [hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/SparkRDDWriteClient.java]
       │
       ├─► initTable(WriteOperationType.UPSERT, instantTime)
       ├─► table.validateUpsertSchema()
       ├─► preWrite(...)                                  [BaseHoodieWriteClient]
       │     [hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/BaseHoodieWriteClient.java]
       │
       ├─► table.upsert(context, instantTime, records)
       │     │
       │     ├─► HoodieSparkCopyOnWriteTable → SparkUpsertCommitActionExecutor
       │     │     [hudi-client/hudi-spark-client/.../table/action/commit/SparkUpsertCommitActionExecutor.java]
       │     │
       │     └─► HoodieSparkMergeOnReadTable → SparkUpsertDeltaCommitActionExecutor
       │           [hudi-client/hudi-spark-client/.../table/action/deltacommit/SparkUpsertDeltaCommitActionExecutor.java]
       │
       │     Both delegate identically:
       │       HoodieWriteHelper.newInstance().write(
       │           instantTime, records, context, table,
       │           config.shouldCombineBeforeUpsert(),     ← hoodie.combine.before.upsert
       │           config.getUpsertShuffleParallelism(),
       │           this, operationType)
       │
       └─► postWrite(...) → commit to timeline
```

### 3.2 BaseWriteHelper.write() — Dedup, Tag, Execute

`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/commit/BaseWriteHelper.java`

```
BaseWriteHelper.write()
  │
  ├─► 1. combineOnCondition(shouldCombine, records, parallelism, table)
  │       └─► deduplicateRecords(...)                     PRECOMBINE (section 4)
  │
  ├─► 2. if table.getIndex().requiresTagging(operationType):
  │       tag(dedupedRecords, context, table)
  │         └─► table.getIndex().tagLocation(records, context, table)
  │                                                       INDEX TAGGING (section 6)
  │
  └─► 3. executor.execute(taggedRecords)
          └─► BaseSparkCommitActionExecutor.execute()
                [hudi-client/hudi-spark-client/.../table/action/commit/BaseSparkCommitActionExecutor.java]
                │
                ├─► prepareWorkloadProfile(records)       count updates/inserts
                │     └─► buildProfile(): group by (partitionPath, currentLocation)
                │           location present → WorkloadStat.addUpdates(loc, count)
                │           location absent  → WorkloadStat.addInserts(count)
                │
                ├─► saveWorkloadProfileMetadataToInflight  marker for rollback
                │
                ├─► getPartitioner(profile)
                │     CoW → UpsertPartitioner             SMALL FILES (section 7)
                │     MoR → SparkUpsertDeltaCommitPartitioner
                │
                ├─► mapPartitionsAsRDD(records, partitioner)
                │     Spark shuffle: each record routed to its bucket
                │
                └─► handleUpsertPartition(instantTime, partition, recordItr, partitioner)
                      │
                      ├─► BucketType.UPDATE → handleUpdate(partitionPath, fileId, recordItr)
                      │     CoW: merge handle              (section 8)
                      │     MoR: HoodieAppendHandle        (section 9)
                      │
                      └─► BucketType.INSERT → handleInsert(idPfx, recordItr)
                            SparkLazyInsertIterable + CreateHandleFactory
                            → HoodieCreateHandle writes new base file
```

The tagged location drives **file-group routing**, not a strict existing-key vs new-key split: `HoodieRecord.getCurrentLocation() != null` (set by the index to `HoodieRecordLocation(instantTime, fileId)`) routes the record to the UPDATE bucket of file group `fileId`, while `null` marks it an insert. Two caveats blur the key/file-group line: untagged inserts can still be assigned to the UPDATE bucket of an existing under-sized file group by small-file bin-packing (section 7), and hash-based bucket indexes tag the bucket's existing file group deterministically without verifying the key is actually present in it — the merge handle reconciles either way when it merges incoming records against the file's contents.

---

## 4. Precombine / Deduplication

Before tagging, an upsert batch may contain several records with the same key (e.g. multiple CDC events for one row). **Precombine** collapses them to one record per key.

### 4.1 Call Flow

```
BaseWriteHelper.deduplicateRecords(records, table, parallelism)
  [hudi-client/hudi-client-common/.../table/action/commit/BaseWriteHelper.java]
  │
  ├─► orderingFieldNames = HoodieRecordUtils.getOrderingFieldNames(
  │       readerContext.getMergeMode(), table.getMetaClient())
  │       ← hoodie.table.ordering.fields (alias: hoodie.table.precombine.field)
  │
  ├─► bufferedRecordMerger = BufferedRecordMergerFactory.create(
  │       readerContext, mergeMode, recordMerger, payloadClass, schema, props, partialUpdateMode)
  │
  └─► HoodieWriteHelper.deduplicateRecords(...)           Spark implementation
        [hudi-client/hudi-client-common/.../table/action/commit/HoodieWriteHelper.java]
        │
        ├─► records.mapToPair(record →
        │       key = isGlobalIndex ? recordKey : HoodieKey)   global index dedups
        │                                                      across partitions
        └─► .reduceByKey((previous, next) →
                reduceRecords(...)
                  └─► bufferedRecordMerger.deltaMerge(newRec, oldRec)
                        winner chosen by RecordMergeMode:
                          COMMIT_TIME_ORDERING → one duplicate survives; which
                              one is NOT batch-order deterministic (the
                              previous/next order within reduceByKey is
                              uncertain — see note in BaseWriteHelper)
                          EVENT_TIME_ORDERING  → larger ordering value wins
                          CUSTOM               → payload.preCombine() / merger
            , parallelism)
```

Under `COMMIT_TIME_ORDERING`, intra-batch duplicates have no reliable "latest" — deduplicate upstream or set ordering fields if the choice matters.

### 4.2 Ordering Fields

| Config | Where | Meaning |
|---|---|---|
| `hoodie.table.ordering.fields` | `HoodieTableConfig.ORDERING_FIELDS` (`hudi-common/src/main/java/org/apache/hudi/common/table/HoodieTableConfig.java`) | Comma-separated columns whose values order record versions. Alias of the deprecated `hoodie.table.precombine.field`. |
| `hoodie.datasource.write.precombine.field` | DataSource write option | Writer-side way to set the ordering field. |
| `hoodie.combine.before.upsert` | `HoodieWriteConfig.COMBINE_BEFORE_UPSERT` (default `true`) | Run dedup before upsert. |
| `hoodie.combine.before.insert` | `HoodieWriteConfig.COMBINE_BEFORE_INSERT` (default `false`) | Run dedup before insert. |

The ordering value is extracted per record into a `Comparable` (`OrderingValues` in `hudi-common/src/main/java/org/apache/hudi/common/util/OrderingValues.java`; multiple fields become an `ArrayComparable`). The same ordering value is used again at every later merge point: CoW rewrite, MoR log merge at read time, and compaction. This is what makes Hudi upserts resilient to late-arriving data under `EVENT_TIME_ORDERING`.

---

## 5. Payload and Record Merger Concepts

Hudi has two generations of merge APIs. Both answer the same question — *given an old and a new version of a record with the same key, what survives?* — at dedup time, CoW rewrite time, MoR read time, and compaction time.

### 5.1 RecordMergeMode (table-level semantics, 1.x)

`hudi-common/src/main/java/org/apache/hudi/common/config/RecordMergeMode.java`, configured via `hoodie.record.merge.mode` (`HoodieTableConfig.RECORD_MERGE_MODE`):

| Mode | Semantics | Default when |
|---|---|---|
| `COMMIT_TIME_ORDERING` | Record from the later commit wins (overwrite-latest). | No ordering field set |
| `EVENT_TIME_ORDERING` | Record with the larger ordering (event-time) value wins. | Ordering field set |
| `CUSTOM` | Delegated to a custom `HoodieRecordMerger` or legacy payload class. | Explicitly configured |

### 5.2 HoodieRecordMerger (new API)

`hudi-common/src/main/java/org/apache/hudi/common/model/HoodieRecordMerger.java`

```java
BufferedRecord<T> merge(BufferedRecord<T> older, BufferedRecord<T> newer,
                        RecordContext<T> recordContext, TypedProperties props);
BufferedRecord<T> partialMerge(...);       // partial-update aware
String getMergingStrategy();               // strategy UUID
```

Engine-native implementations avoid Avro round-trips:

| Merger | Path | Semantics |
|---|---|---|
| `HoodieAvroRecordMerger` | `hudi-common/.../common/model/HoodieAvroRecordMerger.java` | Bridges to legacy `HoodieRecordPayload.combineAndGetUpdateValue()` |
| `OverwriteWithLatestMerger` | `hudi-common/.../common/model/OverwriteWithLatestMerger.java` | Commit-time ordering |
| `DefaultSparkRecordMerger` | `hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/DefaultSparkRecordMerger.java` | Event-time ordering on Spark `InternalRow` |
| `OverwriteWithLatestSparkRecordMerger` | `hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/OverwriteWithLatestSparkRecordMerger.java` | Commit-time ordering on Spark rows |

Strategy UUIDs (constants on `HoodieRecordMerger`) tie a table to compatible mergers via `hoodie.record.merge.strategy.id` (`HoodieTableConfig.RECORD_MERGE_STRATEGY_ID`): `EVENT_TIME_BASED_MERGE_STRATEGY_UUID`, `COMMIT_TIME_BASED_MERGE_STRATEGY_UUID`, `CUSTOM_MERGE_STRATEGY_UUID`, and `PAYLOAD_BASED_MERGE_STRATEGY_UUID` (used by Spark SQL MERGE INTO).

In the 1.x file-group reader the merge runs over lightweight `BufferedRecord` wrappers (`hudi-common/src/main/java/org/apache/hudi/common/table/read/BufferedRecord.java` — engine row + record key + ordering value + delete flag) driven by `BufferedRecordMerger` (`hudi-common/.../common/table/read/buffer/`), so merging never forces Avro serialization for engine-native records.

### 5.3 HoodieRecordPayload (legacy API, still supported)

`hudi-common/src/main/java/org/apache/hudi/common/model/HoodieRecordPayload.java`

```java
T preCombine(T oldValue, Properties props);                       // dedup within a batch
Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue,
                                               Schema schema, Properties props);
                                                                  // merge vs stored record;
                                                                  // Option.empty() == DELETE
Option<IndexedRecord> getInsertValue(Schema schema, Properties props);
Comparable<?> getOrderingValue();
```

Configured for writes via `hoodie.datasource.write.payload.class`; the table-side keys are `hoodie.compaction.payload.class` (`HoodieTableConfig.PAYLOAD_CLASS_NAME`, deprecated after 1.0.0) and `hoodie.table.legacy.payload.class` (`LEGACY_PAYLOAD_CLASS_NAME`, 1.1.0+, records the payload the table was created with) — 1.x tables primarily rely on the merge mode/strategy configs of section 5.1 instead. Common implementations (all under `hudi-common/src/main/java/org/apache/hudi/common/model/`):

| Payload | Semantics |
|---|---|
| `OverwriteWithLatestAvroPayload` | Incoming record always wins (commit-time ordering); `preCombine` picks larger ordering value. |
| `DefaultHoodieRecordPayload` | Event-time ordering: compares `hoodie.payload.ordering.field` between stored and incoming record in `combineAndGetUpdateValue`; supports custom delete markers (`DELETE_KEY`/`DELETE_MARKER`). |
| `EventTimeAvroPayload` | Event-time ordering variant. |
| `PartialUpdateAvroPayload` | Field-level partial update: null fields in the newer record are filled from the older one. |
| `OverwriteNonDefaultsWithLatestAvroPayload` | Overwrites only fields differing from schema defaults. |
| `AWSDmsAvroPayload` | Interprets the AWS DMS `Op` column (`D` → delete). |
| `MySqlDebeziumAvroPayload` / `PostgresDebeziumAvroPayload` (`.../model/debezium/`) | CDC ordering by binlog position / LSN, delete on `op = d`. |

A delete in both APIs is expressed by an **empty merge result**: `combineAndGetUpdateValue` returning `Option.empty()` (or a `BufferedRecord` with the delete flag) removes the key from the merged view — see the companion `delete_mechanisms.md`.

---

## 6. Index Tagging

Tagging answers "where does this key currently live?" and turns an upsert batch into explicit updates and inserts.

### 6.1 API

`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/index/HoodieIndex.java`

```java
HoodieData<HoodieRecord<R>> tagLocation(HoodieData<HoodieRecord<R>> records,
                                        HoodieEngineContext context, HoodieTable table);
boolean isGlobal();          // key uniqueness across partitions?
boolean canIndexLogFiles();  // can locate records that only exist in logs?
boolean isImplicitWithStorage();
```

After tagging, each record either has `currentLocation = HoodieRecordLocation(instantTime, fileId)` (`hudi-common/src/main/java/org/apache/hudi/common/model/HoodieRecordLocation.java`) → UPDATE of that file group, or `currentLocation = null` → INSERT.

### 6.2 Index Types

`SparkHoodieIndexFactory` (`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/index/SparkHoodieIndexFactory.java`) instantiates from `hoodie.index.type` (`HoodieIndexConfig`; engine defaults: **SIMPLE** for Spark/Java, **INMEMORY** for Flink):

| IndexType | Implementation (path under `hudi-client/...`) | Global | How it tags |
|---|---|---|---|
| `SIMPLE` | `hudi-client-common/.../index/simple/HoodieSimpleIndex.java` | no | Join incoming keys against (key, location) pairs read from latest base files of affected partitions. |
| `GLOBAL_SIMPLE` | `.../simple/HoodieGlobalSimpleIndex.java` | yes | Same, over all partitions. |
| `BLOOM` | `hudi-client-common/.../index/bloom/HoodieBloomIndex.java` | no | Prune candidate files by key-range + bloom filters from parquet footers (or metadata-table `bloom_filters` partition), then verify candidates by reading actual keys. |
| `GLOBAL_BLOOM` | `.../bloom/HoodieGlobalBloomIndex.java` | yes | Bloom over all partitions. |
| `BUCKET` | `hudi-client-common/.../index/bucket/HoodieSimpleBucketIndex.java` (default engine SIMPLE) or consistent-hashing variant (`HoodieSparkConsistentBucketIndex`) | no | `hash(recordKey) % numBuckets` deterministically maps key → fileId; no lookup I/O. `canIndexLogFiles()` is `false` for the SIMPLE engine and `true` for consistent hashing (base `HoodieBucketIndex`). |
| `RECORD_LEVEL_INDEX` / `GLOBAL_RECORD_LEVEL_INDEX` | `hudi-spark-client/.../index/SparkMetadataTableRecordLevelIndex.java`, `SparkMetadataTableGlobalRecordLevelIndex.java` | no / yes | Point lookup in the metadata table `record_index` partition, which stores `recordKey → (partition, fileId)` mappings maintained on every commit. |
| `INMEMORY` | `hudi-client-common/.../index/inmemory/HoodieInMemoryHashIndex.java` | yes | In-memory map (testing / Flink state analog). |

Global indexes enforce key uniqueness across partitions, so an update arriving with a changed partition path triggers update-partition-path handling (delete in old partition + insert in new one). Non-global indexes treat the same key in another partition as a new record.

### 6.3 Bloom Index Tagging Flow (representative)

```
HoodieBloomIndex.tagLocation()
  │
  ├─► 1. extract (partitionPath, recordKey) pairs from input
  ├─► 2. load latest base files per partition (file system view)
  ├─► 3. prune candidates per file:
  │        min/max record key range check (footer or metadata col stats)
  │        bloom filter membership check (footer or metadata bloom_filters)
  ├─► 4. verify: read record keys of candidate files,
  │        join with incoming keys → (HoodieKey, HoodieRecordLocation)
  └─► 5. left-outer-join back to input records:
           match    → record.setCurrentLocation(location)   UPDATE
           no match → location stays null                   INSERT
```

---

## 7. Small-File Handling

Hudi bin-packs inserts into existing under-sized file groups during upsert, so updates and inserts both flow through the same partitioner instead of requiring a separate "optimize" job.

### 7.1 UpsertPartitioner (CoW)

`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/table/action/commit/UpsertPartitioner.java`

```
UpsertPartitioner(profile, context, table, config, operationType)
  │
  ├─► assignUpdates(profile)
  │     one UPDATE bucket per (partitionPath, fileId) seen in the workload profile
  │
  └─► assignInserts(profile, context)
        │
        ├─► averageRecordSize = recordSizeEstimator.averageBytesPerRecord(
        │       recent commit metadata)            ← bootstrapped from
        │                                            hoodie.copyonwrite.record.size.estimate
        │
        ├─► smallFiles = getSmallFilesForPartitions(...)
        │     base files with size < hoodie.parquet.small.file.limit (default 100 MB)
        │     from latest committed file slices
        │
        ├─► FIRST: top up small files
        │     recordsToAppend = (hoodie.parquet.max.file.size − smallFile.size)
        │                       / averageRecordSize
        │     → routed as an UPDATE bucket for that fileId
        │       (the small base file is rewritten, growing toward 120 MB default max)
        │
        └─► THEN: remaining inserts → new INSERT buckets
              insertRecordsPerBucket = hoodie.copyonwrite.insert.split.size, or
              auto-tuned to maxFileSize / averageRecordSize when
              hoodie.copyonwrite.insert.auto.split = true
              each bucket gets a fresh fileId (FSUtils.createNewFileIdPfx())
```

`getPartition(key)` then routes: tagged records via `updateLocationToBucket(fileId)`; untagged records via MD5-hash weighted assignment across the partition's insert buckets (deterministic for retries).

### 7.2 SparkUpsertDeltaCommitPartitioner (MoR)

`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/table/action/deltacommit/SparkUpsertDeltaCommitPartitioner.java`

For MoR, "small" is measured on the whole **file slice** (base + logs) normalized to parquet-equivalent bytes via `hoodie.logfile.to.parquet.compression.ratio`:

- If the index `canIndexLogFiles()` (e.g. consistent-hashing bucket index, Flink state; **not** the default SIMPLE-engine bucket index): any slice whose normalized size < max file size is a small-file target — inserts can go straight to its **log file**.
- Otherwise: only slices with a base file **and no log files** qualify (an insert packed into a slice with logs would be invisible to a log-blind index); candidates capped by `hoodie.merge.small.file.group.candidates.limit` (default 1).

Inserts assigned to a small slice are routed as updates to that fileId, which on MoR means a log append (section 9) — except in the log-blind-index case above, where `BaseSparkDeltaCommitActionExecutor.handleUpdate` falls back to a CoW-style base-file rewrite for those small-file corrections (`hudi-client/hudi-spark-client/.../deltacommit/BaseSparkDeltaCommitActionExecutor.java`).

---

## 8. Copy-on-Write Update (Base File Rewrite)

### 8.1 Call Stack

```
BaseSparkCommitActionExecutor.handleUpdate(partitionPath, fileId, recordItr)
  [hudi-client/hudi-spark-client/.../table/action/commit/BaseSparkCommitActionExecutor.java]
  │
  ├─► getUpdateHandle(...) → merge handle instance
  │     class from hoodie.write.merge.handle.class
  │     default: FileGroupReaderBasedMergeHandle (since 1.1.0)
  │     legacy:  HoodieWriteMergeHandle
  │
  └─► IOUtils.runMerge(mergeHandle, instantTime, fileId)
        └─► mergeHandle.doMerge()

LEGACY PATH — HoodieWriteMergeHandle
  [hudi-client/hudi-client-common/src/main/java/org/apache/hudi/io/HoodieWriteMergeHandle.java]
  │
  ├─► init: populateIncomingRecordsMap(recordItr)
  │     keyToNewRecords: ExternalSpillableMap<key, HoodieRecord>
  │     (spills to disk above hoodie.memory.merge.max.size)
  │     create marker file MERGE for the new base file
  │
  ├─► doMerge() → HoodieMergeHelper.runMerge(table, this)
  │     [hudi-client/hudi-client-common/.../table/action/commit/HoodieMergeHelper.java]
  │     │
  │     ├─► open old base file with HoodieFileReader
  │     │     (handles schema evolution / bootstrap projection)
  │     │
  │     └─► for each oldRecord in base file: mergeHandle.write(oldRecord)
  │           │
  │           ├─► key in keyToNewRecords?
  │           │     YES → recordMerger.merge(old, new, ctx, props)
  │           │             result empty   → record dropped (DELETE)
  │           │             result present → writeUpdateRecord(...) → fileWriter
  │           │     NO  → passthrough: write oldRecord unchanged
  │           │
  │           └─► mark key as processed (writtenRecordKeys)
  │
  ├─► writeIncomingRecords()
  │     remaining keyToNewRecords entries (keys not present in the old base
  │     file — e.g. small-file inserts) appended as new records
  │
  └─► close() → WriteStatus (new base file: same fileId, new instantTime)

NEW DEFAULT — FileGroupReaderBasedMergeHandle
  [hudi-client/hudi-client-common/src/main/java/org/apache/hudi/io/FileGroupReaderBasedMergeHandle.java]
  │
  └─► feeds incoming records and the existing file slice through
      HoodieFileGroupReader, which performs the same key-based merge with
      BufferedRecordMerger (section 10.2), then streams merged records to
      the new base file writer — one engine-agnostic merge implementation
      shared by writes, reads, and compaction.
```

### 8.2 CoW Update Data Flow Example

```
BEFORE (file group f1):                  UPSERT batch:
┌──────────────────────────────┐         {id=42, val="X", ts=105}
│ f1_20260601.parquet          │
│ id=41 val="A" ts=100         │         tagLocation → id=42 found in f1
│ id=42 val="B" ts=101  ◄──────┼─update  → UPDATE bucket for f1
│ id=43 val="C" ts=102         │
└──────────────────────────────┘

MERGE (HoodieWriteMergeHandle.write per old record):
  id=41 → not in incoming map → passthrough
  id=42 → merge(old ts=101, new ts=105) → EVENT_TIME_ORDERING: new wins
  id=43 → not in incoming map → passthrough

AFTER:
┌──────────────────────────────┐
│ f1_20260610.parquet          │  ← NEW base file version, same fileId f1
│ id=41 val="A" ts=100         │
│ id=42 val="X" ts=105         │  ← updated
│ id=43 val="C" ts=102         │
└──────────────────────────────┘
┌──────────────────────────────┐
│ f1_20260601.parquet          │  ← old version retained for time travel,
└──────────────────────────────┘    removed later by the clean service

Timeline: 20260610.commit (completed) — readers atomically switch to the
latest file slice of f1.
```

The old file is **not** deleted at commit time (unlike Iceberg's `OverwriteFiles`); Hudi keeps prior file slice versions for snapshot isolation, incremental reads, and time travel until the cleaner reclaims them.

---

## 9. Merge-on-Read Update (Log Append)

### 9.1 Call Stack

```
BaseSparkDeltaCommitActionExecutor.handleUpdate(partitionPath, fileId, recordItr)
  [hudi-client/hudi-spark-client/.../table/action/deltacommit/BaseSparkDeltaCommitActionExecutor.java]
  │
  ├─► small-file correction case (log-blind index + fileId in small-file list)
  │     → super.handleUpdate(...)  i.e. CoW-style base file rewrite
  │
  └─► otherwise:
        HoodieAppendHandle(config, instantTime, table, partitionPath, fileId, recordItr)
        [hudi-client/hudi-client-common/src/main/java/org/apache/hudi/io/HoodieAppendHandle.java]
          │
          ├─► init(): locate latest file slice for fileId,
          │     createLogWriter → HoodieLogFormat.Writer on the slice's log file
          │     (rolls to a new log version when size threshold exceeded)
          │
          ├─► doAppend(): for each incoming record
          │     │
          │     ├─► prepareRecord(): merge-mode aware preprocessing,
          │     │     delete records diverted to recordsToDeleteWithPositions
          │     │
          │     ├─► buffer into recordList until block size limit
          │     │     (hoodie.logfile.data.block.max.size)
          │     │
          │     └─► appendDataAndDeleteBlocks(header, ...)
          │           │
          │           ├─► data block:
          │           │     HoodieAvroDataBlock     (hoodie.logfile.data.block.format=avro)
          │           │     HoodieParquetDataBlock  (=parquet)
          │           │     header: INSTANT_TIME, SCHEMA
          │           │
          │           ├─► delete block: HoodieDeleteBlock(deleteRecords)
          │           │
          │           └─► writer.appendBlocks(blocks)    sequential append,
          │                 [hudi-common/.../common/table/log/HoodieLogFormat.java]
          │                 block-level magic + length + CRC for corruption detection
          │
          └─► close() → WriteStatus (HoodieDeltaWriteStat: log files written,
                base file unchanged)

Timeline: 20260610.deltacommit (completed)
```

Inserts on MoR go to **base files** via `HoodieCreateHandle` unless the index `canIndexLogFiles()` (consistent-hashing bucket index, Flink state index — the default SIMPLE-engine bucket index returns `false`), in which case `handleInsert` uses an `AppendHandleFactory` and even inserts land in log files of new file groups.

### 9.2 Log File Anatomy

Log file name (`FSUtils.makeLogFileName`, `hudi-common/src/main/java/org/apache/hudi/common/fs/FSUtils.java`):

```
.{fileId}_{deltaCommitTime}.log.{version}_{writeToken}
e.g.  .f1-uuid_20260610093000000.log.1_0-52-101
```

```
File group f1 (MoR) over time:

  f1_20260601.parquet                       base file (commit C1)
  .f1_20260605...log.1_...   ◄─ deltacommit C2: data block [upd id=42]
  .f1_20260608...log.2_...   ◄─ deltacommit C3: data block [upd id=41,43]
                                             + delete block [id=44]
  ── file slice (C1) = base + log.1 + log.2 ──

Snapshot read of the slice (HoodieFileGroupReader):
  base record  ⊕  log records merged by key & ordering value  →  latest view
```

### 9.3 MoR Update Data Flow Example

```
BEFORE:                                   UPSERT batch:
┌──────────────────────────────┐          {id=42, val="X", ts=105}
│ f1_20260601.parquet (base)   │
│ id=41 val="A" ts=100         │          AFTER (deltacommit):
│ id=42 val="B" ts=101         │          base file UNCHANGED
│ id=43 val="C" ts=102         │          ┌───────────────────────────────┐
└──────────────────────────────┘          │ .f1_20260610.log.1            │
                                          │ AvroDataBlock:                │
                                          │   {id=42, val="X", ts=105}    │
                                          └───────────────────────────────┘

At READ time (snapshot query):
  id=41 → base only                → "A"
  id=42 → base ⊕ log, ts 105 > 101 → "X"
  id=43 → base only                → "C"
```

---

## 10. Compaction: Folding Logs into Base Files

Compaction is the deferred half of every MoR update. See the companion `compaction_callstack.md` for the full treatment; the merge-relevant core:

### 10.1 Schedule and Execute

```
SCHEDULING
ScheduleCompactionActionExecutor.execute()
  [hudi-client/hudi-client-common/.../table/action/compact/ScheduleCompactionActionExecutor.java]
  │
  ├─► trigger check: hoodie.compact.inline.trigger.strategy
  │     NUM_COMMITS (default): hoodie.compact.inline.max.delta.commits (default 5)
  │     TIME_ELAPSED, NUM_AND_TIME, NUM_OR_TIME variants
  │
  ├─► CompactionStrategy selects file slices:
  │     hoodie.compaction.strategy → LogFileSizeBasedCompactionStrategy (default)
  │     [.../compact/strategy/LogFileSizeBasedCompactionStrategy.java]
  │     orders by accumulated log size, bounded by hoodie.compaction.target.io
  │
  └─► HoodieCompactionPlan (one HoodieCompactionOperation per file slice:
      partition, fileId, base file, log files)
      → saved as <instant>.compaction.requested on the timeline

EXECUTION
RunCompactionActionExecutor.execute()
  [.../table/action/compact/RunCompactionActionExecutor.java]
  │
  └─► for each HoodieCompactionOperation (distributed across executors):
        compactor.compact(...)
          │
          └─► FileGroupReaderBasedMergeHandle          (hoodie.compact.merge.handle.class)
                │
                ├─► HoodieFileGroupReader over the file slice
                │     base file iterator ⊕ log blocks, merged per key
                │     with the SAME RecordMergeMode / merger / ordering
                │     fields as the write path
                │
                └─► write merged stream → new base file
                      same fileId, instantTime = compaction instant

Timeline: <instant>.commit (compaction completes as a commit action);
the new file slice (base only) supersedes base+logs for subsequent reads.
```

### 10.2 The Shared Merge Engine

Both MoR snapshot reads and compaction use `HoodieFileGroupReader` (`hudi-common/src/main/java/org/apache/hudi/common/table/read/HoodieFileGroupReader.java`) with a `FileGroupRecordBuffer`:

- `KeyBasedFileGroupRecordBuffer` (`hudi-common/.../common/table/read/buffer/KeyBasedFileGroupRecordBuffer.java`) — accumulates log records in a (spillable) map `key → BufferedRecord`, merging successive versions with `BufferedRecordMerger.deltaMerge`; then iterates the base file, calling `merge(baseRecord, bufferedLogRecord)` per key; delete records win or lose by ordering value.
- `PositionBasedFileGroupRecordBuffer` — same, keyed by record position in the base file when positions were recorded in log headers (cheaper than key comparison).
- Legacy scanner: `HoodieMergedLogRecordScanner` (`hudi-common/.../common/table/log/HoodieMergedLogRecordScanner.java`) with an `ExternalSpillableMap`, used by older read paths.

Because dedup (section 4), CoW rewrite (section 8), MoR read, and compaction all resolve to the same `RecordMergeMode`/merger/ordering-field configuration persisted in `hoodie.properties`, a record converges to the same value no matter when the merge physically happens.

---

## 11. Spark SQL MERGE INTO

### 11.1 Plan Resolution

```
MERGE INTO target t USING source s ON t.id = s.id WHEN MATCHED ... / NOT MATCHED ...
       │
       ▼
Spark parser → MergeIntoTable logical plan                       [Spark]
       │
       ▼
HoodieSparkSessionExtension registers analysis rules
  [hudi-spark-datasource/hudi-spark/src/main/scala/org/apache/spark/sql/hudi/HoodieSparkSessionExtension.scala]
       │
       ▼
HoodieAnalysis.ResolveImplementations (post-hoc rule)
  [hudi-spark-datasource/hudi-spark/src/main/scala/org/apache/spark/sql/hudi/analysis/HoodieAnalysis.scala]
  matches MergeIntoTable whose target resolves to a Hudi table
       │
       ▼
MergeIntoHoodieTableCommand(mergeInto)
  [hudi-spark-datasource/hudi-spark3-common/src/main/scala/org/apache/spark/sql/hudi/command/MergeIntoHoodieTableCommand.scala]
  (spark4 twin in hudi-spark4-common)
```

### 11.2 MergeIntoHoodieTableCommand.run()

```
run()
  │
  ├─► 1. buildMergeIntoConfig(): table meta, record key fields,
  │       ordering fields, payload class, partition fields
  │
  ├─► 2. validate():
  │       ON condition must equate ALL record key fields of the target
  │       with simple (possibly cast) source attributes — equality only
  │       (keyless tables get a more flexible join-based path);
  │       insert actions must assign the record key fields;
  │       ordering fields must be assigned in update/insert actions
  │       only under EVENT_TIME_ORDERING; update actions must assign
  │       the record key only on MoR with partial updates disabled,
  │       and on MoR target-table fields may not appear on the RHS
  │       (checkUpdatingActions / checkInsertingActions)
  │
  ├─► 3. getProcessedInputDf():
  │       record-keyed table   → use the source dataset directly (index
  │                              tagging will identify matches later)
  │       primary-keyless table→ LEFT OUTER JOIN source ⋈ target on the
  │                              merge condition to attach the target's
  │                              _hoodie_record_key meta column to updates
  │       then derive missing key/ordering columns from the ON clause
  │       and assignments
  │
  ├─► 4. serialize WHEN clauses into payload properties (Base64 expressions):
  │       hoodie.payload.update.condition.assignments   WHEN MATCHED → UPDATE
  │       hoodie.payload.insert.condition.assignments   WHEN NOT MATCHED → INSERT
  │       hoodie.payload.delete.condition               WHEN MATCHED → DELETE
  │       + hoodie.payload.record.schema, hoodie.payload.combined.schema,
  │         hoodie.payload.original.avro.payload
  │
  └─► 5. HoodieSparkSqlWriter.write(... ,
  │       OPERATION = upsert (insert only when the table has no
  │           ordering fields AND there are no update clauses),
  │       PAYLOAD_CLASS_NAME = ExpressionPayload,
  │       RECORD_MERGE_MODE = the table's merge mode, inherited
  │           (inferred from table config for table versions < 8),
  │       RECORD_MERGE_STRATEGY_ID = PAYLOAD_BASED_MERGE_STRATEGY_UUID)
  │       [hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/HoodieSparkSqlWriter.scala]
  │       → standard upsert pipeline of sections 3–9
  │
  └─► 6. refresh table in catalog
```

For tables **with record keys** there is no Spark-side join: MERGE INTO writes the source dataset through the standard upsert pipeline, and index tagging plays the role of the join. For **primary-keyless** tables the command does join source and target (left-outer, on the merge condition) to recover `_hoodie_record_key` for matched rows, and runs a *prepped* upsert — `_hoodie.spark.sql.merge.into.prepped` (`HoodieWriteConfig.SPARK_SQL_MERGE_INTO_PREPPED_KEY`) is set only in this keyless case. In both variants, clause evaluation is deferred into the merge itself via a special payload.

### 11.3 ExpressionPayload — Clauses Evaluated at Merge Time

`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/command/payload/ExpressionPayload.scala`

```
ExpressionPayload extends DefaultHoodieRecordPayload
  │
  ├─► combineAndGetUpdateValue(targetRecord, schema, props)     key MATCHED
  │     │
  │     ├─► joinRecord(source, target) → combined row
  │     ├─► for (condition, assignments) in update clauses
  │     │     (NO ordering guarantee among them — see note below):
  │     │     evaluate condition with code-generated SafeProjection
  │     │     first hit → apply assignments → merged record
  │     │                 (then doRecordMerge with the original payload for
  │     │                  ordering-aware semantics)
  │     ├─► no update hit → evaluate delete condition
  │     │     hit → Option.empty()                              DELETE
  │     └─► nothing matched → HoodieRecord.SENTINEL             row unchanged
  │
  └─► getInsertValue(schema, props)                             key NOT MATCHED
        │
        ├─► for (condition, assignments) in insert clauses:
        │     first hit → build insert record
        └─► no hit → SENTINEL (record discarded)

MoR nuance: an update arriving via a log file has no stored record to join at
write time, so for MoR the matched-clause evaluation also runs through
getInsertValue (flagged internally) and partial-update schemas
(hoodie.spark.sql.merge.into.partial.updates = true, table version ≥ 8) let
MERGE write only assigned columns into the log block.
```

Note the evaluation order is not strict SQL clause order: matched **update** clauses are grouped and tried before the single allowed **delete** condition, which is only evaluated when no update clause matched — even if `WHEN MATCHED ... THEN DELETE` was written before an update clause in the statement. Moreover, the relative order *among* update clauses (and likewise among insert clauses) is not a contract either: `serializeConditionalAssignments` stores them in a Scala `Map` and `ExpressionPayload.getEvaluator` iterates it via `toSeq`, so the first condition to evaluate true wins in an unspecified order — write the conditions of a clause group to be mutually exclusive rather than relying on clause precedence. Conditional UPDATE/DELETE/INSERT and multi-clause MERGE otherwise behave identically on CoW and MoR — the difference is only *where* the merged result lands (rewritten base file vs log block).

### 11.4 SQL UPDATE and DELETE

| Command | Class (in `hudi-spark-datasource/hudi-spark3-common/.../sql/hudi/command/`) | Translation |
|---|---|---|
| `UPDATE` | `UpdateHoodieTableCommand.scala` | Reads matching rows, applies SET expressions, writes back as a plain **upsert** (no ExpressionPayload). Record key / partition columns may not be assigned. |
| `DELETE` | `DeleteHoodieTableCommand.scala` | Writes matching keys with operation `delete` — see the companion `delete_mechanisms.md`. |

With `hoodie.spark.sql.optimized.writes.enable=true` (default since 0.14.0) these UPDATE/DELETE commands run "prepped" writes: the read side supplies Hudi meta columns (`_hoodie_record_key`, `_hoodie_partition_path`, `_hoodie_file_name`), so the write path can skip index lookup entirely. The flag does not govern MERGE INTO — record-keyed MERGE relies on index tagging, and only the primary-keyless variant runs prepped (section 11.2).

---

## 12. Configuration Summary

| Property | Default | Purpose |
|---|---|---|
| `hoodie.datasource.write.operation` | `upsert` | Write operation (upsert/insert/bulk_insert/delete/...) |
| `hoodie.table.ordering.fields` (alias `hoodie.table.precombine.field`) | — | Ordering columns for precombine and merge |
| `hoodie.record.merge.mode` | inferred: `EVENT_TIME_ORDERING` if ordering field set, else `COMMIT_TIME_ORDERING` | Merge semantics (1.0+) |
| `hoodie.record.merge.strategy.id` | per mode | Merger strategy UUID for CUSTOM mode |
| `hoodie.datasource.write.payload.class` (write) / `hoodie.compaction.payload.class` (table, deprecated after 1.0.0) | `OverwriteWithLatestAvroPayload` lineage | Legacy payload class; 1.x tables use merge mode/strategy configs instead |
| `hoodie.combine.before.upsert` | `true` | Deduplicate batch before upsert |
| `hoodie.index.type` | `SIMPLE` (Spark/Java), `INMEMORY` (Flink) | Index used for tagging |
| `hoodie.parquet.small.file.limit` | `104857600` (100 MB) | Base files below this are insert bin-packing targets |
| `hoodie.parquet.max.file.size` | `125829120` (120 MB) | Target max base file size |
| `hoodie.copyonwrite.record.size.estimate` | `1024` | Bootstrap record-size estimate (bytes) |
| `hoodie.logfile.to.parquet.compression.ratio` | `0.35` | Log→parquet size normalization for MoR small files |
| `hoodie.write.merge.handle.class` | `FileGroupReaderBasedMergeHandle` (1.1.0+) | CoW merge handle implementation |
| `hoodie.compact.merge.handle.class` | `FileGroupReaderBasedMergeHandle` | Compaction merge handle |
| `hoodie.logfile.data.block.format` | avro | Log data block format (avro/parquet) |
| `hoodie.compact.inline` | `false` | Run compaction inline after each write |
| `hoodie.compact.inline.max.delta.commits` | `5` | Delta commits between compactions (NUM_COMMITS trigger) |
| `hoodie.compaction.strategy` | `LogFileSizeBasedCompactionStrategy` | File-slice selection for compaction |
| `hoodie.spark.sql.optimized.writes.enable` | `true` (0.14.0+) | Prepped SQL UPDATE/DELETE (skip re-tagging); MERGE prepped mode is separate (`_hoodie.spark.sql.merge.into.prepped`, primary-keyless tables only) |
| `hoodie.spark.sql.merge.into.partial.updates` | `true` (1.0.0+) | Partial-update log blocks for MoR MERGE INTO |

---

## 13. Key Classes Reference

| Step | Class | Module / Path | Key Methods |
|---|---|---|---|
| **Entry point** | `SparkRDDWriteClient` | `hudi-client/hudi-spark-client/.../client/SparkRDDWriteClient.java` | `upsert()`, `preWrite()`, `postWrite()` |
| **Orchestration** | `BaseHoodieWriteClient` | `hudi-client/hudi-client-common/.../client/BaseHoodieWriteClient.java` | `commit()`, table service hooks |
| **CoW executor** | `SparkUpsertCommitActionExecutor` | `hudi-client/hudi-spark-client/.../action/commit/` | `execute()` |
| **MoR executor** | `SparkUpsertDeltaCommitActionExecutor` | `hudi-client/hudi-spark-client/.../action/deltacommit/` | `execute()` |
| **Shared executor base** | `BaseSparkCommitActionExecutor` | `hudi-client/hudi-spark-client/.../action/commit/` | `execute()`, `handleUpdate()`, `handleInsert()`, `buildProfile()` |
| **Dedup + tag + run** | `BaseWriteHelper` / `HoodieWriteHelper` | `hudi-client/hudi-client-common/.../action/commit/` | `write()`, `deduplicateRecords()`, `tag()` |
| **Workload stats** | `WorkloadProfile` | `hudi-client/hudi-client-common/.../table/WorkloadProfile.java` | per-partition insert/update counts |
| **CoW partitioner** | `UpsertPartitioner` | `hudi-client/hudi-spark-client/.../action/commit/` | `assignUpdates()`, `assignInserts()`, `getSmallFiles()`, `getPartition()` |
| **MoR partitioner** | `SparkUpsertDeltaCommitPartitioner` | `hudi-client/hudi-spark-client/.../action/deltacommit/` | `getSmallFiles()` (file-slice aware) |
| **Index API** | `HoodieIndex` | `hudi-client/hudi-client-common/.../index/HoodieIndex.java` | `tagLocation()`, `isGlobal()`, `canIndexLogFiles()` |
| **Index factory** | `SparkHoodieIndexFactory` | `hudi-client/hudi-spark-client/.../index/` | `createIndex()` |
| **CoW merge handle (legacy)** | `HoodieWriteMergeHandle` | `hudi-client/hudi-client-common/.../io/` | `doMerge()`, `write(oldRecord)`, `writeIncomingRecords()` |
| **CoW/compaction merge handle (default)** | `FileGroupReaderBasedMergeHandle` | `hudi-client/hudi-client-common/.../io/` | file-group-reader based merge |
| **Merge driver** | `HoodieMergeHelper` | `hudi-client/hudi-client-common/.../action/commit/` | `runMerge()` |
| **MoR append handle** | `HoodieAppendHandle` | `hudi-client/hudi-client-common/.../io/` | `doAppend()`, `appendDataAndDeleteBlocks()` |
| **Log format** | `HoodieLogFormat` / `HoodieLogFile` | `hudi-common/.../common/table/log/`, `hudi-common/.../common/model/` | `Writer.appendBlocks()` |
| **Compaction schedule** | `ScheduleCompactionActionExecutor` | `hudi-client/hudi-client-common/.../action/compact/` | `execute()` → `HoodieCompactionPlan` |
| **Compaction run** | `RunCompactionActionExecutor` | `hudi-client/hudi-client-common/.../action/compact/` | `execute()` |
| **Compaction strategy** | `LogFileSizeBasedCompactionStrategy` | `hudi-client/hudi-client-common/.../compact/strategy/` | `orderAndFilter()` |
| **Merged read/compact** | `HoodieFileGroupReader` + `KeyBasedFileGroupRecordBuffer` | `hudi-common/.../common/table/read/` | `getClosableIterator()`, `processDataBlock()` |
| **Merger API** | `HoodieRecordMerger` | `hudi-common/.../common/model/HoodieRecordMerger.java` | `merge()`, `partialMerge()`, `getMergingStrategy()` |
| **Merge mode** | `RecordMergeMode` | `hudi-common/.../common/config/RecordMergeMode.java` | COMMIT_TIME / EVENT_TIME / CUSTOM |
| **Legacy payload API** | `HoodieRecordPayload` | `hudi-common/.../common/model/HoodieRecordPayload.java` | `preCombine()`, `combineAndGetUpdateValue()` |
| **MERGE INTO command** | `MergeIntoHoodieTableCommand` | `hudi-spark-datasource/hudi-spark3-common/.../sql/hudi/command/` | `run()`, `validate()` |
| **MERGE payload** | `ExpressionPayload` | `hudi-spark-datasource/hudi-spark-common/.../sql/hudi/command/payload/` | `combineAndGetUpdateValue()`, `getInsertValue()` |
| **SQL routing** | `HoodieAnalysis` | `hudi-spark-datasource/hudi-spark/.../sql/hudi/analysis/` | `ResolveImplementations` |
| **SQL write entry** | `HoodieSparkSqlWriter` | `hudi-spark-datasource/hudi-spark-common/.../hudi/` | `write()` |
| **Table config** | `HoodieTableConfig` | `hudi-common/.../common/table/HoodieTableConfig.java` | `RECORD_MERGE_MODE`, `ORDERING_FIELDS`, `PAYLOAD_CLASS_NAME` |
| **Write config** | `HoodieWriteConfig` | `hudi-client/hudi-client-common/.../config/HoodieWriteConfig.java` | `shouldCombineBeforeUpsert()`, `MERGE_HANDLE_CLASS_NAME` |
