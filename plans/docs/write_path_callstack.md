# Hudi Write Path Call Stack

This document traces the full call stack of a Hudi write: from the entry points (Spark
DataFrame API, Spark SQL DML, Structured Streaming, the write-client API, Flink streaming
sink), through record preparation (key generation, deduplication), index lookup / record
tagging, partitioning and bucketing (workload profile, small-file packing), file writing
(create / merge / append handles), commit metadata, timeline state transitions, and the
optional inline table services that run after a commit. Driver/executor boundaries are
marked where relevant, and Copy-on-Write (CoW) vs Merge-on-Read (MoR) differences are
called out throughout.

A write runs in three logical stages that share one timeline instant:

1. **Prepare** — create the `REQUESTED` instant, convert input to `HoodieRecord`s,
   deduplicate, and tag each record with its existing file group via the index.
2. **Write** — build a workload profile, transition the instant to `INFLIGHT`, route
   records to buckets (existing file groups for updates, small or new file groups for
   inserts), and write base/log files through write handles (markers track every file).
3. **Commit** — collect `WriteStatus`es, resolve conflicts, reconcile markers, update the
   metadata table, complete the instant on the timeline, then run inline table services
   (clean, archive, compaction, clustering).

## Key modules and classes

| Module | Role | Key classes |
|---|---|---|
| `hudi-spark-datasource/hudi-spark-common` | Spark datasource entry, SQL writer orchestration | `DefaultSource`, `HoodieSparkSqlWriter`, `HoodieCreateRecordUtils`, `DataSourceUtils`, `HoodieStreamingSink`, `DataSourceOptions` |
| `hudi-spark-datasource/hudi-spark3-common` / `hudi-spark4-common` | Spark SQL DML commands | `InsertIntoHoodieTableCommand`, `MergeIntoHoodieTableCommand`, `UpdateHoodieTableCommand`, `DeleteHoodieTableCommand` |
| `hudi-client/hudi-spark-client` | Spark write client and executors | `SparkRDDWriteClient`, `HoodieSparkCopyOnWriteTable`, `HoodieSparkMergeOnReadTable`, `BaseSparkCommitActionExecutor`, `UpsertPartitioner`, `SparkLazyInsertIterable`, `SparkHoodieIndexFactory` |
| `hudi-client/hudi-client-common` | Engine-agnostic write pipeline, commit, handles | `BaseHoodieWriteClient`, `BaseHoodieTableServiceClient`, `BaseWriteHelper`/`HoodieWriteHelper`, `BaseCommitActionExecutor`, `HoodieCreateHandle`, `HoodieAppendHandle`, `HoodieMergeHandleFactory`, `FileGroupReaderBasedMergeHandle`, `WriteMarkers`, `HoodieWriteConfig` |
| `hudi-client/hudi-flink-client` | Flink write client and handles | `HoodieFlinkWriteClient`, `FlinkWriteHandleFactory`, `FlinkCreateHandle`, `FlinkMergeHandle`, `FlinkAppendHandle` |
| `hudi-flink-datasource/hudi-flink` | Flink streaming pipeline | `HoodieTableSink`, `Pipelines`, `BucketAssignFunction`, `StreamWriteFunction`, `StreamWriteOperatorCoordinator` |
| `hudi-common` | Data model, timeline, commit metadata | `HoodieRecord`, `HoodieKey`, `WriteOperationType`, `HoodieCommitMetadata`, `CommitUtils`, `HoodieActiveTimeline` (+ `ActiveTimelineV2`), `InstantFileNameGeneratorV2` |
| `hudi-utilities` | Ingestion service | `HoodieStreamer`, `StreamSync` |

## 1. Entry points

### 1.1 Spark DataFrame API

```text
df.write.format("hudi").options(...).save(basePath)
└─> DefaultSource.createRelation(sqlContext, mode, optParams, df)
    [hudi-spark-datasource/hudi-spark-common/.../hudi/DefaultSource.scala]
    └─> HoodieSparkSqlWriter.write(sqlContext, mode, optParams, df)        // section 2
        [hudi-spark-datasource/hudi-spark-common/.../hudi/HoodieSparkSqlWriter.scala]
```

### 1.2 Spark SQL DML

All SQL commands build a DataFrame plus a config map and funnel into the same
`HoodieSparkSqlWriter.write(...)` (command classes live in
`hudi-spark-datasource/hudi-spark3-common/.../sql/hudi/command/` with `hudi-spark4-common`
equivalents):

- `INSERT INTO / INSERT OVERWRITE` → `InsertIntoHoodieTableCommand` — derives the
  operation (insert / bulk_insert / insert_overwrite) from config and static partitions.
- `MERGE INTO` → `MergeIntoHoodieTableCommand` — compiles matched/not-matched clauses into
  an `ExpressionPayload` and runs an upsert through `HoodieSparkSqlWriter`.
- `UPDATE` → `UpdateHoodieTableCommand` — rewrites the assignments into an upsert.
- `DELETE FROM` → `DeleteHoodieTableCommand` — runs operation `delete` (see
  `plans/docs/delete_mechanisms.md`).

### 1.3 Spark Structured Streaming

- `HoodieStreamingSink.addBatch(batchId, df)`
  (`hudi-spark-datasource/hudi-spark-common/.../hudi/HoodieStreamingSink.scala`)
  → `HoodieSparkSqlWriter.write(...)` per micro-batch with `StreamingWriteParams`
  (retains the write client across batches, registers async compaction/clustering
  callbacks, records `SPARK_STREAMING_BATCH_ID` in the commit metadata for batch
  idempotency).

### 1.4 Write-client API (engine-level entry)

`SparkRDDWriteClient` (`hudi-client/hudi-spark-client/.../client/SparkRDDWriteClient.java`)
exposes one method per `WriteOperationType`: `upsert`, `upsertPreppedRecords`, `insert`,
`insertPreppedRecords`, `bulkInsert`, `bulkInsertPreppedRecords`, `delete`,
`deletePrepped`, `deletePartitions`, `insertOverwrite`, `insertOverwriteTable`. All share
the same preamble (section 3). `*Prepped` variants skip dedup + index tagging because the
records already carry their `HoodieRecordLocation`.

### 1.5 HoodieStreamer (hudi-utilities)

`StreamSync.writeToSink(...)` (`hudi-utilities/.../utilities/streamer/StreamSync.java`)
switches on `cfg.operation` and calls the matching `SparkRDDWriteClient` method (or the
row-writer `HoodieStreamerDatasetBulkInsertCommitActionExecutor` for bulk insert).

### 1.6 Flink

Streaming/batch SQL and DataStream writes are wired as a Flink operator pipeline; the
commit is performed on the JobManager by an operator coordinator. See section 8.

## 2. Spark datasource orchestration — `HoodieSparkSqlWriter`

`HoodieSparkSqlWriterInternal.writeInternal(...)`
(`hudi-spark-datasource/hudi-spark-common/.../hudi/HoodieSparkSqlWriter.scala`) runs on
the **driver**:

```text
writeInternal(sqlContext, mode, optParams, sourceDf, ...)
├─ resolve table path; create HoodieTableMetaClient
│   (HoodieTableMetaClient.newTableBuilder() bootstraps .hoodie/ for a new table)
├─ operation = deduceOperation(hoodieConfig, params, df)
│   // hoodie.datasource.write.operation, default UPSERT; SaveMode/insert-dup-policy
│   // can rewrite it (e.g. Overwrite -> insert_overwrite_table / bulk_insert)
├─ keyGenerator = HoodieSparkKeyGeneratorFactory.createKeyGenerator(props)
│   // record key + partition path extraction (hoodie.datasource.write.recordkey.field,
│   //  .partitionpath.field, keygenerator class/type)
├─ resolve writer schema (schema evolution / reconciliation against table schema)
├─ commitActionType = CommitUtils.getCommitActionType(operation, tableType)
│   // commit (CoW) | deltacommit (MoR) | replacecommit (insert_overwrite*, delete_partition)
├─ BULK_INSERT + hoodie.datasource.write.row.writer.enable (default true):
│   └─> bulkInsertAsRow(...)            // row-writer fast path: Dataset[Row] is written
│       // via HoodieDatasetBulkInsertHelper without ever converting to RDD[HoodieRecord]
├─ otherwise:
│   ├─ client = DataSourceUtils.createHoodieClient(...)         // SparkRDDWriteClient
│   ├─ instantTime = client.startCommit(commitActionType)       // section 3
│   ├─ hoodieRecords = HoodieCreateRecordUtils.createHoodieRecordRdd(...)
│   │   [.../hudi/HoodieCreateRecordUtils.scala]
│   │   // Dataset[Row] -> RDD[HoodieRecord]: applies the key generator per row and wraps
│   │   // payloads (avro) or rows (spark-native record type) — runs on EXECUTORS
│   ├─ handleInsertDuplicates(...)      // optional insert-dedup per insert-dup policy
│   └─> DataSourceUtils.doWriteOperation(client, records, instantTime, operation, isPrepped)
│       [hudi-spark-datasource/hudi-spark-common/.../hudi/DataSourceUtils.java]
│       └─ switch(operation): client.upsert | insert | bulkInsert | insertOverwrite | ...
└─> commitAndPerformPostOperations(...)
    ├─> client.commit(instantTime, writeStatuses, extraMeta, commitActionType,
    │                 partitionToReplacedFileIds, extraPreCommitFn, writeStatusValidator)
    │   // section 7 — this is where the lazy write DAG is finally executed and the
    │   // instant is completed; there is NO auto-commit inside the action executors
    └─ if async services enabled: client.scheduleCompaction / scheduleClustering
```

## 3. Write-client preamble and the REQUESTED instant

Every `SparkRDDWriteClient.<operation>()` runs the same driver-side preamble:

```text
SparkRDDWriteClient.upsert(records, instantTime)            // [driver]
├─ table = initTable(WriteOperationType.UPSERT, Option.of(instantTime))
│   // creates HoodieSparkTable (CoW or MoR) + timeline-server view, runs upgrade checks
├─ table.validateUpsertSchema()
├─ preWrite(instantTime, operationType, table.getMetaClient())
│   [hudi-client/hudi-client-common/.../client/BaseHoodieWriteClient.java]
│   // sets operation type, starts async cleaner/archiver hooks, pre-write validators
├─> result = table.upsert(context, instantTime, HoodieJavaRDD.of(records))   // sections 4-6
└─> postWrite(result, instantTime, table)
    // for regular writes result.isCommitted() == false -> just returns the WriteStatus
    // RDD; commit happens explicitly via client.commit(...) (section 7)
```

The instant itself is created earlier by `startCommit`:

```text
BaseHoodieWriteClient.startCommit(actionType)               // [driver]
  [hudi-client/hudi-client-common/.../client/BaseHoodieWriteClient.java]
├─ run upgrade if needed; rollback failed writes per
│   hoodie.write.failed.writes.cleaner.policy (EAGER default: roll back now;
│   LAZY for multi-writer: defer to clean, guarded by heartbeats)
├─ txnManager.beginStateChange(...)                          // lock for instant creation
├─ instantTime = createNewInstantTime(false)                 // TrueTime-style generator
├─ start heartbeat (LAZY policy only)
├─ for replace/clustering actions:
│   activeTimeline.createRequestedCommitWithReplaceMetadata(instantTime, actionType)
├─ else: activeTimeline.createNewInstant(
│            metaClient.createNewInstant(REQUESTED, actionType, instantTime))
│   // writes .hoodie/timeline/<t>.commit.requested (empty content)
└─ txnManager.endStateChange(...)
```

Action types come from `CommitUtils.getCommitActionType(operation, tableType)`
(`hudi-common/.../common/util/CommitUtils.java`): `INSERT_OVERWRITE`,
`INSERT_OVERWRITE_TABLE` and `DELETE_PARTITION` always use **`replacecommit`**; all other
operations use **`commit`** on CoW and **`deltacommit`** on MoR.

## 4. Record preparation: deduplication and index tagging

`HoodieSparkCopyOnWriteTable.upsert(...)` dispatches to
`SparkUpsertCommitActionExecutor` (CoW) /
`SparkUpsertDeltaCommitActionExecutor` (MoR), which delegate record prep to the write
helper before partition-and-write:

```text
HoodieWriteHelper.write(instantTime, inputRecords, context, table,
                        shouldCombine, configuredShuffleParallelism, executor, operationType)
  [hudi-client/hudi-client-common/.../table/action/commit/BaseWriteHelper.java
   + HoodieWriteHelper.java]
├─ [executors] deduplicateRecords(records, table, parallelism)     // "precombine"
│   // only if shouldCombine: hoodie.combine.before.upsert (default true) /
│   //                        hoodie.combine.before.insert (default false)
│   // mapToPair(recordKey) -> reduceByKey(BufferedRecordMerger.deltaMerge)
│   // merge honors the table's record merge mode / ordering (precombine) fields
├─ if table.getIndex().requiresTagging(operationType):
│   // HoodieIndex default: UPSERT, DELETE, DELETE_PREPPED only; HoodieBucketIndex
│   // overrides this to ALSO tag INSERT, INSERT_OVERWRITE and BULK_INSERT, because
│   // hash-based routing assigns every record's file group via tagLocation
│   └─ [executors] taggedRecords = table.getIndex().tagLocation(records, context, table)
│       // sets HoodieRecordLocation (fileId + instant) on records whose key already
│       // exists; untagged records are inserts
└─> executor.execute(taggedRecords)                                // section 5
```

### 4.1 Index implementations

`SparkHoodieIndexFactory.createIndex(config)`
(`hudi-client/hudi-spark-client/.../index/SparkHoodieIndexFactory.java`), selected by
`hoodie.index.type` — the default is **SIMPLE** for Spark/Java and `INMEMORY` for Flink
(`HoodieIndexConfig.Builder.getDefaultIndexType`); note the Flink **datasource** sets its
own default `index.type = FLINK_STATE` (`FlinkOptions.INDEX_TYPE`,
`hudi-flink-datasource/hudi-flink/.../configuration/FlinkOptions.java`), realized as the
keyed-state lookup of section 8.2 rather than a `HoodieIndex` implementation:

| Index type | Class | Tagging mechanism |
|---|---|---|
| `SIMPLE` (Spark default) | `HoodieSimpleIndex` | join incoming keys against keys read from the latest base files of affected partitions |
| `GLOBAL_SIMPLE` | `HoodieGlobalSimpleIndex` | same, across all partitions (handles partition-path changes) |
| `BLOOM` / `GLOBAL_BLOOM` | `HoodieBloomIndex` / `HoodieGlobalBloomIndex` (`hudi-client/hudi-client-common/.../index/bloom/`) | prune candidate files by min/max key ranges (from the metadata table's `column_stats` when `hoodie.bloom.index.use.metadata`, else file footers), test bloom filters, then verify candidates by reading actual keys |
| `BUCKET` | `HoodieSimpleBucketIndex` / `HoodieSparkConsistentBucketIndex` | hash(record key) → bucket → fileId; no lookup at all |
| `RECORD_INDEX` / `GLOBAL_RECORD_LEVEL_INDEX` | `SparkMetadataTableGlobalRecordLevelIndex` | point lookups in the metadata table `record_index` partition |
| `RECORD_LEVEL_INDEX` | `SparkMetadataTableRecordLevelIndex` | per-partition record-level index in the metadata table |
| `INMEMORY` | `HoodieInMemoryHashIndex` | in-process map (tests/Flink) |

Driver/executor: the index *plan* (which partitions/files to load) is computed on the
driver, but the actual lookup is a distributed join/shuffle on the **executors**.
Indexes whose `canIndexLogFiles()` returns true can tag records into file groups that
only have log files, which lets MoR route inserts to logs (section 6.3): the
consistent-hashing bucket index (`HoodieSparkConsistentBucketIndex`, inheriting `true`
from `HoodieBucketIndex`) and the in-memory index. The **simple** bucket index
(`HoodieSimpleBucketIndex`) overrides this to false, as do the record-level indexes
(`SparkMetadataTableGlobalRecordLevelIndex.canIndexLogFiles()` returns false, inherited
by `SparkMetadataTableRecordLevelIndex`) — so MoR inserts under those indexes still
create base files.

## 5. Partitioning and bucketing

`BaseSparkCommitActionExecutor.execute(inputRecords)`
(`hudi-client/hudi-spark-client/.../table/action/commit/BaseSparkCommitActionExecutor.java`):

```text
BaseSparkCommitActionExecutor.execute(inputRecords)
├─ [executors→driver] clusteringHandleUpdate(inputRecords)
│   // if updates touch file groups under pending clustering: reject or allow per
│   // hoodie.clustering.updates.strategy
├─ [executors→driver] profile = new WorkloadProfile(buildProfile(records), operationType,
│                                                   index.canIndexLogFiles())
│   // countByKey over (partitionPath, location) — insert counts per partition and
│   // update counts per existing file group, COLLECTED TO THE DRIVER
├─ [driver] partitioner = getPartitioner(profile)        // UpsertPartitioner (section 5.1)
│   // bucket/small-file assignment happens HERE, before the timeline transition
├─ [driver] saveWorkloadProfileMetadataToInflight(profile, instantTime)
│   [hudi-client/hudi-client-common/.../table/action/commit/BaseCommitActionExecutor.java]
│   └─ activeTimeline.transitionRequestedToInflight(requested, Option.of(metadata))
│       // TIMELINE TRANSITION: <t>.commit.requested -> <t>.commit.inflight; the inflight
│       // file persists the workload stats (used by rollback to know planned writes)
├─ [executors] writeStatuses = mapPartitionsAsRDD(records, partitioner)      // section 6
└─ updateIndexAndMaybeRunPreCommitValidations(writeStatuses, result)
    ├─ [executors] writeStatuses.persist(...)    // caches — but the DAG is still only
    │   // materialized when client.commit() dereferences it (section 7)
    ├─ index.updateLocation(writeStatuses, ...)  // e.g. record-index/bucket bookkeeping
    └─ optional pre-commit validators (hoodie.precommit.validators)
```

### 5.1 `UpsertPartitioner` — buckets and small-file packing (CoW)

`UpsertPartitioner` (`hudi-client/hudi-spark-client/.../table/action/commit/UpsertPartitioner.java`)
is a Spark `Partitioner` built **on the driver**; each output partition is one *bucket*
(`BucketInfo` = `bucketType` UPDATE|INSERT, `fileIdPrefix`, `partitionPath`):

```text
UpsertPartitioner(profile, context, table, config, operationType)
├─ assignUpdates(profile)
│   // one UPDATE bucket per existing file group that received updates — every update
│   // is routed back to its own file group (tagged location)
└─ assignInserts(profile, context)
    ├─ getSmallFilesForPartitions(...)   // [executors] list latest base files per
    │   // partition smaller than hoodie.parquet.small.file.limit (default 100 MB)
    ├─ averageBytesPerRecord(timeline, config)   // estimate from past commit metadata
    ├─ first fill small files up to the limit (bucket type UPDATE — they are rewritten
    │   as a merge, "bin-packing" new inserts into existing file groups)
    └─ remaining inserts: new INSERT buckets, each sized by
        hoodie.copyonwrite.insert.split.size (default 500000 records);
        each INSERT bucket creates a brand-new file group (fresh fileId)
```

### 5.2 MoR variant

`SparkUpsertDeltaCommitPartitioner`
(`hudi-client/hudi-spark-client/.../table/action/deltacommit/SparkUpsertDeltaCommitPartitioner.java`)
overrides small-file selection: it looks at the latest **file slices** (base + logs,
estimating log size contribution), and if the index can index log files it can target
file groups that have no base file at all. Updates on MoR do not rewrite the base file —
they become log appends (section 6.3), so "small file" packing matters mainly for routing
*inserts* into existing slices.

## 6. File writing (executors)

```text
BaseSparkCommitActionExecutor.mapPartitionsAsRDD(dedupedRecords, partitioner)
├─ mapToPair(record -> ((key, location), record))
├─ partitionBy(partitioner)            // or repartitionAndSortWithinPartitions when
│                                      //  table.requireSortedRecords()
└─ mapPartitionsWithIndex((bucketIdx, recordItr) ->
       handleUpsertPartition(instantTime, bucketIdx, recordItr, bucketInfoGetter))
    └─ switch (bucketInfo.bucketType):
        ├─ UPDATE -> handleUpdate(partitionPath, fileId, recordItr)     // 6.1 / 6.3
        └─ INSERT -> handleInsert(fileIdPrefix, recordItr)              // 6.2
   // flatMap -> HoodieData<WriteStatus> — LAZY: nothing executes until the commit
   // (or a validator) dereferences the RDD
```

Every handle creates a **marker** before touching data files
(`WriteMarkers.create(partitionPath, fileName, IOType.{CREATE,MERGE,APPEND})`,
`hudi-client/hudi-client-common/.../table/marker/WriteMarkers.java`) under
`.hoodie/.temp/<instantTime>/`. The marker mechanism (`hoodie.write.markers.type`) is
either `TIMELINE_SERVER_BASED` (marker creation batched through the embedded timeline
server) or `DIRECT` (the handle writes marker files itself). The engine-aware default
(`HoodieWriteConfig.Builder.getDefaultMarkersType`) is `TIMELINE_SERVER_BASED` for Spark
only when the embedded timeline server is enabled, and always `DIRECT` for Flink and
Java; `WriteMarkersFactory.get` additionally falls back to `DIRECT` when the timeline
server is unavailable or the base path is on HDFS, and `HoodieStreamingSink` forces
`DIRECT` for Spark structured streaming (the timeline server is closed between
micro-batches). Markers are how failed writes are reconciled and rolled back
(section 7, 10).

### 6.1 UPDATE buckets on CoW — merge into a new base file

```text
handleUpdate(partitionPath, fileId, recordItr)
└─> HoodieMergeHandleFactory.create(operationType, config, instantTime, table, recordItr,
                                    partitionPath, fileId, taskContextSupplier, keyGenOpt)
    [hudi-client/hudi-client-common/.../io/HoodieMergeHandleFactory.java]
    // default hoodie.write.merge.handle.class = FileGroupReaderBasedMergeHandle;
    // overrides: HoodieSortedMergeHandle (requireSortedRecords),
    //            HoodieMergeHandleWithChangeLog (CDC enabled)
    └─ IOUtils.runMerge(mergeHandle, instantTime, fileId)
        ├─ mergeHandle.doMerge()
        │   // FileGroupReaderBasedMergeHandle streams the OLD base file through
        │   // HoodieFileGroupReader with the incoming records as an in-memory/spillable
        │   // overlay, applying the record merge mode (commit-time / event-time /
        │   // custom merger or payload), and writes the merged stream into a NEW base
        │   // file <fileId>_<writeToken>_<instantTime>.parquet in the SAME file group
        └─ mergeHandle.close()          // List<WriteStatus> with HoodieWriteStat
```

### 6.2 INSERT buckets — new base files via lazy iterator

```text
handleInsert(idPfx, recordItr)
└─> new SparkLazyInsertIterable<>(recordItr, ..., new CreateHandleFactory<>(), ...)
    [hudi-client/hudi-spark-client/.../execution/SparkLazyInsertIterable.java]
    └─ HoodieExecutor (hoodie.write.executor.type: SIMPLE default | BOUNDED_IN_MEMORY
       | DISRUPTOR) decouples record production from file writing
        └─ HoodieCreateHandle [hudi-client/hudi-client-common/.../io/HoodieCreateHandle.java]
            ├─ writes HoodiePartitionMetadata (.hoodie_partition_metadata) + CREATE marker
            ├─ HoodieFileWriterFactory -> HoodieParquetWriter (or orc/hfile)
            ├─ write() per record until hoodie.parquet.max.file.size, then roll over to a
            │   new fileId (CopyOnWriteInsertHandler rotates handles)
            └─ close() -> WriteStatus
```

### 6.3 MoR delta path — appends to log files

`BaseSparkDeltaCommitActionExecutor`
(`hudi-client/hudi-spark-client/.../table/action/deltacommit/BaseSparkDeltaCommitActionExecutor.java`)
overrides bucket handling:

```text
handleUpdate(partitionPath, fileId, recordItr)        // MoR
└─> appendHandle = new HoodieAppendHandle(...); appendHandle.doAppend(); close()
    [hudi-client/hudi-client-common/.../io/HoodieAppendHandle.java]
    ├─ finds the latest file slice of the file group; appends to its current log file
    │   .<fileId>_<baseInstant>.log.<version>_<writeToken> (APPEND marker)
    ├─ buffers records into log blocks: HoodieAvroDataBlock by default,
    │   HoodieParquetDataBlock/HoodieHFileDataBlock per hoodie.logfile.data.block.format;
    │   deletes become HoodieDeleteBlocks
    ├─ flushes a block at hoodie.logfile.data.block.max.size (256 MB default);
    │   rolls the log file at hoodie.logfile.max.size (1 GB default)
    └─ close() -> WriteStatus (numUpdates/numDeletes, log files written)

handleInsert(idPfx, recordItr)                        // MoR
├─ if index.canIndexLogFiles(): SparkLazyInsertIterable with AppendHandleFactory
│   // inserts also go to LOG files (file group may have no base file yet)
└─ else: super.handleInsert(...)                      // CoW behavior: new base files
```

**CoW vs MoR in one line:** CoW rewrites the whole base file for every update bucket
(write-amplified, read-optimized); MoR appends row-level deltas to log files
(write-optimized) and defers the rewrite to compaction
(see `plans/docs/compaction_callstack.md`).

### 6.4 Bulk insert

`SparkBulkInsertHelper.bulkInsert(...)`
(`hudi-client/hudi-spark-client/.../table/action/commit/SparkBulkInsertHelper.java`) skips
index lookup and the workload profile entirely: it transitions the instant to inflight,
repartitions/sorts the input per `hoodie.bulkinsert.sort.mode` (default `NONE`;
`GLOBAL_SORT`, `PARTITION_SORT`, `PARTITION_PATH_REPARTITION` variants via
`BulkInsertInternalPartitionerFactory`), and writes new files with create handles via
`BulkInsertMapFunction`. Bucket-index tables route records to their hash buckets through
dedicated bulk-insert partitioners instead (`RDDSimpleBucketBulkInsertPartitioner`,
`RDDConsistentBucketBulkInsertPartitioner`,
`hudi-client/hudi-spark-client/.../execution/bulkinsert/`). The datasource row-writer path (section 2) does the same over
`Dataset[Row]` with `HoodieDatasetBulkInsertHelper` — no `HoodieRecord` conversion at all.

## 7. Commit: metadata and timeline completion

For regular datasource/SQL writes the commit is explicit (no auto-commit):

```text
SparkRDDWriteClient.commit(instantTime, writeStatuses, extraMetadata, commitActionType,
                           partitionToReplacedFileIds, extraPreCommitFunc, writeStatusValidator)
  [hudi-client/hudi-spark-client/.../client/SparkRDDWriteClient.java]
├─ optional streaming writes to the metadata table
│   (streamingMetadataWriteHandler.streamWriteToMetadataTable(...))
├─ [executors->driver] DEREFERENCE THE WRITE DAG: collect HoodieWriteStats and
│   totalRecords / totalErrorRecords from the WriteStatus RDD — this is the moment all
│   file writes of section 6 actually execute (if not already forced)
├─ writeStatusValidator.validate(totalRecords, totalErrorRecords, statuses)
│   // datasource default fails the commit if any error records exist -> returns false,
│   // instant stays inflight and is rolled back later
└─> BaseHoodieWriteClient.commitStats(instantTime, tableWriteStats, extraMetadata,
                                      commitActionType, partitionToReplacedFileIds, ...)
    [hudi-client/hudi-client-common/.../client/BaseHoodieWriteClient.java]
    ├─ metadata = CommitUtils.buildMetadata(writeStats, partitionToReplacedFileIds,
    │       extraMetadata, operationType, schema, commitActionType)   // HoodieCommitMetadata
    ├─ txnManager.beginStateChange(inflightInstant, lastCompletedTxn)
    ├─ preCommit(metadata)
    │   └─ resolveWriteConflict(...)    // OCC: TransactionUtils + hoodie.write.concurrency.mode
    │       // (SINGLE_WRITER default; OPTIMISTIC_CONCURRENCY_CONTROL uses
    │       //  SimpleConcurrentFileWritesConflictResolutionStrategy over overlapping
    │       //  file groups of concurrent instants -> HoodieWriteConflictException)
    ├─ commit(table, commitActionType, instantTime, metadata, writeStats)
    │   ├─ finalizeWrite(table, instantTime, stats)
    │   │   └─ HoodieTable.finalizeWrite -> reconcileAgainstMarkers(...)
    │   │       [hudi-client/hudi-client-common/.../table/HoodieTable.java]
    │   │       // compares marker dir vs committed stats and DELETES stray files left
    │   │       // by failed/duplicate Spark task attempts
    │   ├─ writeToMetadataTable(...)
    │   │   └─ HoodieBackedTableMetadataWriter.update(metadata, instantTime)
    │   │       // updates files / column_stats / bloom_filters / record_index /
    │   │       // secondary-index partitions of <table>/.hoodie/metadata
    │   └─ activeTimeline.saveAsComplete(false, inflightInstant, Option.of(metadata), ...)
    │       [hudi-common/.../timeline/versioning/v2/ActiveTimelineV2.java]
    │       // TIMELINE TRANSITION: <t>.commit.inflight ->
    │       //   <t>_<completionTime>.commit  (completion time = new instant time)
    ├─ txnManager.endStateChange(...)
    ├─ WriteMarkersFactory...quietDeleteMarkerDir(...)     // markers no longer needed
    ├─ mayBeCleanAndArchive(table)
    │   ├─ autoCleanOnCommit()      // hoodie.clean.automatic (true); sync or async per
    │   │                           //  hoodie.clean.async.enabled (false)
    │   └─ autoArchiveOnCommit()    // hoodie.archive.automatic (true); HoodieTimelineArchiver
    │                               //  trims the active timeline per hoodie.keep.{min,max}.commits
    ├─ runTableServicesInline(table, metadata, extraMetadata)
    │   └─> BaseHoodieTableServiceClient.runTableServicesInline(...)
    │       [hudi-client/hudi-client-common/.../client/BaseHoodieTableServiceClient.java]
    │       ├─ inline (or schedule-inline) COMPACTION   // hoodie.compact.inline — MoR only
    │       ├─ inline LOG COMPACTION                    // hoodie.log.compaction.inline
    │       ├─ inline (or schedule-inline) CLUSTERING   // hoodie.clustering.inline
    │       └─ inline partition TTL management          // hoodie.partition.ttl.inline
    └─ emitCommitMetrics(...); commit callbacks (hoodie.write.commit.callback.*)
```

## 8. Flink call stack

### 8.1 Pipeline wiring

```text
HoodieTableSink.getSinkRuntimeProvider(...)
  [hudi-flink-datasource/hudi-flink/.../table/HoodieTableSink.java]
├─ batch bulk_insert      -> Pipelines.bulkInsert(conf, rowType, dataStream)
├─ append-only (INSERT + no upsert semantics) -> Pipelines.append(...)
└─ default upsert         -> Pipelines.bootstrap(...) + Pipelines.hoodieStreamWrite(...)
    [hudi-flink-datasource/hudi-flink/.../sink/utils/Pipelines.java]

RowData
  -> RowDataToHoodieFunction                  // key generation -> HoodieFlinkInternalRow
  -> BootstrapOperator                        // on restore: re-feed existing index records
  -> BucketAssignFunction (keyed by record key)   // index lookup + bucket assignment
  -> keyBy(fileId) -> StreamWriteOperator(StreamWriteFunction)   // buffer + write
  -> StreamWriteOperatorCoordinator (JobManager)                 // instant + commit
// bucket-index variant replaces BucketAssignFunction with hash-based routing into
// BucketStreamWriteOperator; append path uses AppendWriteFunction (no index at all)
```

### 8.2 Tagging and bucketing — `BucketAssignFunction`

(`hudi-flink-datasource/hudi-flink/.../sink/partitioner/BucketAssignFunction.java`) —
the Flink analogue of index tagging + `UpsertPartitioner`, backed by **keyed state**: the
per-key `HoodieRecordGlobalLocation` is the index. Existing key → reuse its fileId
(update; with a global index a partition change emits a delete for the old partition);
new key → `BucketAssigner.addInsert(partition)`
(`.../sink/partitioner/BucketAssigner.java`) which packs small files first (via
`WriteProfile`, same small-file idea as section 5.1) or assigns a new fileId.

### 8.3 Writing and commit — function + coordinator

```text
StreamWriteFunction [.../sink/StreamWriteFunction.java]   // one subtask per write task
├─ processElement: buffer rows per bucket (RowDataBucket, memory-pooled);
│   flushBucket(...) when a bucket/total buffer exceeds write.batch.size /
│   write.task.max.size:
│   ├─ instant = instantToWrite(...)        // requested from the coordinator
│   ├─ writeFunction -> HoodieFlinkWriteClient.upsert/insert(records, bucketInfo, instant)
│   └─ eventGateway.sendEventToCoordinator(WriteMetadataEvent{writeStatuses})
└─ snapshotState (checkpoint): flushRemaining(...) — flush ALL buckets, send the final
    WriteMetadataEvent (lastBatch=true), writeClient.cleanHandles()

StreamWriteOperatorCoordinator [.../sink/StreamWriteOperatorCoordinator.java]  // JobManager
├─ on instant request: startInstant()
│   ├─ writeClient.startCommit(commitAction, metaClient)        // section 3 stack
│   └─ transitionRequestedToInflight(...)                       // requested -> inflight
├─ handleEventFromOperator: buffer WriteMetadataEvents per (checkpoint, instant)
└─ notifyCheckpointComplete(checkpointId)
    ├─ commitInstant(...): merge all tasks' WriteStatuses,
    │   writeClient.commit(instant, statuses, metadata, commitAction, replacedFileIds)
    │   // -> commitStats stack of section 7 (metadata table, saveAsComplete, ...)
    └─ scheduleTableServices(...)    // CompactionUtil.scheduleCompaction /
                                     // ClusteringUtil.scheduleClustering
// One Flink checkpoint == one Hudi instant (deltacommit/commit).
```

### 8.4 Flink write handles (mini-batch model)

`HoodieFlinkWriteClient` (`hudi-client/hudi-flink-client/.../client/HoodieFlinkWriteClient.java`)
writes one explicit bucket per call (no Spark-style shuffle); handles are chosen by
`FlinkWriteHandleFactory` (`hudi-client/hudi-flink-client/.../io/FlinkWriteHandleFactory.java`)
and **reused across mini-batches within a checkpoint** (`bucketToHandles`):

- **CoW**: `FlinkCreateHandle` (new file group), `FlinkMergeHandle` (first mini-batch
  merge with the old base file), `FlinkIncrementalMergeHandle` (subsequent mini-batches
  merge with the file written by the previous flush).
- **MoR**: `FlinkAppendHandle` (extends `HoodieAppendHandle`) appends log blocks; the
  same log file keeps growing across mini-batches; compaction (scheduled by the
  coordinator) later folds logs into base files.

## 9. CoW vs MoR summary

| Aspect | Copy-on-Write | Merge-on-Read |
|---|---|---|
| Commit action | `commit` | `deltacommit` |
| Action executors | `commit` package (`SparkUpsertCommitActionExecutor`, ...) | `deltacommit` package (`SparkUpsertDeltaCommitActionExecutor`, ...) overriding `handleUpdate`/`handleInsert` |
| Update write | rewrite base file via merge handle (`FileGroupReaderBasedMergeHandle`) | append `HoodieDataBlock`s to log file via `HoodieAppendHandle` |
| Insert write | new base file (`HoodieCreateHandle`), small files filled via merge | new base file, or log file when the index `canIndexLogFiles()` |
| Small-file handling | pack inserts into small base files (`UpsertPartitioner`) | pack inserts into small file **slices** (`SparkUpsertDeltaCommitPartitioner`) |
| Delete | merged out of the rewritten base file | `HoodieDeleteBlock` tombstone in the log |
| Post-commit services | clean, archive, clustering | + compaction (see `compaction_callstack.md`) |

## 10. Timeline transitions and failure handling

| Step | Timeline file (under `.hoodie/timeline/`) | State | Performed by |
|---|---|---|---|
| Start commit | `<t>.commit.requested` (empty; replace actions may carry requested metadata) | REQUESTED | `HoodieActiveTimeline.createNewInstant` ← `BaseHoodieWriteClient.startCommit` |
| Workload planned | `<t>.commit.inflight` (`HoodieCommitMetadata` with workload stats) | INFLIGHT | `transitionRequestedToInflight` ← `BaseCommitActionExecutor.saveWorkloadProfileMetadataToInflight` (Spark) / `StreamWriteOperatorCoordinator.startInstant` (Flink) |
| Complete | `<t>_<completionTime>.commit` (full `HoodieCommitMetadata`) | COMPLETED | `ActiveTimelineV2.saveAsComplete` ← `BaseHoodieWriteClient.commitStats` |

(For MoR replace `commit` with `deltacommit`; for `insert_overwrite*` / `delete_partition`
with `replacecommit`, whose completed metadata additionally carries
`partitionToReplacedFileIds`. File naming per `InstantFileNameGeneratorV2`,
`hudi-common/.../timeline/versioning/v2/InstantFileNameGeneratorV2.java`; table versions
< 8 use the legacy `.hoodie/` layout via the v1 classes.)

**Failure / restart semantics:**

- **Task retries within a job**: Spark may run duplicate attempts of a write task; every
  attempt creates markers, and `finalizeWrite → reconcileAgainstMarkers` deletes any data
  file that has a marker but no corresponding `HoodieWriteStat` in the commit metadata.
- **Failed commits**: an instant left in REQUESTED/INFLIGHT is rolled back marker-by-marker
  on the next write (`rollbackFailedWrites` in `startCommit`, EAGER policy) or lazily by
  the cleaner when multi-writing (LAZY policy + heartbeats — an expired heartbeat marks
  the instant abandoned).
- **Error records**: `WriteStatus` accumulates per-record failures; the datasource
  `WriteStatusValidator` aborts the commit (returns false, no timeline completion) if any
  error records exist.
- **Multi-writer conflicts**: with OCC enabled, `preCommit` re-reads instants completed
  after this write started and fails with `HoodieWriteConflictException` when two writers
  touched the same file groups; the loser's instant is rolled back.
- Readers never see partial data: only the atomic completed-instant file makes the new
  file slices visible to the file system view.

## 11. Key configuration reference

| Config | Default | Effect |
|---|---|---|
| `hoodie.datasource.write.operation` | `upsert` | Write operation routed by `HoodieSparkSqlWriter`/`DataSourceUtils` |
| `hoodie.datasource.write.table.type` | `COPY_ON_WRITE` | CoW vs MoR (commit vs deltacommit path) |
| `hoodie.datasource.write.precombine.field` | (table ordering field) | Ordering field for dedup/merging |
| `hoodie.index.type` | `SIMPLE` (Spark/Java) | Index used by `tagLocation` (section 4.1) |
| `hoodie.combine.before.upsert` / `.insert` | `true` / `false` | Pre-write deduplication by record key |
| `hoodie.parquet.small.file.limit` | `104857600` (100 MB) | Base files below this are insert targets |
| `hoodie.copyonwrite.insert.split.size` | `500000` | Records per new insert bucket/file group |
| `hoodie.parquet.max.file.size` | `125829120` (120 MB) | Roll-over size for new base files |
| `hoodie.datasource.write.row.writer.enable` | `true` | Bulk-insert row-writer fast path |
| `hoodie.bulkinsert.sort.mode` | `NONE` | Bulk-insert repartition/sort strategy |
| `hoodie.write.executor.type` | `SIMPLE` | Insert-path producer/consumer executor |
| `hoodie.write.merge.handle.class` | `FileGroupReaderBasedMergeHandle` | CoW update merge implementation |
| `hoodie.logfile.data.block.max.size` / `hoodie.logfile.max.size` | 256 MB / 1 GB | MoR log block flush / log file roll-over |
| `hoodie.logfile.data.block.format` | (avro unless set) | MoR log data block encoding |
| `hoodie.write.markers.type` | engine-derived (see section 6) | Marker mechanism under `.hoodie/.temp/<t>/` |
| `hoodie.write.concurrency.mode` | `SINGLE_WRITER` | OCC conflict resolution on preCommit |
| `hoodie.write.failed.writes.cleaner.policy` | `EAGER` | When failed writes are rolled back |
| `hoodie.clean.automatic` / `hoodie.archive.automatic` | `true` / `true` | Post-commit clean / archive |
| `hoodie.compact.inline` / `hoodie.clustering.inline` | `false` / `false` | Inline table services after commit |
| `hoodie.metadata.enable` | `true` | Maintain `<table>/.hoodie/metadata` on commit |

(Flink option equivalents — `write.operation`, `write.batch.size`, `write.task.max.size`,
`index.type`, `compaction.*` — live in
`hudi-flink-datasource/hudi-flink/.../configuration/FlinkOptions.java`.)
