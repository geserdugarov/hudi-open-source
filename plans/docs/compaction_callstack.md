# Hudi Compaction Call Stack

This document traces the full call stack of Hudi compaction for Merge-on-Read (MoR) tables:
from the entry points that trigger it, through scheduling and plan generation, instant creation
on the timeline, file-slice selection, log merging, base-file writing, commit metadata, and the
timeline state transitions. Both Spark and Flink paths are covered.

Compaction merges the log files of a MoR file slice into its base file, producing a **new file
slice** (new base file with the compaction instant as its base instant time) in the same file
group. It runs in two distinct steps that may execute in different processes at different times:

1. **Schedule** — select file slices, build a `HoodieCompactionPlan`, and persist it as a
   `<instant>.compaction.requested` instant on the timeline.
2. **Execute** — read the plan back, transition the instant to inflight, compact each file
   group, and complete the instant as a regular `<instant>.commit`.

Related but separate: **log compaction** (`LOG_COMPACT`) is a minor variant that merges small
log files into a bigger log file instead of rewriting the base file. It shares most of this call
stack and is noted where it diverges.

## Key modules and classes

| Module | Role | Key classes |
|---|---|---|
| `hudi-client/hudi-client-common` | Engine-agnostic scheduling, execution, commit | `BaseHoodieWriteClient`, `BaseHoodieTableServiceClient`, `ScheduleCompactionActionExecutor`, `RunCompactionActionExecutor`, `HoodieCompactor`, `CompactHelpers`, `FileGroupReaderBasedMergeHandle`, `HoodieMergeHandleFactory`, `HoodieCompactionConfig` |
| `hudi-client/hudi-spark-client` | Spark engine binding | `HoodieSparkMergeOnReadTable`, `HoodieSparkMergeOnReadTableCompactor`, `SparkAsyncCompactService`, `HoodieSparkCompactor` |
| `hudi-client/hudi-flink-client` | Flink engine binding | `HoodieFlinkMergeOnReadTable`, `HoodieFlinkMergeOnReadTableCompactor` |
| `hudi-common` | Plan/metadata model, timeline, file system view | `HoodieCompactionPlan` (Avro), `CompactionOperation`, `CompactionUtils`, `HoodieActiveTimeline` (+ `ActiveTimelineV1/V2`), `HoodieCommitMetadata`, `SyncableFileSystemView`, `HoodieFileGroupReader` |
| `hudi-flink-datasource/hudi-flink` | Flink streaming/offline compaction pipeline | `StreamWriteOperatorCoordinator`, `CompactionPlanOperator`, `CompactOperator`, `CompactionCommitSink`, `HoodieFlinkCompactor`, `CompactionUtil` |
| `hudi-spark-datasource/hudi-spark` | Spark SQL procedures | `RunCompactionProcedure`, `ShowCompactionProcedure` |
| `hudi-utilities` | Standalone job / streamer integration | `HoodieCompactor` (utility), `HoodieStreamer` |
| `hudi-cli` | Interactive commands | `CompactionCommand` |

## 1. Entry points

### 1.1 Spark

- **Inline compaction after a commit** (`hoodie.compact.inline=true`):
  - `BaseHoodieWriteClient.commit(...)`
    - → `runTableServicesInline(table, metadata, extraMetadata)`
    - → `BaseHoodieTableServiceClient.runTableServicesInline(...)`
      (`hudi-client/hudi-client-common/.../client/BaseHoodieTableServiceClient.java`)
      - if `config.inlineCompactionEnabled()` → `inlineCompaction(table, extraMetadata)`
        — schedules via `inlineScheduleCompaction(...)`, then runs `compact(...)` +
        `commitCompaction(...)` in the same job, and `runAnyPendingCompactions(table)`.
      - else if `config.scheduleInlineCompaction()` (`hoodie.compact.schedule.inline=true`)
        → `inlineScheduleCompaction(extraMetadata)` — schedule only, execution deferred.
- **Async compaction service** (same-process background thread):
  - `SparkAsyncCompactService` (extends `AsyncCompactService`,
    `hudi-client/hudi-spark-client/.../async/SparkAsyncCompactService.java`)
    spawns an `"async_compact_thread"` executor that dequeues pending compaction instants,
    setting the `"hoodiecompact"` Spark scheduler pool (`EngineProperty.COMPACTION_POOL_NAME`)
    so users can prioritize compaction.
  - → `HoodieSparkCompactor.compact(instant)`
    (`hudi-client/hudi-spark-client/.../client/HoodieSparkCompactor.java`)
    → `writeClient.compact(instantTime)` then `writeClient.commitCompaction(...)`.
  - Used by `HoodieStreamer` continuous mode
    (`hudi-utilities/.../utilities/streamer/HoodieStreamer.java`) and Spark Structured
    Streaming via `SparkStreamingAsyncCompactService` in `HoodieStreamingSink`
    (`hudi-spark-datasource/hudi-spark-common/.../HoodieStreamingSink.scala`).
- **Spark SQL procedure**: `CALL run_compaction(op => 'schedule'|'execute'|'scheduleandexecute', ...)`
  or `SCHEDULE/RUN COMPACTION ON <table>` → `RunCompactionProcedure.call(...)`
  (`hudi-spark-datasource/hudi-spark/.../command/procedures/RunCompactionProcedure.scala`),
  which drives `scheduleCompaction` / `compact` + `commitCompaction` on a `SparkRDDWriteClient`.
- **Standalone job**: `org.apache.hudi.utilities.HoodieCompactor`
  (`hudi-utilities/.../utilities/HoodieCompactor.java`) with `--mode schedule | execute |
  scheduleAndExecute`.
- **CLI**: `CompactionCommand` (`hudi-cli/.../cli/commands/CompactionCommand.java`) —
  `compaction schedule`, `compaction run`, `compaction scheduleAndExecute`, `compaction
  validate`, `compaction unschedule`, `compaction repair`.

### 1.2 Flink

- **Streaming pipeline (async, default)**: scheduling happens on the job coordinator at
  checkpoint completion; execution happens in dedicated operators chained after the write
  pipeline (see section 6).
- **Offline job**: `HoodieFlinkCompactor.main(...)`
  (`hudi-flink-datasource/hudi-flink/.../sink/compact/HoodieFlinkCompactor.java`) — single-run
  or `--service` loop mode via its inner `AsyncCompactionService`.

## 2. Scheduling call stack (engine-agnostic)

```text
BaseHoodieWriteClient.scheduleCompaction(extraMetadata)
  [hudi-client/hudi-client-common/.../client/BaseHoodieWriteClient.java]
└─> BaseHoodieWriteClient.scheduleTableService(Option.empty(), extraMetadata, TableServiceType.COMPACT)
    └─> BaseHoodieTableServiceClient.scheduleTableServiceInternal(instantTime, extraMetadata, COMPACT)
        [hudi-client/hudi-client-common/.../client/BaseHoodieTableServiceClient.java]
        ├─ txnManager.beginStateChange(...)            // lock for instant creation
        ├─ createNewInstantTime(false)                 // if no instant time was provided
        ├─ table = createTable(config)
        ├─> HoodieTable.scheduleCompaction(context, instantTime, extraMetadata)   // abstract
        │   └─> Hoodie{Spark,Flink,Java}MergeOnReadTable.scheduleCompaction(...)
        │       [e.g. hudi-client/hudi-spark-client/.../table/HoodieSparkMergeOnReadTable.java]
        │       └─> new ScheduleCompactionActionExecutor<>(context, config, this, instantTime,
        │                                                  extraMetadata, WriteOperationType.COMPACT)
        │           .execute()
        └─ txnManager.endStateChange(...)
```

`ScheduleCompactionActionExecutor.execute()`
(`hudi-client/hudi-client-common/.../table/action/compact/ScheduleCompactionActionExecutor.java`):

```text
ScheduleCompactionActionExecutor.execute()
├─ check table type == MERGE_ON_READ (compaction is MoR-only)
├─ for table version < 8 without multi-writer/lazy-clean (Spark engine):
│    abort if any inflight write instant is earlier than the compaction instant
├─> scheduleCompaction()
│   ├─> needCompact(config.getInlineCompactTriggerStrategy())
│   │   // CompactionTriggerStrategy: NUM_COMMITS | NUM_COMMITS_AFTER_LAST_REQUEST |
│   │   //                            TIME_ELAPSED | NUM_AND_TIME | NUM_OR_TIME
│   │   └─ counts delta commits / elapsed time via
│   │      CompactionUtils.getCompletedDeltaCommitsSinceLatestCompaction(timeline) or
│   │      CompactionUtils.getDeltaCommitsSinceLatestCompactionRequest(timeline)
│   └─> planGenerator.generateCompactionPlan(instantTime)        // section 3
├─ if plan has operations:
│   ├─ plan.setExtraMetadata(extraMetadata)
│   └─> activeTimeline.saveToCompactionRequested(
│           new HoodieInstant(REQUESTED, COMPACTION_ACTION, instantTime), plan)
│       // serializes the Avro HoodieCompactionPlan into <instant>.compaction.requested
└─ return Option<HoodieCompactionPlan>
```

The plan generator is pluggable via `hoodie.compaction.plan.generator`
(default `HoodieCompactionPlanGenerator`); for `LOG_COMPACT` a
`HoodieLogCompactionPlanGenerator` is used and the instant is saved with
`saveToLogCompactionRequested(...)` as `<instant>.logcompaction.requested`.

## 3. Plan generation and file-slice selection

`BaseHoodieCompactionPlanGenerator.generateCompactionPlan(compactionInstant)`
(`hudi-client/hudi-client-common/.../table/action/compact/plan/generators/BaseHoodieCompactionPlanGenerator.java`):

```text
generateCompactionPlan(compactionInstant)
├─ partitionPaths = getPartitions()                       // all partitions (or incremental subset)
├─ filterPartitionPathsByStrategy(partitionPaths)         // CompactionStrategy.filterPartitionPaths
├─ fileSystemView = hoodieTable.getSliceView()            // SyncableFileSystemView
├─ pendingFileGroupIds =                                  // never compact a file group twice
│     fileSystemView.getPendingCompactionOperations()
│   ∪ fileSystemView.getFileGroupsInPendingClustering()
│   ∪ fileSystemView.getPendingLogCompactionOperations()  // for log compaction planning
├─ for each partition (parallelized via engineContext):
│   ├─ fileSystemView.getLatestFileSlicesStateless(partitionPath)
│   ├─ filterFileSlice(slice, lastCompletedInstantTime, pendingFileGroupIds, instantRange)
│   ├─ drop log files whose delta commit has not completed before the compaction instant
│   ├─ keep only slices with log files (FileSlice::hasLogFiles)
│   ├─ new CompactionOperation(dataFile, partitionPath, logFiles, metrics)
│   │   // metrics from CompactionStrategy.captureMetrics (slice/log sizes for IO bounding)
│   └─ CompactionUtils.buildHoodieCompactionOperation(op)  // POJO -> Avro
├─> getCompactionPlan(metaClient, operations, partitionPair)
│   └─ HoodieCompactionPlanGenerator:
│      compactionStrategy.generateCompactionPlan(writeConfig, operations,
│                                                pendingCompactionPlans, params, partitionPair)
│      └─ CompactionStrategy.orderAndFilter(...)           // strategy hook
├─ validate: no file group appears in multiple pending compaction plans
└─ return HoodieCompactionPlan { operations, extraMetadata, version, strategy,
                                 preserveHoodieMetadata, missingSchedulePartitions }
```

- **`CompactionStrategy`** (`.../table/action/compact/strategy/CompactionStrategy.java`,
  configured by `hoodie.compaction.strategy`) orders and bounds the selected operations.
  Default is `LogFileSizeBasedCompactionStrategy` (filter by
  `hoodie.compaction.logfile.size.threshold`, order by total log size descending, then bound
  total IO via `BoundedIOCompactionStrategy` and `hoodie.compaction.target.io`). Other
  implementations: `LogFileNumBasedCompactionStrategy`, `DayBasedCompactionStrategy`,
  `PartitionRegexBasedCompactionStrategy`, `UnBoundedCompactionStrategy`,
  `BoundedPartitionAwareCompactionStrategy`, `CompositeCompactionStrategy`.
- **`HoodieCompactionPlan`** is an Avro record
  (`hudi-common/src/main/avro/HoodieCompactionOperation.avsc`, generated class
  `org.apache.hudi.avro.model.HoodieCompactionPlan`). Each `HoodieCompactionOperation` carries
  `baseInstantTime`, `dataFilePath`, `deltaFilePaths`, `fileId`, `partitionPath`, `metrics`,
  `bootstrapFilePath` — i.e. one file slice to compact.

## 4. Execution call stack (Spark shown; Java analogous)

```text
BaseHoodieWriteClient.compact(compactionInstantTime [, shouldComplete])
  [hudi-client/hudi-client-common/.../client/BaseHoodieWriteClient.java]
├─ table = createTable(config); preWrite(instant, WriteOperationType.COMPACT, metaClient)
└─> BaseHoodieTableServiceClient.compact(table, compactionInstantTime, shouldComplete)
    ├─ multi-writer: beginStateChange + heartbeat for the compaction instant
    ├─ if <instant>.compaction.inflight already exists (previous attempt failed):
    │   └─> table.rollbackInflightCompaction(inflightInstant, ...)        // section 7
    │       and reloadActiveTimeline()
    ├─> HoodieTable.compact(context, compactionInstantTime)               // abstract
    │   └─> HoodieSparkMergeOnReadTable.compact(...)
    │       [hudi-client/hudi-spark-client/.../table/HoodieSparkMergeOnReadTable.java]
    │       └─> new RunCompactionActionExecutor<>(context, config, this, instantTime,
    │                new HoodieSparkMergeOnReadTableCompactor<>(), WriteOperationType.COMPACT)
    │           .execute()
    ├─ partialUpdateTableMetadata(...)        // streaming-writes-to-MDT hook
    └─ if shouldComplete: commitCompaction(compactionInstantTime, writeMetadata, table)  // section 5
```

`RunCompactionActionExecutor.execute()`
(`hudi-client/hudi-client-common/.../table/action/compact/RunCompactionActionExecutor.java`):

```text
RunCompactionActionExecutor.execute()
├─> compactor.preCompact(table, pendingCompactionTimeline, operationType, instantTime)
│   // HoodieSparkMergeOnReadTableCompactor: verify <instant>.compaction.requested exists
├─ compactionPlan = CompactionUtils.getCompactionPlan(metaClient, instantTime)
│   // deserializes the Avro plan from the requested instant file
├─ schema evolution: InternalSchemaCache
│     .getInternalSchemaAndAvroSchemaForClusteringAndCompaction(metaClient, instantTime)
│   → copies config with resolved internal/Avro schema if present
├─> statuses = compactor.compact(context, operationType, compactionPlan, table, configCopy, instantTime)
├─> compactor.maybePersist(statuses, context, config, instantTime)
│   // Spark: persist the HoodieData<WriteStatus> RDD so the DAG is not recomputed
└─ return HoodieWriteMetadata { writeStatuses, commitMetadata(SCHEMA_KEY, operationType=COMPACT),
                                committed=false }
```

`HoodieCompactor.compact(...)`
(`hudi-client/hudi-client-common/.../table/action/compact/HoodieCompactor.java`):

```text
HoodieCompactor.compact(context, operationType, compactionPlan, table, config, compactionInstantTime)
├─ if plan empty → return context.emptyHoodieData()
├─ TIMELINE TRANSITION (requested -> inflight):
│   instant = instantGenerator.getCompactionRequestedInstant(compactionInstantTime)
│   activeTimeline.transitionCompactionRequestedToInflight(instant)
│   metaClient.reloadActiveTimeline()
├─ reader schema: TableSchemaResolver.getTableSchema() (unless internal schema configured)
├─ operations = plan.getOperations().map(CompactionOperation::convertFromAvroRecordInstance)
├─ maxInstantTime = last completed commit/deltacommit/rollback instant   // log-read upper bound
├─ readerContextFactory = context.getReaderContextFactory(metaClient)
│   // engine-specific HoodieReaderContext (Spark/Avro/Flink record representation)
└─> context.parallelize(operations)                                      // one task per file slice
       .map(op -> compact(config, op, compactionInstantTime,
                          readerContextFactory.getContext(), table,
                          maxInstantTime, taskContextSupplier))
       .flatMap(List::iterator)                                          // HoodieData<WriteStatus>
```

### 4.1 Per-file-group compaction: log merging and base-file writing

```text
HoodieCompactor.compact(writeConfig, operation, instantTime, readerContext, table,
                        maxInstantTime, taskContextSupplier)
└─> HoodieMergeHandleFactory.create(config, instantTime, table, operation,
                                    taskContextSupplier, readerContext, maxInstantTime, recordType)
    // [.../io/HoodieMergeHandleFactory.java] — compaction-with-file-group-reader overload:
    // instantiates hoodie.compact.merge.handle.class (default FileGroupReaderBasedMergeHandle);
    // if hoodie.write.merge.handle.fallback is enabled and a custom class fails to instantiate,
    // falls back to the FileGroupReaderBasedMergeHandle default
    ├─ mergeHandle.doMerge()
    └─ return mergeHandle.close()                // List<WriteStatus>
```

(The factory's other overloads — used by the regular upsert/merge write path and the legacy
record-iterator compaction path — apply additional overrides such as `HoodieSortedMergeHandle`
when `table.requireSortedRecords()` or `HoodieMergeHandleWithChangeLog` for CDC; those branches
do not apply to this file-group-reader path.)

`FileGroupReaderBasedMergeHandle.doMerge()`
(`hudi-client/hudi-client-common/.../io/FileGroupReaderBasedMergeHandle.java`) — the default,
file-group-reader-based path:

```text
doMerge()
├─ maxMemoryPerCompaction (HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE) — spillable-map budget
├─ logFiles = operation.getDeltaFileNames() resolved against table base path
├─ HoodieFileGroupReader  [hudi-common/.../common/table/read/HoodieFileGroupReader.java]
│   // The unified read path: streams the old base file (if any) and merge-sorts/merges the
│   // log records on top, honoring the table's record merge mode (commit-time ordering,
│   // event-time ordering, or custom merger/payload), spilling to disk when over budget.
├─ for each merged record (getClosableHoodieRecordIterator()):
│   ├─ record.setCurrentLocation/newLocation(instantTime, fileId)
│   └─ writeToFile(key, record, schema, ...)     // preserves existing record metadata
│       └─ HoodieFileWriter (via HoodieFileWriterFactory) appends to the NEW base file:
│          <fileId>_<writeToken>_<compactionInstantTime>.parquet (or orc/hfile)
└─ close(): finalize file, fill HoodieWriteStat (prevCommit, numWrites/numUpdateWrites,
            totalLogFilesCompacted, totalLogSizeCompacted, file size, ...) → WriteStatus
```

Notes:

- A file slice **with no base file** (insert-only file group on a pure-log index path) is
  handled the same way — the file group reader simply has no base file input, and the handle
  writes a brand-new base file.
- Markers are created for the new base file via the usual write-handle marker mechanism, so a
  failed compaction can be cleaned up / rolled back reliably.
- `WriteStatus`/`HoodieWriteStat` flow back as `HoodieData<WriteStatus>` inside
  `HoodieWriteMetadata` and become the commit metadata's per-partition write stats.

## 5. Commit: metadata and timeline completion

```text
BaseHoodieWriteClient.commitCompaction(compactionInstantTime, compactionWriteMetadata, tableOpt)
└─> BaseHoodieTableServiceClient.commitCompaction(...)
    ├─ triggerWritesAndFetchWriteStats(writeMetadata)   // dereference the lazy write DAG
    ├─ CommonClientUtils.stitchCompactionHoodieWriteStats(writeMetadata, writeStats)
    └─> completeCompaction(metadata, table, compactionCommitTime, partialMetadataWriteStats)
        ├─ handleWriteErrors(writeStats, TableServiceType.COMPACT)
        ├─ txnManager.beginStateChange(compactionInflightInstant)
        ├─ preCommit(metadata)                          // conflict resolution hooks
        ├─ finalizeWrite(table, compactionCommitTime, writeStats)
        ├─ writeToMetadataTable(...)                    // update files/col-stats/RLI partitions
        │   └─ metadataWriter.update(metadata, instantTime)
        ├─> CompactHelpers.completeInflightCompaction(table, compactionCommitTime, metadata)
        │   [.../table/action/compact/CompactHelpers.java]
        │   └─> activeTimeline.transitionCompactionInflightToComplete(
        │           false, getCompactionInflightInstant(instant), commitMetadata)
        ├─ txnManager.endStateChange(...)
        ├─ WriteMarkersFactory...quietDeleteMarkerDir(...)
        └─ metrics.updateCommitMetrics(..., COMPACTION_ACTION)
```

The completed metadata is a `HoodieCommitMetadata`
(`hudi-common/.../common/model/HoodieCommitMetadata.java`) with
`operationType = COMPACT`, `compacted = true`, the writer schema under `SCHEMA_KEY`, and
`partitionToWriteStats` describing every new base file. `CompactHelpers.createCompactionMetadata(...)`
builds it from the write statuses (used by Flink and CLI paths).

## 6. Flink call stack

### 6.1 Streaming pipeline (async compaction)

Scheduling — on the JobManager:

```text
StreamWriteOperatorCoordinator.notifyCheckpointComplete(checkpointId)
  [hudi-flink-datasource/hudi-flink/.../sink/StreamWriteOperatorCoordinator.java]
├─ commitInstants(...)                                   // complete the delta commit
└─> scheduleTableServices(committed)
    └─> CompactionUtil.scheduleCompaction(writeClient, isDeltaTimeCompaction, committed)
        [hudi-flink-datasource/hudi-flink/.../util/CompactionUtil.java]
        └─> writeClient.scheduleCompaction(Option.empty())   // same stack as section 2
```

Execution — operators wired by `Pipelines.compact(...)`
(`hudi-flink-datasource/hudi-flink/.../sink/utils/Pipelines.java`):
`CompactionPlanOperator` (parallelism 1) → keyed by operation index → `CompactOperator`
(parallelism = `compaction.tasks`) → `CompactionCommitSink` (parallelism 1).

```text
CompactionPlanOperator.notifyCheckpointComplete(checkpointId)
  [.../sink/compact/CompactionPlanOperator.java]
└─> DataTableCompactionPlanHandler.collectCompactionOperations(checkpointId, output)
    [.../sink/compact/handler/DataTableCompactionPlanHandler.java]
    ├─ reloadActiveTimeline(); take FIRST REQUESTED instant of filterPendingCompactionTimeline()
    ├─ plan = CompactionUtils.getCompactionPlan(metaClient, instant)
    └─> doCollectCompactionOperations(instant, plan, output)
        ├─ TIMELINE TRANSITION: transitionCompactionRequestedToInflight(instant)
        ├─ delete stale marker dir
        └─ emit one CompactionPlanEvent per CompactionOperation

CompactOperator.processElement(CompactionPlanEvent)
  [.../sink/compact/CompactOperator.java]
└─> DataTableCompactHandler.compact(executor, event, collector, needReloadMetaClient)
    [.../sink/compact/handler/DataTableCompactHandler.java]
    └─> doCompaction(...)
        ├─ CompactionUtil.setAvroSchema(writeConfig, metaClient)     // schema evolution
        ├─> new HoodieFlinkMergeOnReadTableCompactor<>()
        │       .compact(writeConfig, event.getOperation(), instantTime,
        │                taskContextSupplier, readerContext, table)
        │   [hudi-client/hudi-flink-client/.../table/action/compact/HoodieFlinkMergeOnReadTableCompactor.java]
        │   // per-operation path of section 4.1: merge-handle factory -> doMerge -> close
        └─ emit CompactionCommitEvent(instant, fileId, writeStatuses, taskID)

CompactionCommitSink.invoke(CompactionCommitEvent)
  [.../sink/compact/CompactionCommitSink.java]
└─> DataTableCompactionCommitHandler.commitIfNecessary(event)
    [.../sink/compact/handler/DataTableCompactionCommitHandler.java]
    ├─ buffer events per instant; wait until all operations of the plan reported
    ├─ on any failure / error records (unless FlinkOptions.IGNORE_FAILED):
    │   └─ rollbackCompaction(instant)        // CompactionUtil.rollbackCompaction
    └─> completeCompaction(instant, statuses)
        ├─ metadata = CompactHelpers.createCompactionMetadata(table, instant,
        │                 HoodieListData.eager(statuses), schema)
        └─> writeClient.completeCompaction(metadata, table, instant)   // section 5 stack
```

Key Flink options (`hudi-flink-datasource/hudi-flink/.../configuration/FlinkOptions.java`):
`compaction.schedule.enabled` (default true), `compaction.async.enabled` (default true),
`compaction.trigger.strategy` (`num_commits` default | `num_commits_after_last_request` |
`time_elapsed` | `num_and_time` | `num_or_time`), `compaction.delta_commits` (default 5),
`compaction.delta_seconds` (default 3600), `compaction.tasks`. The metadata table has its own
mirrored set (`metadata.compaction.*`), scheduled from the same coordinator hook.

### 6.2 Offline compaction

`HoodieFlinkCompactor.main(...)` runs its inner `AsyncCompactionService.compact()` once (with
retries) or, with `--service`, in a loop sleeping `--min-compaction-interval-seconds` between
rounds. There is no `schedule`/`execute` mode enum (`FlinkCompactionConfig` exposes boolean
flags instead); each round:

1. if `--schedule` is set, schedules a new plan via `writeClient.scheduleCompaction(...)`
   (section 2 stack) — the flag is discouraged in favor of scheduling inside the writer job;
2. **always executes**: selects pending `compaction.requested` instants with
   `CompactionPlanStrategies.getStrategy(cfg)` (`--plan-select-strategy`: `num_instants`
   default / `all` / `instants`, ordered by `--seq FIFO|LIFO`), rolls back stale inflight
   attempts (`--retry-last-failed-job`), and loads each plan via
   `CompactionUtils.getCompactionPlan`;
3. transitions the selected instants requested → inflight in the driver, then submits a job
   wired as `CompactionPlanSourceFunction → CompactOperator → CompactionCommitSink`
   (`HoodieFlinkCompactor.java`) — a bounded source over the pre-selected plans replaces the
   streaming `CompactionPlanOperator` of section 6.1.

## 7. Timeline transitions and failure handling

| Step | Timeline file (under `.hoodie/timeline/`) | State / action | Performed by |
|---|---|---|---|
| Schedule | `<t>.compaction.requested` (contains Avro `HoodieCompactionPlan`) | REQUESTED / `compaction` | `HoodieActiveTimeline.saveToCompactionRequested` ← `ScheduleCompactionActionExecutor.execute` |
| Start execution | `<t>.compaction.inflight` | INFLIGHT / `compaction` | `transitionCompactionRequestedToInflight` ← `HoodieCompactor.compact` (Spark/Java) or `DataTableCompactionPlanHandler` (Flink) |
| Complete | `<t>_<completionTime>.commit` (`HoodieCommitMetadata`, `operationType=COMPACT`) | COMPLETED / **`commit`** | `transitionCompactionInflightToComplete` ← `CompactHelpers.completeInflightCompaction` |

Concrete transition logic lives in `ActiveTimelineV1` / `ActiveTimelineV2`
(`hudi-common/.../common/table/timeline/versioning/{v1,v2}/`); note the completed instant uses
the regular **`COMMIT_ACTION`**, not `compaction` — a completed compaction looks like any other
commit to readers. (Log compaction completes as a `deltacommit` instead.)

**Failure / restart semantics:**

- A pending compaction plan is a contract: once `<t>.compaction.requested` exists, ingestion
  writers treat the file groups in the plan as having a new base instant `t` and route updates
  to **new log files on top of the future file slice**; the file system view merges pending
  compaction into its slice computation (`getLatestMergedFileSlicesBeforeOrOn`, pending
  operations from `CompactionUtils.getAllPendingCompactionOperations`). Compaction therefore
  never blocks ingestion, and the plan is never silently discarded — only `compaction
  unschedule` (CLI) removes it.
- If execution dies mid-way, the next `compact(...)` call finds `<t>.compaction.inflight` and
  calls `HoodieTable.rollbackInflightCompaction(...)`
  (`hudi-client/hudi-client-common/.../table/HoodieTable.java`), which rolls back the partial
  data files (marker-based) and then
  `revertInstantFromInflightToRequested(inflightInstant)` — the instant returns to REQUESTED
  with the plan intact, ready for retry. Flink does the equivalent through
  `CompactionUtil.rollbackCompaction(...)`.
- After completion the instant is eventually moved to the archived timeline by
  `HoodieTimelineArchiver` (`hudi-client/hudi-client-common/.../client/timeline/`).

## 8. Key configuration reference

| Config | Default | Effect |
|---|---|---|
| `hoodie.compact.inline` | `false` | Schedule **and execute** compaction inside the writer commit path |
| `hoodie.compact.schedule.inline` | `false` | Only schedule inline; execute elsewhere (async/offline) |
| `hoodie.compact.inline.trigger.strategy` | `NUM_COMMITS` | Trigger predicate (see `CompactionTriggerStrategy`) |
| `hoodie.compact.inline.max.delta.commits` | `5` | Delta commits before compaction triggers |
| `hoodie.compact.inline.max.delta.seconds` | `3600` | Elapsed time before compaction triggers |
| `hoodie.compaction.strategy` | `LogFileSizeBasedCompactionStrategy` | Operation ordering/filtering |
| `hoodie.compaction.target.io` | `512000` MB | IO bound for bounded strategies |
| `hoodie.compaction.logfile.size.threshold` | `0` | Min total log size for a slice to qualify |
| `hoodie.compaction.plan.generator` | `HoodieCompactionPlanGenerator` | Pluggable plan generation |
| `hoodie.compact.merge.handle.class` | `FileGroupReaderBasedMergeHandle` | Per-file-group merge implementation |
| `hoodie.memory.merge.max.size` | engine-derived | Spillable-map budget for the merge |
| `hoodie.log.compaction.enable` / `hoodie.log.compaction.inline` | `false` | Enable / inline the minor log-compaction variant |

(Flink equivalents in section 6.1.)
