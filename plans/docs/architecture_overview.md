# Apache Hudi — Architecture Overview

This document provides a high-level architecture overview of Apache Hudi: the major layers and
Maven modules, the timeline and table layout on storage, the write/read path abstractions, table
services, the metadata table, storage/IO, sync integrations, and packaging.

**Related docs:** [compaction_callstack.md](compaction_callstack.md) | [delete_mechanisms.md](delete_mechanisms.md) | [update_and_merge_mechanisms.md](update_and_merge_mechanisms.md)

---

## 1. Layered Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              QUERY / COMPUTE ENGINES                         │
│  ┌──────────┐ ┌──────────┐ ┌─────────────┐ ┌──────────┐ ┌─────────────────┐  │
│  │  Spark   │ │  Flink   │ │ Presto /    │ │ Hive /   │ │ Standalone Java │  │
│  │ 3.4–4.2  │ │ 1.18–2.1 │ │ Trino       │ │ MapReduce│ │ apps / Kafka    │  │
│  └────┬─────┘ └────┬─────┘ └──────┬──────┘ └────┬─────┘ └───────┬─────────┘  │
└───────┼────────────┼──────────────┼─────────────┼───────────────┼────────────┘
        │            │              │             │               │
┌───────▼────────────▼──────────────▼─────────────▼───────────────▼────────────┐
│                            ENGINE INTEGRATIONS                               │
│  ┌──────────────┐ ┌──────────────┐ ┌───────────┐ ┌───────────┐ ┌──────────┐  │
│  │ hudi-spark-  │ │ hudi-flink-  │ │ hudi-trino│ │ hudi-     │ │ hudi-    │  │
│  │ datasource/  │ │ datasource/  │ │ -plugin,  │ │ hadoop-mr │ │ kafka-   │  │
│  │ (DataSource, │ │ (SQL conn.,  │ │ presto    │ │ (Input-   │ │ connect  │  │
│  │  SQL, proc.) │ │  stream sink)│ │ bundle    │ │  Formats) │ │ (sink)   │  │
│  └──────┬───────┘ └──────┬───────┘ └─────┬─────┘ └─────┬─────┘ └────┬─────┘  │
└─────────┼────────────────┼───────────────┼─────────────┼────────────┼────────┘
          │                │               │             │            │
┌─────────▼────────────────▼───────────────┼─────────────┼────────────▼────────┐
│                    WRITE CLIENTS  (hudi-client/)       │ (read-only paths    │
│                                                        │  go straight to     │
│  hudi-client-common  — engine-agnostic write pipeline, │  hudi-common)       │
│    BaseHoodieWriteClient, HoodieTable, HoodieIndex,    │                     │
│    table service action executors, transactions/locks  ▼                     │
│  ┌────────────────────┐ ┌────────────────────┐ ┌────────────────────┐        │
│  │ hudi-spark-client  │ │ hudi-flink-client  │ │ hudi-java-client   │        │
│  │ SparkRDDWriteClient│ │HoodieFlinkWriteCli.│ │HoodieJavaWriteCli. │        │
│  └────────────────────┘ └────────────────────┘ └────────────────────┘        │
└──────────────────────────────────┬───────────────────────────────────────────┘
                                   │
┌──────────────────────────────────▼───────────────────────────────────────────┐
│                         HUDI COMMON  (hudi-common/)                          │
│                                                                              │
│   HoodieTableMetaClient, HoodieTimeline / HoodieActiveTimeline               │
│   HoodieRecord, HoodieKey, HoodieData<T>, HoodieEngineContext                │
│   FileSystemViewManager, HoodieTableFileSystemView (file groups/slices)      │
│   HoodieFileGroupReader (base + log merging), HoodieLogFormat                │
│   Metadata table reader (HoodieBackedTableMetadata, HoodieMetadataPayload)   │
│                                                                              │
│   ◄── hudi-timeline-service: embedded server caching the file system view   │
└──────────────────────────────────┬───────────────────────────────────────────┘
                                   │
┌──────────────────────────────────▼───────────────────────────────────────────┐
│                    STORAGE / IO  (hudi-io/, hudi-hadoop-common/)             │
│                                                                              │
│   HoodieStorage, StoragePath, Option<T>, HFile reader/writer                 │
│   HoodieHadoopStorage + Parquet/ORC/HFile/Avro file readers & writers        │
│   Cloud helpers: hudi-aws (S3, DynamoDB locks), hudi-gcp, hudi-azure         │
└──────────────────────────────────┬───────────────────────────────────────────┘
                                   │
┌──────────────────────────────────▼───────────────────────────────────────────┐
│                               STORAGE                                        │
│              S3  |  GCS  |  ADLS  |  HDFS  |  Local FS                       │
└──────────────────────────────────────────────────────────────────────────────┘
```

Two properties distinguish Hudi's architecture from a pure table format:

- **The timeline is the source of truth.** Every change to the table is an *instant* on an
  event-log-like timeline (`.hoodie/timeline/`); readers and writers reconstruct table state by
  replaying it.
- **Writes are a first-class subsystem.** Indexing (record key → file group lookup), merging,
  and table services (compaction, clustering, cleaning) live in the repo itself rather than being
  delegated entirely to engines.

---

## 2. Module Structure

```
hudi/
├── hudi-io/                       Lowest layer: HoodieStorage, StoragePath, Option<T>,
│                                  HFile format, compression. No Hadoop/Spark/Flink deps.
├── hudi-hadoop-common/            Hadoop implementation of the storage layer:
│                                  HoodieHadoopStorage, Parquet/ORC/HFile readers & writers.
├── hudi-common/                   Core data model, timeline, table meta client, file system
│                                  view, file group reader, metadata table reader.
│
├── hudi-client/
│   ├── hudi-client-common/        Engine-agnostic write client, HoodieTable, index framework,
│   │                              table service action executors, locks/transactions.
│   │                              (No Scala imports — enforced by checkstyle import-control.)
│   ├── hudi-spark-client/         SparkRDDWriteClient, HoodieSparkTable, Spark index impls.
│   ├── hudi-flink-client/         HoodieFlinkWriteClient, HoodieFlinkTable, Flink state index.
│   └── hudi-java-client/          HoodieJavaWriteClient, HoodieJavaTable (no cluster engine).
│
├── hudi-spark-datasource/
│   ├── hudi-spark-common/         DefaultSource, HoodieSparkSqlWriter, HoodieFileIndex,
│   │                              relations (CoW/MoR/incremental/CDC), index pruning support.
│   ├── hudi-spark/                Main datasource module: SQL extensions, DML commands,
│   │                              procedures (`org.apache.spark.sql.hudi.command`).
│   ├── hudi-spark3-common/        Spark 3.x shared adapter code.
│   ├── hudi-spark4-common/        Spark 4.x shared adapter code.
│   └── hudi-spark3.4.x/ 3.5.x/ 4.0.x/ 4.1.x/ 4.2.x/   Version-specific adapters.
│
├── hudi-flink-datasource/
│   ├── hudi-flink/                HoodieTableFactory/Sink/Source, StreamWriteFunction,
│   │                              StreamWriteOperatorCoordinator, compaction/clustering ops.
│   └── hudi-flink1.18.x/ … 2.1.x/ Version-specific Flink adapters.
│
├── hudi-hadoop-mr/                Hive/MapReduce InputFormats (HoodieParquetInputFormat,
│                                  HoodieParquetRealtimeInputFormat). Also used by Presto.
├── hudi-trino-plugin/             Native Trino connector (artifact `trino-hudi`: HudiPlugin,
│                                  HudiConnectorFactory). Built standalone against the Trino
│                                  parent POM (own CI job), outside the root Maven reactor.
├── hudi-timeline-service/         Embedded Javalin-based server for the cached file system
│                                  view (TimelineService, RequestHandler, marker handlers).
│
├── hudi-sync/
│   ├── hudi-sync-common/          HoodieSyncTool / HoodieSyncClient abstractions.
│   ├── hudi-hive-sync/            HiveSyncTool — Hive Metastore registration.
│   ├── hudi-datahub-sync/         DataHub metadata emission.
│   └── hudi-adb-sync/             Alibaba AnalyticDB sync.
│
├── hudi-utilities/                HoodieStreamer ingestion, offline table service jobs
│                                  (HoodieCompactor, HoodieClusteringJob, HoodieIndexer,
│                                  HoodieCleaner), validators, snapshot exporter.
├── hudi-kafka-connect/            Kafka Connect sink (HoodieSinkConnector / HoodieSinkTask).
├── hudi-cli/                      Interactive shell for table/timeline inspection & repair.
│
├── hudi-aws/                      Glue Catalog sync, DynamoDB & S3-based lock providers,
│                                  CloudWatch metrics.
├── hudi-gcp/                      BigQuery sync, GCS storage lock client.
├── hudi-azure/                    Azure storage lock client.
│
├── hudi-examples/                 Spark / Flink / Java / dbt / k8s examples.
├── hudi-platform-service/         hudi-metaserver (Thrift table/timeline server; profile-gated).
├── hudi-tests-common, hudi-integ-test/   Shared test infra and long-running test suites.
│
└── packaging/                     Shaded deployment bundles (see section 12).
```

---

## 3. Table Layout & Timeline

Hudi organizes data as **file groups**: all versions of one horizontal slice of data, identified
by a `fileId`, living inside one partition. Each write produces a new **file slice** within the
group (a base file and/or log files stamped with the instant time). Table version 9
(`HoodieTableVersion.NINE`, release 1.1.0) is current; it uses timeline layout v2, introduced
with table version 8 (1.0.0). Older tables (v6, 0.14.x) remain readable through the
`versioning/v1` timeline classes.

```
/table_base_path/
├── .hoodie/                                  METAFOLDER_NAME (table metadata)
│   ├── hoodie.properties                     Table config: name, type, version, key fields
│   ├── timeline/                             TIMELINEFOLDER_NAME (layout v2 / table v8+)
│   │   ├── 20260611120000123.commit.requested
│   │   ├── 20260611120000123.inflight
│   │   ├── 20260611120000123_20260611120005456.commit     completed instants carry a
│   │   ├── 20260611130000789.deltacommit.requested        completion timestamp
│   │   └── history/                          archived (LSM-tree) timeline
│   ├── .metadata/                            Metadata table — an internal MoR Hudi table
│   │   ├── files/  column_stats/  bloom_filters/  record_index/ ...
│   │   └── .hoodie/                          with its own timeline
│   └── .temp/                                Marker files for in-progress writes
│
├── partition1/                               (e.g. date=2026-06-11)
│   ├── .hoodie_partition_metadata
│   ├── <fileId1>_<writeToken>_<instantT1>.parquet     base file        ┐ file slice @ T1
│   ├── .<fileId1>_<instantT1>.log.1_<writeToken>      log file (MoR)   ┘ (file group fileId1)
│   ├── .<fileId1>_<instantT1>.log.2_<writeToken>
│   └── <fileId1>_<writeToken>_<instantT3>.parquet     new slice @ T3 (after compaction)
└── partition2/ ...
```

**Timeline state machine.** Every action transitions through three states, each a file on the
timeline (`HoodieInstant`, transitions managed by `HoodieActiveTimeline`):

```
   REQUESTED  ──────────►  INFLIGHT  ──────────►  COMPLETED
   (plan serialized)       (execution started)    (commit metadata serialized)

   Actions (HoodieTimeline constants, hudi-common/.../timeline/HoodieTimeline.java):
     commit          CoW write or compaction result
     deltacommit     MoR write (appends to log files) / metadata table write
     replacecommit   clustering / insert_overwrite / delete_partition results
     clustering      clustering plan & execution (table v8+)
     compaction      scheduled compaction plan (completes as `commit`)
     logcompaction   minor log-file consolidation
     clean | rollback | restore | savepoint | indexing | schemacommit
```

Key classes:

| Concern | Class | Location |
|---|---|---|
| Entry point to table metadata | `HoodieTableMetaClient` | `hudi-common/.../common/table/HoodieTableMetaClient.java` |
| Immutable timeline view | `HoodieTimeline`, `BaseTimelineV2` | `hudi-common/.../common/table/timeline/` |
| Instant CRUD / transitions | `HoodieActiveTimeline` | same package |
| Layout v1 (table ≤v7) vs v2 (v8+) | `versioning/v1`, `versioning/v2` | `hudi-common/.../timeline/versioning/` |
| File groups / slices resolution | `HoodieTableFileSystemView` | `hudi-common/.../common/table/view/` |
| View lifecycle & caching modes | `FileSystemViewManager` | same package (memory / spillable / remote) |

---

## 4. Key Abstractions

```
            ┌─────────────────────────┐
            │ HoodieTableMetaClient   │   hudi-common — gateway to .hoodie/
            │                         │
            │ getActiveTimeline()   ──┼──► HoodieActiveTimeline
            │ getTableConfig()      ──┼──► HoodieTableConfig (hoodie.properties)
            │ getStorage()          ──┼──► HoodieStorage
            └────────────┬────────────┘
                         │ wrapped by
                         ▼
            ┌─────────────────────────┐    HoodieTable<T, I, K, O>
            │ HoodieTable             │    hudi-client-common/.../table/HoodieTable.java
            │                         │    Generic over payload, input, key, output:
            │ upsert(...) insert(...) │      Spark: I = HoodieData<HoodieRecord<T>>
            │ compact(...) clean(...) │      Flink: I = List<HoodieRecord<T>>
            │ getIndex()              │    Subclasses: HoodieSparkTable / HoodieFlinkTable /
            │ getFileSystemView()     │      HoodieJavaTable, each in CoW + MoR variants
            └────────────┬────────────┘
                         │ orchestrated by
                         ▼
            ┌─────────────────────────┐
            │ BaseHoodieWriteClient   │    hudi-client-common/.../client/
            │                         │
            │ startCommit()           │    Engine subclasses:
            │ upsert() / insert() /   │      SparkRDDWriteClient   (hudi-spark-client)
            │   bulkInsert()/delete() │      HoodieFlinkWriteClient (hudi-flink-client)
            │ commit()                │      HoodieJavaWriteClient  (hudi-java-client)
            │ scheduleCompaction()    │
            │ ...table services via ──┼──► BaseHoodieTableServiceClient
            └─────────────────────────┘

   Engine abstraction (hudi-common/.../common/engine/, .../common/data/):
     HoodieEngineContext   map / flatMap / reduce over distributed collections
       ├── HoodieSparkEngineContext   (wraps JavaSparkContext)
       ├── HoodieFlinkEngineContext
       └── HoodieLocalEngineContext   (plain Java, used by Java client & tools)
     HoodieData<T>         lazy engine-agnostic collection (RDD / list backed)

   Data model (hudi-common/.../common/model/):
     HoodieKey       = record key + partition path (unique record identity)
     HoodieRecord<T> = HoodieKey + payload + current/new HoodieRecordLocation
     HoodieRecordMerger / payload classes  — merge semantics (see update_and_merge doc)

   Utility (hudi-io):
     Option<T>       Hudi's serializable Optional — java.util.Optional is banned
```

---

## 5. Write Path

```
  Engine entry (HoodieSparkSqlWriter, StreamWriteFunction, HoodieStreamer, ...)
       │
       ▼
  BaseHoodieWriteClient.upsert(records, instantTime)            hudi-client-common
       │
       ├─ 1. startCommit(): create <instant>.<action>.requested on the timeline;
       │        action from metaClient.getCommitActionType() — `commit` (CoW),
       │        `deltacommit` (MoR); insert_overwrite / delete_partition
       │        operations start a `replacecommit` instead
       │
       ├─ 2. preWrite / deduplication (precombine on HoodieRecordMerger)
       │
       ├─ 3. TAG: HoodieIndex.tagLocation(records, table)
       │        annotate each record with its existing file group (update)
       │        or leave untagged (insert)
       │
       ├─ 4. PARTITION: commit action executor packs records into buckets
       │        (table/action/commit/ — small-file handling assigns inserts
       │         into existing under-sized file groups)
       │
       ├─ 5. WRITE: per-bucket IO handles (hudi-client-common/.../io/)
       │        HoodieCreateHandle           new base file        (insert, CoW+MoR)
       │        HoodieMergeHandle /          read old base file,  (update, CoW;
       │          FileGroupReaderBasedMergeHandle  merge, rewrite   compaction)
       │        HoodieAppendHandle /         append delta blocks  (update, MoR)
       │          FileGroupReaderBasedAppendHandle  to .log files
       │        HoodieConcatHandle           append w/o merging   (insert overwrite paths)
       │
       ├─ 6. UPDATE INDEX + METADATA TABLE
       │        HoodieIndex.updateLocation(); HoodieBackedTableMetadataWriter.update()
       │
       └─ 7. COMMIT: write HoodieCommitMetadata, transition instant to COMPLETED,
                reconcile marker files, then run inline/async table services
```

Concurrency control lives in `hudi-client-common/.../client/transaction/`:
`TransactionManager` + `LockManager` with pluggable `LockProvider`s — filesystem- and
ZooKeeper-based in core, `DynamoDBBasedLockProvider` in `hudi-aws`, storage-conditional-write
locks (`StorageBasedLockProvider` with S3/GCS/Azure clients) for cloud object stores.

---

## 6. Index Subsystem

`HoodieIndex` (`hudi-client/hudi-client-common/.../index/HoodieIndex.java`) maps record keys to
file groups during step 3 above. `IndexType` options:

| Type | Mechanism | Scope |
|---|---|---|
| `SIMPLE` / `GLOBAL_SIMPLE` | Join incoming keys against keys read from existing base files | partition / global |
| `BLOOM` / `GLOBAL_BLOOM` | Bloom filters (+ key-range pruning) stored in base file footers or the metadata table | partition / global |
| `BUCKET` | Hash record key → fixed or consistent-hashing bucket = file group; no lookup needed | partition |
| `GLOBAL_RECORD_LEVEL_INDEX` | Key → location mapping stored in the metadata table `record_index` partition (replaces the deprecated `RECORD_INDEX`) | global |
| `RECORD_LEVEL_INDEX` | Same metadata-table mapping, keyed by (partition path, record key) | partition |
| `FLINK_STATE` | Flink keyed state holds key → location (default for Flink streaming) | global |
| `INMEMORY` | In-memory map (testing / Java client) | global |

Engine factories (`SparkHoodieIndexFactory`, `FlinkHoodieIndexFactory`) instantiate the
implementations under `index/bloom`, `index/bucket`, `index/simple`, `index/inmemory`.

---

## 7. Table Services

Most table services follow a two-step pattern: **schedule** (serialize a plan into a
`.requested` instant) and **execute** (run the plan, complete the instant) — compaction,
log compaction, clustering, cleaning, rollback, restore, and async indexing all work this way.
A few actions skip scheduling and hit the timeline directly: `SavepointActionExecutor` writes
an inflight `savepoint` instant and completes it in one step. Action executors live under
`hudi-client/hudi-client-common/.../table/action/`:

```
  table/action/
  ├── compact/    ScheduleCompactionActionExecutor, RunCompactionActionExecutor,
  │               HoodieCompactor — merge MoR log files into new base files
  │               (see compaction_callstack.md)
  ├── cluster/    ClusteringPlanActionExecutor + strategies — rewrite/sort small file
  │               groups into bigger ones; completes as `replacecommit`
  ├── clean/      CleanActionExecutor, CleanPlanner — delete file slices no longer
  │               needed by queries/savepoints (retention policies)
  ├── rollback/   Undo a failed/incomplete instant (delete its files & log blocks)
  ├── restore/    Roll the table back to a savepoint (chain of rollbacks)
  ├── savepoint/  Pin a consistent snapshot against cleaning
  ├── index/      Build metadata-table index partitions asynchronously (`indexing` action)
  ├── ttl/        Partition time-to-live management (drops expired partitions)
  └── bootstrap/  Import existing parquet tables into Hudi without rewriting data
```

Services can run **inline** (same job, after each commit), **async** (separate threads in the
same process — e.g. `SparkAsyncCompactService`, Flink compaction operators), or **offline** as
standalone Spark jobs from `hudi-utilities` (`HoodieCompactor`, `HoodieClusteringJob`,
`HoodieCleaner`, `HoodieIndexer`, `HoodieTTLJob`). `BaseHoodieTableServiceClient` coordinates
scheduling/execution across all three modes.

---

## 8. Metadata Table

The metadata table is an internal MoR Hudi table at `<table>/.hoodie/.metadata`, written in the
same transaction scope as the data table and read through HFile for point lookups. Each
top-level partition is an index (`MetadataPartitionType`, `hudi-common/.../metadata/`):

```
  .hoodie/.metadata/
  ├── files/             partition → file listing        (eliminates storage LIST calls)
  ├── column_stats/      per-file column min/max/null    (data skipping)
  ├── partition_stats/   per-partition column stats      (partition pruning)
  ├── bloom_filters/     per-file bloom filters          (bloom index lookups)
  ├── record_index/      record key → (partition,fileId) (record-level indexes)
  ├── expr_index_*/      expression (functional) indexes
  └── secondary_index_*/ secondary key → record key
```

- **Write side:** `HoodieBackedTableMetadataWriter` (`hudi-client-common`, engine subclass
  `SparkHoodieBackedTableMetadataWriter`) converts each data-table commit into metadata-table
  deltacommits; records are `HoodieMetadataPayload` (`hudi-common`).
- **Read side:** `HoodieBackedTableMetadata` (`hudi-common/.../metadata/`) serves file listings
  and index lookups; `BaseTableMetadata` falls back to direct storage listing when disabled.
- **Query integration (Spark):** `*IndexSupport` classes in `hudi-spark-common`
  (`ColumnStatsIndexSupport`, `RecordLevelIndexSupport`, `BloomFiltersIndexSupport`,
  `BucketIndexSupport`, `ExpressionIndexSupport`, `SecondaryIndexSupport`) prune the candidate
  files inside `HoodieFileIndex`.

---

## 9. Read Path — Spark Integration

```
┌────────────────────────────────────────────────────────────────────────┐
│  Spark SQL / DataFrame:  spark.read.format("hudi") …                   │
│  SQL extensions: HoodieSparkSessionExtension (hudi-spark)              │
│    analysis rules + DML commands (MERGE INTO, UPDATE, DELETE, CTAS)    │
│    + stored procedures (run_compaction, run_clustering, …)             │
└──────────────────────────────┬─────────────────────────────────────────┘
                               │
                               ▼
┌────────────────────────────────────────────────────────────────────────┐
│  DefaultSource (hudi-spark-common/.../DefaultSource.scala)             │
│    write → HoodieSparkSqlWriter → SparkRDDWriteClient (section 5)      │
│    read  → relation per query type:                                    │
│       snapshot CoW: HadoopFsRelation via HoodieHadoopFsRelationFactory │
│       snapshot MoR: MergeOnReadSnapshotRelation / HoodieMergeOnReadRDDV2│
│       incremental:  (MergeOnRead)IncrementalRelationV1/V2              │
│       CDC:          cdc/ package relations                             │
└──────────────────────────────┬─────────────────────────────────────────┘
                               │
                               ▼
┌────────────────────────────────────────────────────────────────────────┐
│  HoodieFileIndex (hudi-spark-common)                                   │
│    timeline + metadata table → candidate files                         │
│    partition pruning + data skipping via *IndexSupport (section 8)     │
│    file slice resolution via SparkHoodieTableFileIndex →               │
│      FileSystemViewManager → HoodieTableFileSystemView                 │
└──────────────────────────────┬─────────────────────────────────────────┘
                               │
                               ▼
┌────────────────────────────────────────────────────────────────────────┐
│  HoodieFileGroupReader (hudi-common/.../common/table/read/)            │
│    engine-agnostic merge of base file + log files for one file slice,  │
│    driven by HoodieReaderContext (Spark impl: SparkFileFormat-         │
│    InternalRowReaderContext); applies record merger / event-time or    │
│    commit-time ordering; same reader is reused by compaction and the   │
│    FileGroupReaderBased* write handles                                 │
└────────────────────────────────────────────────────────────────────────┘
```

Hive and Presto read through `hudi-hadoop-mr` (`HoodieParquetInputFormat` for CoW,
`HoodieParquetRealtimeInputFormat` merging logs for MoR). Trino's native connector is developed
in this repo under `hudi-trino-plugin/` (Maven artifact `trino-hudi`: `HudiPlugin`,
`HudiConnectorFactory`, `HudiMetadata`, `HudiSplitManager`, `HudiPageSourceProvider`), built
standalone against the Trino parent POM; `packaging/hudi-trino-bundle` shades the Hudi core
modules it consumes (`hudi-common`, `hudi-client-common`, `hudi-java-client`,
`hudi-hadoop-mr`, …).

---

## 10. Flink Integration

```
  DataStream / Flink SQL  (CREATE TABLE ... WITH ('connector'='hudi'))
        │
        ▼
  HoodieTableFactory ──► HoodieTableSink / HoodieTableSource
        │                  (hudi-flink-datasource/hudi-flink/.../table/)
        ▼
  Streaming write pipeline (sink/ package):
    RowDataToHoodieFunction → BucketAssignFunction (or bucket index hashing)
        → StreamWriteFunction          buffers records, flushes per checkpoint
        → StreamWriteOperatorCoordinator   on checkpoint complete: commits the
                                           instant on the timeline (JobManager side)
    sink/compact/   CompactionPlanOperator → CompactOperator → CompactionCommitSink
    sink/clustering, sink/bulk_insert, sink/append, sink/bootstrap  — variants
        │
        ▼
  HoodieFlinkWriteClient (hudi-client/hudi-flink-client) — same client-common
  pipeline as Spark, with FLINK_STATE or bucket index and List-based HoodieData
```

Offline Flink jobs `HoodieFlinkCompactor` / `HoodieFlinkClusteringJob` mirror the Spark
utilities. The source side supports bounded snapshot reads and incremental streaming reads
(`HoodieTableSource` with split monitors).

---

## 11. Storage / IO Layer & Timeline Server

```
  hudi-io (no Hadoop dependency)             hudi-hadoop-common
  ┌─────────────────────────────┐            ┌──────────────────────────────────┐
  │ HoodieStorage (abstract)    │◄───────────│ HoodieHadoopStorage              │
  │   create/open/list/delete/  │ implements │   wraps hadoop FileSystem        │
  │   rename                    │            │   (s3a, gs, abfs, hdfs, file)    │
  │ StoragePath, StorageConfig. │            │ HoodieHadoopIOFactory            │
  │ HFile reader/writer         │            │   HoodieAvroParquetReader/Writer │
  │   (hudi-io/.../io/hfile/)   │            │   ORC / HFile / Avro log formats │
  └─────────────────────────────┘            └──────────────────────────────────┘

  Log format (hudi-common/.../common/table/log/): HoodieLogFormat reader/writer,
  block types (data/delete/rollback), used by MoR writes and HoodieFileGroupReader.
```

**Timeline server.** To avoid every executor listing storage and rebuilding the file system
view, the driver embeds a `TimelineService` (`hudi-timeline-service`): a lightweight HTTP server
fronting a cached `HoodieTableFileSystemView`. Executors use `RemoteHoodieTableFileSystemView`
(`hudi-common`) as a client. The same server hosts the **marker** endpoints
(timeline-server-based markers under `.hoodie/.temp/`) used to track and reconcile partially
written files on commit/rollback.

---

## 12. Sync Integrations

After commits, *sync tools* publish the table to external catalogs so query engines can discover
it. The common contract is in `hudi-sync/hudi-sync-common`:

```
  HoodieSyncTool.syncHoodieTable()        HoodieSyncClient (timeline-aware diffing:
        │                                  which partitions changed since last sync)
        ├── HiveSyncTool          hudi-sync/hudi-hive-sync — Hive Metastore (HMS or JDBC);
        │                          registers CoW table + MoR _ro/_rt views
        ├── AwsGlueCatalogSyncTool hudi-aws — AWS Glue Data Catalog
        ├── BigQuerySyncTool       hudi-gcp — Google BigQuery (manifest-based)
        ├── DataHubSyncTool        hudi-sync/hudi-datahub-sync — DataHub metadata
        └── AdbSyncTool            hudi-sync/hudi-adb-sync — Alibaba AnalyticDB
```

Sync runs inline from writers (`HoodieSparkSqlWriter`, Flink `StreamWriteOperatorCoordinator`,
`HoodieStreamer`) or standalone via each tool's `main()`.

---

## 13. Ingestion Utilities (hudi-utilities)

`HoodieStreamer` (`hudi-utilities/.../streamer/HoodieStreamer.java`, formerly DeltaStreamer) is
a self-contained Spark ingestion service:

```
  Source (sources/: Kafka avro/json/proto, DFS parquet/csv/orc, JDBC, Pulsar,
          Kinesis, S3/GCS event streams, Hudi incremental source, debezium CDC)
     │  SourceFormatAdapter + checkpointing (checkpointing/)
     ▼
  SchemaProvider (schema/: registry-based, file-based, JDBC, …)
     │
     ▼
  Transformer chain (transform/: SQL-based, flattening, chained, custom)
     │
     ▼
  WriteClient (upsert/insert/bulk_insert) + inline table services + sync tools
```

`HoodieMultiTableStreamer` fans the same pipeline out over many tables; continuous mode runs
ingestion and async compaction in one long-lived job. The module also ships operational tools:
validators (`HoodieMetadataTableValidator`, `HoodieDataTableValidator`), `HoodieSnapshotExporter`,
`HoodieRepairTool`, and `HoodieDropPartitionsTool`.

---

## 14. Packaging

`packaging/` produces shaded bundles so each runtime needs a single jar (dependencies are
relocated to avoid classpath clashes):

| Bundle | Contents / use |
|---|---|
| `hudi-spark-bundle` | Spark datasource + client + sync; `--jars` for Spark jobs |
| `hudi-flink-bundle` | Flink connector + client; for Flink SQL/DataStream jobs |
| `hudi-utilities-bundle` | HoodieStreamer + everything it needs (includes Spark bundle classes) |
| `hudi-utilities-slim-bundle` | Utilities without the Spark datasource (pair with spark-bundle) |
| `hudi-hadoop-mr-bundle` | InputFormats for Hive/MapReduce |
| `hudi-presto-bundle` / `hudi-trino-bundle` | Shaded Hudi core deps consumed by the Presto connector and the in-repo `hudi-trino-plugin` |
| `hudi-hive-sync-bundle` / `hudi-datahub-sync-bundle` | Standalone catalog sync tools |
| `hudi-aws-bundle` / `hudi-gcp-bundle` / `hudi-azure-bundle` | Cloud-specific add-ons (locks, sync, metrics) |
| `hudi-kafka-connect-bundle` | Kafka Connect sink plugin |
| `hudi-timeline-server-bundle` | Standalone timeline server deployment |
| `hudi-cli-bundle` | hudi-cli shell distribution |
| `hudi-integ-test-bundle` | Long-running integration test suite |

Version matrices are selected at build time via Maven profiles (`-Dspark3.5`, `-Dflink1.20`,
`-Dscala-2.13`, …); each engine-version adapter module compiles against exactly one engine
release, and the bundles pick the active pair.
