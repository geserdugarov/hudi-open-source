# Hudi Read Path Call Stack

This document traces the full call stack of a Hudi read on Spark: from SQL/DataSource table
resolution, through relation and `FileIndex` construction, timeline loading and file-system-view
resolution, metadata-table-based partition pruning and data skipping, latest file-slice
selection, down to the executor-side base/log file readers. Copy-on-Write (CoW) and
Merge-on-Read (MoR) differences are called out throughout.

A Hudi read runs in two distinct stages:

1. **Planning (driver)** — resolve the table to a `HadoopFsRelation`, list partitions and file
   slices as of the latest completed instant via the timeline + file system view (backed by the
   metadata table), prune partitions and files using metadata-table indexes, and emit one
   `PartitionDirectory` per partition (slices with log or bootstrap files embedded as
   `FileSlice` objects, base-file-only slices as plain files).
2. **Execution (executors)** — base-file-only slices (typical CoW) are read as regular Parquet
   files; for slices with log files (MoR snapshot) or bootstrap files, `HoodieFileGroupReader`
   reads the base file and merges the log-file records on top according to the table's record
   merge mode.

This doc focuses on the current default path on master (Hudi 1.x): the file-group-reader-based
`FileFormat`. Legacy relations (`BaseFileOnlyRelation`, `MergeOnReadSnapshotRelation`) survive
only for special cases (e.g. reading the metadata table itself) and are noted briefly.
Incremental, CDC, and time-travel queries are mentioned only at their entry points.

## Key modules and classes

| Module | Role | Key classes |
|---|---|---|
| `hudi-spark-datasource/hudi-spark-common` | Spark integration: relation resolution, file index, file format | `DefaultSource`, `HoodieHadoopFsRelationFactory` (+ concrete factories), `HoodieFileGroupReaderBasedFileFormat`, `HoodieFileIndex`, `SparkHoodieTableFileIndex`, `PartitionDirectoryConverter`, index supports (`ColumnStatsIndexSupport`, `RecordLevelIndexSupport`, ...) |
| `hudi-spark-datasource/hudi-spark` | SQL extension and analysis rules | `HoodieSparkSessionExtension`, `HoodieAnalysis` |
| `hudi-spark-datasource/hudi-spark3.x.x / 4.x.x` | Version-specific adapters | `HoodieSpark3xAnalysis` (V2→V1 fallback), `Spark3xHoodiePartitionFileSliceMapping`, `Spark3xParquetReader` |
| `hudi-common` | Engine-agnostic file index base, timeline, file system view, unified file-group reader | `BaseHoodieTableFileIndex`, `HoodieTableMetaClient`, `HoodieActiveTimeline`, `CompletionTimeQueryView`, `FileSystemViewManager`, `AbstractTableFileSystemView`, `HoodieTableFileSystemView`, `HoodieFileGroup`, `FileSlice`, `HoodieFileGroupReader`, `FileGroupRecordBuffer` (+ subclasses), `HoodieMergedLogRecordReader`, `BufferedRecordMergerFactory` |
| `hudi-common` (metadata) | Metadata table access | `HoodieTableMetadata`, `HoodieBackedTableMetadata` |
| `hudi-client/hudi-spark-client` | Spark reader context | `SparkFileFormatInternalRowReaderContext`, `HoodiePartitionFileSliceMapping` (trait) |
| `hudi-timeline-service` | Optional remote file-system-view server | `RequestHandler` (serves `RemoteHoodieTableFileSystemView` calls) |

## 1. Entry points: table resolution

### 1.1 DataFrame API

`spark.read.format("hudi").load(path)` resolves the `hudi` (or `org.apache.hudi`) data source to
`DefaultSource`
(`hudi-spark-datasource/hudi-spark-common/.../org/apache/hudi/DefaultSource.scala`), a Spark
DataSource **V1** `RelationProvider`:

```text
DefaultSource.createRelation(sqlContext, optParams [, schema])
├─ resolve the table base path from the "path" option; glob paths and
│  hoodie.datasource.read.paths are rejected ("Glob paths are not supported ... as of 1.2.0")
├─ DataSourceOptionsHelper.parametersWithReadDefaults(...)  // fills hoodie.datasource.query.type etc.
├─ HoodieTableMetaClient.builder()...build()        // loads hoodie.properties + timeline
└─> DefaultSource.createRelation(sqlContext, metaClient, schema, parameters)  // object method
```

### 1.2 Spark SQL (`SELECT ... FROM hudi_table`)

- `HoodieSparkSessionExtension`
  (`hudi-spark-datasource/hudi-spark/.../sql/hudi/HoodieSparkSessionExtension.scala`) injects the
  Hudi parser, resolution/post-hoc rules from `HoodieAnalysis`
  (`.../sql/hudi/analysis/HoodieAnalysis.scala`), and optimizer rules.
- Catalog resolution produces a `DataSourceV2Relation` over `HoodieInternalV2Table`
  (`hudi-spark-datasource/hudi-spark-common/.../sql/hudi/catalog/HoodieInternalV2Table.scala`),
  but Hudi deliberately does **not** implement V2 scans: a version-specific
  `DataSourceV2ToV1Fallback` rule (e.g. `HoodieSpark34DataSourceV2ToV1Fallback` in
  `hudi-spark-datasource/hudi-spark3.4.x/.../analysis/HoodieSpark34Analysis.scala`) rewrites it
  to a V1 `LogicalRelation` by calling `new DefaultSource().createRelation(...)` with the
  catalog table's config — i.e. SQL reads converge on the same stack as section 1.1.

### 1.3 Query-type routing

`DefaultSource.createRelation(...)` (the object method) dispatches on
`(tableType, queryType, isBootstrappedTable)` where `queryType` comes from
`hoodie.datasource.query.type` (default `snapshot`; other values `read_optimized`,
`incremental`):

```text
(tableType, queryType, isBootstrap) match
├─ (COW, snapshot, false) | (COW, read_optimized, false) | (MOR, read_optimized, false)
│   └─> new HoodieCopyOnWriteSnapshotHadoopFsRelationFactory(...).build()
│       // base files only — MoR read-optimized intentionally uses the CoW factory
├─ (MOR, snapshot, _)
│   └─> new HoodieMergeOnReadSnapshotHadoopFsRelationFactory(...).build()
├─ (COW, incremental, _) → HoodieCopyOnWriteIncrementalHadoopFsRelationFactoryV1|V2(...)
├─ (MOR, incremental, _) → HoodieMergeOnReadIncrementalHadoopFsRelationFactoryV1|V2(...)
│   // V1 vs V2 chosen by requested incremental format / table version (completion-time
│   // based incremental queries on table version >= 8)
├─ incremental + hoodie.datasource.query.incremental.format=cdc
│   └─> HoodieCopyOnWriteCDCHadoopFsRelationFactory / HoodieMergeOnReadCDCHadoopFsRelationFactory
└─ (_, _, true) → HoodieCopyOnWriteSnapshotHadoopFsRelationFactory
    // remaining bootstrap cases (CoW snapshot/read-optimized, MoR read-optimized);
    // MoR snapshot on a bootstrap table is matched by the (MOR, snapshot, _) case above
```

Time travel is snapshot routing plus the `as.of.instant` option, which caps the query instant
used by the file index (section 4).
Reads of the **metadata table itself** still fall back to the legacy `BaseFileOnlyRelation` /
`MergeOnReadSnapshotRelation` (`hudi-spark-datasource/hudi-spark-common/.../org/apache/hudi/`).

## 2. Relation building: HadoopFsRelation factories

`HoodieHadoopFsRelationFactory.scala`
(`hudi-spark-datasource/hudi-spark-common/.../org/apache/hudi/HoodieHadoopFsRelationFactory.scala`)
defines `HoodieBaseHadoopFsRelationFactory` and the concrete factories. Each factory assembles a
plain Spark `HadoopFsRelation` whose pieces are all Hudi-aware:

```text
Hoodie{CopyOnWrite,MergeOnRead}SnapshotHadoopFsRelationFactory.build()
└─> HadoopFsRelation(
      location       = buildFileIndex(),     // HoodieFileIndex (section 3)
      partitionSchema,
      dataSchema,                            // from TableSchemaResolver / schema evolution
      bucketSpec     = None,
      fileFormat     = buildFileFormat(),    // HoodieFileGroupReaderBasedFileFormat (section 5)
      options)(sparkSession)
```

Factory differences:

- **`HoodieCopyOnWriteSnapshotHadoopFsRelationFactory`** — `isMOR = false`; creates
  `HoodieFileIndex(..., shouldEmbedFileSlices = true)` (no log files). Used for CoW
  snapshot/read-optimized **and** MoR read-optimized queries.
- **`HoodieMergeOnReadSnapshotHadoopFsRelationFactory`** — `isMOR = true`; creates
  `HoodieFileIndex(..., includeLogFiles = true, shouldEmbedFileSlices = true)`.
- Incremental factories use `HoodieIncrementalFileIndex`
  (`.../org/apache/hudi/HoodieIncrementalFileIndex.scala`), which filters file slices to those
  written in the requested instant range.

`buildFileFormat()` constructs the executor-side reader:

```text
new HoodieFileGroupReaderBasedFileFormat(
    tablePath, HoodieTableSchema(...), tableName,
    queryTimestamp,            // latest completed instant or as.of.instant
    mandatoryFields,           // record key / precombine fields needed for merging
    isMOR, isBootstrap, isIncremental,
    validCommits,              // for schema-evolution (InternalSchema) resolution
    shouldUseRecordPosition,   // hoodie.merge.use.record.positions (default true)
    requiredFilters, isMultipleBaseFileFormatsEnabled, baseFileFormat)
```

## 3. Planning: HoodieFileIndex listing and pruning

`HoodieFileIndex`
(`hudi-spark-datasource/hudi-spark-common/.../org/apache/hudi/HoodieFileIndex.scala`) implements
Spark's `FileIndex` on top of `SparkHoodieTableFileIndex` (same module), which extends the
engine-agnostic `BaseHoodieTableFileIndex` (`hudi-common/src/main/java/org/apache/hudi/BaseHoodieTableFileIndex.java`).

```text
HoodieFileIndex.listFiles(partitionFilters, dataFilters)        // called by Spark planner
├─ extract nested-partition filters from dataFilters if needed
├─> filterFileSlices(dataFilters, partitionFilters)
│   ├─> prunePartitionsAndGetFileSlices(dataFilters, partitionFilters)
│   │   ├─ PARTITION PRUNING:
│   │   │   ├─ partition filters present
│   │   │   │   └─> listMatchingPartitionPaths(partitionFilters)
│   │   │   │       [SparkHoodieTableFileIndex.scala]
│   │   │   │       ├─ getAllQueryPartitionPaths() if already cached, else
│   │   │   │       └─ tryPushDownPartitionPredicates(...)   // prefix analysis: equality
│   │   │   │          values on leading partition columns become a path prefix, listed via
│   │   │   │          tableMetadata.getPartitionPathWithPathPrefixes(...) (metadata table)
│   │   │   ├─ no partition filters but data skipping enabled
│   │   │   │   ├─> PartitionStatsIndexSupport.prunePartitions(this, dataFilters)
│   │   │   │   │   // PARTITION_STATS metadata partition: per-partition col min/max
│   │   │   │   └─ else ExpressionIndexSupport.prunePartitions(...)
│   │   │   └─ else: all partitions
│   │   └─> getInputFileSlices(prunedPartitions)              // section 4
│   │       [BaseHoodieTableFileIndex.java]
│   └─> lookupCandidateFilesInMetadataTable(dataFilters, prunedSlices, isPruned)
│       // DATA SKIPPING (below) → Option[Set[fileName]]; file slices whose base/log files
│       // are not candidates are dropped
└─> prepareFileSlices(prunedSlices)
    ├─ shouldEmbedFileSlices = true (file-group-reader path):
    │   └─> PartitionDirectoryConverter.convertFileSliceToPartitionDirectory(...)
    │       [.../org/apache/hudi/PartitionDirectoryConverter.scala]
    │       // the listed "file" is a delegate FileStatus whose size estimates
    │       // base + log size (drives Spark's split/parallelism planning)
    │       ├─ slice has log files or a bootstrap base file:
    │       │   // partition values are wrapped in a HoodiePartitionFileSliceMapping
    │       │   // carrying the FileSlice to the executor for merged reading (section 5)
    │       └─ base-file-only slice (typical CoW):
    │           // plain partition values — the executor reads the base file as a
    │           // regular parquet file, no slice mapping and no file-group reader
    └─ legacy path: flatten base + log files into plain FileStatus entries
```

### 3.1 Data skipping via metadata-table indexes

`lookupCandidateFilesInMetadataTable` runs only when `hoodie.enable.data.skipping` (default
`true`) and the metadata table is available (`hoodie.metadata.enable`, default `true` for
readers). It walks `indicesSupport` in order and returns the first non-empty pruning result
(all classes in `hudi-spark-datasource/hudi-spark-common/.../org/apache/hudi/`):

1. **`RecordLevelIndexSupport`** — for `EqualTo`/`In` predicates on the record key: looks up
   exact file locations in the `record_index` metadata partition
   (`metadataTable.readRecordIndexLocationsWithKeys`). Snapshot-at-latest queries only.
2. **`PartitionBucketIndexSupport` / `BucketIndexSupport`** — for tables using the bucket index:
   hashes equality predicates on the bucket key to a bucket ID set and keeps only matching file
   names.
3. **`SecondaryIndexSupport`** — like RLI but for columns covered by a secondary index
   (`readSecondaryIndexLocationsWithKeys`).
4. **`ExpressionIndexSupport`** — expression indexes (column stats or bloom filters defined over
   expressions, e.g. `from_unixtime(ts)`).
5. **`BloomFiltersIndexSupport`** — record-key predicates probed against per-file bloom filters
   stored in the `bloom_filters` metadata partition.
6. **`ColumnStatsIndexSupport`** — translates data filters into min/max predicates
   (`translateIntoColumnStatsIndexFilterExpr`, e.g. `price > 100` → `price_max >= 100`),
   loads the `column_stats` partition (`getRecordsByKeyPrefixes`), transposes it into one row
   per file, and keeps files whose stats ranges may match. Files absent from the index are
   always kept (no false negatives).

Failures are governed by `hoodie.fileIndex.dataSkippingFailureMode` (default `fallback`: skip
pruning rather than fail the query).

## 4. Timeline and file-system-view resolution

### 4.1 Timeline loading

`HoodieTableMetaClient` (`hudi-common/.../common/table/HoodieTableMetaClient.java`) reads
`hoodie.properties` and lazily creates the `HoodieActiveTimeline` from the instant files under
`.hoodie/timeline/` (table version ≥ 8; `.hoodie/` directly for version 6). On table version 8+
completed instant files embed a **completion time**
(`<requestedTime>_<completionTime>.<action>`), and a `CompletionTimeQueryView`
(`hudi-common/.../common/table/timeline/CompletionTimeQueryView.java`) answers
"when did instant X complete" — used both for log-file-to-slice assignment and uncommitted-file
filtering below.

The file index picks the **query instant**:

```text
BaseHoodieTableFileIndex
├─ activeTimeline = metaClient.getCommitsAndCompactionTimeline()
├─ if !shouldIncludePendingCommits: timeline.filterCompletedAndCompactionInstants()
└─ queryInstant = specified as.of.instant, else timeline.lastInstant()   // latest completed
```

### 4.2 Building the file system view

`BaseHoodieTableFileIndex.loadFileSlicesForPartitions(partitions)`:

```text
loadFileSlicesForPartitions(partitions)
├─> listPartitionPathFiles(partitions)
│   └─> tableMetadata.getAllFilesInPartitions(paths)
│       [hudi-common/.../metadata/HoodieBackedTableMetadata.java]
│       // reads the FILES partition of the metadata table (.hoodie/.metadata) — itself a
│       // Hudi MoR table read through HoodieFileGroupReader; falls back to direct storage
│       // listing when hoodie.metadata.enable=false
├─ new HoodieTableFileSystemView(metaClient, activeTimeline, allFiles)
│   [hudi-common/.../common/table/view/HoodieTableFileSystemView.java]
└─> filterFiles(...)   // slice selection, section 4.3
```

(For long-running services the view normally comes from `FileSystemViewManager.createViewManager`
(`hudi-common/.../common/table/view/FileSystemViewManager.java`), which supports `MEMORY`
(default), `SPILLABLE_DISK`, `EMBEDDED_KV_STORE` (RocksDB), and `REMOTE_FIRST`/`REMOTE_ONLY`
via `RemoteHoodieTableFileSystemView` calling the `hudi-timeline-service` REST endpoints.)

`AbstractTableFileSystemView`
(`hudi-common/.../common/table/view/AbstractTableFileSystemView.java`) turns raw file listings
into file groups and slices:

```text
init(metaClient, visibleActiveTimeline)
├─ completionTimeQueryView = ... (table version 8+)
├─ pendingCompactionOperations = CompactionUtils.getAllPendingCompactionOperations(metaClient)
├─ fileGroupsInPendingClustering = ClusteringUtils.getAllFileGroupsInPendingClusteringPlans(...)
└─ resetFileGroupsReplaced(timeline)     // fileGroupId -> replacing instant, from
                                         // replacecommit/cluster instants

addFilesToView(partition, pathInfoList)
└─> buildFileGroups(...)
    ├─ group base files and log files by fileId
    └─ for each fileId: new HoodieFileGroup(partition, fileId, timeline)
        ├─ addBaseFile(...)              // one FileSlice per (fileId, baseInstantTime)
        └─ addLogFile(completionTimeQueryView, logFile)
            [hudi-common/.../common/model/HoodieFileGroup.java]
            // v8+: a log file belongs to the slice whose base instant is the latest instant
            // with completion time <= the log's delta commit completion time;
            // v6: log file name embeds the base instant time directly
```

### 4.3 Latest file slice selection

`BaseHoodieTableFileIndex.filterFiles(...)`:

```text
queryInstant present
  └─> fsView.getLatestMergedFileSlicesBeforeOrOn(partitionPath, queryInstant)
else
  └─> fsView.getLatestFileSlices(partitionPath)
```

Inside `AbstractTableFileSystemView`, every slice stream applies these visibility rules:

- **Replaced file groups are excluded** — `isFileGroupReplaced(fileGroupId)` drops file groups
  replaced by a completed clustering/insert-overwrite (`replacecommit`) at or before the query
  instant.
- **Uncommitted files are excluded** — table v8+: `filterUncommittedFiles(...)` keeps a base
  file only if `completionTimeQueryView.isCompleted(baseInstantTime)` and log files only for
  completed delta commits. Table v6: `filterBaseFileAfterPendingCompaction(...)` hides a base
  file whose instant is a pending compaction (only its log files remain visible).
- **Pending compaction is merged, not hidden** — `getLatestMergedFileSlicesBeforeOrOn` calls
  `fetchMergedFileSlice(fileGroup, fileSlice)`: if the latest slice's base instant equals a
  pending compaction instant (base file not yet written), the slice is merged with the previous
  slice via `mergeCompactionPendingFileSlices(...)` so the reader sees the old base file plus
  log files of **both** slices. This is how MoR snapshot reads stay correct while compaction is
  in flight.

CoW vs MoR at this stage:

- **CoW** — file slices are just base files (`FileSlice.getLogFiles()` empty); the
  `TableFileSystemView.BaseFileOnlyView` semantics apply.
- **MoR snapshot** — slices carry base file + sorted log files (`SliceView` semantics) and the
  merged-pending-compaction logic above.
- **MoR read-optimized** — routed through the CoW factory, so the file index simply ignores log
  files: each slice contributes only its latest base file (data in un-compacted logs is not
  visible).

## 5. Execution: HoodieFileGroupReaderBasedFileFormat

`HoodieFileGroupReaderBasedFileFormat`
(`hudi-spark-datasource/hudi-spark-common/.../sql/execution/datasources/parquet/HoodieFileGroupReaderBasedFileFormat.scala`)
extends `ParquetFileFormat`. Spark plans a normal `FileSourceScanExec` over the
`PartitionDirectory`s from section 3. A `PartitionedFile` for a slice with log files or a
bootstrap base file carries a `HoodiePartitionFileSliceMapping`
(`hudi-client/hudi-spark-client/.../org/apache/hudi/HoodiePartitionFileSliceMapping.scala`,
version-specific impls like `Spark3HoodiePartitionFileSliceMapping`) as its partition values;
base-file-only slices carry plain partition values (section 3).

```text
buildReaderWithPartitionValues(sparkSession, dataSchema, partitionSchema, requiredSchema,
                               filters, options, hadoopConf)
├─ supportBatch decision:
│   ├─ supportVectorizedRead = !isIncremental && !isBootstrap && parquet batch supported
│   └─ supportReturningBatch = !isMOR && supportVectorizedRead
│       // MoR snapshot ALWAYS reads row-by-row: merging happens per record
├─ broadcast base-file readers (vectorized + row-based variants)
└─ return (file: PartitionedFile) => {
     file.partitionValues match {
       case mapping: HoodiePartitionFileSliceMapping =>
         mapping.getSlice(FSUtils.getFileIdFromFilePath(filePath)) match {
           case Some(fileSlice) if logFiles present || merging needed =>
             reader = HoodieFileGroupReader.builder()
                 .withReaderContext(new SparkFileFormatInternalRowReaderContext(...))
                 .withHoodieTableMetaClient(metaClient)
                 .withLatestCommitTime(queryTimestamp)
                 .withBaseFileOption(fileSlice.getBaseFile)     // may be empty (log-only slice)
                 .withLogFiles(fileSlice.getLogFiles)
                 .withDataSchema / withRequestedSchema / withInternalSchemaOpt(...)
                 .withStart(file.start).withLength(...)         // split bounds in base file
                 .withShouldUseRecordPosition(shouldUseRecordPosition)
                 .build()
             reader.getClosableIterator()                       // merged InternalRows
             // + appendPartitionAndProject(...) re-attaches partition columns
           case _ => readBaseFile(...)   // no slice for this fileId / count(*) pushdown
         }
       case _ => readBaseFile(...)       // plain partition values: base-file-only slice
     }
   }
```

CoW snapshot reads therefore do **not** go through `HoodieFileGroupReader` at all: their slices
have no log files, so `PartitionDirectoryConverter` left the partition values unwrapped and the
scan falls to the `readBaseFile(...)` branch — a regular parquet read that, because
`supportReturningBatch` is true for CoW, returns vectorized columnar batches without per-record
overhead. `HoodieFileGroupReader` is engaged only on the slice-mapping branch, i.e. for MoR
slices with log files and for bootstrap slices.

## 6. HoodieFileGroupReader: base/log merging

`HoodieFileGroupReader`
(`hudi-common/.../common/table/read/HoodieFileGroupReader.java`) is the unified, engine-agnostic
file-slice reader (the same class compaction and the Hive/Flink readers use). The engine plugs
in via `HoodieReaderContext` (`hudi-common/.../common/engine/HoodieReaderContext.java`); Spark's
implementation is `SparkFileFormatInternalRowReaderContext`
(`hudi-client/hudi-spark-client/.../org/apache/hudi/SparkFileFormatInternalRowReaderContext.scala`,
on top of `BaseSparkInternalRowReaderContext` in the same module), which reads base files
through the version-specific `SparkParquetReader` (e.g.
`hudi-spark-datasource/hudi-spark3.5.x/.../Spark35ParquetReader.scala`) honoring pushed filters
and schema evolution.

```text
HoodieFileGroupReader.initRecordIterators()
├─> makeBaseFileIterator()
│   ├─ no base file → EmptyIterator                       // log-only file slice
│   ├─ bootstrap base file → makeBootstrapBaseFileIterator(...)
│   │   // reads skeleton file (hudi meta columns) + original data file and zips them via
│   │   // readerContext.mergeBootstrapReaders(...)
│   └─ else readerContext.getFileRecordIterator(path, start, length, dataSchema, requiredSchema)
│       └─ Spark: SparkParquetReader → ClosableIterator<InternalRow>
└─> recordBufferLoader.getRecordBuffer(...)               // only if log files exist
    [hudi-common/.../common/table/read/buffer/DefaultFileGroupRecordBufferLoader.java]
    ├─ hoodie.datasource.merge.type == skip_merge → UnmergedFileGroupRecordBuffer
    ├─ sortOutputs → SortedKeyBasedFileGroupRecordBuffer
    ├─ shouldUseRecordPosition && parquet base file → PositionBasedFileGroupRecordBuffer
    ├─ else → KeyBasedFileGroupRecordBuffer
    └─> LogScanningRecordBufferLoader.scanLogFiles(...)
        └─> HoodieMergedLogRecordReader (builder)
            [hudi-common/.../common/table/log/HoodieMergedLogRecordReader.java]
```

### 6.1 Log scanning

`HoodieMergedLogRecordReader` / `BaseHoodieLogRecordReader`
(`hudi-common/.../common/table/log/`) iterate the slice's log files via `HoodieLogFormatReader`:

```text
scanInternal(...)
├─ pass 1: walk log blocks across all log files
│   ├─ skip CORRUPT blocks; collect data/delete blocks per instant
│   ├─ skip blocks of instants not completed on the timeline (uncommitted data) and
│   │  blocks outside the instant range (incremental reads)
│   └─ COMMAND blocks (rollback) → drop the blocks of the target instant
└─ pass 2: feed surviving blocks to the record buffer
    ├─ HoodieDataBlock   → recordBuffer.processDataBlock(block, keySpec)
    └─ HoodieDeleteBlock → recordBuffer.processDeleteBlock(block)
```

The buffer accumulates the **net effect of all log records per key** in an
`ExternalSpillableMap` (`hudi-common/.../common/util/collection/ExternalSpillableMap.java`) —
in-memory up to `hoodie.memory.merge.max.size` (default 1 GB), then spilling to disk
(`hoodie.memory.spillable.map.path`, BITCASK or BTreeMap format). Multiple log records for the
same key are
combined as they arrive via `BufferedRecordMerger.deltaMerge(...)`.

### 6.2 Merging with the base file

`FileGroupRecordBuffer` subclasses
(`hudi-common/.../common/table/read/buffer/`) implement the streamed merge:

```text
KeyBasedFileGroupRecordBuffer.doHasNext()              // default
├─ while baseFileIterator.hasNext():
│   ├─ key = readerContext.getRecordKey(baseRecord)
│   ├─ logRecord = records.remove(key)                 // O(1) spillable-map lookup
│   ├─ logRecord == null → emit base record as-is
│   └─ else → bufferedRecordMerger.finalMerge(baseRecord, logRecord)
│       // delete → skip (unless emitting deletes); update → emit merged record
└─ then drain remaining log-only records (inserts that never hit the base file)
```

`PositionBasedFileGroupRecordBuffer` keys the map by **parquet row position** instead of record
key when log blocks carry record positions in their headers
(`hoodie.merge.use.record.positions`, default `true`, table version ≥ 8): the base file is read
with a synthetic row-index column and merging is a positional lookup, avoiding key extraction
and string comparison. It transparently falls back to key-based merging when positions are
missing.

Merge semantics come from the table's `RecordMergeMode`
(`BufferedRecordMergerFactory`, `hudi-common/.../common/table/read/BufferedRecordMergerFactory.java`):

- **`COMMIT_TIME_ORDERING`** — latest write wins (log record replaces base record; later log
  record replaces earlier).
- **`EVENT_TIME_ORDERING`** — compare the ordering (precombine) field; the record with the
  larger ordering value wins, so late-arriving data does not overwrite newer events.
- **`CUSTOM`** — delegate to the table's `HoodieRecordMerger` / legacy payload class
  (e.g. `ExpressionPayload` for MERGE INTO).

Partial-update mode wraps the merger to combine column subsets instead of whole records.

## 7. CoW vs MoR summary

| Aspect | Copy-on-Write | Merge-on-Read (snapshot) | Merge-on-Read (read-optimized) |
|---|---|---|---|
| Relation factory | `HoodieCopyOnWriteSnapshotHadoopFsRelationFactory` | `HoodieMergeOnReadSnapshotHadoopFsRelationFactory` | CoW factory (base files only) |
| File index flags | `shouldEmbedFileSlices=true` | `includeLogFiles=true, shouldEmbedFileSlices=true` | same as CoW |
| Slice content | base file only | base file + log files (merged across pending compaction) | latest base file only |
| Timeline visibility | completed `commit`s | completed `commit` + `deltacommit` + pending compaction handling | completed base files only |
| Executor read | plain base-file read (`readBaseFile`, vectorized when supported); no `HoodieFileGroupReader` | row-by-row `HoodieFileGroupReader` merge (`supportReturningBatch=false`) | plain base-file read, vectorized |
| Freshness | always full data | full data incl. un-compacted log records | misses data not yet compacted |

## 8. Key configuration reference

| Config | Default | Effect |
|---|---|---|
| `hoodie.datasource.query.type` | `snapshot` | `snapshot` / `read_optimized` / `incremental` routing (section 1.3) |
| `hoodie.metadata.enable` (read side) | `true` | Use metadata table FILES partition for listings instead of direct storage listing |
| `hoodie.enable.data.skipping` | `true` | Enable index-based file pruning in `HoodieFileIndex` |
| `hoodie.fileIndex.dataSkippingFailureMode` | `fallback` | On index lookup failure: skip pruning (`fallback`) or fail (`strict`) |
| `hoodie.datasource.read.file.index.listing.mode` | `lazy` | Lazy vs eager partition listing in the file index |
| `hoodie.datasource.merge.type` | `payload_combine` | `skip_merge` reads base + log records without merging (`UnmergedFileGroupRecordBuffer`) |
| `hoodie.merge.use.record.positions` | `true` | Position-based (parquet row index) merging instead of key-based |
| `hoodie.memory.merge.max.size` | `1073741824` (1 GB) | Spillable-map memory budget for buffered log records |
| `hoodie.memory.spillable.map.path` | derived (java tmpdir) | Spill directory for the external spillable map |
| `hoodie.datasource.read.begin.instanttime` / `end.instanttime` | — | Incremental query bounds |
| `as.of.instant` | — | Time-travel: query instant used for slice selection |
| `hoodie.filesystem.view.type` | `MEMORY` | File-system-view storage (`SPILLABLE_DISK`, `EMBEDDED_KV_STORE`, `REMOTE_FIRST`, ...) |
