# DSv2 Read Support - Incremental PR Plan

## Context

Commit `fb398211d2cb` introduced a minimal DSv2 read skeleton for Hudi's Spark integration with 6 new Java classes and modifications to `HoodieInternalV2Table` and `HoodieCatalog`. The skeleton establishes the full Spark DSv2 interface chain (`ScanBuilder` → `Scan` → `Batch` → `InputPartition` → `PartitionReaderFactory` → `PartitionReader<InternalRow>`) but leaves two critical methods as stubs:

- `HoodieScan.toBatch()` - driver-side file planning
- `HoodiePartitionReader.initialize()` - executor-side file group reading

All changes are gated behind `hoodie.datasource.read.use.dsv2=true` (default `false`), keeping the existing V1 read path unaffected. The reference architecture is the Iceberg DSv2 read blueprint at `docs/claude/iceberg/ICEBERG_SPARK_DSV2_READ_BLUEPRINT.md`.

---

## PR 1: Wire End-to-End Read (HoodieScan.toBatch + HoodiePartitionReader.initialize)

**Goal**: Make a basic end-to-end DSv2 read work for both COW and MOR tables.

### HoodieScan.toBatch() — Driver Side

Implement file planning by reusing the existing `HoodieFileIndex`:

1. Build `HoodieTableMetaClient` from `tablePath` + Hadoop conf from `spark.sessionState().newHadoopConf()`
2. Create `HoodieFileIndex` (the existing Scala class at `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/HoodieFileIndex.scala`):
   ```scala
   HoodieFileIndex(spark, metaClient, Some(tableSchema), options,
     includeLogFiles = true, shouldEmbedFileSlices = true)
   ```
3. Call `fileIndex.filterFileSlices(dataFilters=Seq.empty, partitionFilters=Seq.empty)` to get all file slices (no filter pushdown in this PR)
4. Get `latestCommitTime` from `metaClient.getCommitsAndCompactionTimeline.filterCompletedInstants.lastInstant()`
5. Create `HoodieInputPartition` for each `FileSlice`, wrapping partition path, table path, and latest commit time
6. Return `new HoodieBatch(partitions, tableSchema, requiredSchema, options)`

### HoodiePartitionReader.initialize() — Executor Side

Wire `HoodieFileGroupReader` following the pattern in `HoodieFileGroupReaderBasedFileFormat.buildReaderWithPartitionValues()` (line 252-307):

1. Build `HoodieTableMetaClient` from `partition.getTablePath()` + storage conf
2. Create `SparkColumnarFileReader` via `SparkAdapterSupport.sparkAdapter.createParquetFileReader(false, sqlConf, options, hadoopConf)` — non-vectorized since MOR merging is row-based
3. Create `SparkFileFormatInternalRowReaderContext(baseFileReader, filters=Seq.empty, requiredFilters=Seq.empty, storageConf, tableConfig)` (line 61-66 of `SparkFileFormatInternalRowReaderContext.scala`)
4. Convert `tableSchema`/`requiredSchema` from `StructType` to `HoodieSchema` via `HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema()`
5. Build `HoodieFileGroupReader` via builder:
   ```java
   HoodieFileGroupReader.newBuilder()
     .withReaderContext(readerContext)
     .withHoodieTableMetaClient(metaClient)
     .withLatestCommitTime(partition.getLatestCommitTime())
     .withFileSlice(partition.getFileSlice())
     .withDataSchema(dataSchema)
     .withRequestedSchema(requestedSchema)
     .withProps(props)
     .withShouldUseRecordPosition(shouldUseRecordPosition)
     .build()
   ```
6. Obtain `this.iterator = fileGroupReader.getClosableIterator()`

### Serialization Concerns

The `HoodiePartitionReaderFactory` must carry serializable Hadoop config to executors:
- Add `SerializableConfiguration` field (from `org.apache.spark.util.SerializableConfiguration`)
- Pass it from `HoodieBatch` → `HoodiePartitionReaderFactory` → `HoodiePartitionReader`
- On the executor, unwrap to `Configuration` for `HoodieTableMetaClient` and reader context construction

### Files Modified
- `hudi-spark-datasource/hudi-spark-common/src/main/java/org/apache/hudi/spark/read/HoodieScan.java` — implement `toBatch()`
- `hudi-spark-datasource/hudi-spark-common/src/main/java/org/apache/hudi/spark/read/HoodiePartitionReader.java` — implement `initialize()`
- `hudi-spark-datasource/hudi-spark-common/src/main/java/org/apache/hudi/spark/read/HoodiePartitionReaderFactory.java` — add `SerializableConfiguration`
- `hudi-spark-datasource/hudi-spark-common/src/main/java/org/apache/hudi/spark/read/HoodieBatch.java` — pass serializable config through

### New Test
- `hudi-spark-datasource/hudi-spark/src/test/java/org/apache/hudi/spark/read/TestHoodieDSv2Read.java`
  - Write a COW table, read with `hoodie.datasource.read.use.dsv2=true`, verify count and row values match V1 path
  - Write a MOR table with upserts (base + log files), read with DSv2, verify merged results match V1 path
  - Test both non-partitioned and partitioned tables
  - Test basic column pruning (`SELECT col1, col2 FROM table`)

### Verification
```bash
mvn test -Punit-tests -pl hudi-spark-datasource/hudi-spark -Dtest=TestHoodieDSv2Read
```

---

## PR 2: Implement Filter Classification and Partition Pruning

**Goal**: Replace the stub `pushFilters()` in `HoodieScanBuilder` with proper filter classification and wire partition/data filters through to `HoodieFileIndex.filterFileSlices()`.

### Filter Classification in HoodieScanBuilder.pushFilters()

Classify each Spark `Filter` into three buckets (following Iceberg's pattern):
1. **Partition filters** — reference only partition columns; used for partition pruning
2. **Data filters** — reference data columns; pushed to `HoodieFileIndex` for data skipping (column stats, bloom, record index)
3. **Unsupported filters** — cannot be converted; returned to Spark for post-scan evaluation

Use `metaClient.getTableConfig().getPartitionFields()` to identify partition columns. The `HoodieScanBuilder` needs access to partition column names — pass them from `HoodieInternalV2Table.newScanBuilder()`.

Return **unsupported** filters from `pushFilters()` (i.e., only what Spark must evaluate post-scan).

### Wire Filters to HoodieScan.toBatch()

- Convert partition/data `Filter[]` to Spark `Expression[]` using `SparkAdapter.translateFilter()` or `DataSourceStrategy.translateFilter()`
- Call `fileIndex.filterFileSlices(dataFilters, partitionFilters)` instead of passing empty sequences

### Wire Data Filters to HoodiePartitionReader

- Pass data filters through `HoodiePartitionReaderFactory` → `HoodiePartitionReader` → `SparkFileFormatInternalRowReaderContext` constructor so Parquet predicate pushdown is effective at the file level

### Files Modified
- `hudi-spark-datasource/hudi-spark-common/src/main/java/org/apache/hudi/spark/read/HoodieScanBuilder.java` — implement proper `pushFilters()` with 3-way classification
- `hudi-spark-datasource/hudi-spark-common/src/main/java/org/apache/hudi/spark/read/HoodieScan.java` — pass classified filters to `filterFileSlices()`
- `hudi-spark-datasource/hudi-spark-common/src/main/java/org/apache/hudi/spark/read/HoodiePartitionReader.java` — pass data filters to reader context
- `hudi-spark-datasource/hudi-spark-common/src/main/java/org/apache/hudi/spark/read/HoodiePartitionReaderFactory.java` — carry filters
- `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/catalog/HoodieInternalV2Table.scala` — pass partition columns to `HoodieScanBuilder`

### New Tests
- Extend `TestHoodieDSv2Read.java`:
  - Partition filter: write multi-partition table, read with `WHERE partition_col = 'X'`, verify results match
  - Data filter: write table, read with `WHERE data_col > N`, verify results match V1
  - Unsupported filter: verify UDF-based filters are returned to Spark
  - Combined: partition + data filters together

### Verification
```bash
mvn test -Punit-tests -pl hudi-spark-datasource/hudi-spark -Dtest=TestHoodieDSv2Read
```

---

## PR 3: Implement Statistics Reporting for CBO

**Goal**: Return meaningful statistics from `HoodieScan.estimateStatistics()` so Spark's Cost-Based Optimizer can make better decisions (e.g., broadcast joins for small tables).

### Implementation

In `HoodieScan.estimateStatistics()`:
- **sizeInBytes**: Sum file sizes from all `FileSlice`s' base files (`getBaseFile().get().getFileSize()`) and log files (`getLogFiles().mapToLong(HoodieLogFile::getFileSize).sum()`). Reference: `HoodieFileIndex.sizeInBytes` (line 433+ of `HoodieFileIndex.scala`).
- **numRows**: If available from the latest commit metadata (`TOTAL_RECORDS_WRITTEN`), use it. Otherwise return `OptionalLong.empty()`.

Since `estimateStatistics()` may be called before `toBatch()`, the file planning should be lazy-initialized and cached (compute file slices once, reuse in both methods).

### Files Modified
- `hudi-spark-datasource/hudi-spark-common/src/main/java/org/apache/hudi/spark/read/HoodieScan.java` — implement `estimateStatistics()` with lazy file slice computation

### Tests
- Verify via `spark.table("t").queryExecution.optimizedPlan.stats` that `sizeInBytes > 0`
- Verify CBO picks broadcast join for small tables

---

## PR 4: Schema Evolution Support

**Goal**: Handle schema evolution where the table schema changes across commits.

### Implementation

- Resolve `InternalSchema` from commit metadata via `TableSchemaResolver.getTableInternalSchemaFromCommitMetadata()` (following the pattern at lines 129-159 of `HoodieHadoopFsRelationFactory.scala`)
- Pass `InternalSchema` to `HoodieFileGroupReader.Builder.withInternalSchema()`
- Set schema evolution configuration in the Hadoop conf (`SparkInternalSchemaConverter.HOODIE_TABLE_PATH`, `HOODIE_VALID_COMMITS_LIST`) following the pattern in `HoodieFileGroupReaderBasedFileFormat.setSchemaEvolutionConfigs()` (line 331-336)

### Files Modified
- `hudi-spark-datasource/hudi-spark-common/src/main/java/org/apache/hudi/spark/read/HoodieScan.java` — resolve InternalSchema on driver
- `hudi-spark-datasource/hudi-spark-common/src/main/java/org/apache/hudi/spark/read/HoodiePartitionReader.java` — pass InternalSchema to file group reader builder

### Tests
- Write data, evolve schema (add/rename column), write more data, read with DSv2, verify all columns appear

---

## PR 5: Columnar (Vectorized) Read for COW

**Goal**: Enable vectorized Parquet reads for pure COW file slices (no log files) to significantly improve read performance.

### Implementation

For COW file slices with no log files, bypass `HoodieFileGroupReader` entirely and use Spark's native vectorized Parquet reader (which returns `ColumnarBatch` instead of `InternalRow`).

- `HoodiePartitionReaderFactory.supportColumnarReads()` — return `true` for file slices with only base files (no log files)
- `HoodiePartitionReaderFactory.createColumnarReader()` — create a `HoodieColumnarPartitionReader` that uses `sparkAdapter.createParquetFileReader(true, ...)` with vectorized=true
- New class `HoodieColumnarPartitionReader implements PartitionReader<ColumnarBatch>` — reads base file directly via Spark Parquet vectorized reader

This follows the V1 pattern where `supportBatch` is true only when `!isMOR` (line 151 of `HoodieFileGroupReaderBasedFileFormat.scala`).

### Files Modified
- `hudi-spark-datasource/hudi-spark-common/src/main/java/org/apache/hudi/spark/read/HoodiePartitionReaderFactory.java` — implement `supportColumnarReads()` and `createColumnarReader()`
- `hudi-spark-datasource/hudi-spark-common/src/main/java/org/apache/hudi/spark/read/HoodieInputPartition.java` — add `hasLogFiles()` method
- New: `hudi-spark-datasource/hudi-spark-common/src/main/java/org/apache/hudi/spark/read/HoodieColumnarPartitionReader.java`

### Tests
- Read COW table, verify columnar batch output via `explain()`
- Compare results with row-based reads

---

## Dependency Graph

```
PR 1 (End-to-end read) ─────┬──→ PR 2 (Filter pushdown)
                             ├──→ PR 3 (Statistics)
                             ├──→ PR 4 (Schema evolution)
                             └──→ PR 5 (Columnar reads)
```

PRs 2-5 are independent of each other and can be developed in parallel after PR 1.

---

## Key Files Reference

| File | Role |
|------|------|
| `hudi-spark-datasource/hudi-spark-common/src/main/java/org/apache/hudi/spark/read/HoodieScan.java` | Driver-side scan planning — `toBatch()` is the most critical stub |
| `hudi-spark-datasource/hudi-spark-common/src/main/java/org/apache/hudi/spark/read/HoodiePartitionReader.java` | Executor-side reading — `initialize()` wires `HoodieFileGroupReader` |
| `hudi-spark-datasource/hudi-spark-common/src/main/java/org/apache/hudi/spark/read/HoodieScanBuilder.java` | Filter/column pushdown from Spark optimizer |
| `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/HoodieFileGroupReaderBasedFileFormat.scala` | V1 reference pattern (lines 252-307) — how `HoodieFileGroupReader` is built |
| `hudi-client/hudi-spark-client/src/main/scala/org/apache/hudi/SparkFileFormatInternalRowReaderContext.scala` | Spark reader context — constructor at line 61-66 |
| `hudi-common/src/main/java/org/apache/hudi/common/table/read/HoodieFileGroupReader.java` | Engine-agnostic file group reader — Builder at lines 363-527 |
| `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/HoodieFileIndex.scala` | File index — `filterFileSlices()` at lines 223-294 |
| `hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/common/SparkReaderContextFactory.java` | Reference for creating `SparkFileFormatInternalRowReaderContext` from Java |
