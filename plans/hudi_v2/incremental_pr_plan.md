# DSv2 Read Implementation — Incremental PR Plan

This document breaks the DSv2 read implementation (RFC-98) into a sequence of incremental PRs.
Each PR is self-contained, reviewable, and produces a working state.

---

## PR 1: Proof-of-Concept — DSv2 / DSv1 Coexistence (DataFrame API + SQL)

**Status: DONE** (implemented on branch `hudi-worktree-v2`)

**Goal:** Prove that DSv2 and DSv1 can coexist without interfering with each other across **both** activation paths: DataFrame API (`format("hudi_v2")`) and SQL queries (`hoodie.datasource.read.use.v2`). The DSv2 read path returns correct schema and empty results (stub). Writes fall back to the existing DSv1 pipeline in both modes.

**Branch:** `dsv2-read-01-coexistence-poc`

### New Files

All in `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/`:

1. **`HoodieDataSourceV2.scala`** — `TableProvider + DataSourceRegister`, `shortName() = "hudi_v2"`.
   Resolves table path from options, creates `HoodieSparkV2Table`.

2. **`HoodieSparkV2Table.scala`** — `Table + SupportsRead + SupportsWrite + V2TableWithV1Fallback`.
   - `capabilities()`: `BATCH_READ`, `V1_BATCH_WRITE`, `OVERWRITE_BY_FILTER`, `TRUNCATE`, `ACCEPT_ANY_SCHEMA`.
   - `newScanBuilder()`: returns `HoodieScanBuilder` (stub).
   - `newWriteBuilder()`: returns `HoodieV1WriteBuilder` (reused from `org.apache.spark.sql.hudi.catalog`).
   - `v1Table`: returns `CatalogTable` for V1 write fallback.
   - Lazy-initializes `HoodieTableMetaClient` and resolves table schema (same pattern as `HoodieInternalV2Table`).

3. **`HoodieScanBuilder.scala`** — `ScanBuilder + SupportsPushDownFilters + SupportsPushDownRequiredColumns`.
   Stub: accepts filters and column pruning, passes them to `HoodieBatchScan`.

4. **`HoodieBatchScan.scala`** — `Scan + Batch`.
   Stub: `planInputPartitions()` returns an empty array. `readSchema()` returns the pruned schema.
   `createReaderFactory()` returns `HoodiePartitionReaderFactory`.

5. **`HoodieInputPartition.scala`** — `InputPartition`. Serializable case class holding file slice metadata (placeholder fields for now).

6. **`HoodiePartitionReaderFactory.scala`** — `PartitionReaderFactory`. Creates `HoodiePartitionReader`.

7. **`HoodiePartitionReader.scala`** — `PartitionReader[InternalRow]`. Stub: immediately returns `false` from `next()`.

### New SPI Files

In `hudi-spark-datasource/hudi-spark-common/src/main/resources/META-INF/services/`:

1. **Append to** `org.apache.spark.sql.sources.DataSourceRegister`:
   ```
   org.apache.spark.sql.hudi.v2.HoodieDataSourceV2
   ```

2. **Create** `org.apache.spark.sql.connector.catalog.TableProvider`:
   ```
   org.apache.spark.sql.hudi.v2.HoodieDataSourceV2
   ```

### New Config

Add `hoodie.datasource.read.use.v2` (default `false`) to `DataSourceReadOptions` after line 300 (after `USE_PARTITION_VALUE_EXTRACTOR_ON_READ`, before the deprecated section). This config controls whether `HoodieCatalog.loadTable()` returns the DSv2 table for SQL queries.

### Modified Files

1. **`DataSourceOptions.scala`** — Add `USE_V2_READ` config property with key `hoodie.datasource.read.use.v2`.

2. **`HoodieCatalog.scala`** — Modify `loadTable()` (method at line 123) to check `hoodie.datasource.read.use.v2`:
   ```
   if (v2ReadEnabled) {
     // NEW: return HoodieSparkV2Table for DSv2 reads
     new HoodieSparkV2Table(spark, path, Some(catalogTable), Some(ident.toString))
   } else if (schemaEvolutionEnabled) {
     v2Table                    // existing: HoodieInternalV2Table
   } else {
     v2Table.v1TableWrapper     // existing: V1Table wrapper
   }
   ```
   Priority: `v2ReadEnabled` > `schemaEvolutionEnabled` > default V1.

3. **`HoodieSparkBaseAnalysis.scala`** — Add case arm to `HoodieV1OrV2Table` extractor (line 351) for `HoodieSparkV2Table`:
   ```scala
   case v2: HoodieSparkV2Table => v2.catalogTable
   ```
   This ensures DDL operations (DROP, ALTER, RENAME, SHOW PARTITIONS, TRUNCATE) recognize the new table type.

4. **`HoodieInternalV2Table.scala`** — Enhanced `HoodieV1WriteBuilder`:
   - Widened visibility from `private` to `private[hudi]` so that `HoodieSparkV2Table` can reuse it directly.
   - Added column alignment in `InsertableRelation.insert()`: renames and casts incoming DataFrame columns to match the table's user schema (handles generic names like `col1`, `col2` from SQL `VALUES` clauses and uncast types).
   - Changed from `data.write.format("org.apache.hudi").save()` to calling `HoodieSparkSqlWriter.write()` directly with `schemaFromCatalog` for proper schema reconciliation.

### New Test Files

In `hudi-spark-datasource/hudi-spark/src/test/scala/org/apache/spark/sql/hudi/v2/`:

1. **`TestDSv2CoexistenceWithDSv1.scala`** — Functional test (`@Tag("functional")`) that verifies:

   **DataFrame API tests:**
   - **Write via `format("hudi")`, read via `format("hudi")`** — works as before (baseline).
   - **Write via `format("hudi")`, read via `format("hudi_v2")`** — returns correct schema, empty result (stub).
   - **Write via `format("hudi_v2")`, read via `format("hudi")`** — write falls back to DSv1, read returns real data.
   - **Write via `format("hudi_v2")`, read via `format("hudi_v2")`** — write works (fallback), read returns stub.
   - Both formats resolve independently (no SPI conflicts).
   - `EXPLAIN` plan for `format("hudi_v2")` read shows `BatchScanExec` (not `FileSourceScanExec`).

   **SQL query tests (catalog path):**
   - Create a Hudi table via `CREATE TABLE ... USING hudi`.
   - Insert data via `INSERT INTO`.
   - **Config `false` (default):** `SELECT * FROM table` returns real data via DSv1 (`FileSourceScanExec` in plan).
   - **Config `true`:** `SELECT * FROM table` returns empty result via DSv2 stub (`BatchScanExec` in plan), with correct schema.
   - **Config `true`:** `INSERT INTO table` still works (DSv1 write fallback via `V2TableWithV1Fallback`).
   - **Config `true`:** After insert, switch config back to `false` and verify data is readable via DSv1.
   - **Config `true`:** DDL operations (`ALTER TABLE`, `DROP TABLE`) are unaffected.
   - Schema evolution path (`hoodie.schema.on.read.enable=true`) is unaffected by the new config.

### What This PR Does NOT Do

- Does not read any actual data (stub returns empty results).
- Does not touch any existing DSv1 read or write code paths (except `HoodieCatalog.loadTable()` branching).

### Verification Checklist

- [x] `spark.read.format("hudi").load(path)` works unchanged. (`testBaselineDSv1WriteAndRead`)
- [x] `spark.read.format("hudi_v2").load(path)` resolves without errors, returns empty DataFrame with correct schema. (`testDSv1WriteAndDSv2Read`, `testDSv2ReadSchemaAndPlan`)
- [x] `df.write.format("hudi_v2").mode("append").save(path)` falls back to DSv1 write pipeline. (`testDSv2WriteViaDataFrameAPI`)
- [x] `SELECT * FROM table` with config `false` uses DSv1, returns real data (no regression). (`testSqlConfigFalseUsesDSv1`)
- [x] `SELECT * FROM table` with config `true` uses DSv2 stub, returns empty result with correct schema. (`testSqlConfigTrueUsesDSv2Stub`)
- [x] `INSERT INTO table` works with config `true` (V1 write fallback). (`testSqlInsertWithV2ReadEnabled`)
- [x] DDL operations unaffected with config `true`. (`testDdlOperationsWithV2ReadEnabled`)
- [x] Schema evolution path unaffected. (`testSchemaEvolutionPathUnaffectedByV2Config`)
- [x] `EXPLAIN` shows `BatchScanExec` for DSv2 path, `FileSourceScanExec` for DSv1 path. (`testExplainShowsDSv2`)
- [x] Existing unit and functional tests pass with no changes.

### Bonus: Benchmark Infrastructure

Not originally planned, but added alongside PR1:
- `benchmarks/README.md` — benchmark documentation for COW/MOR, DSv1/DSv2 comparison
- `benchmarks/hudi_benchmark.scala` — comprehensive Spark shell benchmark script (DSv2 disabled by default since reads are still stubs)
- `hudi-spark-datasource/hudi-spark/src/test/scala/org/apache/spark/sql/execution/benchmark/CowTableReadBenchmark.scala` — COW vectorized reader micro-benchmark

---

## PR 2: CoW Snapshot Read — File Listing and Base File Reading

**Status: DONE**

**Goal:** `format("hudi_v2")` and `SELECT` with config `true` read actual data from CoW snapshot tables by wiring `HoodieBatchScan.planInputPartitions()` to `HoodieFileIndex` and implementing base file reading in `HoodiePartitionReader`.

**Branch:** `dsv2-read-02-cow-snapshot-read`

**Depends on:** PR 1 (DONE)

### Modified Files

1. **`HoodieBatchScan.scala`** — Implement `planInputPartitions()`:
   - Create `HoodieFileIndex` with `includeLogFiles = false` (CoW only).
   - Convert pushed filters (`Array[Filter]`) to Catalyst `Expression`s for partition/data filter separation.
   - Call `fileIndex.listFiles(partitionFilters, dataFilters)` to get `Seq[PartitionDirectory]`.
   - Map each file in each `PartitionDirectory` to a `HoodieInputPartition`.
   - Return `Array[InputPartition]`.

2. **`HoodieInputPartition.scala`** — Finalize fields:
   - `filePath: String` (base file path).
   - `partitionPath: String`.
   - `start: Long`, `length: Long` (for file split, initially `0` and file size).
   - `basePath: String`.
   - `dataSchema: StructType` (serialized required schema).
   - `partitionValues: InternalRow` (partition column values).
   - `options: Map[String, String]`.

3. **`HoodiePartitionReaderFactory.scala`** — Pass Hadoop config broadcast, required schema, and table options to reader.

4. **`HoodiePartitionReader.scala`** — Implement CoW base file reading:
   - Use Spark's `ParquetFileFormat` (or appropriate file format based on table config) to build a reader iterator.
   - Alternatively, reuse the vectorized Parquet reader used by `HoodieBaseRelation` / `HadoopFsRelation`.
   - Read the base Parquet file for the given partition, applying the required schema projection.
   - Return `InternalRow` records via `next()` / `get()`.

### New/Modified Test Files

1. **`TestDSv2CowSnapshotRead.scala`** — Functional test:
   - Create a CoW table via `format("hudi")` write (INSERT).
   - Read via `format("hudi_v2")` and verify row count and content matches `format("hudi")` read.
   - Read via SQL `SELECT` with `hoodie.datasource.read.use.v2=true` and verify same results.
   - Test with partitioned and non-partitioned tables.
   - Test with multiple commits (latest snapshot correctness).
   - Verify column pruning: read only a subset of columns, confirm schema.

### Verification Checklist

- [x] CoW table reads via `format("hudi_v2")` return correct data matching `format("hudi")`.
- [x] CoW table reads via SQL `SELECT` with config `true` return correct data matching config `false`.
- [x] Partitioned CoW tables return correct data with partition values.
- [x] Column pruning works (only requested columns are read).
- [x] Multiple commits: only latest snapshot is returned.

---

## PR 3: CoW Snapshot Read — Filter Pushdown

**Status: DONE**

**Goal:** Partition pruning and data filters are correctly pushed down through `HoodieScanBuilder` and applied via `HoodieFileIndex` data skipping.

**Branch:** `dsv2-read-03-cow-filter-pushdown`

**Depends on:** PR 2 (DONE)

### Modified Files

1. **`HoodieScanBuilder.scala`** — Implement `pushFilters()`:
   - Separate partition filters from data filters.
   - Accept filters that `HoodieFileIndex` can handle (equality, comparison on partition columns; filters eligible for data skipping).
   - Return residual filters for Spark to evaluate post-scan.

2. **`HoodieBatchScan.scala`** — Use pushed filters in `planInputPartitions()`:
   - Pass partition filters to `HoodieFileIndex.listFiles()` for partition pruning.
   - Pass data filters for column stats / bloom filter data skipping.

### New/Modified Test Files

1. **`TestDSv2FilterPushdown.scala`** — Functional test:
   - Create partitioned CoW table with multiple partitions.
   - Read with partition filter via `format("hudi_v2")` — verify only matching partitions are scanned.
   - Read with partition filter via SQL `SELECT ... WHERE partition_col = ...` with config `true`.
   - Read with data filter — verify data skipping reduces file count (when metadata table indices are present).
   - Verify `EXPLAIN` plan shows pushed filters in `BatchScanExec`.
   - Compare filter pushdown behavior with `format("hudi")` for correctness.

### Verification Checklist

- [x] Partition pruning reduces the number of files scanned.
- [x] Data skipping via column stats works when metadata table is enabled.
- [x] Residual filters are correctly returned to Spark.
- [x] `EXPLAIN` shows pushed filters.
- [x] SQL `SELECT ... WHERE` with config `true` applies partition pruning correctly.

---

## PR 4: MoR Snapshot Read — Base + Log File Merging

**Status: DONE**

**Goal:** `format("hudi_v2")` and SQL with config `true` read MoR tables by merging base files with log files.

**Branch:** `dsv2-read-04-mor-snapshot-read`

**Depends on:** PR 3 (DONE)

### Modified Files

1. **`HoodieBatchScan.scala`** — Set `includeLogFiles = true` when `tableType == MERGE_ON_READ`.
   Update `planInputPartitions()` to include log file paths in `HoodieInputPartition`.

2. **`HoodieInputPartition.scala`** — Add log file paths:
   - `logFilePaths: Seq[String]` (list of log files in the file slice).
   - `tableType: HoodieTableType` (to distinguish CoW vs MoR on executor side).

3. **`HoodiePartitionReader.scala`** — Implement MoR merge:
   - When `tableType == MERGE_ON_READ` and log files are present:
     - Reuse `HoodieFileGroupReader` (or `HoodieMergeOnReadRDDV2` internals) to merge base + log files.
     - This is the same merge logic used by `MergeOnReadSnapshotRelation` in DSv1.
   - When no log files: fall through to CoW base file reader (existing).

### New/Modified Test Files

1. **`TestDSv2MorSnapshotRead.scala`** — Functional test:
   - Create MoR table, write initial batch, then update some records (creates log files).
   - Read via `format("hudi_v2")` and verify merged data matches `format("hudi")` read.
   - Read via SQL `SELECT` with config `true` and verify same results.
   - Test with compacted file slices (base only) and uncompacted (base + logs).
   - Test with log-only file groups (no base file, only log files).
   - Test with deletes in log files.

### Verification Checklist

- [x] MoR tables with only base files return correct data.
- [x] MoR tables with base + log files return correctly merged data.
- [x] MoR tables with log-only file groups return correct data.
- [x] Deletes in log files are properly applied.
- [x] Results match `format("hudi")` / SQL with config `false` for all cases.

---

## PR 5: Incremental and CDC Queries

**Status: DONE**

**Goal:** Support incremental queries (`hoodie.datasource.query.type=incremental`) and CDC queries through the DSv2 path.

**Branch:** `dsv2-read-05-incremental-cdc`

**Depends on:** PR 4 (DONE)

### Modified Files

1. **`HoodieScanBuilder.scala`** — Check `options` for `hoodie.datasource.query.type`:
   - `snapshot` (default): existing snapshot scan logic.
   - `incremental`: build an incremental scan with begin/end instant times.
   - `read_optimized`: CoW-like scan (base files only) even for MoR tables.

2. **`HoodieBatchScan.scala`** — Support incremental file listing:
   - For incremental queries, use `HoodieFileIndex` with incremental timeline filtering or delegate to the incremental relation factory logic from `DefaultSource.createRelation()`.
   - For CDC queries, resolve CDC file slices.

3. **`HoodiePartitionReader.scala`** — Handle incremental/CDC record iteration:
   - For incremental: filter records by commit time range, or reuse `IncrementalRelation` internals.
   - For CDC: use CDC log reader.

### New/Modified Test Files

1. **`TestDSv2IncrementalRead.scala`** — Functional test:
   - Create table, write multiple batches.
   - Read incrementally via `format("hudi_v2")` with begin/end instant times.
   - Verify only records from the specified range are returned.
   - Compare with `format("hudi")` incremental read.
   - Test both CoW and MoR incremental reads.

2. **`TestDSv2CDCRead.scala`** — Functional test:
   - Enable CDC on table, write inserts + updates + deletes.
   - Read CDC via `format("hudi_v2")` and verify change records.

### Verification Checklist

- [ ] Incremental CoW read returns correct records for commit range.
- [ ] Incremental MoR read returns correct records for commit range.
- [ ] CDC read returns correct change records (insert/update/delete markers).
- [ ] Results match `format("hudi")` for all query types.

---

## PR 6: Advanced Pushdowns — Aggregates, Limit, TopN

**Status: DONE**

**Goal:** Leverage DSv2 pushdown interfaces for performance optimizations not possible with DSv1.

**Branch:** `dsv2-read-06-advanced-pushdowns`

**Depends on:** PR 5 (DONE)

### Modified Files

1. **`HoodieScanBuilder.scala`** — Implement additional pushdown interfaces:
   - `SupportsPushDownAggregates`: push `COUNT(*)`, `MIN`, `MAX` to metadata table.
   - `SupportsPushDownLimit`: limit the number of rows read.
   - `SupportsPushDownTopN`: combine ORDER BY + LIMIT for early termination.

2. **`HoodieBatchScan.scala`** — Respect limit in `planInputPartitions()` (stop planning after enough partitions to satisfy limit).

### New/Modified Test Files

1. **`TestDSv2AdvancedPushdowns.scala`** — Functional test:
   - `SELECT COUNT(*) FROM table` — verify aggregate pushdown in plan.
   - `SELECT * FROM table LIMIT 10` — verify limit pushdown.
   - `SELECT * FROM table ORDER BY col LIMIT 10` — verify TopN pushdown.
   - Verify correctness of pushed-down results.

### Verification Checklist

- [ ] `COUNT(*)` is answered from metadata without scanning files (when metadata table has record count).
- [ ] `LIMIT` reduces the number of partitions/rows read.
- [ ] Results are correct for all pushdown scenarios.

---

## Summary — PR Dependency Graph

```
PR 1: Coexistence POC                    ✅ DONE
      (DSv2 stub + DSv1 write fallback + catalog config + HoodieCatalog.loadTable)
  │
  v
PR 2: CoW Snapshot Read                  ✅ DONE
      (HoodieFileIndex wiring + base file reading)
  │
  v
PR 3: Filter Pushdown                    ✅ DONE
      (partition pruning + data skipping)
  │
  v
PR 4: MoR Snapshot Read                  ✅ DONE
      (base + log file merging)
  │
  v
PR 5: Incremental & CDC Queries          ✅ DONE
      (query type routing + incremental/CDC readers)
  │
  v
PR 6: Advanced Pushdowns                 ✅ DONE
      (aggregates, limit, TopN)
```

## PR Sizing Estimates

| PR | New Files | Modified Files | New Test Files | Scope |
|----|-----------|----------------|----------------|-------|
| 1  | 7 + 2 SPI | 4 (`DataSourceOptions`, `HoodieCatalog`, `HoodieSparkBaseAnalysis`, `HoodieInternalV2Table`) | 1 | Medium |
| 2  | 0 | 4 | 1 | Medium |
| 3  | 0 | 2 | 1 | Small |
| 4  | 0 | 3 | 1 | Medium |
| 5  | 0 | 3 | 2 | Medium-Large |
| 6  | 0 | 2 | 1 | Medium |

## Key Design Decisions

1. **Catalog integration in PR 1**: The `hoodie.datasource.read.use.v2` config and `HoodieCatalog.loadTable()` modification are included in PR 1 to prove the full activation path (both DataFrame API and SQL) from the start. Since PR 1 returns stub (empty) results, this is safe — the config defaults to `false`, and enabling it only swaps the read path without affecting writes or DDL.

2. **Reuse `HoodieV1WriteBuilder` from `HoodieInternalV2Table`**: Instead of extracting a separate class, widen the visibility of the existing `HoodieV1WriteBuilder` from `private` to `private[hudi]`. This avoids code duplication and keeps a single write builder implementation shared between `HoodieInternalV2Table` and `HoodieSparkV2Table`.

3. **File reading in `HoodiePartitionReader`**: Reuse Spark's built-in Parquet/ORC readers (same as `HadoopFsRelation` uses) rather than reimplementing. For MoR, reuse `HoodieFileGroupReader` which is the existing merge implementation.

4. **Filter conversion**: DSv2 `Filter` objects need conversion to Catalyst `Expression` objects for `HoodieFileIndex`. Use existing `HoodieCatalystExpressionUtils.convertToCatalystExpression()` utility.

5. **`HoodieFileIndex` reuse**: The same `HoodieFileIndex` class is used by both DSv1 and DSv2 paths. No modifications to `HoodieFileIndex` are needed — it already provides the `listFiles()` and `filterFileSlices()` APIs that `HoodieBatchScan` needs.

6. **Parameter-based activation vs short name**: Both `format("hudi_v2")` (DataFrame API) and `hoodie.datasource.read.use.v2` (SQL/catalog) are supported. The short name approach is primary for DataFrame; the config approach is for SQL queries where users don't control the format string.

7. **Config priority in `HoodieCatalog.loadTable()`**: `v2ReadEnabled` > `schemaEvolutionEnabled` > default V1. This means DSv2 read takes precedence when explicitly enabled, schema evolution falls back to the existing `HoodieInternalV2Table`, and the default remains unchanged.
