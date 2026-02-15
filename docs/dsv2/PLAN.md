# DSv2 Read Support - Incremental PR Plan

## Context

Commit `fb398211d2cb` introduced a minimal DSv2 read skeleton for Hudi's Spark integration with 6 new Java classes and modifications to `HoodieInternalV2Table` and `HoodieCatalog`. The skeleton establishes the full Spark DSv2 interface chain (`ScanBuilder` → `Scan` → `Batch` → `InputPartition` → `PartitionReaderFactory` → `PartitionReader<InternalRow>`).

All changes are gated behind `hoodie.datasource.read.use.dsv2=true` (default `false`), keeping the existing V1 read path unaffected. The reference architecture is the Iceberg DSv2 read blueprint at `docs/claude/iceberg/ICEBERG_SPARK_DSV2_READ_BLUEPRINT.md`.

### Completed Work

| Commit | PR | Status |
|--------|----|--------|
| `bfa0f7febce5` | PR 1 — Wire end-to-end DSv2 read path | Done |
| `f9d913ee7b7a` | PR 2 — Filter classification and partition pruning | Done |
| `a0ce18d90e23` | PR 5 — Columnar (vectorized) Parquet reader for COW | Done |

### Remaining from original plan

- PR 3 — Statistics reporting for CBO
- PR 4 — Schema evolution support

---

## The DSv2 Read + DSv1 Write Coexistence Problem

### Problem Statement

Commit `0d6289c84387` enabled `TableProvider` on `BaseDefaultSource`. This is required for DSv2 API to be called at all. However, there are **two blockers** preventing DSv2 reads from actually working while keeping DSv1 writes safe.

### Blocker 1: V2→V1 Fallback Rule Unconditionally Kills DSv2 Reads

`HoodieSpark35DataSourceV2ToV1Fallback` (and equivalents for Spark 3.3, 3.4, 4.0) is **always registered** as an analysis rule in `HoodieAnalysis.customResolutionRules()`. It unconditionally converts **every** `DataSourceV2Relation(HoodieInternalV2Table)` back to a V1 `LogicalRelation`:

```scala
// HoodieSpark35Analysis.scala:50-63
override def apply(plan: LogicalPlan): LogicalPlan = plan match {
  case _: AlterTableCommand => plan
  case iis@InsertIntoStatement(rv2@DataSourceV2Relation(v2Table: HoodieInternalV2Table, ...), ...) =>
    iis.copy(table = convertToV1(rv2, v2Table))     // writes → always V1
  case _ =>
    plan.resolveOperatorsDown {
      case rv2@DataSourceV2Relation(v2Table: HoodieInternalV2Table, ...) =>
        convertToV1(rv2, v2Table)                     // reads → ALSO forced to V1!
    }
}
```

**Impact**: Even when `HoodieCatalog.loadTable()` returns `HoodieInternalV2Table` (when `dsv2ReadEnabled=true`), this analysis rule immediately converts the DSv2 read plan back to V1. The entire DSv2 read implementation is dead code at runtime.

### Blocker 2: `TableProvider` on `BaseDefaultSource` Breaks Format-Path Writes

There are two access paths to Hudi tables in Spark:

| Access Path | Entry Point | Example |
|-------------|-------------|---------|
| **Catalog path** | `HoodieCatalog.loadTable()` | `SELECT * FROM table`, `INSERT INTO table` |
| **Format path** | `BaseDefaultSource` (via `DataSourceRegister`) | `spark.read.format("hudi").load(path)`, `df.write.format("hudi").save(path)` |

When `BaseDefaultSource` implements `TableProvider`:
1. `df.write.format("hudi").save(path)` → Spark calls `TableProvider.getTable()` → `HoodieInternalV2Table`
2. `HoodieInternalV2Table` reports `V1_BATCH_WRITE` capability → Spark calls `newWriteBuilder()`
3. `HoodieV1WriteBuilder` accesses `hoodieCatalogTable` → tries to build `HoodieTableMetaClient`
4. **For new tables**: `HoodieTableMetaClient` construction **fails** because no table exists at the path yet
5. **For path-based tables not in catalog**: `HoodieCatalogTable(spark, TableIdentifier(tableName))` **fails** because the table isn't registered in the session catalog

The DSv1 write path (`DefaultSource.createRelation()`) handles both cases correctly because `HoodieSparkSqlWriter.write()` bootstraps table metadata as part of the write operation.

### Two Access Paths, Different Solutions

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Spark User Query                                │
│   spark.sql("SELECT * FROM t")    spark.read.format("hudi").load(p)   │
│              ↓                                    ↓                    │
│     ┌────────────────┐                  ┌──────────────────┐           │
│     │ HoodieCatalog  │                  │ BaseDefaultSource│           │
│     │  .loadTable()  │                  │  (TableProvider) │           │
│     └───────┬────────┘                  └────────┬─────────┘           │
│             ↓                                    ↓                     │
│   ┌─────────────────────┐               ┌──────────────────┐          │
│   │ dsv2ReadEnabled?    │               │  getTable()      │          │
│   │ YES → V2Table       │               │  → V2Table       │          │
│   │ NO  → V1Table wrap  │               └────────┬─────────┘          │
│   └─────────┬───────────┘                        ↓                    │
│             ↓                           ┌──────────────────────┐       │
│   ┌─────────────────────────┐           │ V2→V1 fallback rule  │       │
│   │ V2→V1 fallback rule     │           │ READ → keep V2       │       │
│   │ READ → keep V2          │           │ WRITE → convert V1   │       │
│   │ WRITE → convert to V1   │           └──────────────────────┘       │
│   └─────────────────────────┘                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## PR 6: Make V2→V1 Fallback Rule Read-Aware (Prerequisite for All DSv2 Reads)

**Goal**: Update `HoodieSpark*DataSourceV2ToV1Fallback` rules so that DSv2 reads work when enabled, while writes always fall back to V1.

### Problem

The current rule treats reads and writes identically — it converts ALL `DataSourceV2Relation(HoodieInternalV2Table)` nodes to V1 `LogicalRelation`. This means the DSv2 read path (HoodieScan, HoodieBatch, HoodiePartitionReader) never executes.

### Implementation

Update `HoodieSpark35DataSourceV2ToV1Fallback` (and equivalents for 3.3, 3.4, 4.0) to:

1. **Writes (`InsertIntoStatement`)**: Always convert to V1 (unchanged behavior)
2. **Reads**: Check `hoodie.datasource.read.use.dsv2` config. If enabled, **skip fallback** so the V2 read path executes. If disabled, fall back to V1 (unchanged default behavior).

```scala
case class HoodieSpark35DataSourceV2ToV1Fallback(sparkSession: SparkSession)
  extends Rule[LogicalPlan] with ProvidesHoodieConfig {

  private lazy val dsv2ReadEnabled: Boolean =
    sparkSession.conf.getOption("hoodie.datasource.read.use.dsv2")
      .exists(_.toBoolean)

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case _: AlterTableCommand => plan

    // Writes: ALWAYS fall back to V1 (preserves DSv1 write performance)
    case iis@InsertIntoStatement(
        rv2@DataSourceV2Relation(v2Table: HoodieInternalV2Table, _, _, _, _),
        _, _, _, _, _, _) =>
      iis.copy(table = convertToV1(rv2, v2Table))

    case _ =>
      plan.resolveOperatorsDown {
        case rv2@DataSourceV2Relation(v2Table: HoodieInternalV2Table, _, _, _, _) =>
          if (dsv2ReadEnabled) {
            rv2  // Keep V2 relation — DSv2 read path will execute
          } else {
            convertToV1(rv2, v2Table)  // Fall back to V1 (default)
          }
      }
  }
  // convertToV1 unchanged
}
```

### Files Modified

- `hudi-spark-datasource/hudi-spark3.5.x/src/main/scala/org/apache/spark/sql/hudi/analysis/HoodieSpark35Analysis.scala`
- `hudi-spark-datasource/hudi-spark3.4.x/src/main/scala/org/apache/spark/sql/hudi/analysis/HoodieSpark34Analysis.scala`
- `hudi-spark-datasource/hudi-spark3.3.x/src/main/scala/org/apache/spark/sql/hudi/analysis/HoodieSpark33Analysis.scala`
- `hudi-spark-datasource/hudi-spark4.0.x/src/main/scala/org/apache/spark/sql/hudi/analysis/HoodieSpark40Analysis.scala`

### Tests

- **Catalog-path DSv2 read**: Set `hoodie.datasource.read.use.dsv2=true`, run `spark.sql("SELECT * FROM hudi_table")`, verify the physical plan uses `BatchScanExec` (V2) instead of `FileSourceScanExec` (V1)
- **Catalog-path V1 read (default)**: With default config, verify `FileSourceScanExec` is still used
- **Catalog-path write**: Run `INSERT INTO hudi_table ...`, verify it uses V1 write path regardless of DSv2 read config

### Verification
```bash
mvn test -Punit-tests -pl hudi-spark-datasource/hudi-spark -Dtest=TestHoodieDSv2Read
```

### Note

After this PR, DSv2 reads work **through the catalog path only** (SQL table names). This is sufficient for most production use cases. Format-path DSv2 reads require PR 7.

---

## PR 7: Enable `TableProvider` on `BaseDefaultSource` for Format-Path DSv2 Reads

**Goal**: Make DSv2 reads work for `spark.read.format("hudi").load(path)` while keeping all writes on DSv1.

### Why This Is Hard

When `BaseDefaultSource` implements `TableProvider`, Spark routes **all** format-path operations (reads AND writes) through `getTable()` → `HoodieInternalV2Table`. The V1Write bridge (`HoodieV1WriteBuilder`) requires `HoodieCatalogTable` which:
- Needs `HoodieTableMetaClient` (fails for new tables — no metadata on disk)
- Needs the table in the session catalog (fails for path-based writes not registered in catalog)

### Approach: Lightweight V2 Table for Format Path

Create a new `HoodieFormatPathV2Table` specifically for format-path access. Unlike `HoodieInternalV2Table` (which requires full catalog metadata), this table:

1. **Only supports reads** via DSv2 (`SupportsRead` + `BATCH_READ`)
2. **Reports `V1_BATCH_WRITE`** capability with a simplified `V1Write` that passes raw user options through to `DefaultSource.createRelation()` without needing `HoodieCatalogTable`

```scala
class HoodieFormatPathV2Table(spark: SparkSession, path: String,
                               userOptions: java.util.Map[String, String])
  extends Table with SupportsRead with SupportsWrite {

  // Lazy — only evaluated if a read actually happens
  private lazy val metaClient = HoodieTableMetaClient.builder()
    .setBasePath(path)
    .setConf(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf))
    .build()

  // Schema resolution for reads (only called when reading existing tables)
  override def schema(): StructType = {
    val resolver = new TableSchemaResolver(metaClient)
    SparkInternalSchemaConverter.constructSparkSchemaFromTableSchema(resolver.getTableAvroSchema)
  }

  override def capabilities(): util.Set[TableCapability] = Set(
    BATCH_READ, V1_BATCH_WRITE, OVERWRITE_BY_FILTER, TRUNCATE, ACCEPT_ANY_SCHEMA
  ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val partitionColumns = metaClient.getTableConfig.getPartitionFields
      .orElse(Array.empty[String])
    new HoodieScanBuilder(spark, path, schema(), userOptions, partitionColumns)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    // Simplified: pass raw user options through to V1 write path
    new HoodieFormatPathV1WriteBuilder(info.options, userOptions, spark)
  }
}
```

### Simplified V1Write for Format Path

```scala
private class HoodieFormatPathV1WriteBuilder(writeOptions: CaseInsensitiveStringMap,
                                              originalUserOptions: java.util.Map[String, String],
                                              spark: SparkSession)
  extends SupportsTruncate with SupportsOverwrite {

  private var overwrite = false

  override def truncate(): WriteBuilder = { overwrite = true; this }
  override def overwrite(filters: Array[Filter]): WriteBuilder = { overwrite = true; this }

  override def build(): V1Write = new V1Write {
    override def toInsertableRelation: InsertableRelation = {
      new InsertableRelation {
        override def insert(data: DataFrame, overwriteFlag: Boolean): Unit = {
          val mode = if (overwrite || overwriteFlag) SaveMode.Overwrite else SaveMode.Append
          // Pass raw user options — no HoodieCatalogTable needed
          // "org.apache.hudi" resolves to DefaultSource (no TableProvider),
          // so this goes through pure V1 CreatableRelationProvider.createRelation()
          data.write.format("org.apache.hudi")
            .mode(mode)
            .options(originalUserOptions)
            .save()
        }
      }
    }
  }
}
```

Key design choices:
- Uses `format("org.apache.hudi")` which resolves to `org.apache.hudi.DefaultSource` — this class does NOT implement `TableProvider`, so the recursive call goes through pure V1 `CreatableRelationProvider.createRelation()` with no V2 overhead
- Passes raw user options (path, write config, etc.) directly — no `HoodieCatalogTable` derivation needed
- Works for both new and existing tables because the actual table creation happens inside `HoodieSparkSqlWriter.write()`

### Update BaseDefaultSource

```scala
class BaseDefaultSource extends DefaultSource with DataSourceRegister with TableProvider {

  override def shortName(): String = "hudi"

  def inferSchema: StructType = new StructType()
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = inferSchema

  override def getTable(schema: StructType,
                        partitioning: Array[Transform],
                        properties: java.util.Map[String, String]): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    val path = options.get("path")
    if (path == null)
      throw new HoodieException("'path' cannot be null")

    // Use lightweight table for format-path access
    new HoodieFormatPathV2Table(SparkSession.active, path, properties)
  }
}
```

### Update V2→V1 Fallback Rule

The fallback rule from PR 6 handles `HoodieInternalV2Table` (catalog path). Extend it to also handle `HoodieFormatPathV2Table`:

```scala
case _ =>
  plan.resolveOperatorsDown {
    // Catalog path V2 tables
    case rv2@DataSourceV2Relation(v2Table: HoodieInternalV2Table, _, _, _, _) =>
      if (dsv2ReadEnabled) rv2 else convertToV1(rv2, v2Table)
    // Format path V2 tables
    case rv2@DataSourceV2Relation(v2Table: HoodieFormatPathV2Table, _, _, _, _) =>
      if (dsv2ReadEnabled) rv2 else convertFormatPathToV1(rv2)
  }
```

For the format-path V1 fallback (`convertFormatPathToV1`), create a V1 relation using `DefaultSource.createRelation()` with the original user options — the same as what Spark would do without `TableProvider`.

### Files Modified

- `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/BaseDefaultSource.scala` — use `HoodieFormatPathV2Table`
- New: `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/catalog/HoodieFormatPathV2Table.scala`
- `hudi-spark-datasource/hudi-spark3.5.x/src/main/scala/org/apache/spark/sql/hudi/analysis/HoodieSpark35Analysis.scala` — handle format-path V2 table
- Same for Spark 3.3, 3.4, 4.0 analysis files

### Tests

- **Format-path DSv2 read**: `spark.read.format("hudi").option("hoodie.datasource.read.use.dsv2", "true").load(path)` → verify `BatchScanExec` in physical plan
- **Format-path V1 read (default)**: Without DSv2 config → verify `FileSourceScanExec`
- **Format-path write (new table)**: `df.write.format("hudi").option(...).save(newPath)` → verify table is created correctly
- **Format-path write (existing table)**: `df.write.format("hudi").mode("append").save(existingPath)` → verify append works
- **Mixed read-write**: Write via format path, then read via DSv2 format path, verify consistency

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

## Dependency Graph

```
PR 6 (Read-aware fallback rule) ──→ PR 7 (Format-path TableProvider)
           │
           ├──→ PR 3 (Statistics)
           └──→ PR 4 (Schema evolution)
```

PR 6 is the prerequisite for everything — without it, DSv2 reads never execute.
PR 7 depends on PR 6 (extends the fallback rule).
PRs 3, 4 are independent of PR 7 (they work with catalog-path reads enabled by PR 6).

---

## Key Files Reference

| File | Role |
|------|------|
| `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/BaseDefaultSource.scala` | Format-path entry point — `TableProvider` implementation |
| `hudi-spark-datasource/hudi-spark3.5.x/src/main/scala/org/apache/spark/sql/hudi/analysis/HoodieSpark35Analysis.scala` | V2→V1 fallback rule — **must be updated for DSv2 reads to work** |
| `hudi-spark-datasource/hudi-spark/src/main/scala/org/apache/spark/sql/hudi/analysis/HoodieAnalysis.scala` | Rule registration — `customResolutionRules()` always adds V2→V1 fallback |
| `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/catalog/HoodieCatalog.scala` | Catalog-path entry — `loadTable()` gates V1/V2 based on config |
| `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/catalog/HoodieInternalV2Table.scala` | V2 table (catalog path) — `V1_BATCH_WRITE` + `SupportsRead` + `SupportsWrite` |
| `hudi-spark-datasource/hudi-spark-common/src/main/java/org/apache/hudi/spark/read/HoodieScan.java` | Driver-side scan planning — `toBatch()` |
| `hudi-spark-datasource/hudi-spark-common/src/main/java/org/apache/hudi/spark/read/HoodiePartitionReader.java` | Executor-side reading — `initialize()` wires `HoodieFileGroupReader` |
| `hudi-spark-datasource/hudi-spark-common/src/main/java/org/apache/hudi/spark/read/HoodieScanBuilder.java` | Filter/column pushdown from Spark optimizer |
| `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/DefaultSource.scala` | DSv1 entry point — `RelationProvider` + `CreatableRelationProvider` (no `TableProvider`) |
