# PR1: DSv2/DSv1 Coexistence Proof-of-Concept

## Context

Apache Hudi currently uses only the Spark DataSource V1 (DSv1) read path. RFC-98 plans to add DSv2 read support for advanced pushdown optimizations. This PR is the first step: proving DSv2 and DSv1 can coexist without interfering. The DSv2 read stub returns correct schema but empty results. Writes always fall back to DSv1.

Two activation paths:
- **DataFrame API**: `spark.read.format("hudi_v2").load(path)` → DSv2 stub read
- **SQL/Catalog**: `hoodie.datasource.read.use.v2=true` → `HoodieCatalog.loadTable()` returns DSv2 table

## New Config

Add `hoodie.datasource.read.use.v2` (default `false`) in `DataSourceReadOptions` after line 300 (after `USE_PARTITION_VALUE_EXTRACTOR_ON_READ`, before the deprecated section):

**File**: `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/DataSourceOptions.scala`

```scala
val USE_V2_READ: ConfigProperty[String] = ConfigProperty
  .key("hoodie.datasource.read.use.v2")
  .defaultValue("false")
  .markAdvanced()
  .sinceVersion("1.3.0")
  .withDocumentation("When enabled, SQL/catalog queries use the DSv2 read path (HoodieSparkV2Table) " +
    "instead of the default DSv1 path. The DataFrame API can also use format(\"hudi_v2\") directly.")
```

## New Source Files (7 files)

All in `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/v2/`:

| # | File | Interfaces | Purpose |
|---|------|-----------|---------|
| 1 | `HoodieDataSourceV2.scala` | `TableProvider + DataSourceRegister + CreatableRelationProvider` | `shortName()="hudi_v2"`, resolves path → `HoodieSparkV2Table`. `CreatableRelationProvider` enables DataFrame API writes via `df.write.format("hudi_v2")` |
| 2 | `HoodieSparkV2Table.scala` | `Table + SupportsRead + SupportsWrite + V2TableWithV1Fallback` | Core V2 table. `newScanBuilder()` → stub. `newWriteBuilder()` returns `HoodieV1WriteBuilder` (reused from `org.apache.spark.sql.hudi.catalog`). Schema from `HoodieCatalogTable` or `TableSchemaResolver` |
| 3 | `HoodieScanBuilder.scala` | `ScanBuilder + SupportsPushDownFilters + SupportsPushDownRequiredColumns` | Stub: accepts column pruning, returns all filters as post-scan |
| 4 | `HoodieBatchScan.scala` | `Scan + Batch` | Stub: `planInputPartitions()` → empty array, `readSchema()` → pruned schema |
| 5 | `HoodieInputPartition.scala` | `InputPartition` | Placeholder case class (not used in PR1) |
| 6 | `HoodiePartitionReaderFactory.scala` | `PartitionReaderFactory` | Stub factory creating `HoodiePartitionReader` |
| 7 | `HoodiePartitionReader.scala` | `PartitionReader[InternalRow]` | Stub: `next()` → `false` |

Reuse patterns from `HoodieInternalV2Table.scala` for schema resolution and `ProvidesHoodieConfig` for write config building. Use `HoodieSchemaConversionUtils.convertHoodieSchemaToStructType()` for Avro→StructType.

## SPI Files

**Modify** `hudi-spark-datasource/hudi-spark-common/src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`:
- Append: `org.apache.spark.sql.hudi.v2.HoodieDataSourceV2`

**Create** `hudi-spark-datasource/hudi-spark-common/src/main/resources/META-INF/services/org.apache.spark.sql.connector.catalog.TableProvider`:
- Content: `org.apache.spark.sql.hudi.v2.HoodieDataSourceV2`

## Modified Existing Files (4 files)

### 1. `HoodieCatalog.scala` — `loadTable()` (method starts at line 123, branching at line 149)

**File**: `.../org/apache/spark/sql/hudi/catalog/HoodieCatalog.scala`

After `v2Table` is created (line 138) and before the existing `schemaEvolutionEnabled` check (line 140), add `v2ReadEnabled` check. If true, return `HoodieSparkV2Table(...)` instead. Priority: `v2ReadEnabled` > `schemaEvolutionEnabled` > default V1.

### 2. `HoodieSparkBaseAnalysis.scala` — `HoodieV1OrV2Table` extractor (line 351)

**File**: `.../org/apache/spark/sql/hudi/analysis/HoodieSparkBaseAnalysis.scala`

Add case arm: `case v2: HoodieSparkV2Table => v2.catalogTable` so DDL operations (DROP, ALTER, RENAME, SHOW PARTITIONS, TRUNCATE) recognize the new table type.

### 3. `DataSourceOptions.scala` — add config (after line 300)

**File**: `.../org/apache/hudi/DataSourceOptions.scala`

### 4. `HoodieInternalV2Table.scala` — `HoodieV1WriteBuilder` enhanced

**File**: `.../org/apache/spark/sql/hudi/catalog/HoodieInternalV2Table.scala`

- Changed `HoodieV1WriteBuilder` visibility from `private` to `private[hudi]` so it can be reused by `HoodieSparkV2Table` in the `v2` package.
- Added column alignment in `InsertableRelation.insert()`: renames and casts incoming DataFrame columns to match the table's user schema. This handles generic column names (e.g., `col1`, `col2` from SQL `VALUES` clauses) and uncast types (e.g., `DECIMAL` literals for `DOUBLE` columns).
- Changed from `data.write.format("org.apache.hudi").save()` to calling `HoodieSparkSqlWriter.write()` directly with `schemaFromCatalog` for proper schema reconciliation with the full table schema (including meta-fields). The config map must include both `buildHoodieConfig()` (Hive sync, key generator, ordering fields) and `buildHoodieInsertConfig()` to preserve existing behavior.

## Test File

**File**: `hudi-spark-datasource/hudi-spark/src/test/scala/org/apache/spark/sql/hudi/v2/TestDSv2CoexistenceWithDSv1.scala`

Extends `SparkClientFunctionalTestHarness` (Java, JUnit 5 + `@Tag("functional")`). Override `conf` with `getSparkSqlConf` for HoodieCatalog. Uses `val _spark = spark; import _spark.implicits._` workaround since `spark()` is a method (not a stable identifier).

### Test Cases

**DataFrame API:**
1. `testBaselineDSv1WriteAndRead` — `format("hudi")` write+read → `FileSourceScanExec`, 3 rows
2. `testDSv1WriteAndDSv2Read` — `format("hudi")` write, `format("hudi_v2")` read → `BatchScanExec`, correct schema, 0 rows
3. `testDSv1WriteAndDSv1ReadAfterDSv2Read` — `format("hudi")` write, `format("hudi_v2")` read (DSv2 stub), then `format("hudi")` read → DSv1 still works correctly after DSv2 read, `FileSourceScanExec`, 3 rows
4. `testDSv2ReadSchemaAndPlan` — `format("hudi")` write, `format("hudi_v2")` read → validates schema matches (column names + types), `BatchScanExec` in plan, 0 rows (stub)
5. `testDSv2WriteViaDataFrameAPI` — `format("hudi_v2")` write falls back to DSv1, then `format("hudi")` read returns real data

**SQL/Catalog:**
6. `testSqlConfigFalseUsesDSv1` — CREATE TABLE, INSERT, SELECT with `use.v2=false` → `FileSourceScanExec`, real data
7. `testSqlConfigTrueUsesDSv2Stub` — SELECT with `use.v2=true` → `BatchScanExec`, empty result, correct schema
8. `testSqlSwitchBetweenV1AndV2Reads` — INSERT data, toggle `use.v2` between `true` and `false`, verify DSv2 read returns empty (stub) with `BatchScanExec` and DSv1 read returns real data with `FileSourceScanExec`
9. `testSqlInsertWithV2ReadEnabled` — SQL INSERT with `use.v2=true`, verify write works via V1 fallback, then read back via DSv1
10. `testDdlOperationsWithV2ReadEnabled` — ALTER TABLE ADD COLUMNS and DROP TABLE work correctly with `use.v2=true`
11. `testSchemaEvolutionPathUnaffectedByV2Config` — Schema evolution path (`hoodie.schema.on.read.enable=true`) still uses `HoodieInternalV2Table`, unaffected by the `use.v2` config
12. `testExplainShowsDSv2` — EXPLAIN plan string contains "BatchScan" for V2, "FileScan" for V1

Physical plan verification uses two complementary approaches matching existing codebase patterns:
- **Pattern matching**: `df.queryExecution.executedPlan match { case ProjectExec(_, scan: FileSourceScanExec) => ... }` for type-safe checks (pattern from `TestNestedSchemaPruningOptimization.scala`)
- **String-based**: `df.queryExecution.executedPlan.toString().contains("BatchScan")` for simpler assertions and EXPLAIN plan verification (pattern from `TestHoodiePruneFileSourcePartitions.scala`)

## Implementation Order

1. Config in `DataSourceOptions.scala`
2. Stub files (bottom-up): InputPartition → Reader → ReaderFactory → BatchScan → ScanBuilder → V2Table (reuses existing `HoodieV1WriteBuilder`) → DataSourceV2
3. SPI files (modify + create)
4. `HoodieSparkBaseAnalysis.scala` extractor update
5. `HoodieCatalog.scala` loadTable modification
6. Test file

## Verification

Build and test commands per CLAUDE.md instructions. Environment variables are loaded from `./.env` file.

```bash
# Load environment from .env
export $(grep -v '^#' .env | xargs)

# Build the changed modules (skip tests for speed)
JAVA_HOME=${JAVA_HOME_PATH} mvn clean package -DskipTests \
  -Dmaven.repo.local=${MAVEN_REPO_PATH} \
  -Djava11 -Djava.version=11 \
  -Dspark3.5 -Dflink1.20 \
  -Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true \
  -Dmaven.wagon.httpconnectionManager.ttlSeconds=25 -Dmaven.wagon.http.retryHandler.count=5 \
  -pl hudi-spark-datasource/hudi-spark-common,hudi-spark-datasource/hudi-spark -am

# Run the new functional test
JAVA_HOME=${JAVA_HOME_PATH} mvn test -Pfunctional-tests \
  -Dmaven.repo.local=${MAVEN_REPO_PATH} \
  -Djava11 -Djava.version=11 \
  -Dspark3.5 -Dflink1.20 \
  -Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true \
  -Dmaven.wagon.httpconnectionManager.ttlSeconds=25 -Dmaven.wagon.http.retryHandler.count=5 \
  -pl hudi-spark-datasource/hudi-spark \
  -Dtest=TestDSv2CoexistenceWithDSv1
```
