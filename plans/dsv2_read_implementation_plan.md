# Plan: Implementing Spark DataSource V2 (DSv2) Read Support

This plan describes how to re-implement RFC-98 ("Spark Datasource V2 Read") from scratch on this
branch. The design source is `rfc/rfc-98/rfc-98.md` (available on the `dsv2-rfc` branch); a working
reference implementation for COW tables exists on the `dsv2-poc` branch (12 commits,
~4.5k insertions). The POC was built on an older base; this branch has since dropped Spark 3.3,
added Spark 4.2, and gained Lance base-file support, so the POC cannot be cherry-picked verbatim
— see
"[Adaptations relative to the POC](#7-adaptations-relative-to-the-poc)".

**Related docs:** [plans/docs/architecture_overview.md](docs/architecture_overview.md) |
[plans/docs/read_path_callstack.md](docs/read_path_callstack.md) |
[plans/docs/write_path_callstack.md](docs/write_path_callstack.md)

---

## 1. Goal and Approach

Move Hudi Spark reads to the DSv2 API to unlock scan-level pushdowns that the V1 path cannot
express: filter pushdown (`SupportsPushDownFilters`), column pruning
(`SupportsPushDownRequiredColumns`), limit pushdown (`SupportsPushDownLimit`), and aggregate
pushdown (`SupportsPushDownAggregates`, answered from column-stats metadata without scanning data).

The approach is **hybrid**: DSv2 for reads, DSv1 fallback for writes. Hudi's write path (indexing,
precombine, upsert routing, file sizing, table services, multi-writer concurrency) cannot be
expressed through DSv2's `WriteBuilder` → `BatchWrite` → `DataWriter` API without loss of
functionality, so writes continue to flow through `HoodieSparkSqlWriter` via
`V2TableWithV1Fallback`.

Two opt-in entry points, both defaulting to current behavior:

| API | Activation | Behavior when unsupported |
|---|---|---|
| DataFrame | `spark.read.format("hudi_v2").load(path)` | explicit `HoodieException` |
| SQL/catalog | `SET hoodie.datasource.read.use.v2=true` | silent fallback to the V1 read path |

The existing `format("hudi")` path is untouched; regression risk is confined to the shared classes
listed in §4.1.

### Initial scope (this plan)

COW snapshot reads and explicitly requested MOR read-optimized reads, single Parquet base format,
no incremental/CDC/bootstrap. Time travel (`as.of.instant`) **is** in scope. Everything else falls
back to V1 (SQL path) or errors (DataFrame path) until later phases land. This matches what the
POC delivered and what RFC-98 records as "Current state" (phases 1–3 + parts of phase 8).

---

## 2. Current State of This Branch

- `DefaultSource` / `BaseDefaultSource` (V1 `RelationProvider`) serve all `format("hudi")` reads and
  writes. SQL reads resolve through `HoodieCatalog.loadTable`, which returns a `V1Table` wrapper by
  default, or `HoodieInternalV2Table` only when `hoodie.schema.on.read.enable=true` (the
  HUDI-4178 fallback; see the "PLEASE READ CAREFULLY" comments in `HoodieCatalog.scala` and
  `HoodieSparkBaseAnalysis.scala` — both get removed by this work).
- `HoodieInternalV2Table` (`hudi-spark-common`, package `org.apache.spark.sql.hudi.catalog`)
  implements `V2TableWithV1Fallback` for the schema-evolution DDL path. Its private
  `HoodieV1WriteBuilder` is the write-fallback we will reuse.
- Per-version `HoodieSparkXXDataSourceV2ToV1Fallback` analysis rules (3.4, 3.5, 4.0, 4.1, 4.2)
  convert `DataSourceV2Relation(HoodieInternalV2Table)` back to V1 `LogicalRelation`s.
- Supported Spark versions: **3.4, 3.5, 4.0, 4.1, 4.2** (3.3 was dropped in `facb517ef957`,
  4.2 added in `77f5851a5d53`).
- `HoodieFileIndex.filterFileSlices(...)` and
  `HoodieFileIndex.convertFilterForTimestampKeyGenerator(...)` exist and are the planning
  primitives the DSv2 scan builds on. `SparkAdapter.createParquetFileReader(...)` returns the
  shared `SparkColumnarFileReader`. `HoodieSparkSqlWriter.write(..., schemaFromCatalog)` already
  accepts a catalog schema (needed by the write fallback).
- None of the POC code or its side-fixes are present (verified: no `USE_V2_READ` config, no
  `org.apache.spark.sql.hudi.v2` package, no `toList()` conversion fixes).

---

## 3. Target Architecture

All new read-path classes go into package `org.apache.spark.sql.hudi.v2` inside
`hudi-spark-datasource/hudi-spark-common`.

### 3.1 New classes

| Class | Spark interfaces | Responsibility |
|---|---|---|
| `HoodieDataSourceV2` | `TableProvider`, `DataSourceRegister`, `CreatableRelationProvider` | SPI entry point for `format("hudi_v2")`. `shortName() = "hudi_v2"`. `getTable` builds a `HoodieSparkV2Table` from the `path` property; `createRelation` is the V1 write fallback for `df.write.format("hudi_v2")` (delegates to `HoodieSparkSqlWriter.write`/`bootstrap`, mirrors `DefaultSource.createRelation`). |
| `HoodieSparkV2Table` | `Table`, `SupportsRead`, `SupportsWrite`, `V2TableWithV1Fallback` | Routes reads to `HoodieScanBuilder`, writes to `HoodieV1WriteBuilder`. Resolves schema/partitioning/properties from `HoodieCatalogTable` (SQL path) or `HoodieTableMetaClient` + constructor options (DataFrame path). Merges per-query options in `newScanBuilder` (see §3.3). |
| `HoodieV2ReadSupport` | (Scala object) | `isSupportedByDSv2(metaClient, options)` — the supportability gate; `resolveReadOptions(spark, explicitOptions)` — layers `hoodie.*`/`spark.hoodie.*` session confs under explicit options and applies `DataSourceOptionsHelper.parametersWithReadDefaults` for DSv1 parity. |
| `HoodieScanBuilder` | `ScanBuilder`, `SupportsPushDownFilters`, `SupportsPushDownRequiredColumns`, `SupportsPushDownLimit`, `SupportsPushDownAggregates` | Collects pushdowns; plans input partitions via `HoodieFileIndex` in `build()`. Returns `HoodieLocalScan` when an aggregate was fully answered from column stats, else `HoodieBatchScan`. The single largest class (~640 lines in the POC) — see §3.4 for its correctness rules. |
| `HoodieBatchScan` | `Scan`, `Batch`, `SupportsReportStatistics` | Holds pre-planned partitions, broadcasts the Parquet reader + hadoop conf, reports byte-size statistics for CBO, creates `HoodiePartitionReaderFactory`. |
| `HoodieLocalScan` | `Scan`, `LocalScan` | Returns pre-computed aggregate rows without touching data files. |
| `HoodieStatistics` | `Statistics` | `sizeInBytes` for Spark CBO (sum of split lengths = sum of base-file sizes). |
| `HoodieInputPartition` | `InputPartition` | `(index, baseFilePath, start, length, partitionValues)` — a contiguous byte range of one base file, split per `spark.sql.files.maxPartitionBytes`; carries partition values in Spark internal format. |
| `HoodiePartitionReaderFactory` | `PartitionReaderFactory` | Creates `HoodiePartitionReader`s on executors. |
| `HoodiePartitionReader` | `PartitionReader[InternalRow]` | Reads one split via `SparkColumnarFileReader.read(...)` with pushed filters, internal-schema (schema evolution) support, Avro→Parquet `MessageType` schema repair for timestamp-millis logical types, an unsafe projection to the output schema when partition columns are appended, and a per-partition limit cap. |

One Java interface from the POC, `HoodiePartialLimitPushDown` (default
`isPartiallyPushed() = true`), existed only to bridge Spark 3.3 (method absent) and 3.4+ (default
method present). Since this branch's minimum is Spark 3.4, **it is not needed**:
`HoodieScanBuilder` can extend `SupportsPushDownLimit` directly and
`override def isPartiallyPushed: Boolean = true` (see §7).

### 3.2 Routing

DataFrame path:

```
spark.read.format("hudi_v2").load(path)
  → DataSourceV2Utils.lookupProvider → HoodieDataSourceV2 (via META-INF/services DataSourceRegister)
  → getTable(schema, partitioning, properties)        // properties carries path + user options
  → HoodieSparkV2Table(spark, path, options)          // no catalogTable
  → newScanBuilder → HoodieScanBuilder → HoodieBatchScan
```

SQL/catalog path (`HoodieCatalog.loadTable`), evaluated in strict precedence order:

| `hoodie.schema.on.read.enable` | `hoodie.datasource.read.use.v2` | Gate | Returned table |
|---|---|---|---|
| `true` | any | not consulted | `HoodieInternalV2Table` (existing schema-evolution path) |
| `false` | `true` | passes | `HoodieSparkV2Table` (DSv2 read) |
| `false` | `true` | fails | `V1Table` wrapper (V1 fallback) |
| `false` | `false` | not consulted | `V1Table` wrapper (existing default) |

Writes against a loaded `HoodieSparkV2Table`:

- `INSERT INTO/OVERWRITE` SQL: the per-version `HoodieSparkXXDataSourceV2ToV1Fallback` rule
  converts `InsertIntoStatement(DataSourceV2Relation(HoodieSparkV2Table))` to a V1
  `LogicalRelation`, so the existing `InsertIntoHoodieTableCommand` analysis rule fires.
- `df.writeTo(...).append()`: `newWriteBuilder` returns `HoodieV1WriteBuilder` (V1_BATCH_WRITE
  capability) — requires a catalog table; DataFrame-API `writeTo` against a path-only table throws.
- `df.write.format("hudi_v2").save(path)`: Spark uses the `CreatableRelationProvider` V1 fallback
  on `HoodieDataSourceV2` itself.

### 3.3 Supportability gate and options resolution

`HoodieV2ReadSupport.isSupportedByDSv2` admits a query iff **all** hold:

- base-file-only semantics: table is COW, or query type is `read_optimized` (MOR snapshot needs
  log merging — not yet implemented);
- single Parquet base format: `!tableConfig.isMultipleBaseFileFormatsEnabled && getBaseFileFormat
  == PARQUET` (this also excludes Lance tables, which landed on this branch after the POC);
- not incremental (`query.type=incremental`) and not CDC (`incremental.format=cdc`);
- not a bootstrap table (`getBootstrapBasePath` absent).

Time travel (`as.of.instant`) is deliberately **not** excluded; it is honored end-to-end (table
schema, internal schema, file-index instant, reader schema repair all resolve as of the instant).

`resolveReadOptions` must run **before** the gate on both paths so that `USE_V2_READ`,
`query.type`, etc. coming from SQL confs, `spark.hoodie.*`, `hudi-defaults.conf`, TBLPROPERTIES,
and `CREATE TABLE ... OPTIONS` are all honored when deciding V1 vs V2 — toggling the flag must not
change query semantics.

Option merge in `HoodieSparkV2Table.newScanBuilder` (DataFrame and SQL paths converge here):

```
explicitOpts = Map("path" -> path) ++ constructorOpts ++ (tableProperties - "path") ++ scanOptions
mergedOpts   = HoodieV2ReadSupport.resolveReadOptions(spark, explicitOpts)
```

Two non-obvious rules baked into that line (both found the hard way during POC review):

- `"path"` must be **dropped from table properties** before merging: catalog tables persist a
  `path` TBLPROPERTY containing only the URI path component (no scheme/authority); letting it win
  would silently retarget an `s3://bucket/table` scan at `file:///table`.
- the constructor `path` must be **promoted into the option map** because
  `HoodieFileIndex.getQueryPaths` requires it, and on the DataFrame path Spark passes it in
  constructor properties, not scan-time options.

### 3.4 Correctness rules inside `HoodieScanBuilder` (from POC review iterations)

These rules are where most review effort went on the POC; each must be preserved (or consciously
revisited) in the re-implementation:

1. **Filter classification in `pushFilters`.** Split convertible filters into:
   - *data-only filters* (reference no partition column): forwarded to Parquet for row-group
     pruning AND returned to Spark for row-wise re-application (Parquet stats only prune
     row-groups);
   - *partition-referencing filters* (partition-only or mixed): used for partition pruning via
     `HoodieFileIndex`, but **never** forwarded to Parquet — the partition column may be absent
     from base files (`drop_partition_columns`) or hold values that disagree with path-derived
     values (TimestampBased/Custom key generators: path `2024/01/01` vs stored
     `2024-01-01 12:00:00`), so row-group stats would prune matching rows. They must still be
     returned for row-wise re-application.
   - Apply `HoodieFileIndex.convertFilterForTimestampKeyGenerator` to partition filters before
     pruning (the file index only does this internally when `shouldEmbedFileSlices=true`; the scan
     builder constructs it with `false`).
2. **Limit pushdown safety.** Only push a limit when **no** filter is re-applied above the scan
   (no pushed data filters, no post-scan filters): capping rows per input partition before
   row-wise filtering would drop later matching rows (`WHERE id > 3 LIMIT 1` could stop at
   `id = 1`). With `isPartiallyPushed = true`, Spark keeps a global `LocalLimit` above the scan
   (the reader cap is per-partition, so `LIMIT N` could otherwise return up to
   `numPartitions × N` rows).
3. **Aggregate pushdown** (`COUNT(*)`, `COUNT(col)`, `MIN`, `MAX`, no `GROUP BY`) is answered from
   the metadata table's column-stats partition (`HoodieBackedTableMetadata.getColumnStats`) only
   when:
   - the column-stats metadata partition is available;
   - **no filters were pushed at all** (data, partition, or post-scan) — stats cannot refine
     unevaluated predicates, so `SELECT COUNT(*) WHERE col > x` must not be answered from the
     unfiltered count;
   - `MIN`/`MAX` target is not float/double (Parquet/Hudi stats exclude NaN; Spark treats NaN as
     greater than any non-NaN, and stats cannot distinguish "all NaN" from "all null");
   - stats are complete for every base file in scope (any missing file ⇒ fall back to scanning);
   - for `COUNT(*)`, pick a column whose stats are complete since the first commit — prefer a
     user-defined record-key field present in the schema, falling back to the first column.
   On success, `build()` returns `HoodieLocalScan`; `supportCompletePushDown` answers true only
   for the same aggregation with no filters.
4. **Partition values from path vs from files.** Mirror DSv1's
   `shouldExtractPartitionValuesFromPartitionPath`: only strip partition columns from the file
   read schema (supplying them from the parsed path) when `drop_partition_columns` was used, the
   user opted in via `EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH`, or bootstrap fast-read is
   requested. When extracting, partition column **types must come from
   `fileIndex.partitionSchema`** (TimestampBasedKeyGenerator parses path values as `StringType`);
   if the file-index type differs from the table-schema type, throw
   `UnsupportedOperationException` rather than feed string path values to the Parquet reader.
5. **Schema evolution.** `fetchInternalSchema` honors `hoodie.schema.on.read.enable` (read from
   options or session conf, mirroring `HoodieBaseRelation.isSchemaEvolutionEnabledOnRead`),
   resolves the internal schema as of the time-travel instant when present, and embeds it into the
   hadoop conf (`SparkInternalSchemaConverter.HOODIE_QUERY_SCHEMA` / `HOODIE_TABLE_PATH` /
   `HOODIE_VALID_COMMITS_LIST`) so the columnar reader does type repair.
6. **Timestamp logical-type repair.** Resolve the table Avro schema (as of the time-travel instant
   when present), convert to a Parquet `MessageType`, and pass it to
   `SparkColumnarFileReader.read` so timestamp-millis columns in older base files don't surface as
   raw longs.
7. **Split planning parity.** Split each base file into ranges of at most
   `spark.sql.files.maxPartitionBytes` using logic shared with the DSv1 path (the POC extracted
   `HoodieDataSourceHelper.computeSplitRanges` for this). Safe because the gate guarantees no log
   files participate.

### 3.5 `HoodieSparkV2Table` details worth preserving

- **Schema includes meta fields** when `populateMetaFields=true` (the default): hiding
  `_hoodie_commit_time` etc. breaks `SELECT _hoodie_commit_time` and meta-key joins. With
  `ACCEPT_ANY_SCHEMA` + the V1 INSERT fallback rule (which realigns columns), DML keeps working.
- **Time-travel schema**: when `as.of.instant` is set, resolve the schema via
  `TableSchemaResolver.getTableSchema(ts)` so columns added later aren't exposed.
- **Path walk-up on the DataFrame path**: when the user-supplied path points at a partition
  directory, walk up to the table base path (`DataSourceUtils.getTablePath`) before building the
  meta client; keep the original path as the query path so the read stays scoped.
- **Capabilities**: `BATCH_READ` always; `V1_BATCH_WRITE`, `TRUNCATE`, `ACCEPT_ANY_SCHEMA` only
  when a catalog table is present. **Do not advertise `OVERWRITE_BY_FILTER`** — the V1 fallback
  rewrites whole partitions and cannot honor arbitrary filter expressions;
  `df.writeTo(t).overwrite(expr)` must be rejected, not silently widened. (The POC also removed
  `OVERWRITE_BY_FILTER` from `HoodieInternalV2Table` and deleted `SupportsOverwrite` from
  `HoodieV1WriteBuilder` for the same reason; `INSERT OVERWRITE ... PARTITION` still works because
  the analysis rule intercepts `InsertIntoStatement` before the V2 planner checks capabilities.)

---

## 4. Changes to Existing Code

### 4.1 Shared classes (regression surface — keep diffs minimal)

| File (under `hudi-spark-datasource/hudi-spark-common`) | Change |
|---|---|
| `org/apache/hudi/DataSourceOptions.scala` | Add `DataSourceReadOptions.USE_V2_READ` (`hoodie.datasource.read.use.v2`, default `"false"`, `markAdvanced`, `sinceVersion` = current dev version — `1.3.0` in the POC; confirm against this branch's version). |
| `org/apache/spark/sql/hudi/catalog/HoodieCatalog.scala` | In `loadTable`: build `HoodieCatalogTable` lazily, resolve options via `HoodieV2ReadSupport.resolveReadOptions(spark, hoodieCatalogTable.catalogProperties)`, and return `HoodieSparkV2Table` when `!schemaEvolutionEnabled && v2ReadEnabled && isSupportedByDSv2`. Replace the HUDI-4178 comment block. |
| `org/apache/spark/sql/hudi/catalog/HoodieInternalV2Table.scala` | Promote `HoodieV1WriteBuilder` from `private` to `private[hudi]` so `HoodieSparkV2Table` can instantiate it. Harden it: drop `SupportsOverwrite`/`OVERWRITE_BY_FILTER`; align incoming DataFrame column names/types to the table's user schema (VALUES clauses arrive as `col1, col2` with uncast literal types); call `HoodieSparkSqlWriter.write` directly with `schemaFromCatalog` (instead of `data.write.format("org.apache.hudi")`) and propagate failures; pass through write options. |
| `org/apache/spark/sql/hudi/analysis/HoodieSparkBaseAnalysis.scala` | Extend the `HoodieV1OrV2Table` extractor to also match `HoodieSparkV2Table` (returns its `catalogTable`) so `TRUNCATE`/`SHOW PARTITIONS`/`DROP PARTITION` DDL keeps resolving. |
| `org/apache/hudi/HoodieDataSourceHelper.scala` | Extract `computeSplitRanges(spark, fileLength)` from `splitFiles` so DSv1 and DSv2 share split-boundary logic. |
| `org/apache/hudi/DefaultSource.scala` | (Optional parity fix from POC) surface `HoodieSparkSqlWriter.bootstrap` failures except under `SaveMode.Ignore`. |

### 4.2 Per-Spark-version modules (×5: 3.4, 3.5, 4.0, 4.1, 4.2)

For each `hudi-spark3.4.x` … `hudi-spark4.2.x` module — note the POC covered only 3.3–4.1, so the
**4.2 changes are new work** mirroring the 4.1 ones (mind the wider `DataSourceV2Relation`
pattern arity in 4.x matchers):

| File | Change |
|---|---|
| `.../adapter/SparkX_YAdapter.scala` | `resolveHoodieTable`: add a `PhysicalOperation(_, _, DataSourceV2Relation(v2: HoodieSparkV2Table, ...))` case returning `v2.catalogTable`; change `isHoodieTable(v2Table)` from the string-match on `"HoodieInternalV2Table"` to `isInstanceOf[HoodieInternalV2Table] || isInstanceOf[HoodieSparkV2Table]`. |
| `.../analysis/HoodieSparkXYAnalysis.scala` | In `HoodieSparkXYDataSourceV2ToV1Fallback`: add an `InsertIntoStatement(DataSourceV2Relation(v2Table: HoodieSparkV2Table, ...))` case (guarded by `v2Table.hoodieCatalogTable.isDefined`) converting to a V1 `LogicalRelation` via `DefaultSource.createRelation` + `buildHoodieConfig`. Match **only** `InsertIntoStatement` — leave bare `DataSourceV2Relation`s untouched so reads flow through `HoodieScanBuilder`, and so `OverwriteByExpression` stays rejected. |

### 4.3 SPI registration

| File | Change |
|---|---|
| `hudi-spark3-common/src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister` | Append `org.apache.spark.sql.hudi.v2.HoodieDataSourceV2`. |
| `hudi-spark4-common/src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister` | Same. |
| `hudi-spark3-common/src/main/resources/META-INF/services/org.apache.spark.sql.connector.catalog.TableProvider` | New file listing `HoodieDataSourceV2` (the POC added it for spark3 only; Spark's lookup goes through `DataSourceRegister`, so verify whether this file is actually consulted before replicating — if it is, add the spark4 twin too). |
| `packaging/hudi-spark-bundle` | Verify the bundle relocations/service-file merging pick up the new package and service entries (the POC needed no pom change, but confirm with a bundle smoke test). |

### 4.4 Prerequisite compile fixes (from the POC)

The POC needed Scala collection-bound fixes in Java callers (`scala.collection.Seq` vs
`scala.collection.immutable.Seq`) that are **not on master**. They only bite for Scala
2.13 / Spark 4.x compilation of code paths the DSv2 work touches; apply them up front if the build
demands it (verify first — master may have diverged):

- `hudi-client/hudi-spark-client/.../SparkReaderContextFactory.java` — `.toSeq()` → `.toList()`;
- `hudi-client/hudi-spark-client/.../SparkValidatorUtils.java` —
  `convertJavaListToScalaSeq` → `convertJavaListToScalaList`;
- `hudi-client/hudi-spark-client/.../SpaceCurveSortingHelper.java` — `row.toSeq().toList()`;
- `hudi-client/hudi-spark-client/.../BulkInsertDataInternalWriterHelper.java` — both conversions.

**Verified 2026-06-11: none of these fixes are needed on this branch.** The four call sites
above compile unchanged under Scala 2.13 — `JavaScalaConverters` is compiled per Scala version, so
its `Seq[A]` returns resolve to `scala.collection.immutable.Seq` under 2.13, matching what the
Spark 4 APIs expect.

Verification commands (per `AGENTS.md`: JDK 17 for Spark 4 profiles, local repo, SSL/retry
flags; `${MAVEN_REPO_PATH}` and the JDK paths come from `./.env`). The Spark 4.2 build + test
invocation, run from the repository root:

```bash
JAVA_HOME=${JAVA17_HOME_PATH} mvn clean install -Punit-tests \
  -pl hudi-spark-datasource/hudi-spark -am \
  -Dspark4.2 -Djava17 -Djava.version=17 \
  -Dtest=skipJavaTests -Dsurefire.failIfNoSpecifiedTests=false \
  -DwildcardSuites=org.apache.spark.sql.hudi.feature.v2,org.apache.spark.sql.hudi.catalog.TestHoodieInternalV2TableWriteFallback \
  -Dmaven.repo.local=${MAVEN_REPO_PATH} \
  -Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true \
  -Dmaven.wagon.httpconnectionManager.ttlSeconds=25 -Dmaven.wagon.http.retryHandler.count=5
```

with `mvn clean compile -DskipTests` variants of the same command line for `-Dspark4.0` and
`-Dspark4.1`. Recorded results:

- `-Dspark4.2` (Scala 2.13.18, Spark 4.2.0-preview4): full 18-module reactor through
  `hudi-spark_2.13` (incl. `hudi-spark-client` and `hudi-spark4.2.x_2.13`, main + test sources)
  builds clean; scalatest summary
  `Tests: succeeded 30, failed 0, canceled 1, ignored 0, pending 0` — the one canceled test is
  the Lance fallback case, which self-cancels via its `lance.skip.tests` assume-guard (the 4.2
  profile sets it to `true`).
- `-Dspark4.0` / `-Dspark4.1`: both compile clean, confirming the per-version pattern arities
  (`DataSourceV2Relation`: 5 fields on 4.0, 6 on 4.1/4.2; `InsertIntoStatement`: 7 fields on
  4.0/4.1, 9 on 4.2).
- `-Dspark3.5` (JDK 11, `-Djava11 -Djava.version=11 -Dflink1.20`), same suites:
  `Tests: succeeded 31, failed 0, canceled 0, ignored 0, pending 0` (Lance runs and passes).
- The SQL INSERT → V1-command routing that the per-version `InsertIntoStatement` matchers
  implement is pinned by committed regression tests in `TestDSv2Fallback`
  ("SQL INSERT with use.v2 on reaches the V1 write command",
  "INSERT OVERWRITE PARTITION with use.v2 on reaches the V1 write command"), so every CI profile
  build re-verifies it.

Trap observed while verifying: building with `-pl` (no `-am`) resolves the **unsuffixed** hudi
`1.3.0-SNAPSHOT` jars (`hudi-spark-client`, `hudi-common`, …) from the shared local repo, and a
concurrent build from another worktree under a different profile can overwrite them with
2.12-built jars. javac then fails with exactly the POC-looking signatures
(`scala.collection.Seq` vs `scala.collection.immutable.Seq`, `cannot access scala.Serializable`)
even though the sources are fine. Always reverify with a self-contained `-am` reactor before
concluding the §4.4 fixes are needed.

Two further POC side-fixes to evaluate (they fix latent issues exposed by DSv2 tests, not DSv2
itself): `FileFormatUtilsForFileGroupReader.toRef` wrapping refs in
`GetArrayItem(CreateArray(...))` to stop Spark re-deriving Parquet pushed filters from required
filters, and `HoodieFileGroupReaderBasedFileFormat` passing `requiredFilters` so incremental
commit-time pruning works without `HoodieSparkSessionExtension`. Include them when the
corresponding tests fail without them; keep them as separate commits.

---

## 5. Implementation Phases

Each phase compiles, passes tests, and is a reviewable PR slice. The POC history on the
`dsv2-poc` branch follows the same ordering and is the line-level reference.

### Phase 1 — Coexistence skeleton

Goal: `format("hudi_v2")` and `use.v2=true` resolve, route writes to V1, and return **empty**
read results; `format("hudi")` is bit-for-bit unaffected.

1. Add `USE_V2_READ` config (§4.1).
2. Add `HoodieDataSourceV2`, `HoodieSparkV2Table`, `HoodieV2ReadSupport`, and stub
   `HoodieScanBuilder`/`HoodieBatchScan`/`HoodieInputPartition`/`HoodiePartitionReaderFactory`/
   `HoodiePartitionReader` returning no partitions/rows.
3. SPI registration (§4.3).
4. `HoodieCatalog.loadTable` gate (§4.1) and `HoodieV1OrV2Table` extractor update.
5. `HoodieV1WriteBuilder` promotion + hardening; per-version adapter/analysis updates (§4.2).
6. Tests: DDL (CREATE/TRUNCATE/SHOW PARTITIONS/DROP PARTITION), INSERT INTO/OVERWRITE via both
   APIs with `use.v2=true`, fallback matrix (schema-on-read precedence, MOR snapshot, incremental,
   CDC, bootstrap, non-Parquet), `EXPLAIN` showing `BatchScanExec` vs `FileSourceScanExec`.

### Phase 2 — COW snapshot read

Goal: correct full-table and projected reads for COW snapshot + MOR read-optimized.

1. `HoodieScanBuilder.build()` plans real partitions: `fileIndex.filterFileSlices` (no filters
   yet) → base files → `computeSplitRanges` → `HoodieInputPartition`s.
2. `HoodiePartitionReader` reads via broadcast `SparkColumnarFileReader` (row-based:
   `FileFormat.OPTION_RETURNING_BATCH -> "false"`), with partition-value handling (§3.4 rule 4),
   schema repair (rule 6), internal-schema embedding (rule 5), and output projection.
3. Column pruning via `pruneColumns`.
4. Time travel: thread `as.of.instant` through schema, file index, and reader (§3.3, §3.5).
5. Tests: partitioned/non-partitioned COW, MOR read-optimized, meta-field projection, time travel,
   `drop_partition_columns`, `extract.partition.values.from.path`, TimestampBasedKeyGenerator,
   schema evolution with/without `hoodie.schema.on.read.enable`, multi-split large files,
   row-count parity with DSv1 on identical data.

### Phase 3 — Filter pushdown

1. Implement `pushFilters` per §3.4 rule 1; feed partition/data filter expressions into
   `fileIndex.filterFileSlices` for partition pruning and (metadata-table) data skipping.
2. Tests: partition pruning (incl. timestamp keygen), data skipping with column stats, mixed
   partition+data filters, filters on partition columns stored vs path-derived, correctness of
   row-wise re-application (results identical to DSv1 for every filter shape).

### Phase 4 — Limit & aggregate pushdown, CBO statistics

1. `pushLimit`/`isPartiallyPushed` per §3.4 rule 2 (no 3.3 guard needed on this branch).
2. `pushAggregation`/`supportCompletePushDown`/`HoodieLocalScan` per §3.4 rule 3.
3. `HoodieBatchScan.estimateStatistics` → `HoodieStatistics(sizeInBytes)`.
4. Tests: `LIMIT` with/without filters across multiple files/splits, `COUNT(*)`/`COUNT(col)`/
   `MIN`/`MAX` with and without column-stats partition, float/double `MIN`/`MAX` not pushed,
   aggregates with filters not pushed, empty table, `EXPLAIN` assertions on pushed
   limits/aggregates.

### Phase 5 — Review hardening

The POC's four review-response commits are a checklist of edge cases to fold into phases 1–4
rather than repeat as an afterthought: options-resolution parity, `path` TBLPROPERTY shadowing,
partition-referencing-filter Parquet exclusion, count-star column selection, NaN min/max,
`OVERWRITE_BY_FILTER` removal, VALUES-clause column alignment in the write fallback, bootstrap
save-mode handling. Budget explicit review time against `git log dsv2-poc` for anything missed.

### Later phases (per RFC-98, out of scope here)

4′ vectorized base-file reads (columnar batches to match V1 perf) → 5 MOR log-merge snapshot via
`HoodieFileGroupReader` → 6 incremental/CDC → 7 bootstrap & non-Parquet/mixed formats → 8
`SupportsPushDownTopN`. Each widens `isSupportedByDSv2` as it lands. The end state and the
`hudi_v2` → `hudi` name-swap sequence are in RFC-98 "Future Work".

---

## 6. Test Plan

Test inventory (mirroring the POC, adjusted paths on this branch):

| Test file | Module | Covers |
|---|---|---|
| `TestHoodieV2ReadSupport.scala` | hudi-spark-common (`feature/v2`) | gate truth table |
| `DSv2PlanAssertions.scala` | hudi-spark (helper) | `BatchScanExec`/`FileSourceScanExec`/`LocalTableScanExec` plan assertions |
| `TestDSv2CoexistenceWithDSv1.scala` | hudi-spark | both formats against the same table, DDL, writes |
| `TestDSv2CowSnapshotRead.scala` | hudi-spark | phase-2 matrix |
| `TestDSv2Fallback.scala` | hudi-spark | full fallback/error matrix (largest suite, ~750 lines in POC) |
| `TestDSv2FilterPushdown.scala` | hudi-spark | phase-3 matrix |
| `TestDSv2Pushdowns.scala` | hudi-spark | limit/aggregate/stats |
| `TestDSv2SchemaEvolution.scala` | hudi-spark | schema-on-read precedence + DSv2 internal-schema reads |

Non-regression: full existing unit/functional suites must pass unchanged (`mvn test -Punit-tests`,
`-Pfunctional-tests`) for **each** supported Spark profile — the per-version analysis/adapter edits
are the likeliest place for version-specific breakage (pattern arity of `DataSourceV2Relation` and
`InsertIntoStatement` differs across 3.4/3.5/4.x).

Performance: TPC-H DSv1-vs-DSv2 comparison per RFC-98 success criteria (no regression on full
reads; ≥10% on projection/filter; ≥20% on limit/aggregate). The `dsv2-benchmark` remote branch has
prior art.

Build commands follow `CLAUDE.md` (Java 11, `-Dspark3.5 -Dflink1.20` default, local repo, SSL
flags). Quick loop:

```bash
mvn clean package -DskipTests -pl hudi-spark-datasource/hudi-spark-common -am
mvn test -Punit-tests -pl hudi-spark-datasource/hudi-spark \
  -Dtest=skipJavaTests -DwildcardSuites=org.apache.spark.sql.hudi.feature.v2
```

---

## 7. Adaptations Relative to the POC

Differences between the POC base and this branch that change the implementation:

1. **Spark 3.3 is gone.** Drop `HoodiePartialLimitPushDown.java` and the
   `HoodieSparkUtils.gteqSpark3_4` guard in `pushLimit`; implement `SupportsPushDownLimit`
   directly with `override def isPartiallyPushed: Boolean = true`. No 3.3 adapter/analysis edits.
2. **Spark 4.2 is new.** Replicate the 4.1 adapter/analysis changes in
   `hudi-spark4.2.x` (`Spark4_2Adapter.resolveHoodieTable`/`isHoodieTable`,
   `HoodieSpark42DataSourceV2ToV1Fallback`). *(Done; compile-verified for 4.0/4.1/4.2 and
   test-verified under `-Dspark4.2` — see §4.4.)*
3. **Lance base files exist on this branch.** No gate change needed (the Parquet-only check already
   excludes them), but add a fallback test for a Lance-base-format table.
4. **Import-order checkstyle now errors** (`65562af43371`): new files must follow the enforced
   order (`org.apache.hudi` → third-party → `javax`/`java`/`scala`).
5. **API drift check.** Before porting each POC file, diff its master-side dependencies; verified
   still present: `HoodieFileIndex.filterFileSlices` / `convertFilterForTimestampKeyGenerator`,
   `SparkAdapter.createParquetFileReader`, `HoodieSparkSqlWriter.write(schemaFromCatalog=...)`,
   `HoodieSchemaConversionUtils.getRecordNameAndNamespace` (lives in hudi-spark-client, which
   hudi-spark-common depends on — confirmed in `hudi-spark-common/pom.xml`).
   Re-verify `HoodieBackedTableMetadata.getColumnStats` and
   `HoodieColumnRangeMetadata.fromColumnStats` signatures, and the 126 intervening commits for
   refactors (e.g. the record-key extraction unification `cb14ca727583`, Lombok refactors).
6. **Scala-collection compile fixes** (§4.4): verified — none are needed on this branch
   (see the verification note in §4.4).

---

## 8. Risks and Open Questions

- **Silent V1 fallback on the SQL path** can mask DSv2 routing bugs in tests — always assert the
  physical plan node type, never just row results (`DSv2PlanAssertions`).
- **Behavior parity is the acceptance bar**: every DSv2 result must match DSv1 for the same query;
  property-style comparison tests (same table, both formats) are the cheapest guard.
- **Per-version pattern arity** in analysis rules is the most fragile code; CI must build all five
  Spark profiles before merge (GitHub Actions `bot.yml` covers 3.5; run the others locally or via
  Azure).
- **Bundle packaging**: confirm `hudi-spark-bundle` ships the new service entries and that shading
  does not relocate `org.apache.spark.sql.hudi.v2` (it lives under the `org.apache.spark` tree,
  which bundles do not relocate — verify with a `spark-shell --jars` smoke test of
  `format("hudi_v2")`).
- **Upstream sync**: RFC-98's first slice is upstream PR
  [apache/hudi#18277](https://github.com/apache/hudi/pull/18277); if it evolves during this work,
  reconcile review feedback from there rather than re-deriving it.
