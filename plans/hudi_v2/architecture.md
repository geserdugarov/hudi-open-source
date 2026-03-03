# Hudi DSv2 Read + DSv1 Write Fallback Architecture

## Goal

Add DataSource V2 read support for Hudi tables registered under the SPI short name `"hudi_v2"`, while preserving 100% backward compatibility with the existing `"hudi"` DSv1 path. Writes fall back to the existing DSv1 write pipeline via `V2TableWithV1Fallback`.

## Design Principles

1. **No renames** -- all existing classes keep their names and packages.
2. **Additive only** -- the existing `format("hudi")` path is untouched; `format("hudi_v2")` activates the new path.
3. **Reuse internals** -- DSv2 read delegates to existing `HoodieFileIndex` (file listing, partition pruning, data skipping) and the file-reading internals from `HoodieBaseRelation`.
4. **Minimal modification surface** -- only `HoodieCatalog.loadTable()` is modified (to optionally return the new V2 table); everything else is new code.
5. **Follow proven patterns** -- Paimon uses V1 for writes / V2 for reads; Iceberg is full V2. We adopt Paimon's hybrid approach as the migration strategy.

## Reference Analysis

### Paimon Pattern (V1 write + V2 read)
- `SparkSource` (DSv1 `RelationProvider`) handles writes via `WriteIntoPaimonTable` command.
- V1 read `createRelation()` returns a stub relation (schema-only); actual reads always go through DSv2.
- `PaimonSparkTableBase` implements `SupportsRead` with `newScanBuilder()` for DSv2 reads.
- `ScanBuilder` supports filter pushdown, column pruning, limit pushdown, aggregation pushdown, TopN pushdown.
- `PaimonBatch` plans splits with bin-packing; `PaimonPartitionReader` reads rows.
- **V2 write is opt-in** via `write.use-v2-write` config; streaming write stays on V1.

### Iceberg Pattern (full V2)
- `IcebergSource` extends `SupportsCatalogOptions`; everything goes through `SparkCatalog`.
- `SparkTable` implements `SupportsRead` and `SupportsWrite`.
- `SparkScanBuilder` handles predicate pushdown, column projection, aggregate pushdown, limit pushdown.
- `SparkBatch` plans tasks; `RowDataReader` / `BatchDataReader` read files.
- Writes go through `SparkWrite` with distribution/ordering requirements.

### Current Hudi State
- `BaseDefaultSource` (SPI `"hudi"`) is pure DSv1: `RelationProvider`, `CreatableRelationProvider`, `StreamSinkProvider`, `StreamSourceProvider`.
- DSv2 APIs are deliberately disabled (HUDI-4178) in `BaseDefaultSource`.
- `HoodieCatalog.loadTable()` always creates a `HoodieInternalV2Table` internally (handling `catalogTable.comment` → properties merging), then checks `schemaEvolutionEnabled`: if true, returns the V2 table directly; otherwise returns `v2Table.v1TableWrapper` (a `V1Table` wrapper). See HUDI-4178 for rationale.
- `HoodieInternalV2Table` extends `Table with SupportsWrite with V2TableWithV1Fallback`; declares `BATCH_READ` capability but does NOT implement `SupportsRead` -- reads always fall back to V1.
- Read path: `DefaultSource.createRelation()` → factory → `HadoopFsRelation` or `HoodieBaseRelation` → `PrunedFilteredScan` → `RDD[Row]`.
- Write path: `DefaultSource.createRelation(mode, df)` → `HoodieSparkSqlWriter.write()`.

## Architecture Overview

### New Package: `org.apache.spark.sql.hudi.v2`

All new DSv2 classes go in package `org.apache.spark.sql.hudi.v2` inside `hudi-spark-common`. This keeps them separate from existing code, indicates they are Spark-specific, and makes the boundary clear.

### New Classes

| Class | Spark Interface | Responsibility |
|-------|-----------------|----------------|
| `HoodieDataSourceV2` | `TableProvider`, `DataSourceRegister`, `CreatableRelationProvider` | SPI entry point for `format("hudi_v2")`. Resolves table path, creates `HoodieSparkV2Table`. `CreatableRelationProvider` enables DataFrame API writes via `df.write.format("hudi_v2")`. |
| `HoodieSparkV2Table` | `Table`, `SupportsRead`, `SupportsWrite`, `V2TableWithV1Fallback` | Represents a Hudi table. Routes reads to DSv2 scan builder, routes writes to DSv1 fallback via `V1Write` using reused `HoodieV1WriteBuilder`. |
| `HoodieScanBuilder` | `ScanBuilder`, `SupportsPushDownFilters`, `SupportsPushDownRequiredColumns` | Collects filter and column pruning pushdowns from Spark optimizer. Builds `HoodieBatchScan`. |
| `HoodieBatchScan` | `Scan`, `Batch` | Plans input partitions using `HoodieFileIndex`. Creates `HoodiePartitionReaderFactory`. |
| `HoodieInputPartition` | `InputPartition` | Serializable descriptor for a file slice or set of file slices to read. |
| `HoodiePartitionReaderFactory` | `PartitionReaderFactory` | Creates `HoodiePartitionReader` for each input partition on executors. |
| `HoodiePartitionReader` | `PartitionReader[InternalRow]` | Reads `InternalRow` records from Hudi files. Delegates to existing file-reading code from `HoodieBaseRelation`. |
| _(no separate write builder)_ | — | V1 write fallback reuses `HoodieV1WriteBuilder` from `HoodieInternalV2Table` directly (visibility widened from `private` to `private[hudi]`). Builds `V1Write` → `InsertableRelation`. |

### SPI Registration

New file: `META-INF/services/org.apache.spark.sql.connector.catalog.TableProvider`
```
org.apache.spark.sql.hudi.v2.HoodieDataSourceV2
```

The existing `META-INF/services/org.apache.spark.sql.sources.DataSourceRegister` file gets a new entry appended:
```
org.apache.hudi.BaseDefaultSource
org.apache.spark.sql.execution.datasources.parquet.LegacyHoodieParquetFileFormat
org.apache.spark.sql.hudi.v2.HoodieDataSourceV2
```

Note: `HoodieDataSourceV2` implements both `TableProvider` and `DataSourceRegister`. The `DataSourceRegister` SPI entry makes it discoverable via `format("hudi_v2")`. The `TableProvider` SPI entry ensures Spark's V2 catalog resolution recognizes it.

### Modified Classes

Four existing files are modified:

1. **`HoodieCatalog.scala`** -- `loadTable()` enhanced to check `hoodie.datasource.read.use.v2` config.
2. **`DataSourceOptions.scala`** -- New `USE_V2_READ` config property added.
3. **`HoodieSparkBaseAnalysis.scala`** -- `HoodieV1OrV2Table` extractor updated to recognize `HoodieSparkV2Table` for DDL operations.
4. **`HoodieInternalV2Table.scala`** -- `HoodieV1WriteBuilder` visibility widened from `private` to `private[hudi]`, column alignment added, write changed to call `HoodieSparkSqlWriter.write()` directly with `schemaFromCatalog`.

**`HoodieCatalog.loadTable()`** -- Enhanced to check a table-level or session-level config (`hoodie.datasource.read.use.v2`). When enabled, returns `HoodieSparkV2Table` instead of `V1Table` wrapper:

```scala
override def loadTable(ident: Identifier): Table = {
  super.loadTable(ident) match {
    case V1Table(catalogTable0) if sparkAdapter.isHoodieTable(catalogTable0) =>
      // Preserve existing comment-handling logic
      val catalogTable = catalogTable0.comment match {
        case Some(v) =>
          val newProps = catalogTable0.properties + (TableCatalog.PROP_COMMENT -> v)
          catalogTable0.copy(properties = newProps)
        case _ =>
          catalogTable0
      }

      val v2ReadEnabled = isV2ReadEnabled(spark, catalogTable)
      val schemaEvolutionEnabled = ProvidesHoodieConfig.isSchemaEvolutionEnabled(spark)

      if (v2ReadEnabled) {
        // NEW: DSv2 read with V1 write fallback
        new HoodieSparkV2Table(spark, catalogTable.location.toString,
          Some(catalogTable), Some(ident.toString))
      } else {
        // Existing logic: create V2 table, return it or its V1 wrapper
        val v2Table = HoodieInternalV2Table(spark, catalogTable.location.toString,
          Some(catalogTable), Some(ident.toString))
        if (schemaEvolutionEnabled) v2Table else v2Table.v1TableWrapper
      }
    case t => t
  }
}
```

## Detailed Component Design

### 1. HoodieDataSourceV2

```scala
package org.apache.spark.sql.hudi.v2

class HoodieDataSourceV2 extends TableProvider with DataSourceRegister {

  override def shortName(): String = "hudi_v2"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = new StructType()

  override def getTable(schema: StructType,
                        partitioning: Array[Transform],
                        properties: java.util.Map[String, String]): Table = {
    val path = properties.get("path")
    HoodieSparkV2Table(SparkSession.active, path)
  }

  override def supportsExternalMetadata(): Boolean = true
}
```

### 2. HoodieSparkV2Table

```scala
package org.apache.spark.sql.hudi.v2

case class HoodieSparkV2Table(spark: SparkSession,
                               path: String,
                               catalogTable: Option[CatalogTable] = None,
                               tableIdentifier: Option[String] = None)
  extends Table with SupportsRead with SupportsWrite with V2TableWithV1Fallback {

  // Lazy-init metaClient and schema (same pattern as HoodieInternalV2Table)
  lazy val metaClient: HoodieTableMetaClient = ...
  lazy val tableSchema: StructType = ...

  override def capabilities(): util.Set[TableCapability] = Set(
    BATCH_READ,           // DSv2 native read
    V1_BATCH_WRITE,       // fallback to DSv1 for writes
    OVERWRITE_BY_FILTER,
    TRUNCATE,
    ACCEPT_ANY_SCHEMA
  ).asJava

  // DSv2 READ: native scan builder
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new HoodieScanBuilder(spark, metaClient, tableSchema, mergedOptions(options))
  }

  // DSv1 WRITE: fallback via V2TableWithV1Fallback
  // Reuses HoodieV1WriteBuilder from HoodieInternalV2Table directly.
  // Its visibility was widened from `private` to `private[hudi]` to allow access from the v2 package.
  // The builder follows the same pattern: SupportsTruncate + SupportsOverwrite → V1Write → InsertableRelation
  // InsertableRelation.insert() aligns incoming columns (rename + cast) to match the table's
  // user schema, then calls HoodieSparkSqlWriter.write() directly with the catalog schema.
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new HoodieV1WriteBuilder(info.options, hoodieCatalogTable, spark)
  }

  override def v1Table: CatalogTable = hoodieCatalogTable.table
}
```

### 3. HoodieScanBuilder

```scala
package org.apache.spark.sql.hudi.v2

class HoodieScanBuilder(spark: SparkSession,
                         metaClient: HoodieTableMetaClient,
                         tableSchema: StructType,
                         options: Map[String, String])
  extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns {

  private var pushedFilters: Array[Filter] = Array.empty
  private var requiredSchema: StructType = tableSchema

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    // Partition filters that Hudi can handle vs. residual
    val (pushed, residual) = filters.partition(canPushDown)
    this.pushedFilters = pushed
    residual  // return filters Spark must still evaluate
  }

  override def pushedFilters(): Array[Filter] = pushedFilters

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  override def build(): Scan = {
    new HoodieBatchScan(spark, metaClient, requiredSchema, pushedFilters, options)
  }
}
```

### 4. HoodieBatchScan

```scala
package org.apache.spark.sql.hudi.v2

class HoodieBatchScan(spark: SparkSession,
                       metaClient: HoodieTableMetaClient,
                       requiredSchema: StructType,
                       pushedFilters: Array[Filter],
                       options: Map[String, String])
  extends Scan with Batch {

  override def readSchema(): StructType = requiredSchema

  override def toBatch(): Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    // Reuse HoodieFileIndex for file listing with partition pruning and data skipping.
    // NOTE: pushedFilters (Array[Filter]) must be converted to Seq[Expression] before
    // passing to HoodieFileIndex methods, which accept Spark Catalyst Expressions, not DSv1 Filters.
    // Use HoodieCatalystExpressionUtils.convertToCatalystExpression(filter, tableSchema) for conversion
    // (existing utility in hudi-spark-client). See also HoodieBaseRelation.convertToExpressions().
    val fileIndex = HoodieFileIndex(spark, metaClient, Some(requiredSchema), options,
      includeLogFiles = metaClient.getTableType == HoodieTableType.MERGE_ON_READ)
    val expressions = pushedFilters.flatMap(f =>
      HoodieCatalystExpressionUtils.convertToCatalystExpression(f, requiredSchema))
    val (partitionFilters, dataFilters) = expressions.partition(isPartitionPredicate)
    // listFileSlices(partitionFilters: Seq[Expression]): Map[String, Seq[FileSlice]]
    // or filterFileSlices(dataFilters, partitionFilters): Seq[(Option[PartitionPath], Seq[FileSlice])]
    val fileSlicesByPartition = fileIndex.filterFileSlices(dataFilters, partitionFilters)
    fileSlicesByPartition.flatMap { case (partOpt, slices) =>
      val partPath = partOpt.map(_.path).getOrElse("")
      slices.map(slice => new HoodieInputPartition(
        slice, partPath, metaClient.getBasePath.toString, options): InputPartition)
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new HoodiePartitionReaderFactory(
      spark.sparkContext.broadcast(
        new SerializableConfiguration(spark.sessionState.newHadoopConf())),
      requiredSchema,
      metaClient.getBasePath.toString,
      metaClient.getTableType,
      options)
  }
}
```

### 5. HoodieInputPartition

```scala
package org.apache.spark.sql.hudi.v2

case class HoodieInputPartition(
    fileSlice: FileSlice,       // FileSlice already implements Serializable
    partitionPath: String,
    basePath: String,
    options: Map[String, String]
) extends InputPartition
```

### 6. HoodiePartitionReaderFactory

```scala
package org.apache.spark.sql.hudi.v2

class HoodiePartitionReaderFactory(
    broadcastConf: Broadcast[SerializableConfiguration],
    requiredSchema: StructType,
    basePath: String,
    tableType: HoodieTableType,
    options: Map[String, String]
) extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val hoodiePart = partition.asInstanceOf[HoodieInputPartition]
    new HoodiePartitionReader(broadcastConf, hoodiePart, requiredSchema, basePath, tableType, options)
  }
}
```

### 7. HoodiePartitionReader

```scala
package org.apache.spark.sql.hudi.v2

class HoodiePartitionReader(
    broadcastConf: Broadcast[SerializableConfiguration],
    partition: HoodieInputPartition,
    requiredSchema: StructType,
    basePath: String,
    tableType: HoodieTableType,
    options: Map[String, String]
) extends PartitionReader[InternalRow] {

  // Internally delegates to existing Hudi file readers:
  // - For CoW: reuses BaseFileReader (inner case class of HoodieBaseRelation companion object)
  // - For MoR: reuses MergeOnReadSnapshotRelation / HoodieMergeOnReadRDDV2 internals
  //            (which use HoodieFileGroupReader for base + log file merging)
  // These are the same code paths used by the DSv1 PrunedFilteredScan.buildScan()

  private val iterator: Iterator[InternalRow] = buildIterator()

  override def next(): Boolean = iterator.hasNext
  override def get(): InternalRow = iterator.next()
  override def close(): Unit = { /* close underlying readers */ }
}
```

## Read Path Flow

```
User code:    spark.read.format("hudi_v2").load("/path/to/table")
                |
Spark:        DataSourceV2Relation resolution via SPI ("hudi_v2")
                |
                v
              HoodieDataSourceV2.getTable()
                |
                v
              HoodieSparkV2Table
                |
                v
              Spark calls newScanBuilder(options)
                |
                v
              HoodieScanBuilder
                |-- pushFilters()   <-- Spark optimizer pushes predicates
                |-- pruneColumns()  <-- Spark optimizer prunes columns
                |-- build()
                |
                v
              HoodieBatchScan
                |-- planInputPartitions()
                |   |
                |   v
                |   HoodieFileIndex (EXISTING, reused)
                |   |-- partition pruning
                |   |-- bloom filter index
                |   |-- column stats index
                |   |-- record-level index
                |   |
                |   v
                |   Array[HoodieInputPartition]
                |
                |-- createReaderFactory()
                    |
                    v
                    HoodiePartitionReaderFactory
                      |
                      v (on each executor)
                    HoodiePartitionReader
                      |-- reuses existing file reading code
                      |   from HoodieBaseRelation internals
                      |
                      v
                    Iterator[InternalRow]
```

## Write Path Flow

```
User code:    spark.write.format("hudi_v2").mode("append").save("/path")
                |
Spark:        Resolves HoodieSparkV2Table via SPI
                |-- capabilities include V1_BATCH_WRITE
                |-- Spark detects V2TableWithV1Fallback
                |
                v
              HoodieSparkV2Table.newWriteBuilder()
                |
                v
              HoodieV1WriteBuilder.build()  (reused from HoodieInternalV2Table)
                |
                v
              V1Write => InsertableRelation.insert(data, overwrite)
                |
                v
              Column alignment: rename + cast incoming DataFrame columns
              to match table's user schema (handles generic names like
              col1, col2 from VALUES clauses and uncast types)
                |
                v
              HoodieSparkSqlWriter.write(sqlContext, mode, config,       [EXISTING]
                alignedData, schemaFromCatalog = catalogSchema)
                |
                v
              HoodieSparkSqlWriter.cleanup()
```

## Configuration

| Config Key | Default | Description |
|-----------|---------|-------------|
| `hoodie.datasource.read.use.v2` | `false` | When `true`, `HoodieCatalog.loadTable()` returns `HoodieSparkV2Table` instead of `V1Table`. Only affects catalog-based table resolution. |

For DataFrame API, the user simply uses `format("hudi_v2")` to opt in. No config needed.

## Compatibility Matrix

### DataFrame API

| Scenario | format("hudi") | format("hudi_v2") |
|----------|----------------|-------------------|
| Batch read | DSv1 (existing) | DSv2 (new) |
| Batch write | DSv1 (existing) | DSv1 fallback (existing) |
| Streaming read | DSv1 (existing) | Not supported (future) |
| Streaming write | DSv1 (existing) | Not supported (future) |

### SQL Queries (`SELECT * FROM table`, `INSERT INTO table`, etc.)

SQL queries do **not** use `format(...)`. They go through the catalog path:
`SparkSession` → `HoodieCatalog.loadTable(ident)` → returns a `Table` object → Spark plans read/write against it.

The table type returned by `loadTable()` determines which API is used:

| `hoodie.datasource.read.use.v2` | `loadTable()` returns | SQL SELECT | SQL INSERT/MERGE/UPDATE/DELETE |
|---|----|----|----|
| `false` (default) | `V1Table` | DSv1 read (existing, unchanged) | DSv1 write (existing, unchanged) |
| `true` | `HoodieSparkV2Table` | **DSv2 read (new)** via `newScanBuilder()` → `BatchScanExec` | DSv1 write fallback via `V2TableWithV1Fallback` → `V1Write` (existing pipeline) |

So for `SELECT * FROM hudi_table`:
- **Default** (`v2.read.enabled=false`): Unchanged behavior. `V1Table` → `HadoopFsRelation` → `FileSourceScanExec`. Same as today.
- **Opted in** (`v2.read.enabled=true`): `HoodieSparkV2Table` → `HoodieScanBuilder` → `HoodieBatchScan` → `BatchScanExec`. New DSv2 read path.

The `schemaEvolution` path remains as a separate branch (returns `HoodieInternalV2Table`, existing behavior).

## Benefits of DSv2 Read

1. **Native pushdown API** -- Spark optimizer uses `V2ScanRelationPushDown` rule which is more structured than DSv1's `PrunedFilteredScan`. This enables future aggregate pushdown and limit pushdown.
2. **Better planning** -- `BatchScanExec` + `DataSourceRDD` instead of `RowDataSourceScanExec` or `FileSourceScanExec`. More consistent execution plan.
3. **Future-proof** -- DSv2 is Spark's strategic direction. Establishes the foundation for full V2 migration (including writes) later.
4. **No regression risk** -- `format("hudi")` path is completely untouched. Users opt in to DSv2 explicitly.

## Implementation Phases

### Phase 1: Coexistence POC
- All 7 new classes as stubs (empty read results), SPI registration, V1 write fallback reusing `HoodieV1WriteBuilder` from `HoodieInternalV2Table` (visibility widened to `private[hudi]`).
- `hoodie.datasource.read.use.v2` config, `HoodieCatalog.loadTable()` modification.
- Proves DSv2 and DSv1 coexist across both DataFrame API (`format("hudi_v2")`) and SQL queries (catalog config) without interference.

### Phase 2: CoW Snapshot Read
- Wire `HoodieBatchScan.planInputPartitions()` to `HoodieFileIndex`, implement base file reading in `HoodiePartitionReader`.
- Column pruning support.

### Phase 3: Filter Pushdown
- Implement `HoodieScanBuilder.pushFilters()` for partition pruning and data skipping via `HoodieFileIndex`.
- Converting DSv2 `Filter` → Spark `Expression` for `HoodieFileIndex`.

### Phase 4: MoR Snapshot Read
- Extend `HoodiePartitionReader` with base + log merge logic, reusing `HoodieFileGroupReader`.

### Phase 5: Incremental and CDC Queries
- Add incremental query support to `HoodieScanBuilder`.
- Route based on query type option.

### Phase 6: Advanced Pushdowns
- `SupportsPushDownAggregates` for metadata-based aggregation.
- `SupportsPushDownLimit` for early termination.
- `SupportsPushDownTopN`.

## File Summary

### New Files
| File | Location |
|------|----------|
| `HoodieDataSourceV2.scala` | `hudi-spark-common/.../org/apache/spark/sql/hudi/v2/` |
| `HoodieSparkV2Table.scala` | `hudi-spark-common/.../org/apache/spark/sql/hudi/v2/` |
| `HoodieScanBuilder.scala` | `hudi-spark-common/.../org/apache/spark/sql/hudi/v2/` |
| `HoodieBatchScan.scala` | `hudi-spark-common/.../org/apache/spark/sql/hudi/v2/` |
| `HoodieInputPartition.scala` | `hudi-spark-common/.../org/apache/spark/sql/hudi/v2/` |
| `HoodiePartitionReaderFactory.scala` | `hudi-spark-common/.../org/apache/spark/sql/hudi/v2/` |
| `HoodiePartitionReader.scala` | `hudi-spark-common/.../org/apache/spark/sql/hudi/v2/` |
| `org.apache.spark.sql.connector.catalog.TableProvider` | `hudi-spark-common/.../META-INF/services/` |

### Modified Files
| File | Change |
|------|--------|
| `HoodieCatalog.scala` | Add `v2ReadEnabled` branch in `loadTable()` |
| `DataSourceOptions.scala` | Add `USE_V2_READ` config property with key `hoodie.datasource.read.use.v2` |
| `HoodieSparkBaseAnalysis.scala` | Add `HoodieSparkV2Table` case arm to `HoodieV1OrV2Table` extractor for DDL operations |
| `HoodieInternalV2Table.scala` | `HoodieV1WriteBuilder` visibility widened from `private` to `private[hudi]`, column alignment added (rename + cast), write changed to call `HoodieSparkSqlWriter.write()` directly with `schemaFromCatalog` |

### Unchanged Files (reused)
| File | Reuse |
|------|-------|
| `BaseDefaultSource.scala` | SPI for `"hudi"`, unchanged |
| `DefaultSource.scala` | DSv1 read/write, unchanged |
| `HoodieFileIndex.scala` | File listing and data skipping, reused by `HoodieBatchScan` |
| `HoodieBaseRelation.scala` | File reading internals, reused by `HoodiePartitionReader` |
| `HoodieSparkSqlWriter.scala` | Write pipeline, reused via V1 fallback |
| `HoodieHadoopFsRelationFactory.scala` | Relation factories, unchanged |
