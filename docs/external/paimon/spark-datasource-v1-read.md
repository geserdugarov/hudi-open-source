# Spark DataSource V1 Read Path

## Introduction

Spark's DataSource V1 API defines a `BaseRelation` abstraction that provides schema information and optionally supports various scan strategies (e.g., `TableScan`, `PrunedFilteredScan`). In Paimon's Spark integration, the V1 read path is a **minimal stub** -- it provides only schema metadata and does not implement any data scanning. All actual data reading is performed through the V2 API via `SparkTable` (which implements `SupportsRead`).

This document explains the V1 read entry point, why it exists, what it does, and why actual reading is delegated to the V2 path.

## Entry Point: `SparkSource.createRelation()`

`org.apache.paimon.spark.SparkSource` implements `CreatableRelationProvider`, which requires a `createRelation()` method. This method is invoked by Spark when using the DataFrame write API (e.g., `df.write.format("paimon").save()`). After writing data, it returns a `BaseRelation` representing the table.

```scala
// SparkSource.scala:74-83
override def createRelation(
    sqlContext: SQLContext,
    mode: SparkSaveMode,
    parameters: Map[String, String],
    data: DataFrame): BaseRelation = {
  val table = loadTable(parameters.asJava).asInstanceOf[FileStoreTable]
  WriteIntoPaimonTable(table, SaveMode.transform(mode), data, Options.fromMap(parameters.asJava))
    .run(sqlContext.sparkSession)
  SparkSource.toBaseRelation(table, sqlContext)
}
```

The returned `BaseRelation` is created by the `toBaseRelation()` helper method.

## The `toBaseRelation()` Stub

The `BaseRelation` returned by Paimon is an anonymous class that implements only two methods:

```scala
// SparkSource.scala:142-147
private def toBaseRelation(table: FileStoreTable, _sqlContext: SQLContext): BaseRelation = {
  new BaseRelation {
    override def sqlContext: SQLContext = _sqlContext
    override def schema: StructType = SparkTypeUtils.fromPaimonRowType(table.rowType())
  }
}
```

This `BaseRelation`:
- Provides `sqlContext` -- the Spark SQL context
- Provides `schema` -- the table's schema converted from Paimon's `RowType` to Spark's `StructType` via `SparkTypeUtils.fromPaimonRowType()`

It does **not** implement:
- `TableScan` (no `buildScan(): RDD[Row]`)
- `PrunedScan` (no `buildScan(requiredColumns: Array[String])`)
- `PrunedFilteredScan` (no `buildScan(requiredColumns, filters)`)
- `CatalystScan`

This means the V1 `BaseRelation` cannot produce any data on its own. It exists solely to satisfy the `CreatableRelationProvider` contract, which requires returning a `BaseRelation` after a write operation.

## Table Loading: `loadTable()`

Both read and write paths share the `loadTable()` method for resolving a Paimon table from the provided options:

```scala
// SparkSource.scala:98-126
private def loadTable(options: JMap[String, String]): DataTable = {
  val path = CoreOptions.path(options)
  val sessionState = PaimonSparkSession.active.sessionState

  val table = if (options.containsKey(CATALOG)) {
    // Catalog-based: look up via Spark's catalog manager
    val catalogName = options.get(CATALOG)
    val dataBaseName = Option(options.get(DATABASE)).getOrElse(CatalogUtils.database(path))
    val tableName = Option(options.get(TABLE)).getOrElse(CatalogUtils.table(path))
    val sparkCatalog = sessionState.catalogManager.catalog(catalogName).asInstanceOf[TableCatalog]
    sparkCatalog
      .loadTable(SparkIdentifier.of(Array(dataBaseName), tableName))
      .asInstanceOf[SparkTable]
      .getTable
      .asInstanceOf[FileStoreTable]
      .copy(options)
  } else {
    // Path-based: create table directly from filesystem
    val catalogContext =
      CatalogContext.create(Options.fromMap(options), sessionState.newHadoopConf())
    copyWithSQLConf(FileStoreTableFactory.create(catalogContext), extraOptions = options)
  }

  // Changelog mode wrapping
  if (Options.fromMap(options).get(SparkConnectorOptions.READ_CHANGELOG)) {
    new AuditLogTable(table)
  } else {
    table
  }
}
```

Two resolution strategies:
1. **Catalog-based**: When a `catalog` option is specified, the table is loaded through Spark's `TableCatalog` API, extracting the underlying `FileStoreTable` from the `SparkTable` wrapper.
2. **Path-based**: When no catalog is specified, a `FileStoreTable` is created directly from the filesystem path using `FileStoreTableFactory.create()`.

If `read.changelog=true` is set, the table is wrapped in an `AuditLogTable`, which exposes changelog data including a `_rowkind` column.

## Why V1 Read Is a Stub

Paimon uses the V2 DataSource API (`SparkTable` implementing `SupportsRead`) for all actual data reading. When Spark resolves a Paimon table through the catalog or via `SparkSource.getTable()`, it obtains a `SparkTable` instance that advertises `BATCH_READ` and `MICRO_BATCH_READ` capabilities. Spark's query planner uses the V2 scan path (`newScanBuilder()` -> `PaimonScanBuilder` -> `PaimonScan` -> `PaimonBatch`) to plan and execute reads.

The V1 `BaseRelation` is only produced as a side effect of the `CreatableRelationProvider.createRelation()` call, which is primarily a write operation. The returned `BaseRelation` is typically used only for schema inference in subsequent operations within the same DataFrame chain, not for data reading.

## Data Flow Summary

```
DataFrame.write.format("paimon").save()
    |
    v
SparkSource.createRelation(sqlContext, mode, parameters, data)
    |
    +-- loadTable(parameters) --> FileStoreTable
    |       |
    |       +-- catalog-based: sparkCatalog.loadTable() -> SparkTable -> FileStoreTable
    |       +-- path-based: FileStoreTableFactory.create()
    |       +-- if read.changelog: wrap in AuditLogTable
    |
    +-- WriteIntoPaimonTable(...).run()  [actual write happens here]
    |
    +-- toBaseRelation(table, sqlContext) --> BaseRelation (schema-only stub)
            |
            +-- sqlContext: SQLContext
            +-- schema: StructType (from table.rowType())
            +-- NO buildScan() -- no data reading capability
```

## Related Documentation

- [Spark DataSource V2 Read Path](spark-datasource-v2-read.md) -- the actual read implementation used by Paimon
- [Spark DataSource V1 Write Path](spark-datasource-v1-write.md) -- the default write path
- [Spark Integration Overview](spark-integration-overview.md) -- high-level integration summary
- [Spark Integration Architecture](spark-integration-architecture.md) -- module structure and shim pattern
