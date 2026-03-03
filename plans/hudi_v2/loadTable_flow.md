# How `HoodieCatalog::loadTable` Is Called

## Key Facts

- `isHoodieTable(provider)` only matches `"hudi"` (case-insensitive), NOT `"hudi_v2"`
- `HoodieDataSourceV2` does NOT implement `SupportsCatalogOptions` => DataFrame API with "hudi_v2" bypasses the catalog entirely
- `BaseDefaultSource` (format "hudi") is pure V1 (`TableProvider` is commented out) => DataFrame API with "hudi" bypasses catalog entirely
- For SQL, tables are registered with a provider (typically "hudi") during `CREATE TABLE ... USING hudi`

## Master Diagram: Is `loadTable` Called?

```
                        +--------------------+
                        |  User Operation    |
                        +--------+-----------+
                                 |
                +----------------+----------------+
                |                                 |
        DataFrame API                        Spark SQL
    .format(...).load(path)              SELECT/INSERT INTO tbl
    df.write.format(...).save()
                |                                 |
        +-------+-------+                        |
        |               |              +---------+---------+
   "hudi"          "hudi_v2"           |                   |
        |               |         provider="hudi"   provider="hudi_v2"
        |               |             |                    |
  BaseDefaultSource  HoodieDataSourceV2                    |
  (pure V1, no       (TableProvider)   |                   |
   TableProvider)         |       HoodieCatalog        HoodieCatalog
        |                 |       .loadTable()         .loadTable()
        |                 |            |                   |
        |                 |      isHoodieTable?       isHoodieTable?
        |                 |        => TRUE              => FALSE
        |                 |            |                   |
   loadTable          loadTable    +---+---+          case t => t
   NOT CALLED         NOT CALLED   | Config|          (passthrough,
                                   | check |           likely fails)
                                   +---+---+
                                       |
                          +------------+------------+
                          |            |            |
                     use.v2=true  schemaEvol=true  default
                          |            |            |
                   HoodieSparkV2  HoodieInternal   V1Table
                      Table        V2Table       (v1TableWrapper)
```

## All 16 Combinations

### Legend

- **loadTable called?** - Whether `HoodieCatalog::loadTable` is invoked
- **Hudi-matched?** - Whether `isHoodieTable` returns true inside loadTable
- **Returns** - What table/relation type is used

```
+-----+-------+---------------+----------+---------+---------+---------------+---------------------------+
|  #  | R/W   | API           | Format   | use.v2  | loadTbl | Hudi-matched? | Result                    |
|     |       |               |          |         | called? |               |                           |
+-----+-------+---------------+----------+---------+---------+---------------+---------------------------+
|  1  | READ  | DataFrame API | hudi     | true    |   NO    |      -        | V1 RelationProvider       |
|  2  | READ  | DataFrame API | hudi     | false   |   NO    |      -        | V1 RelationProvider       |
|  3  | READ  | DataFrame API | hudi_v2  | true    |   NO    |      -        | HoodieSparkV2Table (stub) |
|  4  | READ  | DataFrame API | hudi_v2  | false   |   NO    |      -        | HoodieSparkV2Table (stub) |
+-----+-------+---------------+----------+---------+---------+---------------+---------------------------+
|  5  | READ  | Spark SQL     | hudi     | true    |  YES    |     YES       | HoodieSparkV2Table (stub) |
|  6  | READ  | Spark SQL     | hudi     | false   |  YES    |     YES       | V1Table (full data)       |
|  7  | READ  | Spark SQL     | hudi_v2  | true    |  YES    |      NO       | passthrough (won't work)  |
|  8  | READ  | Spark SQL     | hudi_v2  | false   |  YES    |      NO       | passthrough (won't work)  |
+-----+-------+---------------+----------+---------+---------+---------------+---------------------------+
|  9  | WRITE | DataFrame API | hudi     | true    |   NO    |      -        | V1 CreatableRelProvider   |
| 10  | WRITE | DataFrame API | hudi     | false   |   NO    |      -        | V1 CreatableRelProvider   |
| 11  | WRITE | DataFrame API | hudi_v2  | true    |   NO    |      -        | V1 CreatableRelProvider   |
| 12  | WRITE | DataFrame API | hudi_v2  | false   |   NO    |      -        | V1 CreatableRelProvider   |
+-----+-------+---------------+----------+---------+---------+---------------+---------------------------+
| 13  | WRITE | Spark SQL     | hudi     | true    |  YES    |     YES       | V2Table -> V1 fallback    |
| 14  | WRITE | Spark SQL     | hudi     | false   |  YES    |     YES       | V1Table -> V1 write       |
| 15  | WRITE | Spark SQL     | hudi_v2  | true    |  YES    |      NO       | passthrough (won't work)  |
| 16  | WRITE | Spark SQL     | hudi_v2  | false   |  YES    |      NO       | passthrough (won't work)  |
+-----+-------+---------------+----------+---------+---------+---------------+---------------------------+

(*) Cases 11-12 are supported via CreatableRelationProvider on HoodieDataSourceV2.
    Spark treats DataSourceRegister+TableProvider as V1 source for writes, so the
    CreatableRelationProvider.createRelation() method is called, delegating to
    HoodieSparkSqlWriter.write().
```

### Notes on "format" in SQL context:
- SQL doesn't use `.format()`. The "format" is the **provider** set at `CREATE TABLE ... USING hudi|hudi_v2`.
- In practice, all SQL tables use provider `"hudi"`. Using `"hudi_v2"` as provider is not a standard workflow.

## Detailed Flow for Each Realistic Scenario

### Case 1-2: READ + DataFrame API + "hudi" (use.v2 irrelevant)

```
spark.read.format("hudi").load(path)
        |
        v
DataSourceV2Utils.lookupProvider("hudi")
        |
        v
BaseDefaultSource found
(extends DefaultSource with DataSourceRegister)
(NOT a TableProvider - commented out)
        |
        v
Spark treats as V1 DataSource
        |
        v
DefaultSource.createRelation(spark, params)
        |
        v
MergeOnReadSnapshotRelation / BaseRelation
        |
        v
LogicalRelation -> FileScan -> real data

loadTable: NOT CALLED (pure V1 path, no catalog involved)
use.v2 config: IGNORED
```

### Case 3-4: READ + DataFrame API + "hudi_v2" (use.v2 irrelevant)

```
spark.read.format("hudi_v2").load(path)
        |
        v
DataSourceV2Utils.lookupProvider("hudi_v2")
        |
        v
HoodieDataSourceV2 found
(extends TableProvider with DataSourceRegister)
(does NOT extend SupportsCatalogOptions)
        |
        v
Spark uses TableProvider.getTable() directly
(no catalog routing since no SupportsCatalogOptions)
        |
        v
HoodieDataSourceV2.getTable(schema, partitioning, properties)
        |
        v
HoodieSparkV2Table(SparkSession.active, path)
(no catalogTable, no tableIdentifier)
        |
        v
HoodieScanBuilder -> HoodieBatchScan -> empty partitions (STUB)

loadTable: NOT CALLED (TableProvider path, no catalog)
use.v2 config: IGNORED (always V2 via TableProvider)
```

### Case 5: READ + Spark SQL + "hudi" + use.v2=true

```
SELECT * FROM hudi_table;   -- table created with USING hudi
        |
        v
Spark Analyzer resolves table name via catalog
        |
        v
HoodieCatalog.loadTable(Identifier("hudi_table"))
        |
        v
super.loadTable(ident)
        |
        v
V1Table(catalogTable) where catalogTable.provider = "hudi"
        |
        v
isHoodieTable(catalogTable) => TRUE ("hudi" matches)
        |
        v
v2ReadEnabled = conf("hoodie.datasource.read.use.v2") = true
        |
        v
RETURNS: HoodieSparkV2Table(spark, path, Some(catalogTable), Some(ident))
        |
        v
SupportsRead.newScanBuilder() -> HoodieScanBuilder
        |
        v
HoodieBatchScan -> planInputPartitions() -> Array.empty
        |
        v
Result: correct schema, EMPTY DATA (stub)
```

### Case 6: READ + Spark SQL + "hudi" + use.v2=false (DEFAULT)

```
SELECT * FROM hudi_table;   -- table created with USING hudi
        |
        v
Spark Analyzer resolves table name via catalog
        |
        v
HoodieCatalog.loadTable(Identifier("hudi_table"))
        |
        v
super.loadTable(ident)
        |
        v
V1Table(catalogTable) where catalogTable.provider = "hudi"
        |
        v
isHoodieTable(catalogTable) => TRUE
        |
        v
v2ReadEnabled = false, schemaEvolutionEnabled = false (defaults)
        |
        v
RETURNS: HoodieInternalV2Table(...).v1TableWrapper = V1Table(catalogTable)
        |
        v
Spark uses V1 fallback -> DefaultSource.createRelation()
        |
        v
HoodieFileIndex -> FileScan -> REAL DATA
```

### Case 9-10: WRITE + DataFrame API + "hudi" (use.v2 irrelevant)

```
df.write.format("hudi").mode(...).save(path)
        |
        v
BaseDefaultSource (V1) -> DefaultSource
        |
        v
CreatableRelationProvider.createRelation(spark, mode, params, df)
        |
        v
HoodieSparkSqlWriter.write(spark, mode, params, df)
        |
        v
SparkRDDWriteClient -> upsert/insert/bulk_insert

loadTable: NOT CALLED
```

### Case 11-12: WRITE + DataFrame API + "hudi_v2"

```
df.write.format("hudi_v2").mode(...).save(path)
        |
        v
HoodieDataSourceV2 (TableProvider + DataSourceRegister + CreatableRelationProvider)
        |
        v
Spark treats as V1 source for writes
        |
        v
CreatableRelationProvider.createRelation(sqlContext, mode, params, df)
        |
        v
HoodieSparkSqlWriter.write(sqlContext, mode, params, df)
        |
        v
SparkRDDWriteClient -> upsert/insert/bulk_insert

loadTable: NOT CALLED
```

### Case 13: WRITE + Spark SQL + "hudi" + use.v2=true

```
INSERT INTO hudi_table VALUES (...);   -- table created with USING hudi
        |
        v
Spark Analyzer resolves table via catalog
        |
        v
HoodieCatalog.loadTable(Identifier("hudi_table"))
        |
        v
isHoodieTable => TRUE, v2ReadEnabled = true
        |
        v
RETURNS: HoodieSparkV2Table(spark, path, Some(catalogTable), Some(ident))
        |
        v
SupportsWrite.newWriteBuilder() -> HoodieV1WriteBuilder
        |
        v
V1Write -> InsertableRelation.insert(data, overwrite)
        |
        v
Align columns (rename + cast to table's user schema)
        |
        v
HoodieSparkSqlWriter.write() (direct call with catalog schema)
        |
        v
HoodieSparkSqlWriter.cleanup()
```

### Case 14: WRITE + Spark SQL + "hudi" + use.v2=false (DEFAULT)

```
INSERT INTO hudi_table VALUES (...);   -- table created with USING hudi
        |
        v
Spark Analyzer resolves table via catalog
        |
        v
HoodieCatalog.loadTable(Identifier("hudi_table"))
        |
        v
isHoodieTable => TRUE, v2ReadEnabled = false, schemaEvol = false
        |
        v
RETURNS: V1Table(catalogTable) via v1TableWrapper
        |
        v
Spark V1 write path -> InsertIntoHoodieTableCommand (analysis rule)
        |
        v
HoodieSparkSqlWriter.write(...)
```

## `loadTable` Internal Decision Tree

```
loadTable(ident)
    |
    v
super.loadTable(ident) -----> non-V1Table? -----> return as-is
    |
    v
V1Table(catalogTable)
    |
    v
isHoodieTable(catalogTable)?
    |               |
    NO             YES
    |               |
    v               v
return t      +-----+-----+-----+
(passthrough) |           |     |
              v           v     v
         use.v2=T    schEvol  default
              |         =T      |
              v          |      v
      HoodieSparkV2      |   v2Table.v1TableWrapper
         Table            |   (V1Table)
                          v
                   HoodieInternalV2Table
                   (V2 with V1 fallback)
```

## Key Source Files

| File | Role |
|------|------|
| `hudi-spark-datasource/hudi-spark-common/.../catalog/HoodieCatalog.scala:124-167` | `loadTable` implementation |
| `hudi-spark-datasource/hudi-spark-common/.../v2/HoodieDataSourceV2.scala` | "hudi_v2" TableProvider (DataFrame path) |
| `hudi-spark-datasource/hudi-spark-common/.../BaseDefaultSource.scala` | "hudi" V1 source (DataFrame path) |
| `hudi-client/hudi-spark-client/.../SparkAdapter.scala:145-147` | `isHoodieTable` - only matches "hudi" |
| `hudi-spark-datasource/hudi-spark-common/.../v2/HoodieSparkV2Table.scala` | DSv2 table (SupportsRead + SupportsWrite) |
| `hudi-spark-datasource/hudi-spark-common/.../catalog/HoodieInternalV2Table.scala` | Internal V2 table (SupportsWrite only) |
| `hudi-spark-datasource/hudi-spark-common/.../DataSourceOptions.scala:302-308` | `USE_V2_READ` config definition |
