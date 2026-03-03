<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# RFC-98: Spark Datasource V2 Read

## Proposers

- @geserdugarov

## Approvers

- @yihua
- @vinothchandar

## Status

GH Discussion: https://github.com/apache/hudi/discussions/13955

## Abstract

Data source is one of the foundational APIs in Spark, with two major versions known as "V1" and "V2". 
The representation of a read in the physical plan differs depending on the API version used.
Adopting the V2 API is essential for enhanced control over the data source, deeper integration with the Spark optimizer, and improved overall performance.

First steps towards integrating of Spark Datasource V2 were taken in [RFC-38](../rfc-38/rfc-38.md). 
However, there are multiple issues with advertising Hudi table as V2 without actual implementing certain API, and with using custom relation rule to fall back to V1 API.
As a result, the current implementation of `HoodieCatalog` and `BaseDefaultSource` returns a `V1Table` instead of `HoodieInternalV2Table`,
in order to [address performance regression](https://github.com/apache/hudi/pull/5737).

There was [an attempt](https://github.com/apache/hudi/pull/6442) to implement Spark Datasource V2 read functionality as a regular task, 
but it failed due to the scope of work required.
Therefore, this RFC proposes to discuss design of Spark Datasource V2 integration in advance and to continue working on it accordingly.

## Background

The current implementation of Spark Datasource V2 integration is presented in the schema below:

![Current integration with Spark](initial_integration_with_Spark.jpg)

## Implementation

The approach is hybrid: DSv2 for reads, DSv1 fallback for writes (`V2TableWithV1Fallback`).

Overall proposed architecture for this hybrid approach is shown in the following schema:

![Proposed approach with hybrid V1 write and V2 read](integration_with_DSv2_read.jpg)

### DataFrame API

A new SPI short name `"hudi_v2"` activates the DSv2 path for reading using Spark DataFrame API. 
The existing `"hudi"` path remains unchanged.

<table>
<tr>
<th>Operation</th>
<th>Current implementation</th>
<th>Additional functionality proposed in this RFC</th>
</tr>
<tr>
<td>Write</td>
<td>
<pre>
df.write.format("hudi").mode(...).save(path)
        v
BaseDefaultSource (V1) -> DefaultSource
        v
CreatableRelationProvider.createRelation(...)
        v
HoodieSparkSqlWriter.write(...)
        v
SparkRDDWriteClient -> upsert/insert/bulk_insert
</pre>
</td>
<td>
<pre>
df.write.format("hudi_v2").mode(...).save(path)
        v
HoodieDataSourceV2 (TableProvider + DataSourceRegister + CreatableRelationProvider)
        v
Spark treats as V1 source for writes
        v
CreatableRelationProvider.createRelation(...)
        v
HoodieSparkSqlWriter.write(...)
        v
SparkRDDWriteClient -> upsert/insert/bulk_insert
</pre>
</td>
</tr>
<tr>
<td>Read</td>
<td>
<pre>
spark.read.format("hudi").load(path)
        v
V1 DataSource resolution (via ServiceLoader + DataSourceRegister)
        v
BaseDefaultSource found
(extends DefaultSource with DataSourceRegister)
(not a TableProvider)
        v
Spark treats as V1 DataSource
        v
DefaultSource.createRelation(...)
        v
MergeOnReadSnapshotRelation / BaseRelation
        v
LogicalRelation -> FileScan -> ...
</pre>
</td>
<td>
<pre>
spark.read.format("hudi_v2").load(path)
        v
DataSourceV2Utils.lookupProvider("hudi_v2")
        v
HoodieDataSourceV2 found
(extends TableProvider with DataSourceRegister)
(does not extend SupportsCatalogOptions)
        v
Spark uses TableProvider.getTable() directly
(no catalog routing since no SupportsCatalogOptions)
        v
HoodieDataSourceV2.getTable(...)
        v
HoodieSparkV2Table(...)
(no catalogTable, no tableIdentifier)
        v
HoodieScanBuilder -> HoodieBatchScan -> ...
</pre>
</td>
</tr>
</table>

### SQL Queries

Spark SQL API is managed by new configuration parameter `hoodie.datasource.read.use.v2`, which controls the returned table type.

<table>
<tr>
<th>Operation</th>
<th>Current implementation</th>
<th>Additional functionality proposed in this RFC</th>
</tr>
<tr>
<td>Write</td>
<td>
<pre>
INSERT INTO hudi_table VALUES (...);   -- table created with USING hudi
        v
Spark Analyzer resolves table via catalog
        v
HoodieCatalog.loadTable(Identifier("hudi_table"))
        v
isHoodieTable => true, v2ReadEnabled = false, schemaEvol = false
        v
RETURNS: V1Table(catalogTable) via v1TableWrapper
        v
Spark V1 write path -> InsertIntoHoodieTableCommand (analysis rule)
        v
HoodieSparkSqlWriter.write(...)
</pre>
</td>
<td>
<pre>
INSERT INTO hudi_table VALUES (...);   -- table created with USING hudi
        v
Spark Analyzer resolves table via catalog
        v
HoodieCatalog.loadTable(Identifier("hudi_table"))
        v
isHoodieTable => true, v2ReadEnabled = true
        v
RETURNS: HoodieSparkV2Table(...)
        v
SupportsWrite.newWriteBuilder() -> HoodieV1WriteBuilder
        v
V1Write -> InsertableRelation.insert(data, overwrite)
        v
Align columns (rename + cast to table's user schema)
        v
HoodieSparkSqlWriter.write(...)
</pre>
</td>
</tr>
<tr>
<td>Read</td>
<td>
<pre>
SELECT * FROM hudi_table;   -- table created with USING hudi
        v
Spark Analyzer resolves table name via catalog
        v
HoodieCatalog.loadTable(Identifier("hudi_table"))
        v
super.loadTable(ident)
        v
V1Table(catalogTable) where catalogTable.provider = "hudi"
        v
isHoodieTable(catalogTable) => true
        v
v2ReadEnabled = false, schemaEvolutionEnabled = false (defaults)
        v
RETURNS: HoodieInternalV2Table(...).v1TableWrapper = V1Table(catalogTable)
        v
Spark uses V1 fallback -> DefaultSource.createRelation()
        v
HoodieFileIndex -> FileScan -> ...
</pre>
</td>
<td>
<pre>
SELECT * FROM hudi_table;   -- table created with USING hudi
        v
Spark Analyzer resolves table name via catalog
        v
HoodieCatalog.loadTable(Identifier("hudi_table"))
        v
super.loadTable(ident)
        v
V1Table(catalogTable) where catalogTable.provider = "hudi"
        v
isHoodieTable(catalogTable) => true
        v
v2ReadEnabled = conf("hoodie.datasource.read.use.v2") = true
        v
RETURNS: HoodieSparkV2Table(...)
        v
SupportsRead.newScanBuilder() -> HoodieScanBuilder
        v
HoodieBatchScan -> ...
</pre>
</td>
</tr>
</table>

### Read

All new classes go into package `org.apache.spark.sql.hudi.v2` inside `hudi-spark-common`.

| Class | Spark Interface | Responsibility |
|-------|-----------------|----------------|
| `HoodieDataSourceV2` | `TableProvider`, `DataSourceRegister`, `CreatableRelationProvider` | SPI entry point for `format("hudi_v2")`. `CreatableRelationProvider` enables DataFrame API writes via `df.write.format("hudi_v2")`. |
| `HoodieSparkV2Table` | `Table`, `SupportsRead`, `SupportsWrite`, `V2TableWithV1Fallback` | Routes reads to DSv2, writes to DSv1 fallback via `HoodieV1WriteBuilder`. |
| `HoodieScanBuilder` | `ScanBuilder`, `SupportsPushDownFilters`, `SupportsPushDownRequiredColumns` | Collects filter and column pruning pushdowns. |
| `HoodieBatchScan` | `Scan`, `Batch` | Plans input partitions using existing `HoodieFileIndex`. |
| `HoodieInputPartition` | `InputPartition` | Serializable descriptor for file slices. |
| `HoodiePartitionReaderFactory` | `PartitionReaderFactory` | Creates readers on executors. |
| `HoodiePartitionReader` | `PartitionReader[InternalRow]` | Delegates to existing file-reading code from `HoodieBaseRelation`. |
| `HoodieV1WriteBuilder` (reused) | `SupportsTruncate`, `SupportsOverwrite`, `ProvidesHoodieConfig` | Existing V1 write fallback builder from `HoodieInternalV2Table`, reused by `HoodieSparkV2Table`. |

### Table services

Table services (compaction, clustering, cleaning) are not affected by this change.
They operate via the write client and are triggered independently of the read path.

### Implementation phases

1. **Coexistence POC.** All new classes return empty read results, SPI registration, reuse of `HoodieV1WriteBuilder` for V1 write fallback, `hoodie.datasource.read.use.v2` config, `HoodieV1OrV2Table` extractor update in `HoodieSparkBaseAnalysis` to recognize `HoodieSparkV2Table` for DDL operations.
2. **CoW snapshot read.** Wire `HoodieBatchScan.planInputPartitions()` to `HoodieFileIndex`, implement base file reading in `HoodiePartitionReader`. Column pruning support.
3. **Filter pushdown.** Implement `HoodieScanBuilder.pushFilters()` for partition pruning and data skipping via `HoodieFileIndex`.
4. **MoR snapshot read.** Extend `HoodiePartitionReader` with base + log merge logic, reusing `HoodieFileGroupReader`.
5. **Incremental and CDC queries.** Route based on query type option in `HoodieScanBuilder`.
6. **Advanced pushdowns.** `SupportsPushDownAggregates`, `SupportsPushDownLimit`, `SupportsPushDownTopN`.

## Rollout/Adoption Plan

- The existing `format("hudi")` path is completely untouched, so there is no regression risk.
- For DataFrame API, users opt in by using `format("hudi_v2")`. No config needed.
- For SQL queries, users set `hoodie.datasource.read.use.v2=true` to route reads through DSv2.
- Rollback: switch back to `format("hudi")` or set the config to `false`.

## Test Plan

- Verify that `EXPLAIN` plans show `BatchScanExec` (DSv2) instead of `FileSourceScanExec` (DSv1) when DSv2 is enabled.
- Existing unit and functional tests must pass unchanged (no regressions in DSv1 path).
- New tests for DSv2 read path: CoW snapshot, MoR snapshot, filter pushdown, column pruning.
- TPC-H benchmark to compare DSv1 vs DSv2 read performance at each implementation phase.
