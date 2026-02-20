<!--
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
-->

# Hudi Spark Integration Architecture

This document describes the architecture of Apache Hudi's integration with Apache Spark,
covering the two complementary extension mechanisms, the DataFrame and SQL entry paths,
the write and read pipelines, and the current state of DataSource V1/V2 support.

## Table of Contents

1. [Integration Overview](#1-integration-overview)
2. [Two Complementary Extension Mechanisms](#2-two-complementary-extension-mechanisms)
3. [DataFrame Path vs SQL Path](#3-dataframe-path-vs-sql-path)
4. [Write Path](#4-write-path)
5. [Read Path](#5-read-path)
6. [DataSource V1 and V2 Support](#6-datasource-v1-and-v2-support)
7. [Module Architecture](#7-module-architecture)

---

## 1. Integration Overview

Hudi plugs into Spark through two independent but complementary mechanisms that together
provide full DataFrame API and SQL support:

| Mechanism | Spark Config | What It Enables |
|-----------|-------------|-----------------|
| **Service Provider Interface (SPI)** | Automatic via `META-INF/services` | `df.read.format("hudi")`, `df.write.format("hudi")`, Structured Streaming |
| **Session Extension** | `spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension` | SQL DDL/DML (`CREATE TABLE`, `INSERT INTO`, `MERGE INTO`, `UPDATE`, `DELETE`), stored procedures (`CALL`), custom SQL syntax, Catalyst analysis/optimizer rules |

Both mechanisms converge on the same core components: `HoodieSparkSqlWriter` for writes
and `HoodieFileIndex` / `HoodieBaseRelation` for reads.

```
User
 |
 +-- DataFrame API  -----> SPI (DefaultSource) -----+
 |                                                    |
 +-- SQL Statements -----> Session Extension ---------+----> HoodieSparkSqlWriter (write)
 |                         (parser + rules)           |----> HoodieFileIndex (read)
 +-- Structured Streaming -> SPI (DefaultSource) ----+
```

---

## 2. Two Complementary Extension Mechanisms

### 2.1 Service Provider Interface (SPI) - DataSource Registration

Hudi registers itself as a Spark data source through Java's standard SPI mechanism.

**SPI descriptor file:**
`hudi-spark-common/src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`

```
org.apache.hudi.BaseDefaultSource
org.apache.spark.sql.execution.datasources.parquet.LegacyHoodieParquetFileFormat
```

**Class hierarchy:**

```
DataSourceRegister          (Spark interface - enables format("hudi"))
  ^
  |
DefaultSource               (hudi-spark-common)
  |  implements: RelationProvider       -- read without schema
  |              SchemaRelationProvider  -- read with user-provided schema
  |              CreatableRelationProvider -- write via DataFrame API
  |              StreamSinkProvider      -- Structured Streaming sink
  |              StreamSourceProvider    -- Structured Streaming source
  ^
  |
BaseDefaultSource            (hudi-spark-common)
  |  extends DefaultSource
  |  registers shortName = "hudi"
  |  deliberately disables DS V2 APIs (see HUDI-4178)
```

**What SPI enables:**

```scala
// Read
spark.read.format("hudi").load(path)
spark.read.format("hudi").option("as.of.instant", "20240101").load(path)

// Write
df.write.format("hudi")
  .option("hoodie.table.name", "my_table")
  .mode(SaveMode.Append)
  .save(path)

// Structured Streaming
stream.writeStream.format("hudi").start(path)
spark.readStream.format("hudi").load(path)
```

When Spark encounters `format("hudi")`, it discovers `BaseDefaultSource` via SPI and
delegates to `DefaultSource`'s `createRelation()` (reads) or `createRelation(df)` (writes).

### 2.2 Session Extension - SQL Support via `spark.sql.extensions`

The `HoodieSparkSessionExtension` class implements `SparkSessionExtensions => Unit` and
injects Hudi-specific components into Spark's Catalyst pipeline:

```
HoodieSparkSessionExtension
  |
  +-- injectParser           --> HoodieCommonSqlParser
  +-- injectResolutionRule   --> HoodieAnalysis.customResolutionRules
  +-- injectPostHocResolutionRule --> HoodieAnalysis.customPostHocResolutionRules
  +-- injectOptimizerRule    --> HoodieAnalysis.customOptimizerRules
  +-- injectTableFunction    --> hudi_query, hudi_table_changes, hudi_query_timeline, ...
```

#### Parser Injection

`HoodieCommonSqlParser` wraps Spark's default parser with an ANTLR4 grammar
(`HoodieSqlCommon.g4`) that adds Hudi-specific SQL syntax for compaction, clustering,
procedures, and index operations. Standard SQL passes through to Spark's parser unchanged.

#### Analysis Rules (Resolution Phase)

These rules run during Spark's logical plan analysis to recognize and transform
Hudi-related operations:

| Rule | Purpose |
|------|---------|
| `AdaptIngestionTargetLogicalRelations` | Strips Hudi meta-fields (`_hoodie_commit_time`, etc.) from ingestion targets so INSERT/UPDATE/MERGE resolve correctly against user-visible columns |
| `HoodieSparkDataSourceV2ToV1Fallback` | Converts `DataSourceV2Relation` to `LogicalRelation` for Hudi tables since V2 Read API is not implemented (version-specific: `HoodieSpark35DataSourceV2ToV1Fallback`, etc.) |
| `ResolveReferences` | Resolves Hudi table-valued functions: `hudi_query()`, `hudi_table_changes()`, `hudi_query_timeline()`, `hudi_metadata()`, `hudi_filesystem_view()` |
| `ResolveColumnsForInsertInto` | Handles INSERT with a subset of columns, type coercion, and column alignment (version-specific) |
| `ResolveHudiAlterTableCommand` | Routes ALTER TABLE to Hudi implementations (version-specific) |
| `ResolveImplementationsEarly` | Converts fully-resolved Spark SQL plans to Hudi commands *before* Spark's `DataSourceAnalysis` runs, preventing native Spark implementations from taking over |

#### Post-Hoc Resolution Rules

These run after the main analysis phase:

| Rule | Purpose |
|------|---------|
| `ResolveImplementations` | Converts resolved Spark plans to Hudi command implementations: `MergeIntoTable` -> `MergeIntoHoodieTableCommand`, `UpdateTable` -> `UpdateHoodieTableCommand`, `DeleteFromTable` -> `DeleteHoodieTableCommand`, plus compaction/clustering/index commands |
| `HoodiePostAnalysisRule` | Converts DDL commands: `CreateDataSourceTableCommand` -> `CreateHoodieTableCommand`, `DropTableCommand` -> `DropHoodieTableCommand`, partition operations, column operations, etc. |
| `PostAnalysisRule` | Version-specific post-analysis handling |

#### Optimizer Rules

| Rule | Purpose |
|------|---------|
| `NestedSchemaPruning` | Prunes unnecessary nested columns from schema (version-specific) |
| `HoodiePruneFileSourcePartitions` | Partition pruning executed pre-CBO, replica of Spark's `PruneFileSourcePartitions` (needed because `SparkSessionExtensions` cannot inject into `customEarlyScanPushDownRules`) |

### 2.3 How They Complement Each Other

The two mechanisms serve distinct but complementary roles:

**SPI (DataSource Registration)** provides the *data plane* - the ability to physically
read from and write to Hudi tables through Spark's DataSource API:

- `format("hudi").load()` / `.save()` for batch operations
- Streaming sink/source for continuous processing
- No dependency on `spark.sql.extensions` configuration

**Session Extension** provides the *SQL control plane* - the ability to use standard
SQL syntax and leverage Spark's catalog integration:

- DDL: `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`
- DML: `INSERT INTO`, `MERGE INTO`, `UPDATE`, `DELETE`
- Custom syntax: `CALL run_compaction(...)`, `SHOW COMPACTION ON ...`
- Catalyst optimization rules for query performance

**Together** they ensure that whether a user writes DataFrame code or SQL statements,
the operations are correctly routed through Hudi's write/read infrastructure. SQL DML
commands are resolved by analysis rules into Hudi command objects that internally call
the same `HoodieSparkSqlWriter` that the DataFrame API uses.

```
                  SPI                          Session Extension
              (Data Plane)                    (SQL Control Plane)
           +----------------+              +---------------------+
           | DefaultSource  |              | Parser + Rules      |
           | - read/write   |              | - DDL resolution    |
           | - streaming    |              | - DML resolution    |
           +-------+--------+              | - optimizer rules   |
                   |                       +----------+----------+
                   |                                  |
                   |    SQL commands resolve to       |
                   |    Hudi command objects that      |
                   |    call into the SPI layer        |
                   |                                  |
                   +----------> Shared Core <---------+
                              - HoodieSparkSqlWriter (write)
                              - HoodieFileIndex (read)
                              - SparkRDDWriteClient (engine)
```

---

## 3. DataFrame Path vs SQL Path

### 3.1 DataFrame Path

The DataFrame API goes directly through the SPI:

```
df.write.format("hudi").mode(Append).save(path)
  |
  v
DefaultSource.createRelation(sqlContext, mode, params, df)
  |
  v
HoodieSparkSqlWriter.write(sqlContext, mode, params, df)
```

For reads:

```
spark.read.format("hudi").load(path)
  |
  v
DefaultSource.createRelation(sqlContext, params)
  |
  v
Relation factory creates: HadoopFsRelation or MergeOnReadSnapshotRelation
  (based on table type + query type)
```

### 3.2 SQL Path

SQL statements go through the Session Extension pipeline:

```
spark.sql("INSERT INTO hudi_table SELECT ...")
  |
  v
HoodieCommonSqlParser.parsePlan()     -- Parse phase
  |
  v
Catalyst Analysis (Spark built-in rules + Hudi injected rules)
  |
  v
ResolveImplementationsEarly           -- Convert to Hudi command
  |
  v
InsertIntoHoodieTableCommand.run()    -- Execute command
  |
  v
HoodieSparkSqlWriter.write(...)       -- Same writer as DataFrame path
```

### 3.3 Convergence

Both paths converge on `HoodieSparkSqlWriter.write()` for writes:

```
                    DataFrame API                         SQL DML
                         |                                   |
            DefaultSource.createRelation()     InsertIntoHoodieTableCommand.run()
                         |                     MergeIntoHoodieTableCommand.run()
                         |                     UpdateHoodieTableCommand.run()
                         |                     DeleteHoodieTableCommand.run()
                         |                     CreateHoodieTableAsSelectCommand.run()
                         |                                   |
                         +----------> HoodieSparkSqlWriter.write() <----------+
                                              |
                                              v
                                 DataSourceUtils.doWriteOperation()
                                              |
                                              v
                                    SparkRDDWriteClient
                                    (upsert / insert / bulkInsert / delete)
```

Both paths converge on `DefaultSource.createRelation()` for reads, producing
`HadoopFsRelation` (CoW) or `MergeOnReadSnapshotRelation` (MoR).

---

## 4. Write Path

### 4.1 Entry Points

| Entry Point | Trigger | Flow |
|-------------|---------|------|
| `DefaultSource.createRelation(df)` | `df.write.format("hudi").save()` | Calls `HoodieSparkSqlWriter.write()` |
| `InsertIntoHoodieTableCommand` | `INSERT INTO table ...` | Builds config, calls `HoodieSparkSqlWriter.write()` |
| `MergeIntoHoodieTableCommand` | `MERGE INTO table USING ...` | Reshapes source, calls `HoodieSparkSqlWriter.write()` with `ExpressionPayload` |
| `UpdateHoodieTableCommand` | `UPDATE table SET ...` | Builds config, calls `HoodieSparkSqlWriter.write()` |
| `DeleteHoodieTableCommand` | `DELETE FROM table WHERE ...` | Builds config, calls `HoodieSparkSqlWriter.write()` |
| `CreateHoodieTableAsSelectCommand` | `CREATE TABLE ... AS SELECT` | Initializes table, delegates to `InsertIntoHoodieTableCommand` with bulk insert |

### 4.2 HoodieSparkSqlWriter - Write Orchestrator

`HoodieSparkSqlWriter` (`hudi-spark-common`) is the unified write coordinator.
All write paths converge here.

**Write flow:**

```
HoodieSparkSqlWriter.write(sqlContext, mode, params, sourceDf)
  |
  +-- 1. Validate parameters and table existence
  +-- 2. Initialize/create HoodieTableMetaClient
  +-- 3. Deduce operation type (BULK_INSERT / INSERT / UPSERT / DELETE)
  +-- 4. Reconcile schema (schema evolution support)
  +-- 5. Create SparkRDDWriteClient
  +-- 6. Convert DataFrame -> RDD[HoodieRecord]
  +-- 7. Handle duplicates (for INSERT with dup policy)
  +-- 8. Delegate to DataSourceUtils.doWriteOperation()
  +-- 9. Commit and post-operations (compaction, clustering, meta sync)
```

**Operation type deduction:**

- If `INSERT_DROP_DUPS` or `INSERT_DUP_POLICY=drop/fail` and operation is `UPSERT` -> auto-corrects to `INSERT`
- If no `RECORDKEY_FIELD` set, no explicit operation, and no meta fields -> auto-selects `BULK_INSERT`
- Otherwise -> uses configured `hoodie.operation` (default: `UPSERT`)

**Row Writer path (alternative for bulk insert):**

When `hoodie.datasource.write.row.writer.enable=true` and operation is `BULK_INSERT`,
the write path bypasses RDD conversion and works directly on `Dataset[Row]` via
`BaseDatasetBulkInsertCommitActionExecutor`, providing better Spark SQL integration.

### 4.3 DataSourceUtils.doWriteOperation - Dispatch

```java
DataSourceUtils.doWriteOperation(client, hoodieRecords, instantTime, operation, isPrepped)
  |
  +-- BULK_INSERT        -> client.bulkInsert(records, instantTime, partitioner)
  +-- INSERT             -> client.insert(records, instantTime)
  +-- UPSERT             -> client.upsert(records, instantTime)
  +-- INSERT_OVERWRITE   -> client.insertOverwrite(records, instantTime)
  +-- INSERT_OVERWRITE_TABLE -> client.insertOverwriteTable(records, instantTime)
```

Note: DELETE operations are handled separately in `HoodieSparkSqlWriter.writeInternal()`,
which calls `DataSourceUtils.doDeleteOperation()` directly without going through
`doWriteOperation()`. Similarly, DELETE_PARTITION calls `DataSourceUtils.doDeletePartitionsOperation()`.

### 4.4 SparkRDDWriteClient - Engine-Level Write

`SparkRDDWriteClient` (`hudi-spark-client`) creates a `HoodieSparkTable` (CoW or MoR)
and invokes the appropriate action executor:

```
SparkRDDWriteClient.upsert(records, instantTime)
  |
  v
HoodieSparkTable.create(config, context)    -- CoW or MoR table
  |
  v
Action Executor (e.g., SparkUpsertCommitActionExecutor)
  |
  +-- 1. Tag records with file locations (index lookup)
  +-- 2. Build workload profile
  +-- 3. Get partitioner (determines file group assignment)
  +-- 4. Map partitions: merge old/new records via HoodieMergeHandle
  +-- 5. Write files (Parquet/ORC)
  +-- 6. Return WriteStatus per partition
```

### 4.5 Commit and Post-Operations

```
commitAndPerformPostOperations()
  |
  +-- 1. client.commit()           -- validate WriteStatuses, transition INFLIGHT -> COMPLETED
  +-- 2. scheduleCompaction()      -- if async compaction enabled (MoR tables)
  +-- 3. scheduleClustering()      -- if async clustering enabled
  +-- 4. metaSync()                -- sync to Hive/DataHub/Glue metastores
```

### 4.6 Write Path Diagram

The following diagram illustrates the full write path from entry points to execution
(see also `Hudi-Spark_write_path.jpg`):

```
+-------------------------------------------------------------------+
|                        ENTRY POINTS                                |
+-------------------------------------------------------------------+
|  DataFrame API              SQL DML                Streaming       |
|  df.write.format("hudi")   INSERT INTO             writeStream     |
|    .mode(Append)            MERGE INTO                .format      |
|    .save(path)              UPDATE ... SET            ("hudi")     |
|                             DELETE FROM                             |
|                             CREATE TABLE AS SELECT                  |
+-------------------------------------------------------------------+
                              |
                              v
+-------------------------------------------------------------------+
|              HoodieSparkSqlWriter.write()                          |
|  (hudi-spark-common, Scala)                                        |
|  - Validate params, init MetaClient                                |
|  - Deduce operation, reconcile schema                              |
|  - Convert DataFrame -> RDD[HoodieRecord]                          |
+-------------------------------------------------------------------+
                              |
                 +------------+-------------+
                 |                          |
           RDD path                   Row Writer path
           (upsert/insert/delete)     (bulk insert)
                 |                          |
                 v                          v
+-------------------------------+  +-------------------------------+
| DataSourceUtils               |  | DatasetBulkInsert             |
|   .doWriteOperation()         |  |   CommitActionExecutor        |
| dispatches to                 |  | works on Dataset[Row]         |
| SparkRDDWriteClient methods   |  | directly                      |
+-------------------------------+  +-------------------------------+
                 |                          |
                 v                          |
+-------------------------------+           |
| SparkRDDWriteClient           |           |
|  creates HoodieSparkTable     |           |
|  (CoW or MoR)                 |           |
+-------------------------------+           |
                 |                          |
                 v                          |
+-------------------------------+           |
| Action Executor               |           |
| - Tag records (index lookup)  |           |
| - Partition by file group     |           |
| - Merge old/new records       |           |
| - Write files                 |           |
+-------------------------------+           |
                 |                          |
                 +-----------+--------------+
                             |
                             v
+-------------------------------------------------------------------+
|                 Commit + Post-Operations                           |
|  - Transition INFLIGHT -> COMPLETED on timeline                    |
|  - Schedule async compaction (MoR)                                 |
|  - Schedule async clustering                                       |
|  - Metastore sync (Hive, Glue, DataHub)                           |
+-------------------------------------------------------------------+
```

---

## 5. Read Path

### 5.1 Entry Points

| Entry Point | Trigger | Result |
|-------------|---------|--------|
| `DefaultSource.createRelation()` | `spark.read.format("hudi").load(path)` | `HadoopFsRelation` or `MergeOnReadSnapshotRelation` |
| SQL `SELECT` | `SELECT * FROM hudi_table` | Resolved via catalog, same relation types |
| Table-valued functions | `SELECT * FROM hudi_query(...)` | Custom relations via `ResolveReferences` rule |
| Streaming source | `spark.readStream.format("hudi").load(path)` | `HoodieStreamSourceV1` or `V2` |

### 5.2 Query Type Routing

`DefaultSource.createRelation()` determines the query type from options and routes
to the appropriate relation factory:

```
DefaultSource.createRelation()
  |
  +-- Determine query type from hoodie.datasource.query.type:
  |     "snapshot" (default), "read_optimized", "incremental"
  |
  +-- Determine table type: CoW or MoR
  |
  +-- Route to factory:
        |
        +-- CoW + Snapshot      -> HoodieCopyOnWriteSnapshotHadoopFsRelationFactory
        +-- CoW + Read-Optimized -> HoodieCopyOnWriteSnapshotHadoopFsRelationFactory
        +-- CoW + Incremental   -> HoodieCopyOnWriteIncrementalHadoopFsRelationFactoryV1 (table version < 8)
        |                         HoodieCopyOnWriteIncrementalHadoopFsRelationFactoryV2 (table version >= 8)
        +-- CoW + CDC           -> HoodieCopyOnWriteCDCHadoopFsRelationFactory
        +-- MoR + Snapshot      -> HoodieMergeOnReadSnapshotHadoopFsRelationFactory
        +-- MoR + Read-Optimized -> HoodieCopyOnWriteSnapshotHadoopFsRelationFactory
        +-- MoR + Incremental   -> HoodieMergeOnReadIncrementalHadoopFsRelationFactoryV1 (table version < 8)
        |                         HoodieMergeOnReadIncrementalHadoopFsRelationFactoryV2 (table version >= 8)
        +-- MoR + CDC           -> HoodieMergeOnReadCDCHadoopFsRelationFactory
```

Note: MoR read-optimized queries use the CoW factory because they only read
base (columnar) files without merging log files.

### 5.3 File Indexing and Data Skipping

`HoodieFileIndex` is the core component for file listing and pruning:

```
HoodieFileIndex (implements Spark FileIndex)
  ^
  |
SparkHoodieTableFileIndex (Spark-specific file index)
  ^
  |
BaseHoodieTableFileIndex (Java, engine-agnostic)
```

**Multi-stage filtering pipeline:**

```
HoodieFileIndex.listFiles(partitionFilters, dataFilters)
  |
  +-- Stage 1: Partition pruning
  |     Filter partitions using WHERE clause predicates on partition columns
  |
  +-- Stage 2: Metadata-based data skipping (if enabled)
  |     Apply index chain (in order):
  |       1. RecordLevelIndexSupport   -- point lookups on record keys
  |       2. BucketIndexSupport        -- bucket index filtering
  |          (or PartitionBucketIndexSupport for partition-level simple bucket index)
  |       3. SecondaryIndexSupport     -- secondary index filtering
  |       4. ExpressionIndexSupport    -- functional index filtering
  |       5. BloomFiltersIndexSupport  -- bloom filter-based filtering
  |       6. ColumnStatsIndexSupport   -- min/max/null stats filtering
  |
  +-- Stage 3: Return pruned file list for Spark's scan planning
```

### 5.4 Relation Types

**Snapshot reads (CoW):**
- Uses `HadoopFsRelation` with `HoodieFileIndex`
- Spark's native file scanning (Parquet/ORC vectorized readers)
- No merge needed - base files are complete records

**Snapshot reads (MoR):**
- Uses `HadoopFsRelation` with `HoodieFileGroupReaderBasedFileFormat`
- Or `MergeOnReadSnapshotRelation` (for metadata table reads)
- Merges base files with log files at read time via `HoodieMergeOnReadRDDV2`
- Requires record key and pre-combine fields for accurate merging

**Incremental reads (batch):**
- Batch incremental queries go through `HoodieCopyOnWriteIncrementalHadoopFsRelationFactoryV1/V2`
  (CoW) or `HoodieMergeOnReadIncrementalHadoopFsRelationFactoryV1/V2` (MoR), selected
  based on table version (< 8 uses requested time via V1, >= 8 uses completion time via V2)
- Filters commits between START_COMMIT and END_COMMIT
- Returns only changed records

**Incremental reads (streaming):**
- `IncrementalRelationV1` (table version < 8, uses requested time) and
  `IncrementalRelationV2` (table version >= 8, uses completion time) are used within
  `HoodieStreamSourceV1` / `HoodieStreamSourceV2` for CoW streaming reads
- MoR streaming reads use `MergeOnReadIncrementalRelationV1/V2` respectively

**CDC reads:**
- Uses `HoodieCDCFileIndex` to extract change operations
- Returns operation type (INSERT/UPDATE/DELETE), before/after images, timestamp

### 5.5 Time Travel

```scala
spark.read.format("hudi")
  .option("as.of.instant", "20240101120000")
  .load(path)
```

- `SparkHoodieTableFileIndex` filters timeline to instants <= specified instant
- File index lists files as of that point in time
- Schema resolved from that instant's commit metadata

### 5.6 Streaming Read

```
DefaultSource.createSource()
  |
  +-- Table version < 8  -> HoodieStreamSourceV1 (uses requested time)
  +-- Table version >= 8 -> HoodieStreamSourceV2 (uses completion time)
```

Both implementations:
- Implement Spark's `Source` interface
- Manage checkpoints via `HoodieMetadataLog`
- Support offset modes: earliest, latest, or specific instant
- Return incremental data between offset ranges

### 5.7 Read Path Diagram

```
+-------------------------------------------------------------------+
|                        ENTRY POINTS                                |
+-------------------------------------------------------------------+
|  DataFrame API              SQL SELECT            Streaming Read   |
|  spark.read                 SELECT * FROM          readStream      |
|    .format("hudi")          hudi_table              .format        |
|    .load(path)              WHERE ...               ("hudi")       |
+-------------------------------------------------------------------+
                              |
                              v
+-------------------------------------------------------------------+
|              DefaultSource.createRelation()                        |
|  - Load table metadata (HoodieTableMetaClient)                     |
|  - Determine query type (snapshot/incremental/cdc)                 |
|  - Determine table type (CoW/MoR)                                  |
|  - Route to appropriate relation factory                           |
+-------------------------------------------------------------------+
                              |
              +---------------+----------------+
              |               |                |
         CoW Snapshot     MoR Snapshot     Incremental
              |               |                |
              v               v                v
+------------------+ +------------------+ +------------------+
| HadoopFsRelation | | HadoopFsRelation | | IncrementalRel   |
| + HoodieFileIndex| | + FileGroupReader| | V1 or V2         |
| (native Parquet/ | | (base + log merge| | (commit range    |
|  ORC reader)     | |  at read time)   | |  filtering)      |
+------------------+ +------------------+ +------------------+
              |               |                |
              +-------+-------+----------------+
                      |
                      v
+-------------------------------------------------------------------+
|              HoodieFileIndex (File Listing + Pruning)              |
|  - Partition pruning (predicate-based)                             |
|  - Data skipping via metadata indices:                             |
|    Record Index > Bucket Index (or Partition Bucket Index) >       |
|    Secondary Index > Expression Index > Bloom Filter >             |
|    Column Stats                                                    |
+-------------------------------------------------------------------+
                      |
                      v
+-------------------------------------------------------------------+
|              File Reading                                          |
|  - CoW: Spark native Parquet/ORC readers (vectorized)             |
|  - MoR: HoodieFileGroupReader merges base + log files             |
|  - Schema evolution via InternalSchema                             |
+-------------------------------------------------------------------+
```

---

## 6. DataSource V1 and V2 Support

### 6.1 Current State: DSv1 is Primary

Hudi currently uses **DataSource V1 as its primary integration path** for both reads
and writes. The DSv2 APIs are deliberately disabled in `BaseDefaultSource` (see HUDI-4178)
to avoid performance regressions.

### 6.2 DSv1 Implementation (Full Support)

`DefaultSource` implements all major DSv1 interfaces:

| Interface | Purpose | Used For |
|-----------|---------|----------|
| `RelationProvider` | Read without user schema | `spark.read.format("hudi").load()` |
| `SchemaRelationProvider` | Read with user schema | `spark.read.schema(s).format("hudi").load()` |
| `CreatableRelationProvider` | Write via DataFrame API | `df.write.format("hudi").save()` |
| `StreamSinkProvider` | Streaming writes | `writeStream.format("hudi").start()` |
| `StreamSourceProvider` | Streaming reads | `readStream.format("hudi").load()` |
| `DataSourceRegister` | Format registration | `format("hudi")` shortName |

**DSv1 read components:**
- `HoodieBaseRelation` extends `BaseRelation` + `FileRelation` + `PrunedFilteredScan` (column pruning + filter pushdown)
- `HadoopFsRelation` wraps `HoodieFileIndex` for file-based scanning
- `MergeOnReadSnapshotRelation` for MoR tables

**DSv1 write components:**
- `HoodieSparkSqlWriter` orchestrates writes
- `SparkRDDWriteClient` executes write operations via RDDs

### 6.3 DSv2 Implementation (Minimal/Selective)

Hudi has a *limited* DSv2 implementation used only for specific features:

**`HoodieInternalV2Table`** (`hudi-spark-common`):
- Implements `Table` + `SupportsWrite` + `V2TableWithV1Fallback`
- Advertises capabilities: `BATCH_READ`, `V1_BATCH_WRITE`, `OVERWRITE_BY_FILTER`, `TRUNCATE`, `ACCEPT_ANY_SCHEMA`
- **Does NOT implement V2 Read APIs** (no `ScanBuilder`, `Scan`, or `Batch`)
- Write path deliberately uses V1 via `HoodieV1WriteBuilder`

**`HoodieCatalog`** (`hudi-spark-common`):
- Implements `DelegatingCatalogExtension` + `StagingTableCatalog`
- Supports atomic table creation (`stageCreate`, `stageReplace`, `stageCreateOrReplace`)
- DDL operations: `createTable`, `dropTable`, `alterTable`, `renameTable`
- **Key decision point:** `loadTable()` returns V2 table only when Schema Evolution
  is enabled; otherwise returns V1 wrapper

```scala
// HoodieCatalog.loadTable() logic:
if (schemaEvolutionEnabled) {
  v2Table                    // Use V2 (triggers V2ToV1Fallback rule later)
} else {
  v2Table.v1TableWrapper     // Use V1 directly (no fallback needed)
}
```

### 6.4 V2-to-V1 Fallback Pattern

When `HoodieCatalog` returns a V2 table (Schema Evolution enabled), Spark's analyzer
creates `DataSourceV2Relation` nodes. Since Hudi has no V2 Read API, the
`HoodieSparkDataSourceV2ToV1Fallback` analysis rule intercepts these and converts
them back to V1 `LogicalRelation` nodes:

```
Spark Catalog resolves table as V2
  |
  v
DataSourceV2Relation(HoodieInternalV2Table)
  |
  v
HoodieSparkDataSourceV2ToV1Fallback (analysis rule)
  |
  v
LogicalRelation(HoodieBaseRelation)    -- V1 relation, uses V1 read path
```

This fallback is implemented per Spark version:
- `HoodieSpark33DataSourceV2ToV1Fallback`
- `HoodieSpark34DataSourceV2ToV1Fallback`
- `HoodieSpark35DataSourceV2ToV1Fallback`
- `HoodieSpark40DataSourceV2ToV1Fallback`

**Rule ordering is critical:** The V2ToV1Fallback must run before `ResolveReferences`
and before Spark's `DataSourceAnalysis` to prevent V2 nodes from being processed
by Spark's native V2 execution path.

### 6.5 Summary: DSv1 vs DSv2 by Operation

| Operation | API Used | Notes |
|-----------|----------|-------|
| Batch Read (snapshot) | **DSv1** always | `HadoopFsRelation` or `MergeOnReadSnapshotRelation` |
| Batch Read (incremental) | **DSv1** always | `IncrementalRelationV1/V2` |
| Batch Write (DataFrame) | **DSv1** always | `DefaultSource` -> `HoodieSparkSqlWriter` |
| Batch Write (SQL DML) | **DSv1** always | Hudi commands -> `HoodieSparkSqlWriter` |
| Streaming Read | **DSv1** | `HoodieStreamSourceV1/V2` |
| Streaming Write | **DSv1** | `HoodieStreamingSink` |
| Catalog Operations | **DSv2** (delegated) | `HoodieCatalog` -> `StagingTableCatalog` |
| Schema Evolution | **DSv2 with V1 fallback** | V2 table loaded, then converted to V1 at read time |

### 6.6 Why DSv1 Remains Primary

1. **Maturity**: The DSv1 implementation is well-tested and optimized
2. **Performance**: V2-to-V1 fallback has acknowledged "considerable performance impact"
3. **Complexity**: Full V2 requires implementing `ScanBuilder`, `Scan`, `Batch`,
   `WriteBuilder`, `DataWriter` - a significant rewrite
4. **Hudi's file-based model**: DSv1's `HadoopFsRelation` aligns well with Hudi's
   file group and file slice abstractions
5. **Selective adoption**: V2 catalog APIs are used where they add value
   (atomic DDL, schema evolution) without requiring a complete V2 migration

---

## 7. Module Architecture

### 7.1 Module Dependency Tree

```
hudi-spark-common  (shared across all Spark versions)
  |  Core: DefaultSource, HoodieSparkSqlWriter, HoodieFileIndex,
  |        HoodieBaseRelation, HoodieCatalog, DataSource options
  |
  +-- hudi-spark3-common  (shared across Spark 3.x)
  |     |  DML commands: INSERT, MERGE, UPDATE, DELETE, CTAS
  |     |  BaseSpark3Adapter, partition pruning
  |     |
  |     +-- hudi-spark3.3.x  (Spark 3.3 adapter)
  |     +-- hudi-spark3.4.x  (Spark 3.4 adapter)
  |     +-- hudi-spark3.5.x  (Spark 3.5 adapter - default)
  |
  +-- hudi-spark4-common  (shared across Spark 4.x)
  |     |  DML commands adapted for Spark 4 API
  |     |  BaseSpark4Adapter
  |     |
  |     +-- hudi-spark4.0.x  (Spark 4.0 adapter)
  |
  +-- hudi-spark  (main module, version-independent)
        HoodieSparkSessionExtension, HoodieAnalysis,
        HoodieCommonSqlParser, stored procedures,
        CALL framework, bootstrap providers
```

### 7.2 Version-Specific Adapters

Each Spark version adapter provides:

| Component | Example (Spark 3.5) |
|-----------|-------------------|
| Adapter class | `Spark3_5Adapter` |
| Analysis rules | `HoodieSpark35DataSourceV2ToV1Fallback`, `HoodieSpark35ResolveColumnsForInsertInto` (in `HoodieSpark35Analysis.scala`) |
| SQL parser | `HoodieSpark3_5ExtendedSqlParser` |
| AST builder | `HoodieSpark3_5ExtendedSqlAstBuilder` |
| File scan RDD | `Spark35HoodieFileScanRDD` |
| Avro serde | `HoodieSpark3_5AvroSerializer/Deserializer` |
| Parquet reader | `Spark35ParquetReader` |
| ORC reader | `Spark35OrcReader` |
| Schema utils | `HoodieSpark35SchemaUtils` |
| Catalyst utils | `HoodieSpark35CatalystExpressionUtils` |
| V2ToV1 fallback | `HoodieSpark35DataSourceV2ToV1Fallback` |
| Partition pruning | `Spark3HoodiePruneFileSourcePartitions` |

### 7.3 Spark Version Support Matrix

| Spark | Module | Scala | Java | Build Profile |
|-------|--------|-------|------|---------------|
| 3.3.x | `hudi-spark3.3.x` | 2.12 | 11+ | `-Dspark3.3` |
| 3.4.x | `hudi-spark3.4.x` | 2.12 | 11+ | `-Dspark3.4` |
| 3.5.x | `hudi-spark3.5.x` | 2.12, 2.13 | 11+ | `-Dspark3.5` (default) |
| 4.0.x | `hudi-spark4.0.x` | 2.13 | 17+ | `-Dspark4.0` |
