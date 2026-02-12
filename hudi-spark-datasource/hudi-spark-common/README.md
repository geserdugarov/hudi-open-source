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

# `hudi-spark-common` Module

This module contains the core Spark integration code shared across all Spark versions (3.x and 4.x). It provides the foundational DataSource API implementations, file indexing, index support, query relations, and streaming integrations.

## Overview

The `hudi-spark-common` module serves as the critical bridge between Apache Hudi's core table format and Spark SQL. It provides:

- DataSource V1/V2 implementations for reading and writing Hudi tables
- File indexing with partition pruning and data skipping
- Multiple index support types for query optimization
- Relations for different query types (snapshot, incremental, time-travel)
- Streaming source and sink implementations
- CDC (Change Data Capture) support

## Directory Structure

```
src/main/
├── java/org/apache/hudi/
│   ├── async/                          # Async compaction/clustering services
│   ├── commit/                         # Commit action executors
│   ├── internal/                       # DataSource V2 base classes
│   └── spark/internal/                 # Spark-specific DataSource V2 implementation
├── scala/org/apache/
│   ├── hudi/                           # Core datasource classes
│   │   ├── cdc/                        # Change Data Capture support
│   │   └── util/                       # Utilities
│   └── spark/sql/
│       ├── catalyst/                   # Catalyst extensions
│       │   ├── catalog/                # Catalog integration
│       │   └── plans/logical/          # Logical plan extensions
│       ├── execution/datasources/      # File format readers
│       │   ├── parquet/
│       │   ├── orc/
│       │   └── lance/
│       └── hudi/
│           ├── analysis/               # Query analysis rules
│           ├── catalog/                # HoodieCatalog implementation
│           ├── command/                # DDL/DML commands
│           └── streaming/              # Structured Streaming support
└── resources/
```

## Key Components

### DataSource Entry Points

| Class | Description |
|-------|-------------|
| **DefaultSource** | Main Spark DataSource V1 entry point. Implements `RelationProvider`, `CreatableRelationProvider`, and `StreamSinkProvider`. |
| **BaseDefaultSource** | Base class for DataSource implementations with schema inference and option handling. |
| **HoodieDataSourceInternalTable** | DataSource V2 table implementation for internal write operations. |

### File Index & Query Optimization

| Class | Description |
|-------|-------------|
| **HoodieFileIndex** | Core file index for Hudi tables. Handles partition pruning, file listing, and supports timestamp partition key generators. |
| **SparkHoodieTableFileIndex** | Extended file index with Spark-specific table file handling and metadata table integration. |
| **HoodieIncrementalFileIndex** | File index for incremental queries with time-travel support. |
| **HoodieCDCFileIndex** | File index for CDC queries, extracting CDC file splits with operation metadata. |

### Index Support Classes

| Class | Description |
|-------|-------------|
| **SparkBaseIndexSupport** | Abstract base for all index support implementations. Provides candidate file computation based on query filters with fallback/strict failure modes. |
| **BloomFiltersIndexSupport** | Bloom filter-based file pruning. Filters files that may contain specific record keys. |
| **ColumnStatsIndexSupport** | Column statistics-based pruning. Skips files based on min/max column values. |
| **RecordLevelIndexSupport** | Record-level index lookups. Maps record keys directly to file locations. |
| **BucketIndexSupport** | Hash-based bucket index support for bucketed tables. |
| **PartitionStatsIndexSupport** | Partition-level statistics for coarse-grained pruning. |
| **SecondaryIndexSupport** | Secondary index support framework. |
| **ExpressionIndexSupport** | Functional expression-based indexing. |

### Query Relations

| Class | Description |
|-------|-------------|
| **HoodieBaseRelation** | Base relation supporting Spark SQL queries with schema evolution and merge-on-read operations. |
| **BaseFileOnlyRelation** | Relation for read-optimized snapshots (base files only, no log files). |
| **MergeOnReadSnapshotRelation** | Snapshot relation for MOR tables, merging base files with log files. |
| **IncrementalRelationV1/V2** | Incremental view relations for CoW tables with time-range filtering. |
| **MergeOnReadIncrementalRelationV1/V2** | Incremental view for MOR tables. |
| **TimelineRelation** | Relation for querying Hudi timeline metadata. |

### Write Path

| Class | Description |
|-------|-------------|
| **HoodieSparkSqlWriter** | Main entry point for Spark SQL writes. Orchestrates insert, update, upsert, and delete operations. |
| **HoodieStreamingSink** | Streaming sink for continuous ingestion with micro-batch writes. |
| **HoodieWriterUtils** | Utilities for write operations including schema validation and key generation. |
| **DatasetBulkInsertCommitActionExecutor** | Executor for bulk insert commit operations. |

### CDC Support

| Class | Description |
|-------|-------------|
| **HoodieCDCFileIndex** | File index for CDC queries with operation metadata extraction. |
| **CDCFileGroupIterator** | Iterator for reading CDC file groups. Supports INSERT, UPDATE, DELETE operations with spillable map support. |
| **InternalRowToJsonStringConverter** | Converts Spark InternalRow to JSON strings for CDC records. |

### Spark SQL Commands

| Class | Description |
|-------|-------------|
| **CreateHoodieTableCommand** | CREATE TABLE implementation. |
| **DropHoodieTableCommand** | DROP TABLE implementation. |
| **AlterHoodieTableAddColumnsCommand** | ALTER TABLE ADD COLUMNS. |
| **AlterHoodieTableDropPartitionCommand** | ALTER TABLE DROP PARTITION. |
| **TruncateHoodieTableCommand** | TRUNCATE TABLE implementation. |
| **MergeIntoKeyGenerator** | Key generation for MERGE INTO statements. |
| **SqlKeyGenerator** | SQL expression-based key generator. |

### Catalog Integration

| Class | Description |
|-------|-------------|
| **HoodieCatalog** | Spark catalog implementation for Hudi tables. |
| **HoodieCatalogTable** | Catalog table wrapper with Hudi metadata. |
| **HoodieInternalV2Table** | DataSource V2 table representation. |
| **HoodieStagedTable** | Staged table for atomic operations. |

### Streaming Support

| Class | Description |
|-------|-------------|
| **HoodieStreamSourceV1** | Spark Structured Streaming source (V1 API). |
| **HoodieStreamSourceV2** | Spark Structured Streaming source (V2 API). |
| **HoodieMetadataLog** | Metadata log for streaming offset tracking. |
| **HoodieSourceOffset** | Streaming offset representation. |

### Async Services

| Class | Description |
|-------|-------------|
| **SparkStreamingAsyncCompactService** | Async compaction service for Spark Streaming workloads. |
| **SparkStreamingAsyncClusteringService** | Async clustering service for Spark Streaming workloads. |

## Configuration Classes

| Class | Description |
|-------|-------------|
| **DataSourceOptions** | Enumeration of all Hudi datasource read/write options. |
| **DataSourceReadOptions** | Read-specific configuration options. |
| **DataSourceWriteOptions** | Write-specific configuration options. |
| **HoodieOptionConfig** | Hudi SQL option configuration. |
| **SparkConfigs** | Spark-specific configuration classes. |

## Dependencies

- `hudi-common` - Core data model and timeline
- `hudi-client/hudi-spark-client` - Spark write client
- Apache Spark SQL and Streaming APIs
- Apache Avro for schema and serialization
- Hadoop FileSystem APIs

## Build Instructions

```bash
# Build this module with dependencies
mvn clean package -DskipTests -pl hudi-spark-datasource/hudi-spark-common -am

# Run unit tests
mvn test -Punit-tests -pl hudi-spark-datasource/hudi-spark-common
```

## Related Modules

- [hudi-spark-datasource](../README.md) - Parent module overview
- [hudi-spark3-common](../hudi-spark3-common/README.md) - Spark 3.x specific code
- [hudi-spark4-common](../hudi-spark4-common/README.md) - Spark 4.x specific code
- [hudi-spark](../hudi-spark/README.md) - Main Spark datasource
