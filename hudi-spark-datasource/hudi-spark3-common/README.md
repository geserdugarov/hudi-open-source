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

# `hudi-spark3-common` Module

This module contains Spark 3.x version-specific adapter code shared across Spark 3.3.x, 3.4.x, and 3.5.x versions. It provides the SQL command implementations, partition handling, and data utilities specific to Spark 3.

## Overview

The `hudi-spark3-common` module implements:

- Base Spark 3 adapter with version-agnostic interfaces
- DML commands (INSERT, UPDATE, DELETE, MERGE INTO)
- DDL commands (CREATE TABLE AS SELECT)
- Partition pruning optimization rules
- Partition value mapping for file slices and CDC
- UTF-8 string utilities for Spark 3.x

## Directory Structure

```
src/main/scala/org/apache/
├── hudi/
│   ├── Spark3HoodiePartitionValues.scala
│   ├── Spark3HoodiePartitionFileSliceMapping.scala
│   ├── Spark3HoodiePartitionCDCFileGroupMapping.scala
│   └── Spark3HoodieUTF8String.scala
└── spark/sql/
    ├── Spark3DataFrameUtil.scala
    ├── Spark3HoodieUnsafeUtils.scala
    ├── Spark3HoodieUTF8StringFactory.scala
    ├── adapter/
    │   └── BaseSpark3Adapter.scala
    └── hudi/
        ├── analysis/
        │   └── Spark3HoodiePruneFileSourcePartitions.scala
        └── command/
            ├── CreateHoodieTableAsSelectCommand.scala
            ├── InsertIntoHoodieTableCommand.scala
            ├── UpdateHoodieTableCommand.scala
            ├── DeleteHoodieTableCommand.scala
            └── MergeIntoHoodieTableCommand.scala
```

## Key Components

### Adapter Layer

| Class | Description |
|-------|-------------|
| **BaseSpark3Adapter** | Base implementation of `SparkAdapter` for Spark 3.x. Provides file partitioning, predicate translation, date formatting, relation creation, and DataFrame utilities. Extended by version-specific adapters (Spark3_3Adapter, etc.). |

### SQL Commands

| Class | Description |
|-------|-------------|
| **CreateHoodieTableAsSelectCommand** | CTAS (Create Table As Select) implementation. Handles table creation with automatic initialization, schema alignment, and Hive sync support. |
| **InsertIntoHoodieTableCommand** | INSERT INTO implementation with dynamic/static partition support, column alignment, and overwrite modes. |
| **UpdateHoodieTableCommand** | UPDATE statement implementation. Validates partition/record-key immutability and supports partial updates. |
| **DeleteHoodieTableCommand** | DELETE FROM implementation with condition-based filtering and schema optimization. |
| **MergeIntoHoodieTableCommand** | Full MERGE INTO (MIT) implementation. Supports WHEN MATCHED (UPDATE/DELETE) and WHEN NOT MATCHED (INSERT) clauses with ExpressionPayload for custom merge logic. |

### Analysis Rules

| Class | Description |
|-------|-------------|
| **Spark3HoodiePruneFileSourcePartitions** | Partition pruning optimization rule. Decomposes filters into partition and data filters, performs file slice pruning, and re-computes statistics. |

### Partition Mapping

| Class | Description |
|-------|-------------|
| **Spark3HoodiePartitionValues** | Wrapper for `InternalRow` partition values with type-safe accessors for all Spark data types. |
| **Spark3HoodiePartitionFileSliceMapping** | Maps partition values to `FileSlice` objects for file-level operations. |
| **Spark3HoodiePartitionCDCFileGroupMapping** | Maps partition values to CDC file splits for Change Data Capture queries. |

### Data Utilities

| Class | Description |
|-------|-------------|
| **Spark3DataFrameUtil** | DataFrame creation utilities from `RDD[InternalRow]`. |
| **Spark3HoodieUnsafeUtils** | Unsafe row and partitioning utilities. Handles DataFrame creation from various sources and `HoodieUnsafeRDD` collection. |
| **Spark3HoodieUTF8String** | UTF-8 string wrapper for Hudi compatibility. |
| **Spark3HoodieUTF8StringFactory** | Factory for creating Hudi-wrapped UTF8Strings. |

## MERGE INTO Capabilities

The `MergeIntoHoodieTableCommand` provides comprehensive MERGE INTO support:

- **WHEN MATCHED UPDATE**: Conditional updates with partial column assignment
- **WHEN MATCHED DELETE**: Conditional deletes with single-action enforcement
- **WHEN NOT MATCHED INSERT**: Insert new records with assignment validation
- **Primary keyless tables**: Left-outer join support for tables without primary keys
- **Partial updates**: Optimized mode for MOR tables with null/original value handling
- **Global index detection**: Automatic configuration for global indexing
- **Event-time ordering**: Support for event-time based ordering mode
- **Expression serialization**: Base64 encoding for remote execution

## Design Patterns

### Adapter Pattern
All command classes reference `sparkAdapter` for cross-version compatibility. The `BaseSpark3Adapter` provides version-agnostic implementations of Spark-specific operations.

### Data Writing Command Pattern
SQL commands extend `DataWritingCommand` and implement:
- `run(sparkSession, plan)` - Command execution
- `withNewChildInternal()` - Plan transformation
- Metrics collection via `HoodieCommandMetrics`

### Companion Object Pattern
Command case classes are paired with companion objects containing static validation and utility methods.

## Dependencies

- `hudi-spark-common` - Core shared Spark integration
- Apache Spark SQL 3.x APIs
- Catalyst optimizer framework

## Build Instructions

```bash
# Build with Spark 3.3
mvn clean package -DskipTests -Dspark3.3 -pl hudi-spark-datasource/hudi-spark3-common -am

# Build with Spark 3.4
mvn clean package -DskipTests -Dspark3.4 -pl hudi-spark-datasource/hudi-spark3-common -am

# Build with Spark 3.5 (default)
mvn clean package -DskipTests -Dspark3.5 -pl hudi-spark-datasource/hudi-spark3-common -am
```

## Related Modules

- [hudi-spark-datasource](../README.md) - Parent module overview
- [hudi-spark-common](../hudi-spark-common/README.md) - Core shared code
- [hudi-spark3.3.x](../hudi-spark3.3.x/README.md) - Spark 3.3 specific adapter
- [hudi-spark3.4.x](../hudi-spark3.4.x/README.md) - Spark 3.4 specific adapter
- [hudi-spark3.5.x](../hudi-spark3.5.x/README.md) - Spark 3.5 specific adapter
