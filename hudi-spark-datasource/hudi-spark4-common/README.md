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

# `hudi-spark4-common` Module

This module contains Spark 4.x version-specific adapter code shared across Spark 4.0.x and future Spark 4.x versions. It provides the SQL command implementations, partition handling, and data utilities specific to Spark 4.

## Overview

The `hudi-spark4-common` module implements:

- Base Spark 4 adapter with version-agnostic interfaces
- DML commands (INSERT, UPDATE, DELETE, MERGE INTO)
- DDL commands (CREATE TABLE AS SELECT)
- Partition pruning optimization rules
- Partition value mapping for file slices and CDC
- UTF-8 string utilities for Spark 4.x
- Catalyst expression utilities for Spark 4 API changes

## Directory Structure

```
src/main/scala/org/apache/
├── hudi/
│   ├── Spark4HoodiePartitionValues.scala
│   ├── Spark4HoodiePartitionFileSliceMapping.scala
│   ├── Spark4HoodiePartitionCDCFileGroupMapping.scala
│   └── Spark4HoodieUTF8String.scala
└── spark/sql/
    ├── ExpressionColumnNodeWrapper.scala
    ├── HoodieSpark4CatalystExpressionUtils.scala
    ├── Spark4DataFrameUtil.scala
    ├── Spark4HoodieUnsafeUtils.scala
    ├── Spark4HoodieUTF8StringFactory.scala
    ├── adapter/
    │   └── BaseSpark4Adapter.scala
    └── hudi/
        ├── analysis/
        │   └── Spark4HoodiePruneFileSourcePartitions.scala
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
| **BaseSpark4Adapter** | Base implementation of `SparkAdapter` for Spark 4.x. Handles file partitioning, predicate translation, date formatting, relation creation, and DataFrame utilities. Adapts to Spark 4's new `classic.SparkSession` API and `ColumnConversions` for expression handling. |

### SQL Commands

| Class | Description |
|-------|-------------|
| **CreateHoodieTableAsSelectCommand** | CTAS (Create Table As Select) implementation for Spark 4. Handles table creation with automatic initialization, schema alignment, and Hive sync support. |
| **InsertIntoHoodieTableCommand** | INSERT INTO implementation with dynamic/static partition support, column alignment, and overwrite modes. |
| **UpdateHoodieTableCommand** | UPDATE statement implementation. Validates partition/record-key immutability and supports partial updates. |
| **DeleteHoodieTableCommand** | DELETE FROM implementation with condition-based filtering and schema optimization. |
| **MergeIntoHoodieTableCommand** | Full MERGE INTO (MIT) implementation. Supports WHEN MATCHED (UPDATE/DELETE) and WHEN NOT MATCHED (INSERT) clauses with ExpressionPayload for custom merge logic. |

### Analysis Rules

| Class | Description |
|-------|-------------|
| **Spark4HoodiePruneFileSourcePartitions** | Partition pruning optimization rule. Decomposes filters into partition and data filters, performs file slice pruning, and re-computes statistics. |

### Partition Mapping

| Class | Description |
|-------|-------------|
| **Spark4HoodiePartitionValues** | Wrapper for `InternalRow` partition values with type-safe accessors for all Spark data types. |
| **Spark4HoodiePartitionFileSliceMapping** | Maps partition values to `FileSlice` objects for file-level operations. |
| **Spark4HoodiePartitionCDCFileGroupMapping** | Maps partition values to CDC file splits for Change Data Capture queries. |

### Data Utilities

| Class | Description |
|-------|-------------|
| **Spark4DataFrameUtil** | DataFrame creation utilities from `RDD[InternalRow]`. Uses Spark 4's `classic.SparkSession` API. |
| **Spark4HoodieUnsafeUtils** | Unsafe row and partitioning utilities. Handles DataFrame creation from various sources and `HoodieUnsafeRDD` collection. |
| **Spark4HoodieUTF8String** | UTF-8 string wrapper for Hudi compatibility. |
| **Spark4HoodieUTF8StringFactory** | Factory for creating Hudi-wrapped UTF8Strings. |

### Catalyst Utilities

| Class | Description |
|-------|-------------|
| **HoodieSpark4CatalystExpressionUtils** | Catalyst expression utilities adapted for Spark 4's expression API changes. |
| **ExpressionColumnNodeWrapper** | Wrapper for creating Spark 4 Column objects from Catalyst expressions, adapting to the new `ColumnNode` API. |

## Spark 4.x Specific Changes

The `hudi-spark4-common` module handles several Spark 4.x API changes:

- **Classic vs Connect API**: Spark 4 introduces the Spark Connect architecture. This module uses the `classic.SparkSession` for backward compatibility with existing write paths.
- **Column API**: Uses `ExpressionColumnNodeWrapper` to bridge between Catalyst expressions and the new Column API.
- **ColumnConversions**: Uses `ColumnConversions.expression()` to extract expressions from columns.
- **ParseException**: Updated constructor signature with error class and message parameters.
- **DateFormatter**: Uses reflection-based access via `ReflectUtil.getDateFormatter()`.

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
All command classes reference `sparkAdapter` for cross-version compatibility. The `BaseSpark4Adapter` provides version-agnostic implementations of Spark-specific operations while handling Spark 4's API differences.

### Data Writing Command Pattern
SQL commands extend `DataWritingCommand` and implement:
- `run(sparkSession, plan)` - Command execution
- `withNewChildInternal()` - Plan transformation
- Metrics collection via `HoodieCommandMetrics`

### Companion Object Pattern
Command case classes are paired with companion objects containing static validation and utility methods.

## Dependencies

- `hudi-spark-common` - Core shared Spark integration
- `hudi-spark-client` - Spark write client
- Apache Spark SQL 4.x APIs
- Catalyst optimizer framework
- Jackson for JSON processing (Spark 4 compatible version)

## Build Instructions

```bash
# Build with Spark 4.0 (requires Java 17)
mvn clean package -DskipTests -Dspark4.0 -pl hudi-spark-datasource/hudi-spark4-common -am
```

**Note**: Spark 4.x requires Java 17 or later and Scala 2.13.

## Related Modules

- [hudi-spark-datasource](../README.md) - Parent module overview
- [hudi-spark-common](../hudi-spark-common/README.md) - Core shared code
- [hudi-spark3-common](../hudi-spark3-common/README.md) - Spark 3.x specific code
- [hudi-spark4.0.x](../hudi-spark4.0.x/README.md) - Spark 4.0 specific adapter
