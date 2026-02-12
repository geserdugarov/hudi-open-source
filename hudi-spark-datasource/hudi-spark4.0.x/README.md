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

# `hudi-spark4.0.x` Module

This module provides the Spark 4.0.x-specific adapter implementation for Apache Hudi. It contains version-specific code for SQL parsing, file format handling, Avro serialization, and Catalyst utilities adapted for Spark 4's new APIs.

## Overview

The `hudi-spark4.0.x` module implements:

- `Spark4_0Adapter` extending `BaseSpark4Adapter`
- Version-specific SQL parser extensions
- Parquet and ORC file readers with nested schema pruning
- Lance file format support
- Avro serializer/deserializer wrappers for Spark 4.0 APIs
- Catalyst expression and plan utilities

## Directory Structure

```
src/main/scala/org/apache/
├── hudi/
│   └── (empty - utilities in spark package)
└── spark/sql/
    ├── adapter/
    │   └── Spark4_0Adapter.scala
    ├── avro/
    │   ├── AvroSerializer.scala
    │   ├── AvroDeserializer.scala
    │   └── AvroUtils.scala
    ├── execution/datasources/
    │   ├── orc/
    │   │   └── Spark40OrcReader.scala
    │   └── parquet/
    │       ├── Spark40ParquetReader.scala
    │       ├── Spark40LegacyHoodieParquetFileFormat.scala
    │       └── Spark40DataSourceUtils.scala
    ├── hudi/
    │   ├── analysis/
    │   │   ├── HoodieSpark40Analysis.scala
    │   │   └── Spark40ResolveHudiAlterTableCommand.scala
    │   ├── Spark40NestedSchemaPruning.scala
    │   ├── HoodieSpark40CatalystExpressionUtils.scala
    │   ├── HoodieSpark40CatalystPlanUtils.scala
    │   ├── HoodieSpark40SchemaUtils.scala
    │   └── HoodieSpark40PartitionedFileUtils.scala
    └── parser/
        ├── HoodieSpark4_0ExtendedSqlParser.scala
        └── HoodieSpark4_0ExtendedSqlAstBuilder.scala
```

## Key Components

### Adapter

| Class | Description |
|-------|-------------|
| **Spark4_0Adapter** | Main adapter implementing `SparkAdapter` for Spark 4.0. Extends `BaseSpark4Adapter` (not `BaseSpark3Adapter`). Provides factory methods for Spark 4.0-specific components, simplified table resolution, and Lance file reader support. |

### SQL Parser

| Class | Description |
|-------|-------------|
| **HoodieSpark4_0ExtendedSqlParser** | Extended SQL parser for Hudi commands in Spark 4.0. |
| **HoodieSpark4_0ExtendedSqlAstBuilder** | AST builder for extended SQL statements. |

### Analysis Rules

| Class | Description |
|-------|-------------|
| **HoodieSpark40Analysis** | Catalyst analysis rule for DataSource V2 to V1 fallback. |
| **Spark40ResolveHudiAlterTableCommand** | Resolution logic for ALTER TABLE commands in Spark 4.0. |

### File Readers

| Class | Description |
|-------|-------------|
| **Spark40ParquetReader** | Parquet file reader implementation for Spark 4.0. |
| **Spark40LegacyHoodieParquetFileFormat** | Legacy Parquet format handler. |
| **Spark40OrcReader** | ORC file reader implementation. |
| **Spark40NestedSchemaPruning** | Nested schema pruning for columnar reads. |

### Avro Support

| Class | Description |
|-------|-------------|
| **HoodieSpark4_0AvroSerializer** | Avro serializer wrapper for Spark 4.0. |
| **HoodieSpark4_0AvroDeserializer** | Avro deserializer wrapper for Spark 4.0. |
| **AvroSerializer/AvroDeserializer** | Vendored copies of Spark's native Avro utilities. |

### Catalyst Utilities

| Class | Description |
|-------|-------------|
| **HoodieSpark40CatalystExpressionUtils** | Catalyst expression utilities specific to Spark 4.0. |
| **HoodieSpark40CatalystPlanUtils** | Catalyst logical plan manipulation utilities. |
| **HoodieSpark40SchemaUtils** | Schema conversion and manipulation utilities. |
| **HoodieSpark40PartitionedFileUtils** | Partitioned file handling utilities. |
| **Spark40DataSourceUtils** | Data source utilities. |

### RDD Implementation

| Class | Description |
|-------|-------------|
| **Spark40HoodieFileScanRDD** | RDD-based file scan implementation for Spark 4.0. |

## Spark 4.0 Specific Features

- **Extends BaseSpark4Adapter**: Unlike Spark 3.x modules, this extends `BaseSpark4Adapter` which handles Spark 4's new APIs
- **No ParquetFilters creation in adapter**: `ParquetFilters` handling moved to `BaseSpark4Adapter`
- **Simplified table resolution**: Streamlined method without V2TableWithV1Fallback complexity
- **Scala 2.13 only**: Spark 4.0 requires Scala 2.13
- **Java 17+ required**: Spark 4.0 requires Java 17 or later
- **Lance file reader support**: Continued support for vector format

## Differences from Spark 3.5

| Feature | Spark 3.5 | Spark 4.0 |
|---------|-----------|-----------|
| Base Adapter | `BaseSpark3Adapter` | `BaseSpark4Adapter` |
| ParquetFilters | Adapter creates | BaseSpark4Adapter handles |
| Scala Version | 2.12, 2.13 | 2.13 only |
| Java Version | 11+ | 17+ |
| Column API | Standard | New `ColumnNode` API |
| SparkSession | Standard | `classic.SparkSession` for writes |

## Requirements

- **Java 17+**: Spark 4.0 requires Java 17 or later
- **Scala 2.13**: Only Scala 2.13 is supported
- Apache Spark 4.0.x

## Dependencies

- `hudi-spark4-common` - Shared Spark 4.x code
- `hudi-spark-common` - Core shared Spark integration
- Apache Spark SQL 4.0.x APIs

## Build Instructions

```bash
# Build with Spark 4.0 (requires Java 17)
mvn clean package -DskipTests -Dspark4.0 -pl hudi-spark-datasource/hudi-spark4.0.x -am

# Note: Ensure JAVA_HOME points to Java 17+
export JAVA_HOME=/path/to/java17
mvn clean package -DskipTests -Dspark4.0 -pl hudi-spark-datasource/hudi-spark4.0.x -am
```

## Related Modules

- [hudi-spark-datasource](../README.md) - Parent module overview
- [hudi-spark4-common](../hudi-spark4-common/README.md) - Spark 4.x shared code
- [hudi-spark3.5.x](../hudi-spark3.5.x/README.md) - Spark 3.5 adapter
- [hudi-spark-common](../hudi-spark-common/README.md) - Core shared code
