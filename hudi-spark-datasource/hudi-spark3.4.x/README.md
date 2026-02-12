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

# `hudi-spark3.4.x` Module

This module provides the Spark 3.4.x-specific adapter implementation for Apache Hudi. It contains version-specific code for SQL parsing, file format handling, Avro serialization, and Catalyst utilities.

## Overview

The `hudi-spark3.4.x` module implements:

- `Spark3_4Adapter` extending `BaseSpark3Adapter`
- Version-specific SQL parser extensions
- Parquet and ORC file readers with nested schema pruning
- Lance file format support (introduced in 3.4)
- Avro serializer/deserializer wrappers for Spark 3.4 APIs
- Catalyst expression and plan utilities

## Directory Structure

```
src/main/scala/org/apache/
├── hudi/
│   └── (empty - utilities in spark package)
└── spark/sql/
    ├── adapter/
    │   └── Spark3_4Adapter.scala
    ├── avro/
    │   ├── AvroSerializer.scala
    │   ├── AvroDeserializer.scala
    │   └── AvroUtils.scala
    ├── execution/datasources/
    │   ├── orc/
    │   │   └── Spark34OrcReader.scala
    │   └── parquet/
    │       ├── Spark34ParquetReader.scala
    │       ├── Spark34LegacyHoodieParquetFileFormat.scala
    │       └── Spark34DataSourceUtils.scala
    ├── hudi/
    │   ├── analysis/
    │   │   ├── HoodieSpark34Analysis.scala
    │   │   └── Spark34ResolveHudiAlterTableCommand.scala
    │   ├── Spark34NestedSchemaPruning.scala
    │   ├── HoodieSpark34CatalystExpressionUtils.scala
    │   ├── HoodieSpark34CatalystPlanUtils.scala
    │   ├── HoodieSpark34SchemaUtils.scala
    │   └── HoodieSpark34PartitionedFileUtils.scala
    └── parser/
        ├── HoodieSpark3_4ExtendedSqlParser.scala
        └── HoodieSpark3_4ExtendedSqlAstBuilder.scala
```

## Key Components

### Adapter

| Class | Description |
|-------|-------------|
| **Spark3_4Adapter** | Main adapter implementing `SparkAdapter` for Spark 3.4. Extends `BaseSpark3Adapter` and provides factory methods for version-specific components. Creates `Spark34HoodieFileScanRDD`, supports Lance file reader creation, and handles context lifecycle with exit codes. |

### SQL Parser

| Class | Description |
|-------|-------------|
| **HoodieSpark3_4ExtendedSqlParser** | Extended SQL parser for Hudi commands. Wraps Spark's `ParserInterface` and delegates to the AST builder. |
| **HoodieSpark3_4ExtendedSqlAstBuilder** | AST builder for extended SQL statements. |

### Analysis Rules

| Class | Description |
|-------|-------------|
| **HoodieSpark34Analysis** | Catalyst analysis rule for DataSource V2 to V1 fallback. |
| **Spark34ResolveHudiAlterTableCommand** | Resolution logic for ALTER TABLE commands in Spark 3.4. |

### File Readers

| Class | Description |
|-------|-------------|
| **Spark34ParquetReader** | Parquet file reader implementation with Lance support introduced. |
| **Spark34LegacyHoodieParquetFileFormat** | Legacy Parquet format handler. |
| **Spark34OrcReader** | ORC file reader implementation. |
| **Spark34NestedSchemaPruning** | Nested schema pruning for columnar reads. |

### Avro Support

| Class | Description |
|-------|-------------|
| **HoodieSpark3_4AvroSerializer** | Avro serializer wrapper for Spark 3.4. |
| **HoodieSpark3_4AvroDeserializer** | Avro deserializer wrapper for Spark 3.4. |
| **AvroSerializer/AvroDeserializer** | Vendored copies of Spark's native Avro utilities. |

### Catalyst Utilities

| Class | Description |
|-------|-------------|
| **HoodieSpark34CatalystExpressionUtils** | Catalyst expression utilities specific to Spark 3.4. |
| **HoodieSpark34CatalystPlanUtils** | Catalyst logical plan manipulation utilities. |
| **HoodieSpark34SchemaUtils** | Schema conversion and manipulation utilities. |
| **HoodieSpark34PartitionedFileUtils** | Partitioned file handling utilities. |
| **Spark34DataSourceUtils** | Data source utilities for Parquet-specific operations. |

### RDD Implementation

| Class | Description |
|-------|-------------|
| **Spark34HoodieFileScanRDD** | RDD-based file scan implementation. Extends `FileScanRDD` with `HoodieUnsafeRDD` mixin. |

## Spark 3.4 Specific Features

- **Lance file reader support**: `createLanceFileReader()` returns `Some(SparkLanceReaderBase)` for vector format reading
- **Direct TimestampNTZType comparison**: Uses `DataTypes.TimestampNTZType` directly instead of string matching
- **ParquetFilterPushDownStringPredicate**: Uses updated string predicate pushdown
- **Exit code support**: Stop context uses `jssc.sc.stop(exitCode)` for proper exit handling

## Differences from Spark 3.3

| Feature | Spark 3.3 | Spark 3.4 |
|---------|-----------|-----------|
| Lance Reader | Not supported | Supported |
| TimestampNTZ | String-based detection | Direct type comparison |
| String Filter Pushdown | `ParquetFilterPushDownStringStartWith` | `ParquetFilterPushDownStringPredicate` |
| Context Stop | `jssc.stop()` | `jssc.sc.stop(exitCode)` |

## Dependencies

- `hudi-spark3-common` - Shared Spark 3.x code
- `hudi-spark-common` - Core shared Spark integration
- Apache Spark SQL 3.4.x APIs

## Build Instructions

```bash
# Build with Spark 3.4
mvn clean package -DskipTests -Dspark3.4 -pl hudi-spark-datasource/hudi-spark3.4.x -am
```

## Related Modules

- [hudi-spark-datasource](../README.md) - Parent module overview
- [hudi-spark3-common](../hudi-spark3-common/README.md) - Spark 3.x shared code
- [hudi-spark3.3.x](../hudi-spark3.3.x/README.md) - Spark 3.3 adapter
- [hudi-spark3.5.x](../hudi-spark3.5.x/README.md) - Spark 3.5 adapter
