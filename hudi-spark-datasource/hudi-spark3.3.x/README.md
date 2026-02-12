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

# `hudi-spark3.3.x` Module

This module provides the Spark 3.3.x-specific adapter implementation for Apache Hudi. It contains version-specific code for SQL parsing, file format handling, Avro serialization, and Catalyst utilities.

## Overview

The `hudi-spark3.3.x` module implements:

- `Spark3_3Adapter` extending `BaseSpark3Adapter`
- Version-specific SQL parser extensions
- Parquet and ORC file readers with nested schema pruning
- Avro serializer/deserializer wrappers for Spark 3.3 APIs
- Catalyst expression and plan utilities

## Directory Structure

```
src/main/scala/org/apache/
├── hudi/
│   └── (empty - utilities in spark package)
└── spark/sql/
    ├── adapter/
    │   └── Spark3_3Adapter.scala
    ├── avro/
    │   ├── AvroSerializer.scala
    │   ├── AvroDeserializer.scala
    │   └── AvroUtils.scala
    ├── execution/datasources/
    │   ├── orc/
    │   │   └── Spark33OrcReader.scala
    │   └── parquet/
    │       ├── Spark33ParquetReader.scala
    │       ├── Spark33LegacyHoodieParquetFileFormat.scala
    │       └── Spark33DataSourceUtils.scala
    ├── hudi/
    │   ├── analysis/
    │   │   ├── HoodieSpark33Analysis.scala
    │   │   └── Spark33ResolveHudiAlterTableCommand.scala
    │   ├── Spark33NestedSchemaPruning.scala
    │   ├── HoodieSpark33CatalystExpressionUtils.scala
    │   ├── HoodieSpark33CatalystPlanUtils.scala
    │   ├── HoodieSpark33SchemaUtils.scala
    │   └── HoodieSpark33PartitionedFileUtils.scala
    └── parser/
        ├── HoodieSpark3_3ExtendedSqlParser.scala
        └── HoodieSpark3_3ExtendedSqlAstBuilder.scala
```

## Key Components

### Adapter

| Class | Description |
|-------|-------------|
| **Spark3_3Adapter** | Main adapter implementing `SparkAdapter` for Spark 3.3. Extends `BaseSpark3Adapter` and provides factory methods for version-specific components. Creates `Spark33HoodieFileScanRDD`, handles storage level conversion, and manages Spark context lifecycle. |

### SQL Parser

| Class | Description |
|-------|-------------|
| **HoodieSpark3_3ExtendedSqlParser** | Extended SQL parser for Hudi commands. Wraps Spark's `ParserInterface` and delegates to the AST builder for Hudi-specific syntax. |
| **HoodieSpark3_3ExtendedSqlAstBuilder** | AST builder for extended SQL statements. Converts parse trees to logical plans for Hudi operations. |

### Analysis Rules

| Class | Description |
|-------|-------------|
| **HoodieSpark33Analysis** | Catalyst analysis rule for DataSource V2 to V1 fallback. Ensures compatibility with Hudi's relation implementations. |
| **Spark33ResolveHudiAlterTableCommand** | Resolution logic for ALTER TABLE commands in Spark 3.3. |

### File Readers

| Class | Description |
|-------|-------------|
| **Spark33ParquetReader** | Parquet file reader implementation for Spark 3.3. Handles schema evolution and vectorized reading. |
| **Spark33LegacyHoodieParquetFileFormat** | Legacy Parquet format handler for backward compatibility. |
| **Spark33OrcReader** | ORC file reader implementation for Spark 3.3. |
| **Spark33NestedSchemaPruning** | Nested schema pruning for columnar reads. Optimizes reads by pruning unnecessary nested fields. |

### Avro Support

| Class | Description |
|-------|-------------|
| **HoodieSpark3_3AvroSerializer** | Avro serializer wrapper for Spark 3.3. Handles InternalRow to Avro conversion. |
| **HoodieSpark3_3AvroDeserializer** | Avro deserializer wrapper for Spark 3.3. Handles Avro to InternalRow conversion. |
| **AvroSerializer/AvroDeserializer** | Vendored copies of Spark's native Avro utilities adapted for Hudi's use. |
| **AvroUtils** | Avro utility functions. |

### Catalyst Utilities

| Class | Description |
|-------|-------------|
| **HoodieSpark33CatalystExpressionUtils** | Catalyst expression utilities specific to Spark 3.3. Extends `HoodieSpark3CatalystExpressionUtils`. |
| **HoodieSpark33CatalystPlanUtils** | Catalyst logical plan manipulation utilities. |
| **HoodieSpark33SchemaUtils** | Schema conversion and manipulation utilities. |
| **HoodieSpark33PartitionedFileUtils** | Partitioned file handling utilities. |
| **Spark33DataSourceUtils** | Data source utilities for Parquet-specific operations. |

### RDD Implementation

| Class | Description |
|-------|-------------|
| **Spark33HoodieFileScanRDD** | RDD-based file scan implementation for Spark 3.3. Extends `FileScanRDD` with `HoodieUnsafeRDD` mixin for Hudi-specific functionality. |

## Spark 3.3 Specific Features

- Uses `ParquetFilterPushDownStringStartWith` for string predicate pushdown
- Stop context uses `jssc.stop()` without exit code parameter
- String-based `TimestampNTZType` detection for timestamp handling

## Dependencies

- `hudi-spark3-common` - Shared Spark 3.x code
- `hudi-spark-common` - Core shared Spark integration
- Apache Spark SQL 3.3.x APIs

## Build Instructions

```bash
# Build with Spark 3.3
mvn clean package -DskipTests -Dspark3.3 -pl hudi-spark-datasource/hudi-spark3.3.x -am
```

## Related Modules

- [hudi-spark-datasource](../README.md) - Parent module overview
- [hudi-spark3-common](../hudi-spark3-common/README.md) - Spark 3.x shared code
- [hudi-spark3.4.x](../hudi-spark3.4.x/README.md) - Spark 3.4 adapter
- [hudi-spark3.5.x](../hudi-spark3.5.x/README.md) - Spark 3.5 adapter
