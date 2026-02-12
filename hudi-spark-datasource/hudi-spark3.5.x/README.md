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

# `hudi-spark3.5.x` Module

This module provides the Spark 3.5.x-specific adapter implementation for Apache Hudi. It is the default Spark version for Hudi builds and contains version-specific code for SQL parsing, file format handling, Avro serialization, and Catalyst utilities.

## Overview

The `hudi-spark3.5.x` module implements:

- `Spark3_5Adapter` extending `BaseSpark3Adapter`
- Version-specific SQL parser extensions
- Parquet and ORC file readers with nested schema pruning
- Lance file format support
- Avro serializer/deserializer wrappers for Spark 3.5 APIs
- Catalyst expression and plan utilities
- V2 table with V1 fallback support

## Directory Structure

```
src/main/scala/org/apache/
├── hudi/
│   └── (empty - utilities in spark package)
└── spark/sql/
    ├── adapter/
    │   └── Spark3_5Adapter.scala
    ├── avro/
    │   ├── AvroSerializer.scala
    │   ├── AvroDeserializer.scala
    │   └── AvroUtils.scala
    ├── execution/datasources/
    │   ├── orc/
    │   │   └── Spark35OrcReader.scala
    │   └── parquet/
    │       ├── Spark35ParquetReader.scala
    │       ├── Spark35LegacyHoodieParquetFileFormat.scala
    │       └── Spark35DataSourceUtils.scala
    ├── hudi/
    │   ├── analysis/
    │   │   ├── HoodieSpark35Analysis.scala
    │   │   └── Spark35ResolveHudiAlterTableCommand.scala
    │   ├── Spark35NestedSchemaPruning.scala
    │   ├── HoodieSpark35CatalystExpressionUtils.scala
    │   ├── HoodieSpark35CatalystPlanUtils.scala
    │   ├── HoodieSpark35SchemaUtils.scala
    │   └── HoodieSpark35PartitionedFileUtils.scala
    └── parser/
        ├── HoodieSpark3_5ExtendedSqlParser.scala
        └── HoodieSpark3_5ExtendedSqlAstBuilder.scala
```

## Key Components

### Adapter

| Class | Description |
|-------|-------------|
| **Spark3_5Adapter** | Main adapter implementing `SparkAdapter` for Spark 3.5 (default). Extends `BaseSpark3Adapter` with additional `V2TableWithV1Fallback` support. Provides factory methods for version-specific components, Lance file reader creation, and enhanced table resolution. |

### SQL Parser

| Class | Description |
|-------|-------------|
| **HoodieSpark3_5ExtendedSqlParser** | Extended SQL parser for Hudi commands. |
| **HoodieSpark3_5ExtendedSqlAstBuilder** | AST builder for extended SQL statements. |

### Analysis Rules

| Class | Description |
|-------|-------------|
| **HoodieSpark35Analysis** | Catalyst analysis rule for DataSource V2 to V1 fallback. |
| **Spark35ResolveHudiAlterTableCommand** | Resolution logic for ALTER TABLE commands in Spark 3.5. |

### File Readers

| Class | Description |
|-------|-------------|
| **Spark35ParquetReader** | Parquet file reader implementation for Spark 3.5. |
| **Spark35LegacyHoodieParquetFileFormat** | Legacy Parquet format handler. |
| **Spark35OrcReader** | ORC file reader implementation. |
| **Spark35NestedSchemaPruning** | Nested schema pruning for columnar reads. |

### Avro Support

| Class | Description |
|-------|-------------|
| **HoodieSpark3_5AvroSerializer** | Avro serializer wrapper for Spark 3.5. |
| **HoodieSpark3_5AvroDeserializer** | Avro deserializer wrapper for Spark 3.5. |
| **AvroSerializer/AvroDeserializer** | Vendored copies of Spark's native Avro utilities. |

### Catalyst Utilities

| Class | Description |
|-------|-------------|
| **HoodieSpark35CatalystExpressionUtils** | Catalyst expression utilities specific to Spark 3.5. |
| **HoodieSpark35CatalystPlanUtils** | Catalyst logical plan manipulation utilities. |
| **HoodieSpark35SchemaUtils** | Schema conversion and manipulation utilities. |
| **HoodieSpark35PartitionedFileUtils** | Partitioned file handling utilities. |
| **Spark35DataSourceUtils** | Data source utilities. |

### RDD Implementation

| Class | Description |
|-------|-------------|
| **Spark35HoodieFileScanRDD** | RDD-based file scan implementation. Extends `FileScanRDD` with `HoodieUnsafeRDD` mixin. |

## Spark 3.5 Specific Features

- **V2 Table with V1 Fallback**: Custom `isHoodieTable(v2Table: V2TableWithV1Fallback)` method for enhanced table type detection
- **LegacyBehaviorPolicy**: Import path moved to `org.apache.spark.sql.internal`
- **Lance file reader support**: Continued support from Spark 3.4
- **Default build version**: This is the default Spark version for Hudi builds
- **Scala 2.12 and 2.13 support**: Both Scala versions are supported

## Differences from Spark 3.4

| Feature | Spark 3.4 | Spark 3.5 |
|---------|-----------|-----------|
| V2TableWithV1Fallback | Not available | Custom `isHoodieTable` method |
| LegacyBehaviorPolicy | Standard import | Moved to `internal` package |
| Scala versions | 2.12 only | 2.12 and 2.13 |
| Default build | No | Yes |

## Dependencies

- `hudi-spark3-common` - Shared Spark 3.x code
- `hudi-spark-common` - Core shared Spark integration
- Apache Spark SQL 3.5.x APIs

## Build Instructions

```bash
# Build with Spark 3.5 (default)
mvn clean package -DskipTests -pl hudi-spark-datasource/hudi-spark3.5.x -am

# Build with Spark 3.5 and Scala 2.12
mvn clean package -DskipTests -Dspark3.5 -pl hudi-spark-datasource/hudi-spark3.5.x -am

# Build with Spark 3.5 and Scala 2.13
mvn clean package -DskipTests -Dspark3.5 -Dscala-2.13 -pl hudi-spark-datasource/hudi-spark3.5.x -am
```

## Related Modules

- [hudi-spark-datasource](../README.md) - Parent module overview
- [hudi-spark3-common](../hudi-spark3-common/README.md) - Spark 3.x shared code
- [hudi-spark3.3.x](../hudi-spark3.3.x/README.md) - Spark 3.3 adapter
- [hudi-spark3.4.x](../hudi-spark3.4.x/README.md) - Spark 3.4 adapter
- [hudi-spark4.0.x](../hudi-spark4.0.x/README.md) - Spark 4.0 adapter
