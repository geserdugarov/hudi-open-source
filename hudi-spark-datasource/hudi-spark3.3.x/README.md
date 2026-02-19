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

Version-specific adapter for Spark 3.3.x, handling API differences in Catalyst, schema handling, file scanning, and Avro serialization.

## Key Classes

- `Spark3_3Adapter` - Spark 3.3 adapter implementing version-specific Catalyst, Avro, and file scanning APIs
- `HoodieSpark33Analysis` - Analysis rules for Spark 3.3 SQL resolution
- `HoodieSpark3_3ExtendedSqlParser` - Extended SQL parser with Hudi-specific syntax
- `HoodieSpark3_3ExtendedSqlAstBuilder` - AST builder for Hudi SQL extensions
- `Spark33HoodieFileScanRDD` - Custom file scan RDD with Hudi unsafe row optimizations
- `HoodieSpark3_3AvroSerializer`, `HoodieSpark3_3AvroDeserializer` - Avro serialization/deserialization for Spark 3.3
- `Spark33OrcReader` - ORC file reader for Spark 3.3
- `Spark33ParquetReader` - Parquet file reader for Spark 3.3
- `Spark33LegacyHoodieParquetFileFormat` - Legacy Parquet file format support
- `HoodieSpark33SchemaUtils` - Schema handling utilities
- `HoodieSpark33CatalystExpressionUtils` - Catalyst expression utilities
- `HoodieSpark33CatalystPlanUtils` - Catalyst plan utilities
- `Spark33ResolveHudiAlterTableCommand` - ALTER TABLE command resolver

## Module Dependencies

- `hudi-spark-client` - Spark-specific write client
- `hudi-spark-common` - Cross-version shared Spark integration code
- `hudi-spark3-common` - Spark 3.x shared abstraction layer

## Build

Activated by `-Dspark3.3`. Supports Scala 2.12 only. Requires Java 11.

```bash
mvn clean package -DskipTests -Dspark3.3
```
