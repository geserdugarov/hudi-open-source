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

Version-specific adapter for Spark 4.0.x, handling API differences in Catalyst, schema handling, file scanning, and Avro serialization. Requires Java 17.

## Key Classes

- `Spark4_0Adapter` - Spark 4.0 adapter implementing version-specific Catalyst, Avro, and file scanning APIs
- `HoodieSpark40Analysis` - Analysis rules for Spark 4.0 SQL resolution
- `HoodieSpark4_0ExtendedSqlParser` - Extended SQL parser with Hudi-specific syntax
- `HoodieSpark4_0ExtendedSqlAstBuilder` - AST builder for Hudi SQL extensions
- `Spark40HoodieFileScanRDD` - Custom file scan RDD with Hudi unsafe row optimizations
- `HoodieSpark4_0AvroSerializer`, `HoodieSpark4_0AvroDeserializer` - Avro serialization/deserialization for Spark 4.0
- `Spark40OrcReader` - ORC file reader for Spark 4.0
- `Spark40ParquetReader` - Parquet file reader for Spark 4.0
- `Spark40LegacyHoodieParquetFileFormat` - Legacy Parquet file format support
- `HoodieSpark40SchemaUtils` - Schema handling utilities
- `HoodieSpark40CatalystExpressionUtils` - Catalyst expression utilities
- `HoodieSpark40CatalystPlanUtils` - Catalyst plan utilities
- `Spark40ResolveHudiAlterTableCommand` - ALTER TABLE command resolver

## Module Dependencies

- `hudi-spark-client` - Spark-specific write client
- `hudi-spark-common` - Cross-version shared Spark integration code
- `hudi-spark4-common` - Spark 4.x shared abstraction layer

## Build

Activated by `-Dspark4.0`. Requires **Java 17** (`-Djava17 -Djava.version=17`).

```bash
mvn clean package -DskipTests -Dspark4.0 -Djava17 -Djava.version=17
```
