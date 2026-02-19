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

Version-specific adapter for Spark 3.5.x, handling API differences in Catalyst, schema handling, file scanning, and Avro serialization. This is the default Spark version for the Hudi build.

## Key Classes

- `Spark3_5Adapter` - Spark 3.5 adapter implementing version-specific Catalyst, Avro, and file scanning APIs
- `HoodieSpark35Analysis` - Analysis rules for Spark 3.5 SQL resolution
- `HoodieSpark3_5ExtendedSqlParser` - Extended SQL parser with Hudi-specific syntax
- `HoodieSpark3_5ExtendedSqlAstBuilder` - AST builder for Hudi SQL extensions
- `Spark35HoodieFileScanRDD` - Custom file scan RDD with Hudi unsafe row optimizations
- `HoodieSpark3_5AvroSerializer`, `HoodieSpark3_5AvroDeserializer` - Avro serialization/deserialization for Spark 3.5
- `Spark35OrcReader` - ORC file reader for Spark 3.5
- `Spark35ParquetReader` - Parquet file reader for Spark 3.5
- `Spark35LegacyHoodieParquetFileFormat` - Legacy Parquet file format support
- `HoodieSpark35SchemaUtils` - Schema handling utilities
- `HoodieSpark35CatalystExpressionUtils` - Catalyst expression utilities
- `HoodieSpark35CatalystPlanUtils` - Catalyst plan utilities
- `Spark35ResolveHudiAlterTableCommand` - ALTER TABLE command resolver

## Module Dependencies

- `hudi-spark-client` - Spark-specific write client
- `hudi-spark-common` - Cross-version shared Spark integration code
- `hudi-spark3-common` - Spark 3.x shared abstraction layer

## Build

This is the **default** Spark version profile (`-Dspark3.5`). Supports both Scala 2.12 and 2.13. Requires Java 11.

```bash
# Default build (Spark 3.5, Scala 2.12)
mvn clean package -DskipTests -Dspark3.5

# With Scala 2.13
mvn clean package -DskipTests -Dspark3.5 -Dscala-2.13
```
