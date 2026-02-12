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

# `hudi-spark-datasource` Module

This module contains the Apache Spark integration for Apache Hudi, providing a DataSource API for reading and writing Hudi tables using Spark SQL and DataFrames.

## Overview

The `hudi-spark-datasource` aggregates multiple sub-modules that together provide comprehensive Spark support for Hudi. The modules are organized in a layered architecture to maximize code reuse across different Spark versions while maintaining version-specific optimizations.

## Module Architecture

```
                                 +------------------+
                                 |   hudi-spark     |
                                 | (Main Datasource)|
                                 +--------+---------+
                                          |
                    +---------------------+---------------------+
                    |                                           |
         +----------v-----------+                   +-----------v----------+
         |  hudi-spark3-common  |                   |  hudi-spark4-common  |
         | (Spark 3.x shared)   |                   | (Spark 4.x shared)   |
         +----------+-----------+                   +-----------+----------+
                    |                                           |
    +---------------+---------------+               +-----------+-----------+
    |               |               |               |                       |
+---v---+       +---v---+       +---v---+       +---v---+
|spark  |       |spark  |       |spark  |       |spark  |
|3.3.x  |       |3.4.x  |       |3.5.x  |       |4.0.x  |
+-------+       +-------+       +-------+       +-------+

                    |                                           |
                    +---------------------+---------------------+
                                          |
                              +-----------v-----------+
                              |   hudi-spark-common   |
                              | (All versions shared) |
                              +-----------------------+
```

## Module Descriptions

| Module | Description |
|--------|-------------|
| `hudi-spark-common` | Core Spark integration code shared across all Spark versions. Contains DataSource V1/V2 implementations, file indexing, SQL writers, and incremental read support. |
| `hudi-spark3-common` | Code shared across Spark 3.x versions. Contains Spark 3 adapter interface, DML commands, and partition mapping. |
| `hudi-spark4-common` | Code shared across Spark 4.x versions. Contains Spark 4 adapter interface and 4.x-specific implementations. |
| `hudi-spark3.3.x` | Spark 3.3.x-specific adapter implementation with version-specific SQL parser and file readers. |
| `hudi-spark3.4.x` | Spark 3.4.x-specific adapter implementation. |
| `hudi-spark3.5.x` | Spark 3.5.x-specific adapter implementation (default). |
| `hudi-spark4.0.x` | Spark 4.0.x-specific adapter implementation. |
| `hudi-spark` | Main Spark datasource module containing Spark Session extensions, stored procedures, SQL parser, and logical plans. |

## Spark Version Support

| Spark Version | Module | Scala Version | Java Version | Build Profile |
|---------------|--------|---------------|--------------|---------------|
| 3.3.x | `hudi-spark3.3.x` | 2.12 | 11+ | `-Dspark3.3` |
| 3.4.x | `hudi-spark3.4.x` | 2.12 | 11+ | `-Dspark3.4` |
| 3.5.x (default) | `hudi-spark3.5.x` | 2.12, 2.13 | 11+ | `-Dspark3.5` |
| 4.0.x | `hudi-spark4.0.x` | 2.13 | 17+ | `-Dspark4.0` |

## Build Instructions

```bash
# Build with default Spark version (Spark 3.5 / Scala 2.12)
mvn clean package -DskipTests -pl hudi-spark-datasource/hudi-spark -am

# Build with specific Spark versions
mvn clean package -DskipTests -Dspark3.3
mvn clean package -DskipTests -Dspark3.4
mvn clean package -DskipTests -Dspark3.5
mvn clean package -DskipTests -Dspark3.5 -Dscala-2.13
mvn clean package -DskipTests -Dspark4.0  # Requires Java 17
```

## Running Tests

```bash
# Unit tests
mvn test -Punit-tests -pl hudi-spark-datasource/hudi-spark -am

# Functional tests
mvn test -Pfunctional-tests -pl hudi-spark-datasource/hudi-spark -am

# Run specific Scala test suites
mvn test -Punit-tests -pl hudi-spark-datasource/hudi-spark \
    -DwildcardSuites=org.apache.spark.sql.hudi.dml

# Skip Java tests when running only Scala tests
mvn test -Punit-tests -pl hudi-spark-datasource/hudi-spark \
    -Dtest=skipJavaTests -DwildcardSuites=org.apache.spark.sql.hudi.dml
```

## Key Features

- **DataSource V1/V2 Support**: Full integration with Spark's DataSource API
- **Spark SQL Integration**: Native SQL support for Hudi tables via Spark Session extensions
- **Stored Procedures**: 60+ built-in procedures for table management and operations
- **Time Travel**: Query historical versions of tables
- **Incremental Queries**: Efficient change data capture reads
- **Index Support**: Bloom filters, column statistics, record-level index, and partition stats
- **Streaming Support**: Structured Streaming source for continuous data ingestion
- **CDC Support**: Change Data Capture for tracking row-level changes

## Usage

### Registering Hudi with Spark

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("HudiExample")
  .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
  .getOrCreate()
```

### Writing Data

```scala
df.write
  .format("hudi")
  .option("hoodie.table.name", "my_table")
  .option("hoodie.datasource.write.recordkey.field", "id")
  .option("hoodie.datasource.write.precombine.field", "ts")
  .mode("append")
  .save("/path/to/table")
```

### Reading Data

```scala
// Snapshot query
val df = spark.read.format("hudi").load("/path/to/table")

// Incremental query
val df = spark.read.format("hudi")
  .option("hoodie.datasource.query.type", "incremental")
  .option("hoodie.datasource.read.begin.instanttime", "20230101000000")
  .load("/path/to/table")

// Time travel
val df = spark.read.format("hudi")
  .option("as.of.instant", "20230601120000")
  .load("/path/to/table")
```

## Related Modules

- [hudi-spark-common](hudi-spark-common/README.md) - Core shared Spark integration
- [hudi-spark3-common](hudi-spark3-common/README.md) - Spark 3.x shared code
- [hudi-spark4-common](hudi-spark4-common/README.md) - Spark 4.x shared code
- [hudi-spark](hudi-spark/README.md) - Main Spark datasource
- [hudi-spark-client](../hudi-client/hudi-spark-client/README.md) - Spark write client
