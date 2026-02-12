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

# `hudi-spark-client` Module

This module provides the Spark-specific write client implementation for Apache Hudi. It contains the engine-specific abstractions for Spark RDD operations, table implementations, indexes, I/O handlers, and action executors.

## Overview

The `hudi-spark-client` module implements:

- `SparkRDDWriteClient` - Main write client for Spark
- `HoodieSparkTable` - Abstract table with Copy-on-Write and Merge-on-Read implementations
- `HoodieSparkEngineContext` - Spark execution context
- Index implementations (Bloom, Bucket, Record-level, Expression)
- I/O factory and file readers/writers (Parquet, Lance)
- Record mergers with multiple strategies
- Bulk insert partitioners
- Clustering strategies (planning, execution, update)
- Async compaction and clustering services

## Directory Structure

```
src/main/
├── java/
│   └── org/apache/hudi/
│       ├── async/                              # Async services
│       │   ├── SparkAsyncCompactService.java
│       │   └── SparkAsyncClusteringService.java
│       ├── client/
│       │   ├── SparkRDDWriteClient.java       # Main write client
│       │   ├── SparkRDDReadClient.java
│       │   ├── SparkRDDTableServiceClient.java
│       │   ├── SparkRDDMetadataWriteClient.java
│       │   ├── common/                         # Engine context
│       │   │   ├── HoodieSparkEngineContext.java
│       │   │   └── SparkReaderContextFactory.java
│       │   ├── clustering/                     # Clustering strategies
│       │   │   ├── plan/strategy/
│       │   │   ├── run/strategy/
│       │   │   └── update/strategy/
│       │   ├── model/
│       │   │   └── HoodieInternalRow.java
│       │   └── validator/                      # Pre-commit validators
│       ├── data/                               # RDD wrappers
│       │   ├── HoodieJavaRDD.java
│       │   ├── HoodieJavaPairRDD.java
│       │   └── HoodieSparkRDDUtils.java
│       ├── execution/
│       │   └── bulkinsert/                     # Bulk insert partitioners
│       ├── index/                              # Index implementations
│       │   ├── SparkHoodieIndexFactory.java
│       │   ├── bloom/
│       │   ├── bucket/
│       │   └── expression/
│       ├── io/storage/                         # I/O classes
│       │   ├── HoodieSparkIOFactory.java
│       │   ├── HoodieSparkParquetReader.java
│       │   ├── HoodieSparkParquetWriter.java
│       │   ├── HoodieSparkLanceReader.java
│       │   ├── HoodieSparkLanceWriter.java
│       │   └── row/                            # InternalRow I/O
│       ├── keygen/                             # Key generators
│       │   └── factory/
│       ├── metadata/                           # Metadata writers
│       ├── table/
│       │   ├── HoodieSparkTable.java
│       │   ├── HoodieSparkCopyOnWriteTable.java
│       │   ├── HoodieSparkMergeOnReadTable.java
│       │   └── action/
│       │       ├── bootstrap/
│       │       ├── cluster/
│       │       ├── commit/                     # Write action executors
│       │       ├── compact/
│       │       └── deltacommit/
│       ├── DefaultSparkRecordMerger.java
│       ├── HoodieSparkRecordMerger.java
│       └── OverwriteWithLatestSparkRecordMerger.java
└── scala/
    └── org/apache/
        ├── hudi/                               # Scala utilities
        │   ├── HoodieSparkUtils.scala
        │   ├── SparkAdapterSupport.scala
        │   ├── HoodieConversionUtils.scala
        │   └── ...
        └── spark/sql/
            ├── HoodieSchemaUtils.scala
            ├── HoodieInternalRowUtils.scala
            ├── avro/
            │   ├── HoodieAvroSerializer.scala
            │   └── HoodieAvroDeserializer.scala
            └── hudi/
                ├── SparkAdapter.scala
                └── execution/
```

## Key Components

### Write Clients

| Class | Description |
|-------|-------------|
| **SparkRDDWriteClient** | Main Spark write client orchestrating tag, partition, write, and commit operations. Handles insert, upsert, bulk insert, and delete operations. |
| **SparkRDDReadClient** | Read client for retrieving data from Hudi tables. |
| **SparkRDDTableServiceClient** | Client for table services (clean, compact, cluster). |
| **SparkRDDMetadataWriteClient** | Writes to the Hudi metadata table. |

### Table Implementations

| Class | Description |
|-------|-------------|
| **HoodieSparkTable** | Abstract base class for Spark table implementations. Generic on payload type T. |
| **HoodieSparkCopyOnWriteTable** | Copy-on-Write table implementation. Immutable base files, replaced on update. |
| **HoodieSparkMergeOnReadTable** | Merge-on-Read table implementation. Logs + base files, merged on read. |
| **HoodieSparkMergeOnReadMetadataTable** | Metadata table implementation using MoR format. |

### Engine Context

| Class | Description |
|-------|-------------|
| **HoodieSparkEngineContext** | Spark-specific implementation of `HoodieEngineContext`. Provides map, flatMap, reduce operations over RDDs. |
| **SparkTaskContextSupplier** | Supplies Spark task context for parallelized operations. |
| **SparkReaderContextFactory** | Creates reader contexts for Spark operations. |

### Record Mergers

| Class | Description |
|-------|-------------|
| **DefaultSparkRecordMerger** | Default event-time-based merge strategy. Merges records based on ordering field. |
| **HoodieSparkRecordMerger** | Base Spark record merger with InternalRow support. |
| **OverwriteWithLatestSparkRecordMerger** | Overwrite-with-latest merge strategy. Always takes the newer record. |

### Index Factory & Implementations

| Class | Description |
|-------|-------------|
| **SparkHoodieIndexFactory** | Factory to create index instances (BLOOM, BUCKET, INMEMORY, SIMPLE, GLOBAL_BLOOM, etc.). |
| **SparkHoodieIndex** | Abstract Spark index base class. |
| **HoodieBloomIndex** | Bloom filter-based index for record key filtering. |
| **HoodieGlobalBloomIndex** | Global bloom filter index across all partitions. |
| **HoodieSparkConsistentBucketIndex** | Consistent hashing bucket index. |
| **HoodieSparkExpressionIndex** | Expression-based filtering index. |
| **SparkMetadataTableRecordLevelIndex** | Record-level index using metadata table. |

### I/O Classes

| Class | Description |
|-------|-------------|
| **HoodieSparkIOFactory** | Factory for creating Spark-specific readers/writers. |
| **HoodieSparkParquetReader** | Parquet file reader implementation. |
| **HoodieSparkParquetWriter** | Parquet file writer implementation. |
| **HoodieSparkParquetStreamWriter** | Streaming parquet writer. |
| **HoodieSparkLanceReader** | Lance vector format reader. |
| **HoodieSparkLanceWriter** | Lance vector format writer. |
| **HoodieSparkBootstrapFileReader** | Reader for bootstrap files. |
| **HoodieRowCreateHandle** | Creates InternalRow-based files. |
| **HoodieInternalRowFileWriter** | Writes InternalRow format files. |

### RDD Wrappers

| Class | Description |
|-------|-------------|
| **HoodieJavaRDD** | Wraps Spark `JavaRDD<T>` implementing `HoodieData<T>`. Provides engine-agnostic distributed collection abstraction. |
| **HoodieJavaPairRDD** | Wraps Spark `JavaPairRDD<K,V>` implementing `HoodiePairData<K,V>`. |
| **HoodieSparkRDDUtils** | RDD utilities (caching, partitioning). |

### Bulk Insert Partitioners

| Class | Description |
|-------|-------------|
| **BulkInsertInternalPartitionerFactory** | Factory for bulk insert partitioners. |
| **GlobalSortPartitioner** | Global sort partitioner for RDD. |
| **GlobalSortPartitionerWithRows** | Global sort partitioner for InternalRow. |
| **NonSortPartitioner** | No-sort partitioner. |
| **PartitionPathRepartitionPartitioner** | Repartition by partition path. |
| **PartitionPathRepartitionAndSortPartitioner** | Repartition and sort by partition. |
| **RDDSpatialCurveSortPartitioner** | Spatial curve sort (Hilbert/Z-order). |
| **RDDSimpleBucketBulkInsertPartitioner** | Simple bucket index partitioner. |
| **RDDConsistentBucketBulkInsertPartitioner** | Consistent hash bucket partitioner. |

### Write Action Executors

| Class | Description |
|-------|-------------|
| **BaseSparkCommitActionExecutor** | Abstract base for commit action executors. |
| **SparkInsertCommitActionExecutor** | INSERT operation executor. |
| **SparkUpsertCommitActionExecutor** | UPSERT operation executor. |
| **SparkBulkInsertCommitActionExecutor** | BULK INSERT operation executor. |
| **SparkDeleteCommitActionExecutor** | DELETE operation executor. |
| **SparkInsertOverwriteCommitActionExecutor** | INSERT OVERWRITE executor. |
| **SparkPartitionTTLActionExecutor** | Partition TTL cleanup executor. |

### Delta Commit Executors (Merge-on-Read)

| Class | Description |
|-------|-------------|
| **BaseSparkDeltaCommitActionExecutor** | Abstract base for delta commit executors. |
| **SparkInsertDeltaCommitActionExecutor** | INSERT to MoR table. |
| **SparkUpsertDeltaCommitActionExecutor** | UPSERT to MoR table. |
| **SparkDeleteDeltaCommitActionExecutor** | DELETE to MoR table. |

### Clustering Strategies

**Planning Strategies:**

| Class | Description |
|-------|-------------|
| **SparkSizeBasedClusteringPlanStrategy** | Plans clustering by file size. |
| **SparkConsistentBucketClusteringPlanStrategy** | Plans for consistent bucket index. |
| **SparkSingleFileSortPlanStrategy** | Plans to sort single files. |

**Execution Strategies:**

| Class | Description |
|-------|-------------|
| **SingleSparkJobExecutionStrategy** | Executes clustering in single Spark job. |
| **MultipleSparkJobExecutionStrategy** | Executes across multiple Spark jobs. |
| **SparkSortAndSizeExecutionStrategy** | Sort and size-based execution. |
| **SparkConsistentBucketClusteringExecutionStrategy** | Consistent bucket execution. |

**Update Strategies:**

| Class | Description |
|-------|-------------|
| **SparkAllowUpdateStrategy** | Allows updates during clustering. |
| **SparkRejectUpdateStrategy** | Rejects updates during clustering. |

### Async Services

| Class | Description |
|-------|-------------|
| **SparkAsyncCompactService** | Async compaction service using Spark executor. |
| **SparkAsyncClusteringService** | Async clustering service using Spark executor. |

### Key Generators

| Class | Description |
|-------|-------------|
| **HoodieSparkKeyGeneratorFactory** | Factory for Spark key generators. |
| **SimpleKeyGenerator** | Simple record key from single field. |
| **ComplexKeyGenerator** | Complex record key from multiple fields. |
| **TimestampBasedKeyGenerator** | Timestamp-based key generator. |
| **NonpartitionedKeyGenerator** | For non-partitioned tables. |
| **CustomKeyGenerator** | Custom field-level key generator. |

### Metadata Table

| Class | Description |
|-------|-------------|
| **SparkHoodieBackedTableMetadataWriter** | Writes Hudi-backed metadata tables. |
| **SparkMetadataWriterFactory** | Factory for metadata writers. |
| **SparkHoodieMetadataBulkInsertPartitioner** | Bulk insert partitioner for metadata. |

### Pre-Commit Validators

| Class | Description |
|-------|-------------|
| **SparkPreCommitValidator** | Base pre-commit validator. |
| **SqlQueryPreCommitValidator** | SQL query-based validation. |
| **SqlQueryEqualityPreCommitValidator** | SQL equality validation. |
| **SqlQueryInequalityPreCommitValidator** | SQL inequality validation. |

## Scala Utilities

| Class | Description |
|-------|-------------|
| **HoodieSparkUtils** | General Spark utilities. |
| **SparkAdapterSupport** | Version-specific adapter support trait. |
| **HoodieConversionUtils** | Scala/Java conversion utilities. |
| **SparkConversionUtils** | Spark-specific conversions. |
| **HoodieSchemaUtils** | Schema utilities. |
| **HoodieInternalRowUtils** | InternalRow utilities. |
| **SparkAdapter** | Main Spark adapter for version compatibility. |
| **HoodieAvroSerializer** | Avro serializer. |
| **HoodieAvroDeserializer** | Avro deserializer. |

## Design Patterns

### Factory Pattern
- `SparkHoodieIndexFactory` - Creates index instances
- `HoodieSparkKeyGeneratorFactory` - Creates key generators
- `BulkInsertInternalPartitionerFactory` - Creates partitioners
- `HoodieSparkIOFactory` - Creates I/O readers/writers

### Strategy Pattern
- Clustering plan strategies (size-based, bucket-based)
- Clustering execution strategies (single/multiple jobs)
- Clustering update strategies (allow/reject)

### Template Method Pattern
- `BaseSparkCommitActionExecutor` - Base for commit executors
- `BaseSparkDeltaCommitActionExecutor` - Base for delta executors

### Adapter Pattern
- `SparkAdapter` - Version-specific Spark compatibility
- `HoodieSparkEngineContext` - Adapts Spark for engine-agnostic interface

### Decorator Pattern
- `HoodieJavaRDD` - Wraps Spark JavaRDD
- `HoodieJavaPairRDD` - Wraps Spark JavaPairRDD

## Dependencies

- `hudi-client-common` - Engine-agnostic write client
- `hudi-common` - Core data model and timeline
- Apache Spark Core and SQL APIs
- Apache Avro

## Build Instructions

```bash
# Build with default Spark version
mvn clean package -DskipTests -pl hudi-client/hudi-spark-client -am

# Build with specific Spark versions
mvn clean package -DskipTests -Dspark3.3 -pl hudi-client/hudi-spark-client -am
mvn clean package -DskipTests -Dspark3.4 -pl hudi-client/hudi-spark-client -am
mvn clean package -DskipTests -Dspark3.5 -pl hudi-client/hudi-spark-client -am
mvn clean package -DskipTests -Dspark4.0 -pl hudi-client/hudi-spark-client -am
```

## Related Modules

- [hudi-client-common](../hudi-client-common/README.md) - Engine-agnostic client
- [hudi-spark-datasource](../../hudi-spark-datasource/README.md) - Spark datasource
- [hudi-spark-common](../../hudi-spark-datasource/hudi-spark-common/README.md) - Shared Spark code
- [hudi-common](../../hudi-common/README.md) - Core data model
