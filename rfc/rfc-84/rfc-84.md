<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# RFC-84: Optimized SerDe of `DataStream` in Flink operators

## Proposers

- @geserdugarov

## Approvers

- @danny0405
- @xiarixiaoyao
- @yuzhaojing

## Status

Design is under discussion.

JIRA: [HUDI-8799](https://issues.apache.org/jira/browse/HUDI-8799)

## Abstract

Currently, in the majority of scenarios when Flink writes into Hudi, the first step is row data conversion into Avro record, which is used for key generation, 
and passed to the following operators as part of `HoodieRecord`. Kryo serializer is used to serialize/deserialize those records. 
And as it mentioned in the [claim for this RFC](https://github.com/apache/hudi/pull/12550), Kryo serde costs are huge, which is unacceptable for stream processing.

This RFC suggests to implement data processing with keeping focus on performance for Flink, and considering Flink's internal data types and serialization.

## Background

Currently, `HoodieRecord` is chosen as standardized API for interacting with a single record, see [RFC-46](../rfc-46/rfc-46.md). 
But `HoodieRecord` complex structure leads to high serialization/deserialization costs if it needed to be sent.
So for Flink main use case scenario of stream processing, when we handle each record separately on different operators, 
current approach with initial conversion into `HoodieRecord` becomes unacceptable.

Conversion into `HoodieRecord` should be done only in operators, which actually perform write into HDFS, S3, etc., to prevent excessive costs. 
And also it allows to implement future optimizations with direct write of Flink `RowData` to parquet files if it needed without any intermediate conversions.
In Flink pipelines we could keep internal `RowData` together with only necessary Hudi metadata.
And these metadata should be added considering Flink data types.

There are seven different categories of 
[supported data types](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/fault-tolerance/serialization/types_serialization/) 
in Flink: tuples, POJOs, primitive types, regular classes, values, Hadoop writables, and special types. 
Among presented data types, tuples are less flexible, but offer the best performance. 
In this case there is no need in custom type description and custom serializer, and we could use already presented in Flink 
[`TupleTypeInfo`](https://github.com/apache/flink/blob/b1fe7b4099497f02b4658df7c3de8e45b62b7e21/flink-core/src/main/java/org/apache/flink/api/java/typeutils/TupleTypeInfo.java) 
and 
[`TupleSerializer`](https://github.com/apache/flink/blob/b1fe7b4099497f02b4658df7c3de8e45b62b7e21/flink-core/src/main/java/org/apache/flink/api/java/typeutils/runtime/TupleSerializer.java).
(All links to Flink documentation or code are provided for version 1.20.) 

## Implementation

To prepare implementation plan we should start from current state review.

### Write

All scenarios of Flink writes into Hudi could be presented in a schema below:

![`DataStream` for `HoodieTableSink`](datastream_hoodietablesink.png)

There are two special cases for processing: bulk insert, and append mode (insert operation into MOR and COW without inline clustering), which are seen in the lower part.
For both of these cases there is no conversion of `RowData` into `HoodieRecord`, so our main focus should be on the upper part of the presented schema.

Flink could automatically chain operators together if it's possible, which means that one operator combines multiple transformations.
For chained operator there is no serialization/deserialization between combined transformations.
But transformations separated by some partitioners, like 
[`keyBy()`](https://github.com/apache/flink/blob/b1fe7b4099497f02b4658df7c3de8e45b62b7e21/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/datastream/DataStream.java#L304-L308), 
which uses 
[`KeyGroupStreamPartitioner`](https://github.com/apache/flink/blob/b1fe7b4099497f02b4658df7c3de8e45b62b7e21/flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/partitioner/KeyGroupStreamPartitioner.java), 
or 
[`partitionCustom()`](https://github.com/apache/flink/blob/b1fe7b4099497f02b4658df7c3de8e45b62b7e21/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/datastream/DataStream.java#L398-L402), 
which expects user defined custom partitioner, couldn't be chained.
Those partitioners are marked as purple blocks in the schema, and at those places we will face high serialization/deserialization costs.

### Read

As for reading of Hudi table by Flink, at the first blush there is no need to implement such optimizations there.

### Metadata

We start writing into Hudi table from `DataStream<RowData>`.
Necessary for processing Hudi metadata is marked by red color on the schema above.
We could use 
[`map()`](https://github.com/apache/flink/blob/b1fe7b4099497f02b4658df7c3de8e45b62b7e21/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/datastream/DataStream.java#L611-L614)
transformation to convert incoming `RowData` into `Tuple(Tuple(StringData, ..., StringData), RowData)` 
instead of `HoodieRecord`, where the first sub-tuple will contain necessary metadata. 
In this case return type could be described as `TupleTypeInfo<>(TupleTypeInfo<>(...), dataStream.getType()))`, which will force Flink to use internal `TupleSerializer`.

### Potential problems

1. Key generators are hardly coupled with Avro `GenericRecord`. 
   Therefore, to support all key generators we will have to do intermediate conversion into Avro in operator, that is responsible for getting Hudi key.
   But for some cases it would be possible to use `RowDataKeyGen` directly on `RowData`.
   Further, it would be great to revise key generators hierarchy as a separate task without binding to this RFC.
2. Payloads creation is also hardly coupled with Avro `GenericRecord`. 
   Similar to the previous point, there would be undesirable intermediate conversion into Avro.
   To confirm the need of this trade off, performance measure using benchmarks with profiling will be performed.
3. For preserving of current behavior, proposed optimizations could be tied to corresponding configuration parameter, and will be turned off by default.
   This means that we will have to add some new classes, which will be used instead of, for instance, `RowDataToHoodieFunction`.
   It will lead to not possible restore from Flink checkpoint, which was done using previous behavior, and continue from this checkpoint using new behavior.
   Flink job restart will be needed to switch to new behavior.

## Rollout/Adoption Plan

- What impact (if any) will there be on existing users?
  - Better performance of Flink write into Hudi for main scenarios.
    For instance, for stream write into Hudi with simple bucket index, [total write time decreased by 15%](https://github.com/apache/hudi/pull/12550).
- If we are changing behavior how will we phase out the older behavior?
  - New behavior could be turned on by enabling corresponding setting, by default, previous behavior will be preserved.
  - We could turn on new behavior by default in the following releases after proper testing.
- If we need special migration tools, describe them here.
  - For such kind of changes, there is no need in special migration tools. 
    But if we consider preserving previous and new behavior simultaneously, then Flink restore from old checkpoints with new behavior wouldn't be possible due to changed classes in operator states.
- When will we remove the existing behavior
  - Previous behavior could be removed after testing that there is no problem with new behavior. 

## Test Plan

This RFC will be tested by running of TPC-H benchmark. 
Also corresponding integration tests will be added.
Existing set of test cases will allow to check that nothing is broken from previous behavior.
