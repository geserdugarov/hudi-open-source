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
- @wombatu-kun

## Status

Design is under discussion.

Implementation is ready for non bucket and simple bucket, and wait for review:
https://github.com/apache/hudi/pull/12796

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
transformation to convert incoming `RowData` into a new object `HoodieFlinkInternalRow`.
We don't use `HoodieFlinkRecord` name to prevent confusion because this class doesn't extend `HoodieRecord`.

Structure of `HoodieFlinkInternalRow`:
```Java
public class HoodieFlinkInternalRow implements Serializable {

  private static final long serialVersionUID = 1L;

  // the number of fields without nesting
  protected static final int ARITY = 7;

  // recordKey, partitionPath, isIndexRecord and rowData are final
  private final StringData recordKey;
  private final StringData partitionPath;
  private StringData fileId;
  private StringData instantTime;
  private StringData operationType;

  // there is no rowData for index record
  private final BooleanValue isIndexRecord;
  private final RowData rowData;

  public HoodieFlinkInternalRow(String recordKey, String partitionPath, RowData rowData) {
    this(recordKey, partitionPath, "", "", "", false, rowData);
  }

  public HoodieFlinkInternalRow(String recordKey, String partitionPath, boolean isIndexRecord, RowData rowData) {
    this(recordKey, partitionPath, "", "", "", isIndexRecord, rowData);
  }

  // constructor for index records without row data
  public HoodieFlinkInternalRow(String recordKey, String partitionPath, String fileId, String instantTime) {
    this(recordKey, partitionPath, fileId, instantTime, "", true, null);
  }

  public HoodieFlinkInternalRow(String recordKey,
                                String partitionPath,
                                String fileId,
                                String instantTime,
                                String operationType,
                                boolean isIndexRecord,
                                RowData rowData) {
    this.recordKey = StringData.fromString(recordKey);
    this.partitionPath = StringData.fromString(partitionPath);
    this.fileId = StringData.fromString(fileId);
    this.instantTime = StringData.fromString(instantTime);
    this.operationType = StringData.fromString(operationType);
    this.isIndexRecord = new BooleanValue(isIndexRecord);
    this.rowData = rowData;
  }

  public String getRecordKey() {}

  public String getPartitionPath() {}

  public void setFileId(String fileId) {}
  public String getFileId() {}

  public void setInstantTime(String instantTime) {}
  public String getInstantTime() {}

  public void setOperationType(String operationType) {}
  public String getOperationType() {}

  public boolean isIndexRecord() {}

  public RowData getRowData() {}
}
```

To describe how to serialize and deserialize it properly, we also need to implement `HoodieFlinkInternalRowTypeInfo` and `HoodieFlinkInternalRowSerializer`.

```Java
public class HoodieFlinkInternalRowTypeInfo extends TypeInformation<HoodieFlinkInternalRow> {

  private static final int ARITY = HoodieFlinkInternalRow.ARITY;

  private final RowType rowType;

  private final TypeInformation<RowData> rowDataInfo;

  public HoodieFlinkInternalRowTypeInfo(RowType rowType, TypeInformation<RowData> rowDataInfo) {
    this.rowType = rowType;
    this.rowDataInfo = rowDataInfo;
  }

  @Override
  public boolean isBasicType() {}

  @Override
  public boolean isTupleType() {}

  @Override
  public int getArity() {}

  @Override
  public int getTotalFields() {}

  @Override
  public Class<HoodieFlinkInternalRow> getTypeClass() {}

  @Override
  public boolean isKeyType() {}

  @Override
  public TypeSerializer<HoodieFlinkInternalRow> createSerializer(ExecutionConfig config) {
    return new HoodieFlinkInternalRowSerializer(this.rowType);
  }

  @Override
  public String toString() {}

  @Override
  public boolean equals(Object obj) {}

  @Override
  public int hashCode() {}

  @Override
  public boolean canEqual(Object obj) {}
}
```

```Java
public class HoodieFlinkInternalRowSerializer extends TypeSerializer<HoodieFlinkInternalRow> {

  private static final long serialVersionUID = 1L;

  protected RowType rowType;

  protected RowDataSerializer rowDataSerializer;

  protected StringDataSerializer stringDataSerializer;

  public HoodieFlinkInternalRowSerializer(RowType rowType) {
    this.rowType = rowType;
    this.rowDataSerializer = new RowDataSerializer(rowType);
    this.stringDataSerializer = StringDataSerializer.INSTANCE;
  }

  @Override
  public boolean isImmutableType() {}

  @Override
  public TypeSerializer<HoodieFlinkInternalRow> duplicate() {}

  @Override
  public HoodieFlinkInternalRow createInstance() {}

  @Override
  public HoodieFlinkInternalRow copy(HoodieFlinkInternalRow from) {}

  @Override
  public HoodieFlinkInternalRow copy(HoodieFlinkInternalRow from, HoodieFlinkInternalRow reuse) {}

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(HoodieFlinkInternalRow record, DataOutputView target) throws IOException {
    boolean isIndexRecord = record.isIndexRecord();
    target.writeBoolean(isIndexRecord);
    stringDataSerializer.serialize(StringData.fromString(record.getRecordKey()), target);
    stringDataSerializer.serialize(StringData.fromString(record.getPartitionPath()), target);
    stringDataSerializer.serialize(StringData.fromString(record.getFileId()), target);
    stringDataSerializer.serialize(StringData.fromString(record.getInstantTime()), target);
    stringDataSerializer.serialize(StringData.fromString(record.getOperationType()), target);
    if (!isIndexRecord) {
      rowDataSerializer.serialize(record.getRowData(), target);
    }
  }

  @Override
  public HoodieFlinkInternalRow deserialize(DataInputView source) throws IOException {
    boolean isIndexRecord = source.readBoolean();
    StringData recordKey = stringDataSerializer.deserialize(source);
    StringData partition = stringDataSerializer.deserialize(source);
    StringData fileId = stringDataSerializer.deserialize(source);
    StringData instantTime = stringDataSerializer.deserialize(source);
    StringData operationType = stringDataSerializer.deserialize(source);
    HoodieFlinkInternalRow record;
    if (!isIndexRecord) {
      RowData rowData = rowDataSerializer.deserialize(source);
      record = new HoodieFlinkInternalRow(
          recordKey.toString(),
          partition.toString(),
          fileId.toString(),
          instantTime.toString(),
          operationType.toString(),
          isIndexRecord,
          rowData);
    } else {
      record = new HoodieFlinkInternalRow(
          recordKey.toString(),
          partition.toString(),
          fileId.toString(),
          instantTime.toString());
    }
    return record;
  }

  @Override
  public HoodieFlinkInternalRow deserialize(HoodieFlinkInternalRow reuse, DataInputView source) throws IOException {}

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {}

  @Override
  public boolean equals(Object obj) {}

  @Override
  public int hashCode() {}

  @Override
  public TypeSerializerSnapshot<HoodieFlinkInternalRow> snapshotConfiguration() {}
}
```

### Potential problems

1. Key generators are hardly coupled with Avro `GenericRecord`. 
   Therefore, to support all key generators we will have to do intermediate conversion into Avro in operator, that is responsible for getting Hudi key.
   But for some cases it would be possible to use `RowDataKeyGen` directly on `RowData`.
   Further, it would be great to revise key generators hierarchy as a separate task without binding to this RFC.
2. Payloads creation is also hardly coupled with Avro `GenericRecord`. 
   Similar to the previous point, there would be undesirable intermediate conversion into Avro.
   To confirm the need of this trade off, performance measure using benchmarks with profiling will be performed.
3. For preserving of current behavior, proposed optimizations could be tied to corresponding configuration parameter, and will be turned off by default.
   This means that we will have to add some new classes, which will extend corresponding ones, for instance, `BootstrapRowDataOperator extends BootstrapOperator`.
   It will lead to not possible restore from Flink checkpoint, which was done using previous behavior, and continue from this checkpoint using new behavior.
   Flink job restart will be needed to switch to new behavior.

## Rollout/Adoption Plan

- What impact (if any) will there be on existing users?
  - Better performance of Flink write into Hudi for main scenarios. 
    Total write time [decreased by 20-30%](https://github.com/apache/hudi/pull/12796).
- If we are changing behavior how will we phase out the older behavior?
  - New behavior could be turned on by enabling corresponding setting, by default, previous behavior will be preserved.
  - We could turn on new behavior by default in the following releases after proper testing.
- If we need special migration tools, describe them here.
  - For such kind of changes, there is no need in special migration tools. 
    But if we consider preserving previous and new behavior simultaneously, 
    then Flink restore from old checkpoints with new behavior wouldn't be possible due to changed classes in operator states.
- When will we remove the existing behavior
  - Previous behavior could be removed after testing that there is no problem with new behavior. 

## Test Plan

This RFC will be tested by running of TPC-H benchmark. 
Also corresponding integration tests will be added.
Existing set of test cases will allow to check that nothing is broken from previous behavior.
