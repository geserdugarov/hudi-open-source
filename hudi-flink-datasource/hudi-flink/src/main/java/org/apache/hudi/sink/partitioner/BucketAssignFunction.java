/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.partitioner;

import org.apache.hudi.client.FlinkTaskContextSupplier;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.util.FlinkWriteClients;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.StringDataTypeInfo;
import org.apache.flink.util.Collector;

import java.util.Objects;

/**
 * The function to build the write profile incrementally for records within a checkpoint,
 * it then assigns the bucket with ID using the {@link BucketAssigner}.
 *
 * <p>All the records are tagged with HoodieRecordLocation, instead of real instant time,
 * INSERT record uses "I" and UPSERT record uses "U" as instant time. There is no need to keep
 * the "real" instant time for each record, the bucket ID (partition path & fileID) actually decides
 * where the record should write to. The "I" and "U" tags are only used for downstream to decide whether
 * the data bucket is an INSERT or an UPSERT, we should factor the tags out when the underneath writer
 * supports specifying the bucket type explicitly.
 *
 * <p>The output records should then shuffle by the bucket ID and thus do scalable write.
 *
 * @see BucketAssigner
 */
public class BucketAssignFunction
    extends KeyedProcessFunction<String, HoodieFlinkInternalRow, HoodieFlinkInternalRow>
    implements CheckpointedFunction, CheckpointListener {

  /**
   * Index cache(speed-up) state for the underneath file based(BloomFilter) indices.
   * When a record came in, we do these check:
   *
   * <ul>
   *   <li>Try to load all the records in the partition path where the record belongs to</li>
   *   <li>Checks whether the state contains the record key</li>
   *   <li>If it does, tag the record with the location</li>
   *   <li>If it does not, use the {@link BucketAssigner} to generate a new bucket ID</li>
   * </ul>
   *
   * ValueState here is structured as Tuple2(partition, fileId).
   * We use Flink Tuple2 because this state will be serialized/deserialized.
   * Otherwise, for any chosen data structure we should implement custom serializer.
   */
  private ValueState<Tuple2<StringData, StringData>> indexState;

  /**
   * Bucket assigner to assign new bucket IDs or reuse existing ones.
   */
  private BucketAssigner bucketAssigner;

  private final Configuration conf;

  private final boolean isChangingRecords;

  /**
   * If the index is global, update the index for the old partition path
   * if same key record with different partition path came in.
   */
  private final boolean globalIndex;

  public BucketAssignFunction(Configuration conf) {
    this.conf = conf;
    this.isChangingRecords = WriteOperationType.isChangingRecords(
        WriteOperationType.fromValue(conf.getString(FlinkOptions.OPERATION)));
    this.globalIndex = conf.getBoolean(FlinkOptions.INDEX_GLOBAL_ENABLED)
        && !conf.getBoolean(FlinkOptions.CHANGELOG_ENABLED);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    HoodieWriteConfig writeConfig = FlinkWriteClients.getHoodieClientConfig(this.conf, true);
    HoodieFlinkEngineContext context = new HoodieFlinkEngineContext(
        HadoopFSUtils.getStorageConfWithCopy(HadoopConfigurations.getHadoopConf(this.conf)),
        new FlinkTaskContextSupplier(getRuntimeContext()));
    this.bucketAssigner = BucketAssigners.create(
        getRuntimeContext().getIndexOfThisSubtask(),
        getRuntimeContext().getMaxNumberOfParallelSubtasks(),
        getRuntimeContext().getNumberOfParallelSubtasks(),
        OptionsResolver.isInsertOverwrite(conf),
        HoodieTableType.valueOf(conf.getString(FlinkOptions.TABLE_TYPE)),
        context,
        writeConfig);
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) {
    this.bucketAssigner.reset();
  }

  @Override
  public void initializeState(FunctionInitializationContext context) {
    ValueStateDescriptor<Tuple2<StringData, StringData>> indexStateDesc =
        new ValueStateDescriptor<>(
            "indexState",
            new TupleTypeInfo<>(
                StringDataTypeInfo.INSTANCE,
                StringDataTypeInfo.INSTANCE));
    double ttl = conf.getDouble(FlinkOptions.INDEX_STATE_TTL) * 24 * 60 * 60 * 1000;
    if (ttl > 0) {
      indexStateDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.milliseconds((long) ttl)).build());
    }
    indexState = context.getKeyedStateStore().getState(indexStateDesc);
  }

  @Override
  public void processElement(HoodieFlinkInternalRow value, Context ctx, Collector<HoodieFlinkInternalRow> out) throws Exception {
    if (value.isIndexRecord()) {
      this.indexState.update(
          new Tuple2<>(
              StringData.fromString(value.getPartitionPath()),
              StringData.fromString(value.getFileId())));
    } else {
      processRecord(value, out);
    }
  }

  private void processRecord(HoodieFlinkInternalRow record, Collector<HoodieFlinkInternalRow> out) throws Exception {
    // 1. put the record into the BucketAssigner;
    // 2. look up the state for location, if the record has a location, just send it out;
    // 3. if it is an INSERT, decide the location using the BucketAssigner then send it out.
    String recordKey = record.getRecordKey();
    String partition = record.getPartitionPath();
    RowData row = record.getRowData();

    Tuple2<String, String> location;
    if (isChangingRecords) {
      // Only changing records need looking up the index for the location,
      // append only records are always recognized as INSERT.
      // Structured as Tuple(partition, fileId, instantTime).
      Tuple2<StringData, StringData> indexStateValue = indexState.value();
      if (indexStateValue != null) {
        // Set up the instant time as "U" to mark the bucket as an update bucket.
        String partitionFromState = indexStateValue.getField(0).toString();
        String fileIdFromState = indexStateValue.getField(1).toString();
        if (!Objects.equals(partitionFromState, partition)) {
          // [HUDI-8996] No delete records for Flink upsert if partition path changed
          if (globalIndex) {
            // if partition path changes, emit a delete record for old partition path,
            // then update the index state using location with new partition path.
            HoodieFlinkInternalRow deleteRecord =
                new HoodieFlinkInternalRow(recordKey, partitionFromState, fileIdFromState, "U", "D", false, row);
            out.collect(deleteRecord);
          }
          location = getNewRecordLocation(partition);
        } else {
          location = new Tuple2<>("U", fileIdFromState);
          this.bucketAssigner.addUpdate(partition, fileIdFromState);
        }
      } else {
        location = getNewRecordLocation(partition);
      }
      // always refresh the index
      this.indexState.update(
          new Tuple2<>(
              StringData.fromString(partition),
              StringData.fromString(location.getField(1))));
    } else {
      location = getNewRecordLocation(partition);
    }
    record.setInstantTime(location.getField(0));
    record.setFileId(location.getField(1));

    out.collect(record);
  }

  protected Tuple2<String, String> getNewRecordLocation(String partitionPath) {
    final BucketInfo bucketInfo = this.bucketAssigner.addInsert(partitionPath);
    String instantTime;
    String fileId;
    switch (bucketInfo.getBucketType()) {
      case INSERT:
        // This is an insert bucket, use HoodieRecordLocation instant time as "I".
        // Downstream operators can then check the instant time to know whether
        // a record belongs to an insert bucket.
        instantTime = "I";
        fileId = bucketInfo.getFileIdPrefix();
        break;
      case UPDATE:
        instantTime = "U";
        fileId = bucketInfo.getFileIdPrefix();
        break;
      default:
        throw new AssertionError();
    }
    return new Tuple2<>(instantTime, fileId);
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {
    // Refresh the table state when there are new commits.
    this.bucketAssigner.reload(checkpointId);
  }

  @Override
  public void close() throws Exception {
    this.bucketAssigner.close();
  }
}
