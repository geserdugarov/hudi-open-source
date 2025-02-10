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
import org.apache.hudi.client.model.HoodieFlinkRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.util.FlinkWriteClients;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.StringDataTypeInfo;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Objects;

/**
 * Analogous to {@link BucketAssignFunction} for processing of {@link HoodieFlinkRecord}.
 */
public class BucketAssignRowDataFunction<K, I extends HoodieFlinkRecord, O extends HoodieFlinkRecord> extends BucketAssignFunction<K, I, O> {

  /**
   * State for known record keys, which structured as Tuple(partition, fileId, instantTime).
   * If record key is in the state, then update location from the state.
   * Otherwise, use the {@link BucketAssigner} to generate a new bucket ID.
   */
  private ValueState<Tuple3<StringData, StringData, StringData>> indexState;

  public BucketAssignRowDataFunction(Configuration config) {
    super(config);
  }

  @Override
  public void initializeState(FunctionInitializationContext context) {
    ValueStateDescriptor indexStateDesc =
        new ValueStateDescriptor<>(
            "indexState",
            new TupleTypeInfo<>(
                StringDataTypeInfo.INSTANCE,
                StringDataTypeInfo.INSTANCE,
                StringDataTypeInfo.INSTANCE));
    double ttl = conf.getDouble(FlinkOptions.INDEX_STATE_TTL) * 24 * 60 * 60 * 1000;
    if (ttl > 0) {
      indexStateDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.milliseconds((long) ttl)).build());
    }
    indexState = context.getKeyedStateStore().getState(indexStateDesc);
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
  public void processElement(I value, Context ctx, Collector<O> out) throws Exception {
    if (value.isIndexRecord()) {
      this.indexState.update(
          new Tuple3<>(
              StringData.fromString(value.getPartitionPath()),
              StringData.fromString(value.getFileId()),
              StringData.fromString(value.getInstantTime())));
    } else {
      processHoodieFlinkRecord(value, out);
    }
  }

  private void processHoodieFlinkRecord(HoodieFlinkRecord record, Collector<O> out) throws IOException {
    String recordKey = record.getRecordKey();
    String partition = record.getPartitionPath();
    RowData row = record.getRowData();

    HoodieRecordLocation location;
    if (isChangingRecords) {
      // Only changing records need looking up the index for the location,
      // append only records are always recognized as INSERT.
      // Structured as Tuple(partition, fileId, instantTime).
      Tuple3<StringData, StringData, StringData> indexStateValue = indexState.value();
      if (indexStateValue != null) {
        // Set up the instant time as "U" to mark the bucket as an update bucket.
        String partitionFromState = indexStateValue.getField(0).toString();
        if (!Objects.equals(partitionFromState, partition)) {
          if (globalIndex) {
            // if partition path changes, emit a delete record for old partition path,
            // then update the index state using location with new partition path.
            HoodieFlinkRecord deleteRecord = new HoodieFlinkRecord(recordKey, partitionFromState, row);
            deleteRecord.setOperationType("D");
            out.collect((O) deleteRecord);
          }
          location = getNewRecordLocation(partition);
        } else {
          location = new HoodieRecordLocation("U", indexStateValue.getField(1).toString(), HoodieRecordLocation.INVALID_POSITION);
          this.bucketAssigner.addUpdate(partition, location.getFileId());
        }
      } else {
        location = getNewRecordLocation(partition);
      }
      // always refresh the index
      this.indexState.update(
          new Tuple3<>(
              StringData.fromString(partition),
              StringData.fromString(location.getFileId()),
              StringData.fromString(location.getInstantTime())));
    } else {
      location = getNewRecordLocation(partition);
    }
    record.setFileId(location.getFileId());
    record.setInstantTime(location.getInstantTime());

    out.collect((O) record);
  }
}
