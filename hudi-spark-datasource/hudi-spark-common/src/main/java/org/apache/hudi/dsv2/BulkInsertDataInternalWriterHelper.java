/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.dsv2;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.storage.row.HoodieRowCreateHandle;
import org.apache.hudi.io.storage.row.HoodieUpsertHandle;
import org.apache.hudi.table.HoodieTable;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Helper class for HoodieBulkInsertDataInternalWriter used by Spark datasource v2.
 */
public class BulkInsertDataInternalWriterHelper {

  private static final Logger LOG = LoggerFactory.getLogger(BulkInsertDataInternalWriterHelper.class);

  private final String instantTime;
  private final int taskPartitionId;
  private final long taskId;
  private final long taskEpochId;
  private final HoodieTable hoodieTable;
  private final HoodieWriteConfig writeConfig;
  private final StructType structType;
  private final List<WriteStatus> writeStatusList = new ArrayList<>();

  private HoodieRowCreateHandle handle;
  private String lastKnownPartitionPath = null;
  private String fileIdPrefix;
  private int numFilesWritten = 0;
  private Map<String, Integer> fieldsMap;

  public BulkInsertDataInternalWriterHelper(HoodieTable hoodieTable, HoodieWriteConfig writeConfig,
      String instantTime, int taskPartitionId, long taskId, long taskEpochId, StructType structType) {
    this.hoodieTable = hoodieTable;
    this.writeConfig = writeConfig;
    this.instantTime = instantTime;
    this.taskPartitionId = taskPartitionId;
    this.taskId = taskId;
    this.taskEpochId = taskEpochId;
    this.structType = structType;
    this.fieldsMap = HoodieUpsertHandle.getFieldsMap(structType);
    this.fileIdPrefix = UUID.randomUUID().toString();
  }

  public void write(InternalRow record) throws IOException {
    try {
      String partitionPath = record.getUTF8String(fieldsMap.get("actual_partition_path")).toString();
      Object oldFileId = record.get(fieldsMap.get("file_id"), DataTypes.StringType);
      Object newFileIdPfx = record.get(fieldsMap.get("file_id_pfx_new"), DataTypes.StringType);
      String fileId = oldFileId == null ? newFileIdPfx.toString() : oldFileId.toString();

      if ((lastKnownPartitionPath == null) || !lastKnownPartitionPath.equals(partitionPath) || !handle.canWrite()) {
        LOG.info("Creating new file for partition path " + partitionPath);
        createNewHandle(partitionPath, fileId);
        lastKnownPartitionPath = partitionPath;
      }
      handle.write(record);
    } catch (Throwable t) {
      LOG.error("Global error thrown while trying to write records in HoodieRowCreateHandle ", t);
      throw t;
    }
  }

  public List<WriteStatus> getWriteStatuses() throws IOException {
    close();
    return writeStatusList;
  }

  public void abort() {
  }

  private void createNewHandle(String partitionPath, String fileId) throws IOException {
    if (null != handle) {
      close();
    }
    handle = new HoodieUpsertHandle(hoodieTable, writeConfig, partitionPath, fileId,
        instantTime, taskPartitionId, taskId, taskEpochId, structType);
  }

  public void close() throws IOException {
    if (null != handle) {
      writeStatusList.add(handle.close());
      handle = null;
    }
  }

  private String getNextFileId() {
    return String.format("%s-%d", fileIdPrefix, numFilesWritten++);
  }
}
