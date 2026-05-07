/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.io.storage.row;

import org.apache.avro.Schema;
import org.apache.hudi.HoodieSchemaConversionUtils;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.unsafe.types.UTF8String;

/**
 * Write handle for DSv2 upsert path.
 *
 * Accepts rows in the "enriched DF" schema produced by prepareForUpsert():
 *   [user_col1, ..., _hoodie_record_key, _hoodie_partition_path, file_id, partition_id, file_id_pfx_new]
 *
 * Writes rows in the standard Hudi writer schema:
 *   [_hoodie_commit_time, _hoodie_commit_seqno, _hoodie_record_key, _hoodie_partition_path, _hoodie_file_name, user_col1, ...]
 *
 * Also exposes writePreserved() for old rows already in writer-schema format (read from old parquet).
 */
public class HoodieUpsertHandle extends HoodieRowCreateHandle {

  // Routing columns added by prepareForUpsert. Live on the enriched DataFrame between
  // the Spark SQL writer and the dsv2 DataWriter; never part of user data or table schema.
  public static final String FILE_ID_COL = "file_id";
  public static final String PARTITION_ID_COL = "partition_id";
  public static final String FILE_ID_PFX_NEW_COL = "file_id_pfx_new";

  // Tracking columns added by prepareForUpsert — not part of user data
  private static final Set<String> NON_DATA_COLS = new HashSet<>(Arrays.asList(
      HoodieRecord.COMMIT_TIME_METADATA_FIELD,
      HoodieRecord.COMMIT_SEQNO_METADATA_FIELD,
      HoodieRecord.RECORD_KEY_METADATA_FIELD,
      HoodieRecord.PARTITION_PATH_METADATA_FIELD,
      HoodieRecord.FILENAME_METADATA_FIELD,
      FILE_ID_COL, PARTITION_ID_COL, FILE_ID_PFX_NEW_COL
  ));

  private final StructType inputStructType;
  private final Map<String, Integer> fieldsMap;
  private final int recordKeyPos;
  private final int partitionPathPos;
  private final int[] dataFieldsPos;

  public HoodieUpsertHandle(HoodieTable table, HoodieWriteConfig writeConfig, String partitionPath, String fileId,
                            String instantTime, int taskPartitionId, long taskId, long taskEpochId,
                            StructType inputStructType) {
    super(table, writeConfig, partitionPath, fileId, instantTime, taskPartitionId, taskId, taskEpochId,
        getWriterStructType(writeConfig));
    this.inputStructType = inputStructType;
    this.fieldsMap = getFieldsMap(inputStructType);
    this.recordKeyPos = fieldsMap.get(HoodieRecord.RECORD_KEY_METADATA_FIELD);
    this.partitionPathPos = fieldsMap.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD);
    this.dataFieldsPos = computeDataFieldsPos(inputStructType, fieldsMap);
  }

  /** Writer schema: meta fields (as StringType) at 0-4, then user data columns from writeConfig. */
  private static StructType getWriterStructType(HoodieWriteConfig writeConfig) {
    Schema avroSchema = new Schema.Parser().parse(writeConfig.getWriteSchema());
    StructType structType = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(HoodieSchema.fromAvroSchema(avroSchema));
    if (writeConfig.populateMetaFields()) {
      List<String> metaFields = HoodieRecord.HOODIE_META_COLUMNS;
      List<StructField> dataFields = Arrays.stream(structType.fields())
          .filter(f -> !metaFields.contains(f.name()))
          .collect(Collectors.toList());
      List<StructField> allFields = metaFields.stream()
          .map(f -> new StructField(f, DataTypes.StringType, false, Metadata.empty()))
          .collect(Collectors.toList());
      allFields.addAll(dataFields);
      return new StructType(allFields.toArray(new StructField[0]));
    } else {
      return structType;
    }
  }

  private static int[] computeDataFieldsPos(StructType structType, Map<String, Integer> fieldsMap) {
    return fieldsMap.entrySet().stream()
        .filter(e -> !NON_DATA_COLS.contains(e.getKey()))
        .mapToInt(Map.Entry::getValue)
        .sorted()
        .toArray();
  }

  /**
   * Write a new/updated record from the enriched DF schema.
   * Extracts key and partition from named columns, builds writer-schema row.
   */
  @Override
  public void write(InternalRow row) throws IOException {
    UTF8String recordKey = row.getUTF8String(recordKeyPos);
    UTF8String partitionPath = row.getUTF8String(partitionPathPos);

    UTF8String[] metaFields = new UTF8String[5];
    metaFields[0] = commitTime;
    metaFields[1] = UTF8String.fromString(seqIdGenerator.apply(GLOBAL_SEQ_NO.getAndIncrement()));
    metaFields[2] = recordKey;
    metaFields[3] = partitionPath;
    metaFields[4] = fileName;

    List<Object> fieldValues = new ArrayList<>(dataFieldsPos.length);
    for (int pos : dataFieldsPos) {
      fieldValues.add(row.get(pos, inputStructType.fields()[pos].dataType()));
    }
    InternalRow dataRow = InternalRow.apply(
        scala.collection.JavaConverters.asScalaBufferConverter(fieldValues).asScala());

    InternalRow writerRow = SparkAdapterSupport$.MODULE$.sparkAdapter()
        .createInternalRow(metaFields, dataRow, false);
    writeRow(recordKey, writerRow);
  }

  /**
   * Write a preserved old record (already in writer-schema format, read from existing parquet).
   * The parent-class write() updates commit_time, seqno, and file_name automatically.
   */
  public void writePreserved(InternalRow oldRow) throws IOException {
    super.write(oldRow);
  }

  public static Map<String, Integer> getFieldsMap(StructType structType) {
    StructField[] fields = structType.fields();
    return IntStream.range(0, fields.length)
        .mapToObj(i -> Pair.of(fields[i].name(), i))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }
}
