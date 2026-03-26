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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.unsafe.types.UTF8String;
import org.jetbrains.annotations.NotNull;
import scala.collection.JavaConverters;

public class HoodieUpsertHandle extends HoodieRowCreateHandle {
  private final Map<String, Integer> fieldsMap;
  private final StructType structType;

  public HoodieUpsertHandle(HoodieTable table, HoodieWriteConfig writeConfig, String partitionPath, String fileId,
                            String instantTime, int taskPartitionId, long taskId, long taskEpochId, StructType structType) {
    super(table, writeConfig, partitionPath, fileId, instantTime, taskPartitionId, taskId, taskEpochId,
        getOriginalStructType(writeConfig));
    this.fieldsMap = getFieldsMap(structType);
    this.structType = structType;
  }

  private static StructType getOriginalStructType(HoodieWriteConfig writeConfig) {
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

  private int[] getFieldsPos(String prefix) {
    return fieldsMap.entrySet().stream()
        .filter(e -> e.getKey().startsWith(prefix))
        .map(Map.Entry::getValue)
        .mapToInt(Integer::intValue)
        .sorted()
        .toArray();
  }

  @Override
  public void write(InternalRow row) throws IOException {
    UTF8String recordKey = row.getUTF8String(fieldsMap.get(HoodieRecord.RECORD_KEY_METADATA_FIELD));
    InternalRow newRecord = getNewRecordWithMetaFields(row, recordKey);
    writeRow(recordKey, newRecord);
  }

  private InternalRow getNewRecordWithMetaFields(InternalRow row, UTF8String recordKey) {
    UTF8String[] metaFields = new UTF8String[5];
    metaFields[3] = row.getUTF8String(fieldsMap.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD));
    metaFields[2] = recordKey;
    metaFields[4] = fileName;
    metaFields[1] = shouldPreserveHoodieMetadata ? row.getUTF8String(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD_ORD)
        : UTF8String.fromString(seqIdGenerator.apply(GLOBAL_SEQ_NO.getAndIncrement()));
    metaFields[0] = shouldPreserveHoodieMetadata ? row.getUTF8String(HoodieRecord.COMMIT_TIME_METADATA_FIELD_ORD)
        : commitTime;
    List<Integer> excludeFields = Arrays.asList(
        fieldsMap.get("partition_id"),
        fieldsMap.get("file_id"),
        fieldsMap.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD),
        fieldsMap.get(HoodieRecord.RECORD_KEY_METADATA_FIELD),
        fieldsMap.get(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD),
        fieldsMap.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD),
        fieldsMap.get(HoodieRecord.FILENAME_METADATA_FIELD)
    );
    InternalRow rowWithoutMetaFields = getRowToWrite(row, fieldsMap.values().toArray(new Integer[0]), i -> !excludeFields.contains(i));
    InternalRow rowWithMetaFields = SparkAdapterSupport$.MODULE$.sparkAdapter().createInternalRow(metaFields, rowWithoutMetaFields, false);
    return rowWithMetaFields;
  }

  private @NotNull InternalRow getRowToWrite(InternalRow row, Integer[] fieldsPos, Predicate<Integer> intPredicate) {
    List<Object> fieldValues =  Arrays.stream(fieldsPos).sequential()
        .filter(intPredicate)
        .sorted()
        .map(i -> row.get(i, structType.fields()[i].dataType()))
        .collect(Collectors.toList());
    return InternalRow.apply(JavaConverters.asScalaBufferConverter(fieldValues).asScala());
  }

  public static Map<String, Integer> getFieldsMap(StructType structType) {
    StructField[] fields = structType.fields();
    IntStream indexes = IntStream.range(0, fields.length);
    return indexes.mapToObj(i -> Pair.of(fields[i].name(), i)).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

}
