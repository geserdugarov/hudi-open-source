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

package org.apache.hudi.sink.transform;

import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.factory.HoodieAvroKeyGeneratorFactory;
import org.apache.hudi.util.RowDataToAvroConverters;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import static org.apache.hudi.util.StreamerUtil.flinkConf2TypedProperties;

/**
 * Function that converts Flink {@link RowData} into {@link HoodieFlinkInternalRow}.
 */
public class RowDataToHoodieFunction<I extends RowData, O extends HoodieFlinkInternalRow>
    extends RichMapFunction<I, O> {
  /**
   * Row type of the input.
   */
  private final RowType rowType;

  /**
   * Avro schema of the input.
   */
  private transient Schema avroSchema;

  /**
   * RowData to Avro record converter.
   */
  private transient RowDataToAvroConverters.RowDataToAvroConverter converter;

  /**
   * HoodieKey generator.
   */
  private transient KeyGenerator keyGenerator;

  /**
   * Config options.
   */
  private final Configuration config;

  public RowDataToHoodieFunction(RowType rowType, Configuration config) {
    this.rowType = rowType;
    this.config = config;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.avroSchema = StreamerUtil.getSourceSchema(this.config);
    this.converter = RowDataToAvroConverters.createConverter(this.rowType, this.config.getBoolean(FlinkOptions.WRITE_UTC_TIMEZONE));
    this.keyGenerator =
        HoodieAvroKeyGeneratorFactory
            .createKeyGenerator(flinkConf2TypedProperties(this.config));
  }

  @Override
  public O map(I record) throws Exception {
    // [HUDI-8969] Analyze how to get rid of excessive conversions, should be a subtask for RFC-88
    GenericRecord gr = (GenericRecord) this.converter.convert(this.avroSchema, record);
    final HoodieKey hoodieKey = keyGenerator.getKey(gr);
    return (O) new HoodieFlinkInternalRow(
        hoodieKey.getRecordKey(),
        hoodieKey.getPartitionPath(),
        HoodieOperation.fromValue(record.getRowKind().toByteValue()).getName(),
        record);
  }
}
