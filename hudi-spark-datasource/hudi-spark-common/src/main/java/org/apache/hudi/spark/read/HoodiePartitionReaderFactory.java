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

package org.apache.hudi.spark.read;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.internal.schema.InternalSchema;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.util.SerializableConfiguration;

import java.io.Serializable;
import java.util.Map;

/**
 * DSv2 {@link PartitionReaderFactory} that creates {@link HoodiePartitionReader}
 * instances on executors for row-based reading, and
 * {@link HoodieColumnarPartitionReader} instances for vectorized columnar
 * reading of COW file slices with no log files.
 *
 * <p>Must be {@link Serializable} as it is shipped to executors. Holds no
 * references to {@code SparkSession} or {@code HoodieFileIndex}.
 */
public class HoodiePartitionReaderFactory implements PartitionReaderFactory, Serializable {

  private static final long serialVersionUID = 1L;

  private final StructType tableSchema;
  private final StructType requiredSchema;
  private final Map<String, String> properties;
  private final SerializableConfiguration serializableConf;
  private final Filter[] dataFilters;
  private final Option<InternalSchema> internalSchemaOpt;
  private final boolean columnarReadSupported;

  public HoodiePartitionReaderFactory(StructType tableSchema,
                                       StructType requiredSchema,
                                       Map<String, String> properties,
                                       SerializableConfiguration serializableConf,
                                       Filter[] dataFilters,
                                       Option<InternalSchema> internalSchemaOpt,
                                       boolean columnarReadSupported) {
    this.tableSchema = tableSchema;
    this.requiredSchema = requiredSchema;
    this.properties = properties;
    this.serializableConf = serializableConf;
    this.dataFilters = dataFilters;
    this.internalSchemaOpt = internalSchemaOpt;
    this.columnarReadSupported = columnarReadSupported;
  }

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    HoodieInputPartition hoodiePartition = (HoodieInputPartition) partition;
    return new HoodiePartitionReader(
        hoodiePartition, tableSchema, requiredSchema, properties,
        serializableConf, dataFilters, internalSchemaOpt);
  }

  /**
   * Returns {@code true} for COW file slices (base file only, no log files)
   * when the schema supports Spark's vectorized Parquet reader.
   *
   * <p>Spark requires all partitions to agree on columnar support. For a pure
   * COW table (no log files anywhere), all partitions return {@code true} and
   * Spark uses vectorized reading. If any partition has log files, all
   * partitions fall back to row-based reading.
   */
  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    if (!columnarReadSupported) {
      return false;
    }
    HoodieInputPartition hoodiePartition = (HoodieInputPartition) partition;
    return !hoodiePartition.hasLogFiles()
        && hoodiePartition.getFileSlice().getBaseFile().isPresent();
  }

  @Override
  public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
    HoodieInputPartition hoodiePartition = (HoodieInputPartition) partition;
    return new HoodieColumnarPartitionReader(
        hoodiePartition, requiredSchema, serializableConf);
  }
}
