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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.Map;

/**
 * DSv2 {@link PartitionReaderFactory} that creates {@link HoodiePartitionReader}
 * instances on executors.
 *
 * <p>Must be {@link Serializable} as it is shipped to executors. Holds no
 * references to {@code SparkSession} or {@code HoodieFileIndex}.
 */
public class HoodiePartitionReaderFactory implements PartitionReaderFactory, Serializable {

  private static final long serialVersionUID = 1L;

  private final StructType tableSchema;
  private final StructType requiredSchema;
  private final Map<String, String> properties;

  public HoodiePartitionReaderFactory(StructType tableSchema,
                                       StructType requiredSchema,
                                       Map<String, String> properties) {
    this.tableSchema = tableSchema;
    this.requiredSchema = requiredSchema;
    this.properties = properties;
  }

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    HoodieInputPartition hoodiePartition = (HoodieInputPartition) partition;
    return new HoodiePartitionReader(hoodiePartition, tableSchema, requiredSchema, properties);
  }

  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    return false;
  }
}
