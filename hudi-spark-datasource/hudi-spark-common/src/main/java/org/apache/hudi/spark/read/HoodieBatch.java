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

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;

/**
 * DSv2 {@link Batch} implementation for Hudi tables.
 * Holds the pre-planned list of {@link HoodieInputPartition}s and produces
 * a {@link HoodiePartitionReaderFactory} for executor-side reading.
 */
public class HoodieBatch implements Batch {

  private final List<HoodieInputPartition> partitions;
  private final StructType tableSchema;
  private final StructType requiredSchema;
  private final Map<String, String> properties;

  public HoodieBatch(List<HoodieInputPartition> partitions,
                      StructType tableSchema,
                      StructType requiredSchema,
                      Map<String, String> properties) {
    this.partitions = partitions;
    this.tableSchema = tableSchema;
    this.requiredSchema = requiredSchema;
    this.properties = properties;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    return partitions.toArray(new InputPartition[0]);
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new HoodiePartitionReaderFactory(tableSchema, requiredSchema, properties);
  }
}
