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

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

import java.util.Map;
import java.util.OptionalLong;

/**
 * DSv2 {@link Scan} implementation for Hudi tables.
 *
 * <p>Performs driver-side file planning in {@link #toBatch()} by using
 * {@code HoodieFileIndex} to resolve file slices, then packages them into
 * {@link HoodieInputPartition}s wrapped in a {@link HoodieBatch}.
 *
 * <p>Skeleton implementation — {@code toBatch()} is a stub that will be
 * wired to {@code HoodieFileIndex} in a follow-up change.
 */
public class HoodieScan implements Scan, SupportsReportStatistics {

  private final SparkSession spark;
  private final String tablePath;
  private final StructType tableSchema;
  private final StructType requiredSchema;
  private final Filter[] pushedFilters;
  private final Map<String, String> options;

  public HoodieScan(SparkSession spark, String tablePath,
                     StructType tableSchema, StructType requiredSchema,
                     Filter[] pushedFilters, Map<String, String> options) {
    this.spark = spark;
    this.tablePath = tablePath;
    this.tableSchema = tableSchema;
    this.requiredSchema = requiredSchema;
    this.pushedFilters = pushedFilters;
    this.options = options;
  }

  @Override
  public StructType readSchema() {
    return requiredSchema;
  }

  @Override
  public String description() {
    return "HoodieScan[" + tablePath + "]";
  }

  @Override
  public Batch toBatch() {
    // TODO: create HoodieFileIndex, call filterFileSlices(dataFilters, partitionFilters)
    //       to get file slices, build HoodieTableMetaClient to get latestCommitTime,
    //       then return new HoodieBatch(partitions, tableSchema, requiredSchema, options)
    throw new UnsupportedOperationException("TODO: wire HoodieFileIndex in HoodieScan.toBatch()");
  }

  @Override
  public Statistics estimateStatistics() {
    return new Statistics() {
      @Override
      public OptionalLong sizeInBytes() {
        return OptionalLong.empty();
      }

      @Override
      public OptionalLong numRows() {
        return OptionalLong.empty();
      }
    };
  }
}
