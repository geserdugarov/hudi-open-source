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
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * DSv2 {@link ScanBuilder} for Hudi tables.
 *
 * <p>Implements {@link SupportsPushDownFilters} and
 * {@link SupportsPushDownRequiredColumns} for filter and column pruning
 * pushdown from Spark's optimizer.
 *
 * <p>Classifies each pushed filter into one of three buckets:
 * <ul>
 *   <li><b>Partition filters</b> — reference only partition columns;
 *       used for partition pruning in {@code HoodieFileIndex}</li>
 *   <li><b>Data filters</b> — reference only non-partition columns;
 *       pushed to {@code HoodieFileIndex} for data skipping and to
 *       the Parquet reader for predicate pushdown</li>
 *   <li><b>Unsupported filters</b> — reference a mix of partition and
 *       data columns, or cannot be handled; returned to Spark for
 *       post-scan evaluation</li>
 * </ul>
 */
public class HoodieScanBuilder
    implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns {

  private final SparkSession spark;
  private final String tablePath;
  private final StructType tableSchema;
  private final Map<String, String> options;
  private final Set<String> partitionColumnNames;

  private StructType requiredSchema;
  private Filter[] pushedFilters = new Filter[0];
  private Filter[] partitionFilters = new Filter[0];
  private Filter[] dataFilters = new Filter[0];

  public HoodieScanBuilder(SparkSession spark, String tablePath,
                            StructType tableSchema, Map<String, String> options,
                            String[] partitionColumnNames) {
    this.spark = spark;
    this.tablePath = tablePath;
    this.tableSchema = tableSchema;
    this.requiredSchema = tableSchema;
    this.options = options;
    this.partitionColumnNames = new HashSet<>(Arrays.asList(partitionColumnNames));
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    List<Filter> partitionList = new ArrayList<>();
    List<Filter> dataList = new ArrayList<>();
    List<Filter> unsupportedList = new ArrayList<>();

    for (Filter filter : filters) {
      String[] refs = filter.references();
      if (refs.length == 0) {
        // Filters with no column references (e.g. AlwaysTrue/AlwaysFalse)
        // are safe to push as data filters
        dataList.add(filter);
        continue;
      }

      boolean allPartition = true;
      boolean allData = true;
      for (String ref : refs) {
        if (partitionColumnNames.contains(ref)) {
          allData = false;
        } else {
          allPartition = false;
        }
      }

      if (allPartition) {
        partitionList.add(filter);
      } else if (allData) {
        dataList.add(filter);
      } else {
        // Mixed partition + data columns — Spark must evaluate post-scan
        unsupportedList.add(filter);
      }
    }

    this.partitionFilters = partitionList.toArray(new Filter[0]);
    this.dataFilters = dataList.toArray(new Filter[0]);
    this.pushedFilters = new Filter[partitionFilters.length + dataFilters.length];
    System.arraycopy(partitionFilters, 0, pushedFilters, 0, partitionFilters.length);
    System.arraycopy(dataFilters, 0, pushedFilters, partitionFilters.length, dataFilters.length);

    return unsupportedList.toArray(new Filter[0]);
  }

  @Override
  public Filter[] pushedFilters() {
    return pushedFilters;
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    this.requiredSchema = requiredSchema;
  }

  @Override
  public Scan build() {
    return new HoodieScan(spark, tablePath, tableSchema, requiredSchema,
        partitionFilters, dataFilters, options);
  }
}
