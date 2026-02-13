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

import java.util.Map;

/**
 * DSv2 {@link ScanBuilder} for Hudi tables.
 *
 * <p>Implements {@link SupportsPushDownFilters} and
 * {@link SupportsPushDownRequiredColumns} for filter and column pruning
 * pushdown from Spark's optimizer.
 *
 * <p>Skeleton implementation — all pushed filters are accepted (nothing
 * remains for Spark to evaluate post-scan). Classification of filters into
 * partition vs data vs unsupported is left for a follow-up change.
 */
public class HoodieScanBuilder
    implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns {

  private final SparkSession spark;
  private final String tablePath;
  private final StructType tableSchema;
  private final Map<String, String> options;

  private StructType requiredSchema;
  private Filter[] pushedFilters = new Filter[0];

  public HoodieScanBuilder(SparkSession spark, String tablePath,
                            StructType tableSchema, Map<String, String> options) {
    this.spark = spark;
    this.tablePath = tablePath;
    this.tableSchema = tableSchema;
    this.requiredSchema = tableSchema;
    this.options = options;
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    // Accept all filters as pushed; return empty array (nothing left for Spark).
    // TODO: classify into partition vs data vs unsupported filters.
    this.pushedFilters = filters;
    return new Filter[0];
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
    return new HoodieScan(spark, tablePath, tableSchema, requiredSchema, pushedFilters, options);
  }
}
