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

package org.apache.spark.sql.hudi.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.LocalScan
import org.apache.spark.sql.types.StructType

/**
 * Scan returned when an aggregation was fully answered from column-stats metadata at
 * planning time (see [[HoodieAggregatePushDown]]): no files are read and Spark plans a
 * `LocalTableScanExec` over the pre-computed rows, eliminating the aggregate operator
 * entirely (complete aggregate pushdown).
 *
 * @param outputSchema      one field per pushed aggregate function, matching the
 *                          aggregation output Spark expects (LongType for counts, the
 *                          column type for MIN/MAX)
 * @param computedRows      the single result row of the group-by-free aggregation
 * @param pushedAggregates  display forms of the answered aggregate functions
 */
class HoodieLocalScan(outputSchema: StructType,
                      computedRows: Array[InternalRow],
                      pushedAggregates: Seq[String]) extends LocalScan {

  override def readSchema(): StructType = outputSchema

  override def rows(): Array[InternalRow] = computedRows

  override def description(): String =
    s"HoodieLocalScan PushedAggregates: [${pushedAggregates.mkString(", ")}]"
}
