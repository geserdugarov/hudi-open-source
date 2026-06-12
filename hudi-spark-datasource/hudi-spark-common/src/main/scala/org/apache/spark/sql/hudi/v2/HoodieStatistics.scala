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

import org.apache.spark.sql.connector.read.Statistics

import java.util.OptionalLong

/**
 * Statistics reported by [[HoodieBatchScan]] for Spark's cost-based planning (e.g. join
 * strategy selection). `sizeInBytes` is the sum of the planned split lengths, which equals
 * the total size of the base files left after partition pruning and data skipping — the
 * bytes the scan will actually read. Row counts are not reported: deriving them would
 * require metadata-table lookups that are not worth the planning cost on every scan.
 */
case class HoodieStatistics(totalBytes: Long) extends Statistics {

  override def sizeInBytes(): OptionalLong = OptionalLong.of(totalBytes)

  override def numRows(): OptionalLong = OptionalLong.empty()
}
