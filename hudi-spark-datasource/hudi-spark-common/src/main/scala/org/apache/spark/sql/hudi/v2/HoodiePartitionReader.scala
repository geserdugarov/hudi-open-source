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
import org.apache.spark.sql.connector.read.PartitionReader

import java.util.NoSuchElementException

/**
 * Partition reader for DSv2 snapshot reads.
 *
 * Skeleton: reads no rows. Reading the assigned byte range of the base file via the
 * shared columnar file reader lands with the COW snapshot read phase.
 */
class HoodiePartitionReader(partition: HoodieInputPartition) extends PartitionReader[InternalRow] {

  override def next(): Boolean = false

  override def get(): InternalRow = {
    throw new NoSuchElementException(
      s"No rows available for split [${partition.start}, ${partition.start + partition.length}) " +
        s"of ${partition.baseFilePath}")
  }

  override def close(): Unit = {}
}
