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

package org.apache.hudi.spark.read

import org.apache.hudi.HoodieFileIndex
import org.apache.hudi.common.model.FileSlice
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.fs.HadoopFSUtils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.NoopCache
import org.apache.spark.sql.types.StructType

import java.util.{ArrayList => JArrayList, List => JList, Map => JMap}

import scala.collection.JavaConverters._

/**
 * Scala helper for DSv2 read classes to interact with {@link HoodieFileIndex}.
 *
 * <p>Bridges the Scala-heavy {@code HoodieFileIndex} API into Java-friendly
 * return types so that the Java DSv2 classes ({@link HoodieScan},
 * {@link HoodiePartitionReader}, etc.) can remain pure Java.
 */
object HoodieScanHelper {

  /**
   * Plans input partitions for a DSv2 read by creating a {@link HoodieFileIndex}
   * and collecting all file slices (no filter pushdown).
   *
   * @param spark       active SparkSession (driver side)
   * @param tablePath   base path of the Hudi table
   * @param tableSchema full table schema
   * @param options     merged table properties and read options
   * @return a list of {@link HoodieInputPartition} ready to be shipped to executors
   */
  def planInputPartitions(spark: SparkSession,
                          tablePath: String,
                          tableSchema: StructType,
                          options: JMap[String, String]): JList[HoodieInputPartition] = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    val storageConf = HadoopFSUtils.getStorageConf(hadoopConf)
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(storageConf)
      .setBasePath(tablePath)
      .build()

    val scalaOptions = options.asScala.toMap
    val fileIndex = HoodieFileIndex(spark, metaClient, Some(tableSchema),
      scalaOptions, NoopCache, includeLogFiles = true, shouldEmbedFileSlices = true)

    val allFileSlices = fileIndex.filterFileSlices(
      dataFilters = Seq.empty,
      partitionFilters = Seq.empty)

    val lastInstantOpt = metaClient.getCommitsAndCompactionTimeline
      .filterCompletedInstants().lastInstant()
    val latestCommitTime = if (lastInstantOpt.isPresent) {
      lastInstantOpt.get.requestedTime
    } else {
      ""
    }

    val partitions = new JArrayList[HoodieInputPartition]()
    var index = 0
    for ((_, fileSlices) <- allFileSlices) {
      for (fs <- fileSlices) {
        partitions.add(new HoodieInputPartition(index, fs, fs.getPartitionPath,
          tablePath, latestCommitTime))
        index += 1
      }
    }

    partitions
  }
}
