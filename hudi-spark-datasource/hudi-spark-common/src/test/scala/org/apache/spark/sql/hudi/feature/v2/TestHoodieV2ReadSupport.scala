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

package org.apache.spark.sql.hudi.feature.v2

import org.apache.hudi.DataSourceReadOptions
import org.apache.hudi.common.model.{HoodieFileFormat, HoodieTableType}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.util.{Option => HOption}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hudi.v2.HoodieV2ReadSupport
import org.apache.spark.sql.internal.{SessionState, SQLConf}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.Mockito.{mock, when}

/**
 * Unit tests for [[HoodieV2ReadSupport.isSupportedByDSv2]] gate conditions and
 * [[HoodieV2ReadSupport.resolveReadOptions]] option precedence.
 */
class TestHoodieV2ReadSupport {

  private def mockMetaClient(tableType: HoodieTableType,
                             baseFileFormat: HoodieFileFormat = HoodieFileFormat.PARQUET,
                             multipleBaseFileFormats: Boolean = false,
                             bootstrapBasePath: HOption[String] = HOption.empty[String]()):
  HoodieTableMetaClient = {
    val metaClient = mock(classOf[HoodieTableMetaClient])
    val tableConfig = mock(classOf[HoodieTableConfig])
    when(metaClient.getTableType).thenReturn(tableType)
    when(metaClient.getTableConfig).thenReturn(tableConfig)
    when(tableConfig.getBaseFileFormat).thenReturn(baseFileFormat)
    when(tableConfig.isMultipleBaseFileFormatsEnabled).thenReturn(multipleBaseFileFormats)
    when(tableConfig.getBootstrapBasePath).thenReturn(bootstrapBasePath)
    metaClient
  }

  private def mockSparkSession(sessionConfs: Map[String, String]): SparkSession = {
    val spark = mock(classOf[SparkSession])
    val sessionState = mock(classOf[SessionState])
    val sqlConf = mock(classOf[SQLConf])
    when(spark.sessionState).thenReturn(sessionState)
    when(sessionState.conf).thenReturn(sqlConf)
    when(sqlConf.getAllConfs).thenReturn(sessionConfs)
    spark
  }

  @Test
  def testCowWithDefaultsIsSupported(): Unit = {
    val metaClient = mockMetaClient(HoodieTableType.COPY_ON_WRITE)
    assertTrue(HoodieV2ReadSupport.isSupportedByDSv2(metaClient, Map.empty))
  }

  @Test
  def testMorSnapshotIsNotSupported(): Unit = {
    val metaClient = mockMetaClient(HoodieTableType.MERGE_ON_READ)
    assertFalse(HoodieV2ReadSupport.isSupportedByDSv2(metaClient, Map.empty))
  }

  @Test
  def testMorReadOptimizedIsSupported(): Unit = {
    val metaClient = mockMetaClient(HoodieTableType.MERGE_ON_READ)
    val opts = Map(DataSourceReadOptions.QUERY_TYPE.key ->
      DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
    assertTrue(HoodieV2ReadSupport.isSupportedByDSv2(metaClient, opts))
  }

  @Test
  def testOrcBaseFormatIsNotSupported(): Unit = {
    val metaClient = mockMetaClient(HoodieTableType.COPY_ON_WRITE, baseFileFormat = HoodieFileFormat.ORC)
    assertFalse(HoodieV2ReadSupport.isSupportedByDSv2(metaClient, Map.empty))
  }

  @Test
  def testMultipleBaseFormatsIsNotSupported(): Unit = {
    val metaClient = mockMetaClient(HoodieTableType.COPY_ON_WRITE, multipleBaseFileFormats = true)
    assertFalse(HoodieV2ReadSupport.isSupportedByDSv2(metaClient, Map.empty))
  }

  @Test
  def testIncrementalQueryIsNotSupported(): Unit = {
    val metaClient = mockMetaClient(HoodieTableType.COPY_ON_WRITE)
    val opts = Map(DataSourceReadOptions.QUERY_TYPE.key ->
      DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
    assertFalse(HoodieV2ReadSupport.isSupportedByDSv2(metaClient, opts))
  }

  @Test
  def testCdcFormatIsNotSupported(): Unit = {
    val metaClient = mockMetaClient(HoodieTableType.COPY_ON_WRITE)
    val opts = Map(DataSourceReadOptions.INCREMENTAL_FORMAT.key ->
      DataSourceReadOptions.INCREMENTAL_FORMAT_CDC_VAL)
    assertFalse(HoodieV2ReadSupport.isSupportedByDSv2(metaClient, opts))
  }

  @Test
  def testBootstrapTableIsNotSupported(): Unit = {
    val metaClient = mockMetaClient(
      HoodieTableType.COPY_ON_WRITE,
      bootstrapBasePath = HOption.of("/tmp/bootstrap"))
    assertFalse(HoodieV2ReadSupport.isSupportedByDSv2(metaClient, Map.empty))
  }

  @Test
  def testTimeTravelIsSupported(): Unit = {
    val metaClient = mockMetaClient(HoodieTableType.COPY_ON_WRITE)
    val opts = Map(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key -> "20240101000000000")
    assertTrue(HoodieV2ReadSupport.isSupportedByDSv2(metaClient, opts))
  }

  @Test
  def testResolveReadOptionsAppliesReadDefaults(): Unit = {
    val spark = mockSparkSession(Map.empty)
    val resolved = HoodieV2ReadSupport.resolveReadOptions(spark, Map.empty)
    assertEquals(
      DataSourceReadOptions.QUERY_TYPE.defaultValue,
      resolved(DataSourceReadOptions.QUERY_TYPE.key))
  }

  @Test
  def testResolveReadOptionsLayersSessionConfsUnderExplicitOptions(): Unit = {
    val spark = mockSparkSession(Map(
      DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
      "hoodie.custom.session.key" -> "fromSession"))
    val resolved = HoodieV2ReadSupport.resolveReadOptions(spark, Map(
      DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL))
    // explicit options win over session confs
    assertEquals(
      DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL,
      resolved(DataSourceReadOptions.QUERY_TYPE.key))
    // session confs not shadowed by explicit options are layered in
    assertEquals("fromSession", resolved("hoodie.custom.session.key"))
  }

  @Test
  def testResolveReadOptionsNormalizesSparkHoodieSessionConfs(): Unit = {
    val spark = mockSparkSession(Map(
      "spark.hoodie.custom.session.key" -> "fromSparkPrefixed"))
    val resolved = HoodieV2ReadSupport.resolveReadOptions(spark, Map.empty)
    assertEquals("fromSparkPrefixed", resolved("hoodie.custom.session.key"))
  }

  @Test
  def testResolveReadOptionsHoodieConfWinsOverSparkHoodieConf(): Unit = {
    val spark = mockSparkSession(Map(
      "hoodie.custom.session.key" -> "fromHoodie",
      "spark.hoodie.custom.session.key" -> "fromSparkPrefixed"))
    val resolved = HoodieV2ReadSupport.resolveReadOptions(spark, Map.empty)
    assertEquals("fromHoodie", resolved("hoodie.custom.session.key"))
  }

  @Test
  def testResolveReadOptionsExplicitOptionWinsOverSparkHoodieConf(): Unit = {
    val spark = mockSparkSession(Map(
      "spark.hoodie.custom.session.key" -> "fromSparkPrefixed"))
    val resolved = HoodieV2ReadSupport.resolveReadOptions(spark, Map(
      "hoodie.custom.session.key" -> "explicit"))
    assertEquals("explicit", resolved("hoodie.custom.session.key"))
  }

  @Test
  def testResolveReadOptionsIgnoresNonHoodieSessionConfs(): Unit = {
    val spark = mockSparkSession(Map(
      "spark.sql.shuffle.partitions" -> "42",
      "spark.custom.key" -> "value"))
    val resolved = HoodieV2ReadSupport.resolveReadOptions(spark, Map.empty)
    assertFalse(resolved.contains("spark.sql.shuffle.partitions"))
    assertFalse(resolved.contains("spark.custom.key"))
  }

  @Test
  def testGateConsumesResolvedOptions(): Unit = {
    val metaClient = mockMetaClient(HoodieTableType.COPY_ON_WRITE)
    // query type set only via session conf must be honored by the gate
    val sparkIncremental = mockSparkSession(Map(
      DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL))
    val resolvedIncremental = HoodieV2ReadSupport.resolveReadOptions(sparkIncremental, Map.empty)
    assertFalse(HoodieV2ReadSupport.isSupportedByDSv2(metaClient, resolvedIncremental))
    // an explicit option overriding the session conf must win
    val resolvedSnapshot = HoodieV2ReadSupport.resolveReadOptions(sparkIncremental, Map(
      DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL))
    assertTrue(HoodieV2ReadSupport.isSupportedByDSv2(metaClient, resolvedSnapshot))
  }
}
