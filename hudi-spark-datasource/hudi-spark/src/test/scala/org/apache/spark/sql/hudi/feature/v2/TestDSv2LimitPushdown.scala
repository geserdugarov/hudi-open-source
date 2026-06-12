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

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.hudi.v2.{HoodieBatchScan, HoodieInputPartition}

import java.io.File

/**
 * DSv2 limit pushdown (per-partition limits, partial push, refusal when filters are
 * re-applied or the limit sits under a sort) and [[org.apache.spark.sql.hudi.v2.HoodieStatistics]]
 * reporting from planned split lengths.
 */
class TestDSv2LimitPushdown extends HoodieSparkSqlTestBase {

  /** The single HoodieBatchScan of the plan, collected pre-execution (robust to AQE). */
  private def collectScan(df: DataFrame): HoodieBatchScan = {
    val scans = df.queryExecution.optimizedPlan.collect {
      case r: DataSourceV2ScanRelation => r.scan
    }
    assert(scans.size == 1, s"expected exactly one DSv2 scan, got:\n${df.queryExecution.optimizedPlan}")
    scans.head.asInstanceOf[HoodieBatchScan]
  }

  private def writePartitionedTable(path: String, tableName: String): Unit = {
    val _spark = spark
    import _spark.implicits._
    Seq((1, "a1", 10.0, 1000L, "US"), (2, "a2", 20.0, 2000L, "US"),
      (3, "a3", 30.0, 3000L, "UK"), (4, "a4", 40.0, 4000L, "DE"))
      .toDF("id", "name", "price", "ts", "country")
      .write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "country")
      .option("hoodie.datasource.write.hive_style_partitioning", "true")
      .mode(SaveMode.Overwrite)
      .save(path)
  }

  test("Test hudi_v2 limit pushdown across files") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      writePartitionedTable(path, tableName)

      val df = spark.read.format("hudi_v2").load(path).limit(2)
      val scan = collectScan(df)
      assert(scan.pushedLimit.contains(2), "the limit should be pushed when no filters are pushed")
      assert(scan.description().contains("PushedLimit: 2"))
      assert(df.queryExecution.executedPlan.toString().contains("PushedLimit: 2"),
        s"EXPLAIN should surface the pushed limit, got:\n${df.queryExecution.executedPlan}")
      // Partial push: each of the 3 base-file partitions emits at most 2 rows and Spark's
      // retained global limit cuts the result to exactly 2.
      assertResult(2)(df.collect().length)

      // A limit larger than the table must not lose rows.
      val all = spark.read.format("hudi_v2").load(path).limit(10)
      assert(collectScan(all).pushedLimit.contains(10))
      assertResult(4)(all.collect().length)
    }
  }

  test("Test hudi_v2 limit is not pushed when filters are re-applied") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      val _spark = spark
      import _spark.implicits._
      // Single base file whose first rows fail the filter: a per-partition limit applied
      // before the re-applied filter would return 0 rows instead of 2.
      Seq((1, "a1", 1000L), (2, "a2", 2000L), (3, "a3", 3000L), (4, "a4", 4000L))
        .toDF("id", "name", "ts")
        .write.format("hudi")
        .option("hoodie.table.name", tableName)
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.precombine.field", "ts")
        .mode(SaveMode.Overwrite)
        .save(path)

      val df = spark.read.format("hudi_v2").load(path).where("id >= 3").limit(2)
      val scan = collectScan(df)
      assert(scan.pushedLimit.isEmpty,
        "the limit must not be pushed below a filter that Spark re-applies post-scan")
      assert(!scan.description().contains("PushedLimit"))
      checkAnswer(df.select("id", "name").orderBy("id").collect())(
        Seq(3, "a3"),
        Seq(4, "a4")
      )
    }
  }

  test("Test hudi_v2 limit pushdown across splits of one base file") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      val _spark = spark
      import _spark.implicits._
      Seq.tabulate(100)(i => (i, s"name_$i", i.toLong))
        .toDF("id", "name", "ts")
        .write.format("hudi")
        .option("hoodie.table.name", tableName)
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.precombine.field", "ts")
        .mode(SaveMode.Overwrite)
        .save(path)

      // Force the single base file to split into several byte ranges; only the range
      // holding the row groups emits rows, the rest emit none — the global limit still
      // yields exactly the requested row count.
      withSQLConf("spark.sql.files.maxPartitionBytes" -> "65536",
        "spark.sql.files.openCostInBytes" -> "0") {
        val df = spark.read.format("hudi_v2").load(path).limit(3)
        val scan = collectScan(df)
        val partitions = scan.planInputPartitions().map(_.asInstanceOf[HoodieInputPartition])
        assert(partitions.length > 1,
          s"expected the base file to split into multiple ranges, got ${partitions.length}")
        assertResult(1)(partitions.map(_.baseFilePath).distinct.length)
        assert(scan.pushedLimit.contains(3))
        assertResult(3)(df.collect().length)
      }
    }
  }

  test("Test hudi_v2 limit under a sort is not pushed") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      writePartitionedTable(path, tableName)

      // Top-N pushdown is not implemented: a per-partition limit without the sort order
      // would be wrong, so the limit stays with Spark.
      val df = spark.read.format("hudi_v2").load(path).orderBy("id").limit(2)
      assert(collectScan(df).pushedLimit.isEmpty,
        "a limit under a sort must not be pushed without top-N support")
      checkAnswer(df.select("id", "name").collect())(
        Seq(1, "a1"),
        Seq(2, "a2")
      )
    }
  }

  test("Test hudi_v2 scan reports sizeInBytes from planned splits") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      writePartitionedTable(path, tableName)

      val df = spark.read.format("hudi_v2").load(path)
      val scan = collectScan(df)
      val stats = scan.estimateStatistics()
      val partitions = scan.planInputPartitions().map(_.asInstanceOf[HoodieInputPartition])
      assertResult(partitions.map(_.length).sum)(stats.sizeInBytes().getAsLong)
      assert(!stats.numRows().isPresent, "row counts are not reported")

      // The split lengths cover each planned base file exactly once, so the reported size
      // equals the on-disk size of the scanned files.
      val onDiskBytes = partitions.map(_.baseFilePath).distinct
        .map(p => new File(new org.apache.hadoop.fs.Path(p).toUri.getPath).length()).sum
      assertResult(onDiskBytes)(stats.sizeInBytes().getAsLong)
      // The relation surfaces the same estimate to the planner.
      assertResult(BigInt(onDiskBytes))(df.queryExecution.optimizedPlan.stats.sizeInBytes)

      // Partition pruning shrinks the estimate to the files actually read.
      val prunedScan = collectScan(spark.read.format("hudi_v2").load(path).where("country = 'US'"))
      val prunedSize = prunedScan.estimateStatistics().sizeInBytes().getAsLong
      assert(prunedSize > 0 && prunedSize < stats.sizeInBytes().getAsLong,
        s"pruned scan should report fewer bytes, got $prunedSize of ${stats.sizeInBytes().getAsLong}")
    }
  }
}
