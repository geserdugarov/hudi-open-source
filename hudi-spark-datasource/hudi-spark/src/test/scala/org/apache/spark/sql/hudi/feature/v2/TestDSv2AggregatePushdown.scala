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

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.hudi.v2.HoodieLocalScan

/**
 * DSv2 aggregate pushdown: COUNT(*) / COUNT(col) / MIN / MAX without GROUP BY answered
 * entirely from column-stats metadata via [[HoodieLocalScan]], and the fallback cases —
 * filters, GROUP BY, DISTINCT, float/double MIN/MAX (NaN), missing per-file stats, and a
 * disabled column-stats index.
 */
class TestDSv2AggregatePushdown extends HoodieSparkSqlTestBase {

  /** Asserts the aggregation was answered from metadata: local scan, no aggregate exec. */
  private def assertAggregatePushed(df: DataFrame): Unit = {
    val localScans = df.queryExecution.optimizedPlan.collect {
      case r: DataSourceV2ScanRelation if r.scan.isInstanceOf[HoodieLocalScan] => r.scan
    }
    assert(localScans.size == 1,
      s"expected the aggregate to be answered by a HoodieLocalScan, got:\n${df.queryExecution.optimizedPlan}")
    val physicalPlan = df.queryExecution.executedPlan.toString()
    assert(physicalPlan.contains("LocalTableScan"),
      s"EXPLAIN should plan a LocalTableScan over the pre-computed row, got:\n$physicalPlan")
    assert(!physicalPlan.contains("Aggregate"),
      s"complete pushdown must remove the aggregate operator, got:\n$physicalPlan")
    assert(!physicalPlan.contains("BatchScan"),
      s"no data file may be scanned for a fully answered aggregate, got:\n$physicalPlan")
  }

  /** Asserts the aggregation fell back to a regular scan plus Spark-side aggregation. */
  private def assertAggregateFallsBack(df: DataFrame): Unit = {
    val localScans = df.queryExecution.optimizedPlan.collect {
      case r: DataSourceV2ScanRelation if r.scan.isInstanceOf[HoodieLocalScan] => r.scan
    }
    assert(localScans.isEmpty,
      s"expected fallback to a regular scan, got a local scan:\n${df.queryExecution.optimizedPlan}")
    val physicalPlan = df.queryExecution.executedPlan.toString()
    assert(physicalPlan.contains("BatchScan"), s"fallback must scan the files, got:\n$physicalPlan")
    assert(physicalPlan.contains("Aggregate"),
      s"fallback must keep Spark's aggregate operator, got:\n$physicalPlan")
  }

  /** Two commits, two base files (small-file handling off), `name` partially null. */
  private def writeTwoFileTable(path: String, tableName: String): Unit = {
    val _spark = spark
    import _spark.implicits._
    def write(rows: Seq[(Int, String, Long)], mode: SaveMode): Unit = {
      rows.toDF("id", "name", "ts")
        .write.format("hudi")
        .option("hoodie.table.name", tableName)
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.precombine.field", "ts")
        .option("hoodie.parquet.small.file.limit", "0")
        .mode(mode)
        .save(path)
    }
    write(Seq((1, "a1", 1000L), (2, null, 2000L)), SaveMode.Overwrite)
    write(Seq((3, "b3", 3000L), (4, null, 4000L)), SaveMode.Append)
  }

  test("Test hudi_v2 count min max answered from column stats across files") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      writeTwoFileTable(path, tableName)

      val aggregates = Seq("count(*)", "count(name)", "min(id)", "max(id)", "min(name)", "max(name)", "max(ts)")
      val df = spark.read.format("hudi_v2").load(path).selectExpr(aggregates: _*)
      assertAggregatePushed(df)
      checkAnswer(df.collect())(
        Seq(4L, 2L, 1, 4, "a1", "b3", 4000L)
      )
      // Parity with the DSv1 computed aggregation.
      assertResult(spark.read.format("hudi").load(path).selectExpr(aggregates: _*).collect().toSeq)(
        df.collect().toSeq)
    }
  }

  test("Test hudi_v2 aggregates fall back with filters group by or distinct") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      writeTwoFileTable(path, tableName)

      // A pushed (and re-applied) filter makes per-file stats unusable.
      val filtered = spark.read.format("hudi_v2").load(path).where("id > 1").selectExpr("count(*)")
      assertAggregateFallsBack(filtered)
      checkAnswer(filtered.collect())(Seq(3L))

      // GROUP BY cannot be answered from table-level stats.
      val grouped = spark.read.format("hudi_v2").load(path).groupBy("name").count()
      assertAggregateFallsBack(grouped)
      assertResult(spark.read.format("hudi").load(path).groupBy("name").count()
        .collect().map(_.toSeq).sortBy(_.mkString("|")).toSeq)(
        grouped.collect().map(_.toSeq).sortBy(_.mkString("|")).toSeq)

      // DISTINCT counts are not derivable from value/null counts.
      val distinct = spark.read.format("hudi_v2").load(path).selectExpr("count(distinct name)")
      assertAggregateFallsBack(distinct)
      checkAnswer(distinct.collect())(Seq(2L))
    }
  }

  test("Test hudi_v2 min max on float and double fall back for NaN safety") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      val _spark = spark
      import _spark.implicits._
      // Parquet stats drop NaN while Spark orders NaN above every other value: answering
      // MIN/MAX from stats would return 2.5 for max(d) instead of NaN.
      Seq((1, 1.5f, 2.5d, 100L), (2, Float.NaN, Double.NaN, 200L), (3, -3.5f, -1.5d, 300L))
        .toDF("id", "f", "d", "ts")
        .write.format("hudi")
        .option("hoodie.table.name", tableName)
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.precombine.field", "ts")
        .mode(SaveMode.Overwrite)
        .save(path)

      val minMax = spark.read.format("hudi_v2").load(path).selectExpr("min(f)", "max(f)", "min(d)", "max(d)")
      assertAggregateFallsBack(minMax)
      val row = minMax.collect().head
      assertResult(-3.5f)(row.getFloat(0))
      assert(row.getFloat(1).isNaN, "Spark's max must surface NaN as the largest float")
      assertResult(-1.5d)(row.getDouble(2))
      assert(row.getDouble(3).isNaN, "Spark's max must surface NaN as the largest double")

      // Counts do not depend on bounds: they stay answerable on float/double columns.
      val counts = spark.read.format("hudi_v2").load(path).selectExpr("count(*)", "count(d)")
      assertAggregatePushed(counts)
      checkAnswer(counts.collect())(Seq(3L, 3L))
    }
  }

  test("Test hudi_v2 aggregates on empty table answered as zero and null") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      spark.sql(
        s"""CREATE TABLE $tableName (
           |  id INT,
           |  name STRING,
           |  ts LONG
           |) USING hudi
           |TBLPROPERTIES (
           |  type = 'cow',
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           |)
           |LOCATION '$tablePath'
           """.stripMargin)

      withSQLConf(DataSourceReadOptions.USE_V2_READ.key -> "true") {
        spark.catalog.refreshTable(tableName)
        val df = spark.sql(s"SELECT count(*), count(name), min(id), max(name) FROM $tableName")
        assertAggregatePushed(df)
        checkAnswer(df.collect())(
          Seq(0L, 0L, null, null)
        )
      }
    }
  }

  test("Test hudi_v2 aggregates fall back when per-file stats are missing") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      val _spark = spark
      import _spark.implicits._
      def writer(df: DataFrame): Unit = {
        df.write.format("hudi")
          .option("hoodie.table.name", tableName)
          .option("hoodie.datasource.write.recordkey.field", "id")
          .option("hoodie.datasource.write.precombine.field", "ts")
          // Keep the first file group untouched so it never gains stats for `price`.
          .option("hoodie.parquet.small.file.limit", "0")
          .mode(SaveMode.Append)
          .save(path)
      }
      writer(Seq((1, "a1", 1000L), (2, "a2", 2000L)).toDF("id", "name", "ts"))
      // Schema evolution: `price` (nullable INT — a type MIN/MAX pushdown supports, so the
      // type guard plays no part) exists only in the second file's schema and stats; the
      // first file has no stats record for it, which is the sole reason for the fallback.
      writer(Seq((3, "b3", Option(30), 3000L), (4, "b4", Option(40), 4000L)).toDF("id", "name", "price", "ts"))

      val minPrice = spark.read.format("hudi_v2").load(path).selectExpr("min(price)")
      assertAggregateFallsBack(minPrice)
      checkAnswer(minPrice.collect())(Seq(30))

      // COUNT(col) needs per-file value/null counts for `price` and must fall back too.
      val countPrice = spark.read.format("hudi_v2").load(path).selectExpr("count(price)")
      assertAggregateFallsBack(countPrice)
      checkAnswer(countPrice.collect())(Seq(2L))

      // Aggregates on a fully covered column stay answerable on the same evolved table.
      val minId = spark.read.format("hudi_v2").load(path).selectExpr("min(id)", "max(id)")
      assertAggregatePushed(minId)
      checkAnswer(minId.collect())(Seq(1, 4))

      // COUNT(*) keys off the record-key meta column, which every base file has stats for.
      val countStar = spark.read.format("hudi_v2").load(path).selectExpr("count(*)")
      assertAggregatePushed(countStar)
      checkAnswer(countStar.collect())(Seq(4L))
    }
  }

  test("Test hudi_v2 aggregates fall back without a column stats index") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      val _spark = spark
      import _spark.implicits._
      Seq((1, "a1", 1000L), (2, "a2", 2000L))
        .toDF("id", "name", "ts")
        .write.format("hudi")
        .option("hoodie.table.name", tableName)
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.precombine.field", "ts")
        .option("hoodie.metadata.index.column.stats.enable", "false")
        .mode(SaveMode.Overwrite)
        .save(path)

      val df = spark.read.format("hudi_v2").load(path).selectExpr("count(*)", "min(id)", "max(id)")
      assertAggregateFallsBack(df)
      checkAnswer(df.collect())(Seq(2L, 1, 2))
    }
  }

  test("Test hudi_v2 SQL aggregate with use.v2 matches V1 results") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      spark.sql(
        s"""CREATE TABLE $tableName (
           |  id INT,
           |  name STRING,
           |  amount DOUBLE,
           |  ts LONG
           |) USING hudi
           |TBLPROPERTIES (
           |  type = 'cow',
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           |)
           |LOCATION '$tablePath'
           """.stripMargin)
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'a1', 10.0, 1000), (2, 'a2', 20.0, 2000), (3, null, 30.0, 3000)")

      val query = s"SELECT count(*), count(name), min(id), max(name) FROM $tableName"
      val v1Rows = spark.sql(query).collect().toSeq
      withSQLConf(DataSourceReadOptions.USE_V2_READ.key -> "true") {
        spark.catalog.refreshTable(tableName)
        val v2Df = spark.sql(query)
        assertAggregatePushed(v2Df)
        assertResult(v1Rows)(v2Df.collect().toSeq)
      }
      spark.catalog.refreshTable(tableName)
    }
  }
}
