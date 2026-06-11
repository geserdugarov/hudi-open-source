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
import org.apache.hudi.common.config.TimestampKeyGeneratorConfig.{TIMESTAMP_OUTPUT_DATE_FORMAT, TIMESTAMP_TYPE_FIELD}

import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

import java.io.File

/**
 * Base-file snapshot reads via the DSv2 path (COW snapshot and MOR read_optimized):
 * full-table and projected reads, partitioned and non-partitioned tables, multi-split
 * base files, and row-count/data parity against DSv1 on identical data.
 */
class TestDSv2CowSnapshotRead extends HoodieSparkSqlTestBase {

  private def writeTable(path: String,
                         tableName: String,
                         rows: Seq[(Int, String, Double, Long)],
                         mode: SaveMode = SaveMode.Overwrite,
                         extraOptions: Map[String, String] = Map.empty): Unit = {
    val _spark = spark
    import _spark.implicits._
    rows.toDF("id", "name", "price", "ts")
      .write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .options(extraOptions)
      .mode(mode)
      .save(path)
  }

  private def assertBatchScan(df: DataFrame): Unit = {
    val plan = df.queryExecution.executedPlan.toString()
    assert(plan.contains("BatchScan"), s"DSv2 read should plan a BatchScan, got:\n$plan")
  }

  /** Sorted data rows for order-insensitive DSv1-vs-DSv2 comparison. */
  private def sortedRows(df: DataFrame): Seq[Seq[Any]] =
    df.collect().map(_.toSeq).sortBy(_.mkString("|")).toSeq

  /** Asserts identical results from a DSv1 and a DSv2 read of the same projection. */
  private def assertParityWithDsv1(path: String, columns: String*): Unit = {
    val v1Df = spark.read.format("hudi").load(path).selectExpr(columns: _*)
    val v2Df = spark.read.format("hudi_v2").load(path).selectExpr(columns: _*)
    assertBatchScan(v2Df)
    assertResult(sortedRows(v1Df))(sortedRows(v2Df))
  }

  test("Test hudi_v2 full read of a non-partitioned COW table matches DSv1") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      writeTable(path, tableName, Seq((1, "a1", 10.0, 1000L), (2, "a2", 20.0, 2000L), (3, "a3", 30.0, 3000L)))

      val df = spark.read.format("hudi_v2").load(path)
      assertBatchScan(df)
      assertResult(3)(df.count())
      checkAnswer(df.select("id", "name", "price", "ts").orderBy("id").collect())(
        Seq(1, "a1", 10.0, 1000L),
        Seq(2, "a2", 20.0, 2000L),
        Seq(3, "a3", 30.0, 3000L)
      )

      // Full-row parity, meta fields included: DSv1 and DSv2 expose the same schema.
      assertParityWithDsv1(path, "*")
    }
  }

  test("Test hudi_v2 full read of a partitioned COW table matches DSv1") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      val _spark = spark
      import _spark.implicits._
      Seq((1, "a1", 1000L, "US"), (2, "a2", 2000L, "UK"), (3, "a3", 3000L, "US"), (4, "a4", 4000L, "DE"))
        .toDF("id", "name", "ts", "country")
        .write.format("hudi")
        .option("hoodie.table.name", tableName)
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.precombine.field", "ts")
        .option("hoodie.datasource.write.partitionpath.field", "country")
        .mode(SaveMode.Overwrite)
        .save(path)

      val df = spark.read.format("hudi_v2").load(path)
      assertBatchScan(df)
      assertResult(4)(df.count())
      // Partition column values are read from the base files, where Hudi persists them.
      checkAnswer(df.select("id", "country").orderBy("id").collect())(
        Seq(1, "US"),
        Seq(2, "UK"),
        Seq(3, "US"),
        Seq(4, "DE")
      )

      assertParityWithDsv1(path, "*")
      assertParityWithDsv1(path, "country", "name")
    }
  }

  test("Test hudi_v2 read reflects updates from later commits") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      writeTable(path, tableName, Seq((1, "a1", 10.0, 1000L), (2, "a2", 20.0, 1000L)))
      // Upsert: update id=1 and insert id=3 — the scan must pick only the latest file slices.
      writeTable(path, tableName, Seq((1, "a1_new", 15.0, 2000L), (3, "a3", 30.0, 2000L)), mode = SaveMode.Append)

      val df = spark.read.format("hudi_v2").load(path)
      assertBatchScan(df)
      checkAnswer(df.select("id", "name", "price").orderBy("id").collect())(
        Seq(1, "a1_new", 15.0),
        Seq(2, "a2", 20.0),
        Seq(3, "a3", 30.0)
      )

      assertParityWithDsv1(path, "*")
    }
  }

  test("Test hudi_v2 projected read prunes columns at the scan") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      writeTable(path, tableName, Seq((1, "a1", 10.0, 1000L), (2, "a2", 20.0, 2000L)))

      val df = spark.read.format("hudi_v2").load(path).select("name", "id")
      checkAnswer(df.orderBy("id").collect())(
        Seq("a1", 1),
        Seq("a2", 2)
      )

      // The pruned schema must reach the scan itself, not just a projection above it.
      val batchScans = df.queryExecution.executedPlan.collect { case b: BatchScanExec => b }
      assertResult(1)(batchScans.size)
      assertResult(Set("id", "name"))(batchScans.head.output.map(_.name).toSet)

      assertParityWithDsv1(path, "name", "id")
    }
  }

  test("Test hudi_v2 projects meta fields") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      writeTable(path, tableName, Seq((1, "a1", 10.0, 1000L), (2, "a2", 20.0, 2000L)))

      val df = spark.read.format("hudi_v2").load(path).select("_hoodie_record_key", "id", "_hoodie_commit_time")
      assertBatchScan(df)
      val rows = df.collect().sortBy(_.getInt(1))
      assertResult(Seq("1", "2"))(rows.map(_.getString(0)).toSeq)
      assert(rows.forall(r => r.getString(2) != null && r.getString(2).nonEmpty),
        "every row must carry a non-empty _hoodie_commit_time")

      assertParityWithDsv1(path, "_hoodie_record_key", "id", "_hoodie_commit_time")
    }
  }

  test("Test hudi_v2 MOR read_optimized reads base files only, matching DSv1") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      val morOptions = Map("hoodie.datasource.write.table.type" -> "MERGE_ON_READ")
      writeTable(path, tableName, Seq((1, "a1", 10.0, 1000L), (2, "a2", 20.0, 1000L)), extraOptions = morOptions)
      // The upsert lands in log files (no compaction after two commits), so read_optimized
      // must still see the original values.
      writeTable(path, tableName, Seq((1, "a1_new", 15.0, 2000L)), mode = SaveMode.Append, extraOptions = morOptions)

      val roOption = DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL
      val v1Snapshot = spark.read.format("hudi").load(path)
      checkAnswer(v1Snapshot.select("id", "name", "price").orderBy("id").collect())(
        Seq(1, "a1_new", 15.0),
        Seq(2, "a2", 20.0)
      )

      val v2RoDf = spark.read.format("hudi_v2").option(roOption._1, roOption._2).load(path)
      assertBatchScan(v2RoDf)
      checkAnswer(v2RoDf.select("id", "name", "price").orderBy("id").collect())(
        Seq(1, "a1", 10.0),
        Seq(2, "a2", 20.0)
      )

      val v1RoDf = spark.read.format("hudi").option(roOption._1, roOption._2).load(path)
      assertResult(sortedRows(v1RoDf))(sortedRows(v2RoDf))
    }
  }

  test("Test hudi_v2 splits a large base file into multiple input partitions") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      val _spark = spark
      import _spark.implicits._
      // A single base file comfortably larger than the tiny maxPartitionBytes below.
      (1 to 5000).map(i => (i, s"name_$i", i * 1.5, 1000L)).toDF("id", "name", "price", "ts")
        .repartition(1)
        .write.format("hudi")
        .option("hoodie.table.name", tableName)
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.precombine.field", "ts")
        .mode(SaveMode.Overwrite)
        .save(path)

      withSQLConf("spark.sql.files.maxPartitionBytes" -> "16384") {
        val df = spark.read.format("hudi_v2").load(path)
        assertBatchScan(df)
        val numPartitions = df.rdd.getNumPartitions
        assert(numPartitions > 1,
          s"a large base file should split into multiple input partitions, got $numPartitions")

        // No rows duplicated or dropped at split boundaries: the Parquet reader assigns
        // each row group to the split owning its midpoint.
        assertResult(5000L)(df.count())
        val sum = df.selectExpr("sum(id)").collect().head.getLong(0)
        assertResult((1 to 5000).map(_.toLong).sum)(sum)
      }
    }
  }

  test("Test hudi_v2 supplies partition values from the path for drop_partition_columns tables") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      val _spark = spark
      import _spark.implicits._
      Seq((1, "US", "a1", 1000L), (2, "UK", "a2", 2000L), (3, "US", "a3", 3000L))
        .toDF("id", "country", "name", "ts")
        .write.format("hudi")
        .option("hoodie.table.name", tableName)
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.precombine.field", "ts")
        .option("hoodie.datasource.write.partitionpath.field", "country")
        .option("hoodie.datasource.write.drop.partition.columns", "true")
        .mode(SaveMode.Overwrite)
        .save(path)

      // country is absent from the base files; values must be supplied from the
      // partition path, mirroring DSv1's shouldExtractPartitionValuesFromPartitionPath.
      val df = spark.read.format("hudi_v2").load(path)
      assertBatchScan(df)
      checkAnswer(df.select("id", "country", "name").orderBy("id").collect())(
        Seq(1, "US", "a1"),
        Seq(2, "UK", "a2"),
        Seq(3, "US", "a3")
      )

      assertParityWithDsv1(path, "*")
      assertParityWithDsv1(path, "country", "id")
    }
  }

  test("Test hudi_v2 honors extract.partition.values.from.path read option") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      val _spark = spark
      import _spark.implicits._
      // The partition column sits in the middle of the table schema, so with extraction on
      // the reader output (data columns + appended partition values) differs from the read
      // schema and the projection realignment inside HoodiePartitionReader is exercised.
      Seq((1, "US", "a1", 1000L), (2, "UK", "a2", 2000L))
        .toDF("id", "country", "name", "ts")
        .write.format("hudi")
        .option("hoodie.table.name", tableName)
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.precombine.field", "ts")
        .option("hoodie.datasource.write.partitionpath.field", "country")
        .mode(SaveMode.Overwrite)
        .save(path)

      val extractKey = DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.key
      val v2Df = spark.read.format("hudi_v2").option(extractKey, "true").load(path)
        .select("id", "country", "name", "ts")
      assertBatchScan(v2Df)
      checkAnswer(v2Df.orderBy("id").collect())(
        Seq(1, "US", "a1", 1000L),
        Seq(2, "UK", "a2", 2000L)
      )

      val v1Df = spark.read.format("hudi").option(extractKey, "true").load(path)
        .select("id", "country", "name", "ts")
      assertResult(sortedRows(v1Df))(sortedRows(v2Df))
    }
  }

  test("Test hudi_v2 timestamp keygen table reads stored partition values and rejects path extraction") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      val _spark = spark
      import _spark.implicits._
      // EPOCHMILLISECONDS keygen: the partition path is a formatted date string while the
      // table-schema type of ts stays LONG (path "2004-02-29" vs stored 1078016523000).
      Seq((1, "a1", 1078016523000L), (2, "a2", 1718952603000L))
        .toDF("id", "name", "ts")
        .write.format("hudi")
        .option("hoodie.table.name", tableName)
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.precombine.field", "ts")
        .option("hoodie.datasource.write.partitionpath.field", "ts")
        .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.TimestampBasedKeyGenerator")
        .option(TIMESTAMP_TYPE_FIELD.key, "EPOCHMILLISECONDS")
        .option(TIMESTAMP_OUTPUT_DATE_FORMAT.key, "yyyy-MM-dd")
        .mode(SaveMode.Overwrite)
        .save(path)
      assert(new File(path, "2004-02-29").isDirectory,
        "the partition path should be the keygen-formatted date, not the raw epoch value")

      // Default read: ts is persisted in the base files and must surface as the stored
      // values (path-derived date strings would be wrong here).
      val df = spark.read.format("hudi_v2").load(path)
      assertBatchScan(df)
      checkAnswer(df.select("id", "ts").orderBy("id").collect())(
        Seq(1, 1078016523000L),
        Seq(2, 1718952603000L)
      )
      // DSv1's path-based read types timestamp-keygen partition columns as STRING (the
      // file-index partition schema), while DSv2 keeps the table-schema LONG — matching
      // the SQL/catalog semantics pinned by TestSparkSqlWithTimestampKeyGenerator. The
      // stored values themselves must agree, so compare them in stringified form.
      def stringifiedRows(df: DataFrame): Seq[Seq[String]] =
        df.collect().map(_.toSeq.map(String.valueOf)).sortBy(_.mkString("|")).toSeq
      val v1Df = spark.read.format("hudi").load(path).select("id", "name", "ts")
      assertResult(stringifiedRows(v1Df))(stringifiedRows(df.select("id", "name", "ts")))

      // With extraction requested, the path value is a StringType date that cannot be
      // reconciled with the LONG table type — the scan must reject it explicitly rather
      // than feed string path values into the Parquet partition reader.
      val e = intercept[UnsupportedOperationException] {
        spark.read.format("hudi_v2")
          .option(DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.key, "true")
          .load(path)
          .select("id", "ts")
          .collect()
      }
      assert(e.getMessage.contains("extractPartitionValuesFromPartitionPath"))
    }
  }

  test("Test hudi_v2 SQL read returns identical rows to V1 with use.v2 toggled") {
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
           |PARTITIONED BY (name)
           |LOCATION '$tablePath'
           """.stripMargin)
      spark.sql(s"INSERT INTO $tableName VALUES (1, 10.0, 1000, 'a1'), (2, 20.0, 2000, 'a2'), (3, 30.0, 3000, 'a1')")

      def collectSorted(): (String, Seq[Row]) = {
        val df = spark.sql(s"SELECT id, name, amount FROM $tableName ORDER BY id")
        (df.queryExecution.executedPlan.toString(), df.collect().toSeq)
      }

      val (v1Plan, v1Rows) = collectSorted()
      assert(!v1Plan.contains("BatchScan"), s"V1 read must not use BatchScan, got:\n$v1Plan")

      withSQLConf(DataSourceReadOptions.USE_V2_READ.key -> "true") {
        spark.catalog.refreshTable(tableName)
        val (v2Plan, v2Rows) = collectSorted()
        assert(v2Plan.contains("BatchScan"), s"use.v2=true read should use BatchScan, got:\n$v2Plan")
        assertResult(v1Rows)(v2Rows)
      }
    }
  }
}
