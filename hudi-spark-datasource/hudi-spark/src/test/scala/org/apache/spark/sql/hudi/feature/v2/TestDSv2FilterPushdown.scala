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
import org.apache.hudi.common.config.TimestampKeyGeneratorConfig.{TIMESTAMP_INPUT_DATE_FORMAT, TIMESTAMP_OUTPUT_DATE_FORMAT, TIMESTAMP_TYPE_FIELD}

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.hudi.v2.{HoodieBatchScan, HoodieInputPartition}

import java.io.File

/**
 * DSv2 filter pushdown: partition pruning, timestamp keygen partition-filter conversion,
 * metadata-table data skipping via column stats, mixed data/partition filters,
 * stored-versus-path partition columns, and result parity with DSv1 per filter shape.
 */
class TestDSv2FilterPushdown extends HoodieSparkSqlTestBase {

  private def collectScan(df: DataFrame): HoodieBatchScan = {
    val scans = df.queryExecution.executedPlan.collect { case b: BatchScanExec => b }
    assert(scans.size == 1, s"expected exactly one BatchScan, got:\n${df.queryExecution.executedPlan}")
    scans.head.scan.asInstanceOf[HoodieBatchScan]
  }

  /** Distinct base files the scan will actually read, after pruning and data skipping. */
  private def scannedBaseFiles(df: DataFrame): Seq[String] =
    collectScan(df).planInputPartitions()
      .map(_.asInstanceOf[HoodieInputPartition].baseFilePath).distinct.toSeq

  /** Sorted data rows for order-insensitive DSv1-vs-DSv2 comparison. */
  private def sortedRows(df: DataFrame): Seq[Seq[Any]] =
    df.collect().map(_.toSeq).sortBy(_.mkString("|")).toSeq

  /** Asserts a DSv1 and a DSv2 read of the same path with the same filter return the same rows. */
  private def assertFilterParityWithDsv1(path: String,
                                         filter: String,
                                         readOptions: Map[String, String] = Map.empty): Unit = {
    val v1Df = spark.read.format("hudi").options(readOptions).load(path).where(filter)
    val v2Df = spark.read.format("hudi_v2").options(readOptions).load(path).where(filter)
    collectScan(v2Df) // also asserts the DSv2 path planned a single BatchScan
    assert(sortedRows(v1Df) == sortedRows(v2Df),
      s"DSv1 and DSv2 disagree for filter '$filter':\n" +
        s"DSv1: ${sortedRows(v1Df).mkString("; ")}\nDSv2: ${sortedRows(v2Df).mkString("; ")}")
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

  test("Test hudi_v2 partition filter prunes input partitions") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      writePartitionedTable(path, tableName)

      val allFiles = scannedBaseFiles(spark.read.format("hudi_v2").load(path))
      assertResult(3)(allFiles.size)

      val df = spark.read.format("hudi_v2").load(path).where("country = 'US'")
      val usFiles = scannedBaseFiles(df)
      assert(usFiles.nonEmpty && usFiles.size < allFiles.size,
        s"partition filter should prune base files, got ${usFiles.size} of ${allFiles.size}")
      assert(usFiles.forall(_.contains("country=US")),
        s"all scanned files must sit under the matching partition, got:\n${usFiles.mkString("\n")}")

      val scan = collectScan(df)
      // The partition filter is exploited (reported as pushed) but must not reach Parquet:
      // its values live in the partition path, not necessarily in the base files.
      assert(scan.pushedFilters.nonEmpty, "the partition filter should be reported as pushed")
      assert(scan.pushedParquetFilters.isEmpty,
        s"partition-referencing filters must not be forwarded to Parquet, " +
          s"got: ${scan.pushedParquetFilters.mkString(", ")}")

      checkAnswer(df.select("id", "name", "country").orderBy("id").collect())(
        Seq(1, "a1", "US"),
        Seq(2, "a2", "US")
      )
      assertFilterParityWithDsv1(path, "country = 'US'")
      assertFilterParityWithDsv1(path, "country in ('UK', 'DE')")
      assertFilterParityWithDsv1(path, "country = 'NOT_THERE'")
    }
  }

  test("Test hudi_v2 mixed data and partition filters prune partitions and push data filters to Parquet") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      writePartitionedTable(path, tableName)

      // Conjunctive mix arrives as independent filters: the partition conjunct prunes,
      // the data conjunct goes to Parquet (and data skipping).
      val df = spark.read.format("hudi_v2").load(path).where("country = 'US' and id >= 2")
      val files = scannedBaseFiles(df)
      assert(files.forall(_.contains("country=US")),
        s"the partition conjunct must prune partitions, got:\n${files.mkString("\n")}")
      val scan = collectScan(df)
      assert(scan.pushedParquetFilters.nonEmpty, "the data conjunct should be forwarded to Parquet")
      assert(scan.pushedParquetFilters.forall(_.references.forall(_ == "id")),
        s"only partition-free filters may reach Parquet, got: ${scan.pushedParquetFilters.mkString(", ")}")
      assert(scan.description().contains("PushedFilters"))
      checkAnswer(df.select("id", "name", "country").collect())(
        Seq(2, "a2", "US")
      )
      assertFilterParityWithDsv1(path, "country = 'US' and id >= 2")

      // A single OR mixing partition and data columns cannot prune by partition path and
      // must not go to Parquet; classified as a data filter it still prunes through the
      // metadata-table partition stats index (data skipping is on by default): the DE
      // partition matches neither country='UK' nor id=1's stats, leaving 2 of 3 files.
      val orDf = spark.read.format("hudi_v2").load(path).where("country = 'UK' or id = 1")
      val orFiles = scannedBaseFiles(orDf)
      assertResult(2)(orFiles.size)
      assert(orFiles.exists(_.contains("country=US")) && orFiles.exists(_.contains("country=UK")))
      assert(collectScan(orDf).pushedParquetFilters.isEmpty)
      checkAnswer(orDf.select("id", "name", "country").orderBy("id").collect())(
        Seq(1, "a1", "US"),
        Seq(3, "a3", "UK")
      )
      assertFilterParityWithDsv1(path, "country = 'UK' or id = 1")
    }
  }

  test("Test hudi_v2 data filters push down to Parquet and match DSv1 per filter shape") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      val _spark = spark
      import _spark.implicits._
      Seq((1, "a1", 10.0, 1000L), (2, "a2", 20.0, 2000L), (3, null, 30.0, 3000L), (4, "b4", 40.0, 4000L))
        .toDF("id", "name", "price", "ts")
        .write.format("hudi")
        .option("hoodie.table.name", tableName)
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.precombine.field", "ts")
        .mode(SaveMode.Overwrite)
        .save(path)

      val filterShapes = Seq(
        "id = 2",
        "name <=> 'a1'",
        "id > 2",
        "id >= 2",
        "id < 3",
        "id <= 2",
        "id in (1, 3)",
        "name is null",
        "name is not null",
        "id > 1 and price < 35.0",
        "id = 1 or id = 4",
        "not (id = 2)",
        "name like '%1'",
        "name like '%a%'"
      )
      filterShapes.foreach { filter =>
        val df = spark.read.format("hudi_v2").load(path).where(filter)
        val scan = collectScan(df)
        assert(scan.pushedFilters.nonEmpty, s"filter '$filter' should be reported as pushed")
        assert(scan.pushedParquetFilters.nonEmpty,
          s"filter '$filter' references no partition column and should be forwarded to Parquet")
        assertFilterParityWithDsv1(path, filter)
      }

      // The prefix LIKE is compared with data skipping disabled: DSv1's column-stats
      // translation of StartsWith (DataSkippingUtils) checks the prefix literal for
      // containment within [min, max] as if it were an equality, so with data skipping on
      // DSv1 wrongly skips the file ('a' < min 'a1') and returns no rows. DSv2 converts
      // StringStartsWith to a catalyst Like the translation does not recognize, keeps the
      // file conservatively, and returns the matching rows.
      val noSkip = Map(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "false")
      assertFilterParityWithDsv1(path, "name like 'a%'", noSkip)
      checkAnswer(spark.read.format("hudi_v2").load(path).where("name like 'a%'")
        .select("id", "name").orderBy("id").collect())(
        Seq(1, "a1"),
        Seq(2, "a2")
      )

      // Row-group pruning is advisory only: a filter selecting a strict subset of a single
      // base file's rows must still surface exactly the matching rows.
      checkAnswer(spark.read.format("hudi_v2").load(path).where("id > 2")
        .select("id", "price").orderBy("id").collect())(
        Seq(3, 30.0),
        Seq(4, 40.0)
      )
    }
  }

  test("Test hudi_v2 timestamp keygen partition filters are converted before pruning") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      val _spark = spark
      import _spark.implicits._
      // DATE_STRING keygen with differing input/output formats: base files store the
      // input-format value while the partition path holds the output-format one.
      Seq((1, "a1", "2024/01/01", 1000L), (2, "a2", "2024/02/01", 2000L))
        .toDF("id", "name", "ts", "precomb")
        .write.format("hudi")
        .option("hoodie.table.name", tableName)
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.precombine.field", "precomb")
        .option("hoodie.datasource.write.partitionpath.field", "ts")
        .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.TimestampBasedKeyGenerator")
        .option(TIMESTAMP_TYPE_FIELD.key, "DATE_STRING")
        .option(TIMESTAMP_INPUT_DATE_FORMAT.key, "yyyy/MM/dd")
        .option(TIMESTAMP_OUTPUT_DATE_FORMAT.key, "yyyy-MM-dd")
        .mode(SaveMode.Overwrite)
        .save(path)
      assert(new File(path, "2024-01-01").isDirectory,
        "the partition path should be the keygen-formatted date")

      // An input-format filter value matches the stored data but not the partition path; the
      // scan must convert it to the output format for pruning, then Spark's re-applied filter
      // passes on the stored values.
      val df = spark.read.format("hudi_v2").load(path).where("ts = '2024/01/01'")
      val files = scannedBaseFiles(df)
      assertResult(1)(files.size)
      assert(files.head.contains("2024-01-01"),
        s"pruning must use the converted (output-format) filter, got: ${files.head}")
      val scan = collectScan(df)
      assert(scan.pushedParquetFilters.isEmpty,
        "timestamp keygen partition filters must not reach Parquet: the converted value " +
          "would mismatch the stored one")
      checkAnswer(df.select("id", "ts").collect())(
        Seq(1, "2024/01/01")
      )
    }
  }

  test("Test hudi_v2 long-typed timestamp keygen partition filter degrades to a data filter") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      val _spark = spark
      import _spark.implicits._
      // EPOCHMILLISECONDS keygen: ts stays LONG in the table schema while the partition path
      // (and the file-index partition schema) is a formatted date STRING. A long-literal
      // filter cannot be evaluated against path values, so it must not crash the scan or
      // prune; it still selects the right rows through Parquet pushdown plus re-application.
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

      val df = spark.read.format("hudi_v2").load(path).where("ts = 1078016523000")
      // Path-based pruning is impossible, but as a data filter it still prunes through the
      // metadata-table partition stats on the stored long values (data skipping defaults on).
      val files = scannedBaseFiles(df)
      assertResult(1)(files.size)
      assert(files.head.contains("2004-02-29"))
      // No DSv1 parity assert here: the DSv1 path-based read types ts as STRING with
      // path-derived values (see TestDSv2CowSnapshotRead), so the same long filter matches
      // nothing there — DSv2 follows the SQL/catalog semantics instead.
      checkAnswer(df.select("id", "ts").collect())(
        Seq(1, 1078016523000L)
      )
    }
  }

  test("Test hudi_v2 data skipping with column stats prunes file slices") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      val _spark = spark
      import _spark.implicits._
      def write(rows: Seq[(Int, String, Double, Long)], mode: SaveMode): Unit = {
        rows.toDF("id", "name", "price", "ts")
          .write.format("hudi")
          .option("hoodie.table.name", tableName)
          .option("hoodie.datasource.write.recordkey.field", "id")
          .option("hoodie.datasource.write.precombine.field", "ts")
          .option("hoodie.metadata.enable", "true")
          .option("hoodie.metadata.index.column.stats.enable", "true")
          // Force each commit into its own file group so column stats can skip whole files.
          .option("hoodie.parquet.small.file.limit", "0")
          .mode(mode)
          .save(path)
      }
      write(Seq((1, "a1", 10.0, 1000L), (2, "a2", 20.0, 1000L)), SaveMode.Overwrite)
      write(Seq((100, "b1", 1000.0, 2000L), (101, "b2", 1010.0, 2000L)), SaveMode.Append)

      val readOptions = Map(
        "hoodie.metadata.enable" -> "true",
        DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true")

      val allFiles = scannedBaseFiles(spark.read.format("hudi_v2").options(readOptions).load(path))
      assertResult(2)(allFiles.size)

      val df = spark.read.format("hudi_v2").options(readOptions).load(path).where("price > 500.0")
      val skippedFiles = scannedBaseFiles(df)
      assertResult(1)(skippedFiles.size)
      checkAnswer(df.select("id", "name", "price").orderBy("id").collect())(
        Seq(100, "b1", 1000.0),
        Seq(101, "b2", 1010.0)
      )

      assertFilterParityWithDsv1(path, "price > 500.0", readOptions)
      assertFilterParityWithDsv1(path, "id <= 2", readOptions)
    }
  }

  test("Test hudi_v2 partition filter pruning for path-derived partition values") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      val _spark = spark
      import _spark.implicits._
      // drop_partition_columns: country exists only in the partition path, never in the
      // base files — the filter must prune partitions and must not reach Parquet.
      Seq((1, "US", "a1", 1000L), (2, "UK", "a2", 2000L), (3, "US", "a3", 3000L))
        .toDF("id", "country", "name", "ts")
        .write.format("hudi")
        .option("hoodie.table.name", tableName)
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.precombine.field", "ts")
        .option("hoodie.datasource.write.partitionpath.field", "country")
        .option("hoodie.datasource.write.hive_style_partitioning", "true")
        .option("hoodie.datasource.write.drop.partition.columns", "true")
        .mode(SaveMode.Overwrite)
        .save(path)

      val df = spark.read.format("hudi_v2").load(path).where("country = 'US'")
      val files = scannedBaseFiles(df)
      assertResult(1)(files.size)
      assert(files.head.contains("country=US"))
      assert(collectScan(df).pushedParquetFilters.isEmpty,
        "filters on a partition column absent from the base files must not reach Parquet")
      checkAnswer(df.select("id", "country", "name").orderBy("id").collect())(
        Seq(1, "US", "a1"),
        Seq(3, "US", "a3")
      )
      assertFilterParityWithDsv1(path, "country = 'US'")
      assertFilterParityWithDsv1(path, "country = 'US' and id > 1")
    }
  }

  test("Test hudi_v2 partition filter pruning with extract.partition.values.from.path") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val path = s"${tmp.getCanonicalPath}/$tableName"
      writePartitionedTable(path, tableName)

      // Same stored-column table, but values supplied from the path on read: pruning must
      // behave identically and the partition filter must still stay out of Parquet.
      val extractOptions = Map(DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.key -> "true")
      val df = spark.read.format("hudi_v2").options(extractOptions).load(path).where("country = 'US'")
      val files = scannedBaseFiles(df)
      assertResult(1)(files.size)
      assert(files.head.contains("country=US"))
      assert(collectScan(df).pushedParquetFilters.isEmpty)
      checkAnswer(df.select("id", "name", "country").orderBy("id").collect())(
        Seq(1, "a1", "US"),
        Seq(2, "a2", "US")
      )
      assertFilterParityWithDsv1(path, "country = 'US'", extractOptions)
    }
  }

  test("Test hudi_v2 SQL read with filters matches V1 with use.v2 toggled") {
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

      val queries = Seq(
        s"SELECT id, name, amount FROM $tableName WHERE name = 'a1' ORDER BY id",
        s"SELECT id, name, amount FROM $tableName WHERE amount > 15.0 ORDER BY id",
        s"SELECT id, name, amount FROM $tableName WHERE name = 'a1' AND amount > 15.0 ORDER BY id")
      queries.foreach { query =>
        val v1Rows = spark.sql(query).collect().toSeq
        withSQLConf(DataSourceReadOptions.USE_V2_READ.key -> "true") {
          spark.catalog.refreshTable(tableName)
          val v2Df = spark.sql(query)
          assert(v2Df.queryExecution.executedPlan.toString().contains("BatchScan"),
            s"use.v2=true read should use BatchScan for: $query")
          assertResult(v1Rows)(v2Df.collect().toSeq)
        }
        spark.catalog.refreshTable(tableName)
      }
    }
  }
}
