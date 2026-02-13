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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * End-to-end tests for the DSv2 read path.
 *
 * <p>Verifies that COW and MOR tables can be read correctly through the
 * Spark DataSource V2 interface chain
 * ({@code ScanBuilder → Scan → Batch → InputPartition → PartitionReader}).
 */
@Tag("functional")
public class TestHoodieDSv2Read {

  private static SparkSession spark;

  @TempDir
  File tempDir;

  private String tableName;

  @BeforeAll
  static void initSpark() {
    spark = SparkSession.builder()
        .master("local[2]")
        .appName("TestHoodieDSv2Read")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.warehouse.dir",
            System.getProperty("java.io.tmpdir") + "/spark-warehouse-dsv2-" + System.nanoTime())
        .getOrCreate();
    spark.conf().set("hoodie.datasource.read.use.dsv2", "true");
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @BeforeEach
  void setUp() {
    tableName = "dsv2_test_" + System.nanoTime();
  }

  @AfterEach
  void tearDown() {
    spark.sql("DROP TABLE IF EXISTS " + tableName);
  }

  @Test
  void testCowNonPartitionedRead() {
    String tablePath = new File(tempDir, "cow_np").getAbsolutePath();

    spark.sql(String.format(
        "CREATE TABLE %s (id INT, name STRING, price DOUBLE, ts LONG) "
            + "USING hudi "
            + "TBLPROPERTIES (type = 'cow', primaryKey = 'id', preCombineField = 'ts') "
            + "LOCATION '%s'", tableName, tablePath));

    spark.sql(String.format(
        "INSERT INTO %s VALUES "
            + "(1, 'a1', 10.0, 1000), "
            + "(2, 'a2', 20.0, 2000), "
            + "(3, 'a3', 30.0, 3000)", tableName));

    Dataset<Row> result = spark.sql("SELECT id, name, price, ts FROM " + tableName + " ORDER BY id");
    List<Row> rows = result.collectAsList();

    assertEquals(3, rows.size());
    assertEquals(1, rows.get(0).getInt(0));
    assertEquals("a1", rows.get(0).getString(1));
    assertEquals(10.0, rows.get(0).getDouble(2));
    assertEquals(2, rows.get(1).getInt(0));
    assertEquals(3, rows.get(2).getInt(0));
  }

  @Test
  void testCowPartitionedRead() {
    String tablePath = new File(tempDir, "cow_part").getAbsolutePath();

    spark.sql(String.format(
        "CREATE TABLE %s (id INT, name STRING, price DOUBLE, ts LONG, dt STRING) "
            + "USING hudi "
            + "TBLPROPERTIES (type = 'cow', primaryKey = 'id', preCombineField = 'ts') "
            + "PARTITIONED BY (dt) "
            + "LOCATION '%s'", tableName, tablePath));

    spark.sql(String.format(
        "INSERT INTO %s VALUES "
            + "(1, 'a1', 10.0, 1000, '2024-01-01'), "
            + "(2, 'a2', 20.0, 2000, '2024-01-02'), "
            + "(3, 'a3', 30.0, 3000, '2024-01-01')", tableName));

    Dataset<Row> result = spark.sql("SELECT id, name, price, ts, dt FROM " + tableName);
    assertEquals(3, result.count());

    // Verify partition column values
    Dataset<Row> partition1 = spark.sql(
        "SELECT id FROM " + tableName + " WHERE dt = '2024-01-01' ORDER BY id");
    List<Row> p1Rows = partition1.collectAsList();
    assertEquals(2, p1Rows.size());
    assertEquals(1, p1Rows.get(0).getInt(0));
    assertEquals(3, p1Rows.get(1).getInt(0));
  }

  @Test
  void testMorTableRead() {
    String tablePath = new File(tempDir, "mor").getAbsolutePath();

    spark.sql(String.format(
        "CREATE TABLE %s (id INT, name STRING, price DOUBLE, ts LONG) "
            + "USING hudi "
            + "TBLPROPERTIES ("
            + "  type = 'mor', "
            + "  primaryKey = 'id', "
            + "  preCombineField = 'ts', "
            + "  'hoodie.compact.inline' = 'false'"
            + ") "
            + "LOCATION '%s'", tableName, tablePath));

    // Initial insert (creates base files)
    spark.sql(String.format(
        "INSERT INTO %s VALUES "
            + "(1, 'a1', 10.0, 1000), "
            + "(2, 'a2', 20.0, 2000)", tableName));

    // Upsert via MERGE INTO (creates log files for MOR)
    spark.sql("CREATE OR REPLACE TEMP VIEW mor_source AS "
        + "SELECT * FROM VALUES (1, 'a1_updated', 15.0, CAST(1001 AS LONG)), "
        + "(3, 'a3', 30.0, CAST(3000 AS LONG)) "
        + "AS t(id, name, price, ts)");
    spark.sql(String.format(
        "MERGE INTO %s AS target USING mor_source AS source "
            + "ON target.id = source.id "
            + "WHEN MATCHED THEN UPDATE SET * "
            + "WHEN NOT MATCHED THEN INSERT *", tableName));

    Dataset<Row> result = spark.sql(
        "SELECT id, name, price, ts FROM " + tableName + " ORDER BY id");
    List<Row> rows = result.collectAsList();

    assertEquals(3, rows.size());

    // id=1 should reflect the update
    assertEquals(1, rows.get(0).getInt(0));
    assertEquals("a1_updated", rows.get(0).getString(1));
    assertEquals(15.0, rows.get(0).getDouble(2));

    // id=2 should be unchanged
    assertEquals(2, rows.get(1).getInt(0));
    assertEquals("a2", rows.get(1).getString(1));

    // id=3 should be the new insert
    assertEquals(3, rows.get(2).getInt(0));
    assertEquals("a3", rows.get(2).getString(1));
  }

  @Test
  void testColumnPruning() {
    String tablePath = new File(tempDir, "col_prune").getAbsolutePath();

    spark.sql(String.format(
        "CREATE TABLE %s (id INT, name STRING, price DOUBLE, ts LONG) "
            + "USING hudi "
            + "TBLPROPERTIES (type = 'cow', primaryKey = 'id', preCombineField = 'ts') "
            + "LOCATION '%s'", tableName, tablePath));

    spark.sql(String.format(
        "INSERT INTO %s VALUES (1, 'a1', 10.0, 1000), (2, 'a2', 20.0, 2000)", tableName));

    // Select only a subset of columns
    Dataset<Row> result = spark.sql("SELECT id, name FROM " + tableName + " ORDER BY id");
    assertEquals(2, result.schema().fields().length);
    assertEquals("id", result.schema().fields()[0].name());
    assertEquals("name", result.schema().fields()[1].name());

    List<Row> rows = result.collectAsList();
    assertEquals(2, rows.size());
    assertEquals(1, rows.get(0).getInt(0));
    assertEquals("a1", rows.get(0).getString(1));
  }
}
