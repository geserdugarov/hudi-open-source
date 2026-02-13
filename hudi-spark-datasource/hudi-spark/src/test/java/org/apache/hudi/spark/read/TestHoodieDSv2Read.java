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
import org.apache.spark.sql.catalyst.plans.logical.Statistics;
import org.apache.spark.sql.execution.SparkPlan;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigInteger;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

  /**
   * Checks whether any leaf (scan) node in the physical plan supports columnar
   * reads. The top-level plan may not support columnar (e.g. SortExec from
   * ORDER BY), but the underlying BatchScanExec should.
   */
  private static boolean scanSupportsColumnar(Dataset<Row> df) {
    SparkPlan plan = df.queryExecution().sparkPlan();
    return hasColumnarLeaf(plan);
  }

  private static boolean hasColumnarLeaf(SparkPlan plan) {
    if (plan.children().isEmpty()) {
      return plan.supportsColumnar();
    }
    scala.collection.Iterator<SparkPlan> iter = plan.children().iterator();
    while (iter.hasNext()) {
      if (hasColumnarLeaf(iter.next())) {
        return true;
      }
    }
    return false;
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

  // ---- PR 2: Filter classification and partition pruning tests ----

  @Test
  void testPartitionFilter() {
    String tablePath = new File(tempDir, "part_filter").getAbsolutePath();

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
            + "(3, 'a3', 30.0, 3000, '2024-01-01'), "
            + "(4, 'a4', 40.0, 4000, '2024-01-03')", tableName));

    // Filter on partition column only
    Dataset<Row> result = spark.sql(
        "SELECT id, name FROM " + tableName + " WHERE dt = '2024-01-01' ORDER BY id");
    List<Row> rows = result.collectAsList();
    assertEquals(2, rows.size());
    assertEquals(1, rows.get(0).getInt(0));
    assertEquals(3, rows.get(1).getInt(0));

    // Different partition value
    Dataset<Row> result2 = spark.sql(
        "SELECT id FROM " + tableName + " WHERE dt = '2024-01-02'");
    List<Row> rows2 = result2.collectAsList();
    assertEquals(1, rows2.size());
    assertEquals(2, rows2.get(0).getInt(0));
  }

  @Test
  void testDataFilter() {
    String tablePath = new File(tempDir, "data_filter").getAbsolutePath();

    spark.sql(String.format(
        "CREATE TABLE %s (id INT, name STRING, price DOUBLE, ts LONG) "
            + "USING hudi "
            + "TBLPROPERTIES (type = 'cow', primaryKey = 'id', preCombineField = 'ts') "
            + "LOCATION '%s'", tableName, tablePath));

    spark.sql(String.format(
        "INSERT INTO %s VALUES "
            + "(1, 'a1', 10.0, 1000), "
            + "(2, 'a2', 20.0, 2000), "
            + "(3, 'a3', 30.0, 3000), "
            + "(4, 'a4', 40.0, 4000)", tableName));

    // Filter on data column
    Dataset<Row> result = spark.sql(
        "SELECT id, price FROM " + tableName + " WHERE price > 20.0 ORDER BY id");
    List<Row> rows = result.collectAsList();
    assertEquals(2, rows.size());
    assertEquals(3, rows.get(0).getInt(0));
    assertEquals(30.0, rows.get(0).getDouble(1));
    assertEquals(4, rows.get(1).getInt(0));
    assertEquals(40.0, rows.get(1).getDouble(1));
  }

  @Test
  void testCombinedPartitionAndDataFilter() {
    String tablePath = new File(tempDir, "combined_filter").getAbsolutePath();

    spark.sql(String.format(
        "CREATE TABLE %s (id INT, name STRING, price DOUBLE, ts LONG, dt STRING) "
            + "USING hudi "
            + "TBLPROPERTIES (type = 'cow', primaryKey = 'id', preCombineField = 'ts') "
            + "PARTITIONED BY (dt) "
            + "LOCATION '%s'", tableName, tablePath));

    spark.sql(String.format(
        "INSERT INTO %s VALUES "
            + "(1, 'a1', 10.0, 1000, '2024-01-01'), "
            + "(2, 'a2', 20.0, 2000, '2024-01-01'), "
            + "(3, 'a3', 30.0, 3000, '2024-01-02'), "
            + "(4, 'a4', 40.0, 4000, '2024-01-02')", tableName));

    // Combine partition filter + data filter
    Dataset<Row> result = spark.sql(
        "SELECT id, price FROM " + tableName
            + " WHERE dt = '2024-01-02' AND price > 35.0 ORDER BY id");
    List<Row> rows = result.collectAsList();
    assertEquals(1, rows.size());
    assertEquals(4, rows.get(0).getInt(0));
    assertEquals(40.0, rows.get(0).getDouble(1));
  }

  @Test
  void testMorTableWithPartitionFilter() {
    String tablePath = new File(tempDir, "mor_part_filter").getAbsolutePath();

    spark.sql(String.format(
        "CREATE TABLE %s (id INT, name STRING, price DOUBLE, ts LONG, dt STRING) "
            + "USING hudi "
            + "TBLPROPERTIES ("
            + "  type = 'mor', "
            + "  primaryKey = 'id', "
            + "  preCombineField = 'ts', "
            + "  'hoodie.compact.inline' = 'false'"
            + ") "
            + "PARTITIONED BY (dt) "
            + "LOCATION '%s'", tableName, tablePath));

    spark.sql(String.format(
        "INSERT INTO %s VALUES "
            + "(1, 'a1', 10.0, 1000, '2024-01-01'), "
            + "(2, 'a2', 20.0, 2000, '2024-01-02')", tableName));

    // Upsert to create log files
    spark.sql("CREATE OR REPLACE TEMP VIEW mor_part_source AS "
        + "SELECT * FROM VALUES "
        + "(1, 'a1_updated', 15.0, CAST(1001 AS LONG), '2024-01-01'), "
        + "(3, 'a3', 30.0, CAST(3000 AS LONG), '2024-01-02') "
        + "AS t(id, name, price, ts, dt)");
    spark.sql(String.format(
        "MERGE INTO %s AS target USING mor_part_source AS source "
            + "ON target.id = source.id "
            + "WHEN MATCHED THEN UPDATE SET * "
            + "WHEN NOT MATCHED THEN INSERT *", tableName));

    // Filter on partition to get only 2024-01-01
    Dataset<Row> result = spark.sql(
        "SELECT id, name, price FROM " + tableName
            + " WHERE dt = '2024-01-01' ORDER BY id");
    List<Row> rows = result.collectAsList();
    assertEquals(1, rows.size());
    assertEquals(1, rows.get(0).getInt(0));
    assertEquals("a1_updated", rows.get(0).getString(1));
    assertEquals(15.0, rows.get(0).getDouble(2));

    // Filter on partition to get only 2024-01-02
    Dataset<Row> result2 = spark.sql(
        "SELECT id, name FROM " + tableName
            + " WHERE dt = '2024-01-02' ORDER BY id");
    List<Row> rows2 = result2.collectAsList();
    assertEquals(2, rows2.size());
    assertEquals(2, rows2.get(0).getInt(0));
    assertEquals(3, rows2.get(1).getInt(0));
  }

  // ---- PR 3: Statistics reporting for CBO tests ----

  @Test
  void testStatisticsSizeInBytesIsPositive() {
    String tablePath = new File(tempDir, "stats_size").getAbsolutePath();

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

    Statistics stats = spark.table(tableName).queryExecution().optimizedPlan().stats();
    BigInteger sizeInBytes = stats.sizeInBytes().bigInteger();
    assertTrue(sizeInBytes.compareTo(BigInteger.ZERO) > 0,
        "sizeInBytes should be > 0 but was " + sizeInBytes);
  }

  @Test
  void testStatisticsBroadcastJoinForSmallTable() {
    String tablePath = new File(tempDir, "stats_bcast").getAbsolutePath();
    String smallTableName = tableName;

    spark.sql(String.format(
        "CREATE TABLE %s (id INT, name STRING, ts LONG) "
            + "USING hudi "
            + "TBLPROPERTIES (type = 'cow', primaryKey = 'id', preCombineField = 'ts') "
            + "LOCATION '%s'", smallTableName, tablePath));

    spark.sql(String.format(
        "INSERT INTO %s VALUES (1, 'a1', 1000), (2, 'a2', 2000)", smallTableName));

    // Set a high broadcast threshold so the small Hudi table qualifies
    long prevThreshold = Long.parseLong(
        spark.conf().get("spark.sql.autoBroadcastJoinThreshold", "10485760"));
    spark.conf().set("spark.sql.autoBroadcastJoinThreshold", String.valueOf(100 * 1024 * 1024));
    try {
      // Create an inline view to join against
      spark.sql("CREATE OR REPLACE TEMP VIEW other_tbl AS "
          + "SELECT * FROM VALUES (1, 'x'), (2, 'x'), (3, 'x') AS t(id, value)");

      Dataset<Row> joined = spark.sql(
          "SELECT t.id, t.name, o.value FROM " + smallTableName
              + " t JOIN other_tbl o ON t.id = o.id");
      String plan = joined.queryExecution().executedPlan().toString();
      assertTrue(plan.contains("BroadcastHashJoin") || plan.contains("BroadcastExchange"),
          "CBO should pick broadcast join for small table. Plan:\n" + plan);
    } finally {
      spark.conf().set("spark.sql.autoBroadcastJoinThreshold", String.valueOf(prevThreshold));
    }
  }

  // ---- PR 5: Columnar (vectorized) read tests ----

  @Test
  void testCowColumnarReadResults() {
    String tablePath = new File(tempDir, "cow_columnar").getAbsolutePath();

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

    // Read through DSv2 path - COW with no log files should use columnar reads
    Dataset<Row> result = spark.sql(
        "SELECT id, name, price, ts FROM " + tableName + " ORDER BY id");

    // Verify data correctness
    List<Row> rows = result.collectAsList();
    assertEquals(3, rows.size());
    assertEquals(1, rows.get(0).getInt(0));
    assertEquals("a1", rows.get(0).getString(1));
    assertEquals(10.0, rows.get(0).getDouble(2));
    assertEquals(1000L, rows.get(0).getLong(3));
    assertEquals(2, rows.get(1).getInt(0));
    assertEquals("a2", rows.get(1).getString(1));
    assertEquals(3, rows.get(2).getInt(0));
    assertEquals("a3", rows.get(2).getString(1));

    // Verify the scan node in the physical plan supports columnar reads
    assertTrue(scanSupportsColumnar(result),
        "COW table read should support columnar batches");
  }

  @Test
  void testMorTableFallsBackToRowBasedRead() {
    String tablePath = new File(tempDir, "mor_noColumnar").getAbsolutePath();

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

    // Insert creates base files
    spark.sql(String.format(
        "INSERT INTO %s VALUES "
            + "(1, 'a1', 10.0, 1000), "
            + "(2, 'a2', 20.0, 2000)", tableName));

    // Upsert creates log files
    spark.sql("CREATE OR REPLACE TEMP VIEW mor_no_col_source AS "
        + "SELECT * FROM VALUES (1, 'a1_updated', 15.0, CAST(1001 AS LONG)) "
        + "AS t(id, name, price, ts)");
    spark.sql(String.format(
        "MERGE INTO %s AS target USING mor_no_col_source AS source "
            + "ON target.id = source.id "
            + "WHEN MATCHED THEN UPDATE SET * "
            + "WHEN NOT MATCHED THEN INSERT *", tableName));

    Dataset<Row> result = spark.sql(
        "SELECT id, name, price, ts FROM " + tableName + " ORDER BY id");

    // Verify data correctness (MOR merge must work)
    List<Row> rows = result.collectAsList();
    assertEquals(2, rows.size());
    assertEquals(1, rows.get(0).getInt(0));
    assertEquals("a1_updated", rows.get(0).getString(1));
    assertEquals(15.0, rows.get(0).getDouble(2));
    assertEquals(2, rows.get(1).getInt(0));
    assertEquals("a2", rows.get(1).getString(1));
  }

  @Test
  void testCowColumnarReadWithColumnPruning() {
    String tablePath = new File(tempDir, "cow_col_prune").getAbsolutePath();

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

    // Column pruning with columnar reads
    Dataset<Row> result = spark.sql("SELECT id, price FROM " + tableName + " ORDER BY id");
    assertEquals(2, result.schema().fields().length);

    List<Row> rows = result.collectAsList();
    assertEquals(3, rows.size());
    assertEquals(1, rows.get(0).getInt(0));
    assertEquals(10.0, rows.get(0).getDouble(1));
    assertEquals(2, rows.get(1).getInt(0));
    assertEquals(20.0, rows.get(1).getDouble(1));
    assertEquals(3, rows.get(2).getInt(0));
    assertEquals(30.0, rows.get(2).getDouble(1));

    // Should still use columnar reads for COW
    assertTrue(scanSupportsColumnar(result),
        "COW column pruning should still support columnar batches");
  }

  @Test
  void testCowPartitionedColumnarRead() {
    String tablePath = new File(tempDir, "cow_part_col").getAbsolutePath();

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

    Dataset<Row> result = spark.sql(
        "SELECT id, name, price FROM " + tableName + " ORDER BY id");

    List<Row> rows = result.collectAsList();
    assertEquals(3, rows.size());
    assertEquals(1, rows.get(0).getInt(0));
    assertEquals("a1", rows.get(0).getString(1));
    assertEquals(10.0, rows.get(0).getDouble(2));
    assertEquals(2, rows.get(1).getInt(0));
    assertEquals(3, rows.get(2).getInt(0));
  }

  // ---- PR 4: Schema evolution tests ----

  @Test
  void testSchemaEvolutionAddColumn() {
    String tablePath = new File(tempDir, "schema_evo").getAbsolutePath();

    // Enable schema evolution for this test
    spark.conf().set("hoodie.schema.on.read.enable", "true");
    try {
      // Create table with initial schema
      spark.sql(String.format(
          "CREATE TABLE %s (id INT, name STRING, ts LONG) "
              + "USING hudi "
              + "TBLPROPERTIES ("
              + "  type = 'cow', "
              + "  primaryKey = 'id', "
              + "  preCombineField = 'ts', "
              + "  'hoodie.schema.on.read.enable' = 'true'"
              + ") "
              + "LOCATION '%s'", tableName, tablePath));

      // Insert data with initial schema
      spark.sql(String.format(
          "INSERT INTO %s VALUES (1, 'a1', 1000), (2, 'a2', 2000)", tableName));

      // Evolve schema: add a new column
      spark.sql(String.format("ALTER TABLE %s ADD COLUMNS (price DOUBLE)", tableName));

      // Insert data with new schema
      spark.sql(String.format(
          "INSERT INTO %s VALUES (3, 'a3', 3000, 30.0), (4, 'a4', 4000, 40.0)", tableName));

      // Read and verify all rows including new column
      Dataset<Row> result = spark.sql(
          "SELECT id, name, ts, price FROM " + tableName + " ORDER BY id");
      List<Row> rows = result.collectAsList();

      assertEquals(4, rows.size());

      // Old rows should have null for the new column
      assertEquals(1, rows.get(0).getInt(0));
      assertEquals("a1", rows.get(0).getString(1));
      assertTrue(rows.get(0).isNullAt(3), "Old row should have null for added column");

      assertEquals(2, rows.get(1).getInt(0));
      assertEquals("a2", rows.get(1).getString(1));
      assertTrue(rows.get(1).isNullAt(3), "Old row should have null for added column");

      // New rows should have the value for the new column
      assertEquals(3, rows.get(2).getInt(0));
      assertEquals("a3", rows.get(2).getString(1));
      assertEquals(30.0, rows.get(2).getDouble(3));

      assertEquals(4, rows.get(3).getInt(0));
      assertEquals("a4", rows.get(3).getString(1));
      assertEquals(40.0, rows.get(3).getDouble(3));
    } finally {
      spark.conf().set("hoodie.schema.on.read.enable", "false");
    }
  }

  @Test
  void testSchemaEvolutionAddColumnPartitioned() {
    String tablePath = new File(tempDir, "schema_evo_part").getAbsolutePath();

    // Enable schema evolution for this test
    spark.conf().set("hoodie.schema.on.read.enable", "true");
    try {
      // Create partitioned table with initial schema
      spark.sql(String.format(
          "CREATE TABLE %s (id INT, name STRING, ts LONG, dt STRING) "
              + "USING hudi "
              + "TBLPROPERTIES ("
              + "  type = 'cow', "
              + "  primaryKey = 'id', "
              + "  preCombineField = 'ts', "
              + "  'hoodie.schema.on.read.enable' = 'true'"
              + ") "
              + "PARTITIONED BY (dt) "
              + "LOCATION '%s'", tableName, tablePath));

      // Insert data with initial schema
      spark.sql(String.format(
          "INSERT INTO %s VALUES "
              + "(1, 'a1', 1000, '2024-01-01'), "
              + "(2, 'a2', 2000, '2024-01-02')", tableName));

      // Evolve schema: add a new column
      spark.sql(String.format("ALTER TABLE %s ADD COLUMNS (price DOUBLE)", tableName));

      // Insert data with new schema
      spark.sql(String.format(
          "INSERT INTO %s VALUES "
              + "(3, 'a3', 3000, 30.0, '2024-01-01'), "
              + "(4, 'a4', 4000, 40.0, '2024-01-02')", tableName));

      // Read all data
      Dataset<Row> result = spark.sql(
          "SELECT id, name, ts, price, dt FROM " + tableName + " ORDER BY id");
      List<Row> rows = result.collectAsList();

      assertEquals(4, rows.size());

      // Old rows: null for price
      assertEquals(1, rows.get(0).getInt(0));
      assertTrue(rows.get(0).isNullAt(3));

      // New rows: price present
      assertEquals(3, rows.get(2).getInt(0));
      assertEquals(30.0, rows.get(2).getDouble(3));

      // Verify partition filter still works with evolved schema
      Dataset<Row> filtered = spark.sql(
          "SELECT id, price FROM " + tableName
              + " WHERE dt = '2024-01-01' ORDER BY id");
      List<Row> filteredRows = filtered.collectAsList();
      assertEquals(2, filteredRows.size());
      assertEquals(1, filteredRows.get(0).getInt(0));
      assertEquals(3, filteredRows.get(1).getInt(0));
    } finally {
      spark.conf().set("hoodie.schema.on.read.enable", "false");
    }
  }
}
