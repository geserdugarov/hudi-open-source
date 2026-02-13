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

import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.apache.parquet.schema.MessageType;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.execution.datasources.FileFormat;
import org.apache.spark.sql.execution.datasources.HoodieSparkPartitionedFileUtils;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.datasources.SparkColumnarFileReader;
import org.apache.spark.sql.hudi.SparkAdapter;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.util.SerializableConfiguration;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import scala.Tuple2;
import scala.collection.JavaConverters;

/**
 * DSv2 {@link PartitionReader} that reads a single COW {@link HoodieInputPartition}
 * using Spark's native vectorized Parquet reader, returning {@link ColumnarBatch}
 * objects for improved read performance.
 *
 * <p>This reader bypasses {@code HoodieFileGroupReader} entirely since COW
 * file slices with no log files can be read as plain Parquet files. The
 * vectorized Parquet reader produces columnar batches that Spark can process
 * without row-by-row deserialization, significantly improving throughput.
 *
 * <p>Only used when {@link HoodiePartitionReaderFactory#supportColumnarReads}
 * returns {@code true} for the given partition.
 */
public class HoodieColumnarPartitionReader implements PartitionReader<ColumnarBatch> {

  private final HoodieInputPartition partition;
  private final StructType requiredSchema;
  private final SerializableConfiguration serializableConf;

  private boolean initialized;
  private scala.collection.Iterator<InternalRow> batchIterator;
  private ColumnarBatch currentBatch;

  public HoodieColumnarPartitionReader(HoodieInputPartition partition,
                                        StructType requiredSchema,
                                        SerializableConfiguration serializableConf) {
    this.partition = partition;
    this.requiredSchema = requiredSchema;
    this.serializableConf = serializableConf;
  }

  @Override
  public boolean next() throws IOException {
    if (!initialized) {
      initialize();
      initialized = true;
    }
    if (currentBatch != null) {
      currentBatch.close();
      currentBatch = null;
    }
    if (batchIterator.hasNext()) {
      // The vectorized reader returns ColumnarBatch objects typed as InternalRow
      // due to Scala type erasure in Iterator[InternalRow]
      currentBatch = (ColumnarBatch) (Object) batchIterator.next();
      return true;
    }
    return false;
  }

  @Override
  public ColumnarBatch get() {
    return currentBatch;
  }

  @Override
  public void close() throws IOException {
    if (currentBatch != null) {
      currentBatch.close();
      currentBatch = null;
    }
    if (batchIterator instanceof Closeable) {
      ((Closeable) batchIterator).close();
    }
  }

  /**
   * Initializes the vectorized Parquet reader and creates the batch iterator.
   *
   * <p>Creates a {@link SparkColumnarFileReader} with vectorized=true and
   * returningBatch=true, then reads the base file as a {@link PartitionedFile}.
   */
  private void initialize() {
    Configuration hadoopConf = new Configuration(serializableConf.value());
    HadoopStorageConfiguration storageConf = new HadoopStorageConfiguration(hadoopConf);

    SparkAdapter sparkAdapter = SparkAdapterSupport$.MODULE$.sparkAdapter();
    SQLConf sqlConf = SQLConf.get();

    // Build vectorized Parquet reader with batch returning enabled
    scala.collection.immutable.Map<String, String> readerOptions =
        scala.collection.immutable.Map$.MODULE$.<String, String>empty()
            .$plus(new Tuple2<>(FileFormat.OPTION_RETURNING_BATCH(), "true"));
    SparkColumnarFileReader baseFileReader =
        sparkAdapter.createParquetFileReader(true, sqlConf, readerOptions, hadoopConf);

    // Create PartitionedFile from the base file
    HoodieBaseFile baseFile = partition.getFileSlice().getBaseFile().get();
    StoragePath baseFilePath = baseFile.getStoragePath();
    long fileSize = Math.max(baseFile.getFileSize(), 0L);
    HoodieSparkPartitionedFileUtils pFileUtils = sparkAdapter.getSparkPartitionedFileUtils();
    PartitionedFile partitionedFile = pFileUtils.createPartitionedFile(
        InternalRow.empty(), baseFilePath, 0L, fileSize);

    // Build empty filters for now; data filters are not needed for
    // direct base file reads since Spark applies post-scan filtering
    List<Filter> emptyFilters = Collections.emptyList();
    scala.collection.Seq<Filter> scalaFilters =
        JavaConverters.asScalaBufferConverter(emptyFilters).asScala().toSeq();

    // Read the base file; the iterator yields ColumnarBatch objects
    // (typed as InternalRow due to Scala erasure)
    this.batchIterator = baseFileReader.read(
        partitionedFile, requiredSchema, new StructType(),
        Option.<InternalSchema>empty(), scalaFilters, storageConf,
        Option.<MessageType>empty());
  }
}
