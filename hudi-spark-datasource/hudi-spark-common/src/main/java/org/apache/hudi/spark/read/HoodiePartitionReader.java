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

import org.apache.hudi.HoodieSchemaConversionUtils;
import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.SparkFileFormatInternalRowReaderContext;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.execution.datasources.FileFormat;
import org.apache.spark.sql.execution.datasources.SparkColumnarFileReader;
import org.apache.spark.sql.hudi.SparkAdapter;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import scala.Tuple2;
import scala.collection.JavaConverters;

/**
 * DSv2 {@link PartitionReader} that reads a single {@link HoodieInputPartition}
 * by delegating to {@link HoodieFileGroupReader}.
 */
public class HoodiePartitionReader implements PartitionReader<InternalRow> {

  private final HoodieInputPartition partition;
  private final StructType tableSchema;
  private final StructType requiredSchema;
  private final Map<String, String> properties;
  private final SerializableConfiguration serializableConf;

  private boolean initialized;
  private ClosableIterator<InternalRow> iterator;
  private InternalRow currentRow;
  private HoodieFileGroupReader<InternalRow> fileGroupReader;

  public HoodiePartitionReader(HoodieInputPartition partition,
                                StructType tableSchema,
                                StructType requiredSchema,
                                Map<String, String> properties,
                                SerializableConfiguration serializableConf) {
    this.partition = partition;
    this.tableSchema = tableSchema;
    this.requiredSchema = requiredSchema;
    this.properties = properties;
    this.serializableConf = serializableConf;
  }

  @Override
  public boolean next() throws IOException {
    if (!initialized) {
      initialize();
      initialized = true;
    }
    if (iterator.hasNext()) {
      currentRow = iterator.next();
      return true;
    }
    return false;
  }

  @Override
  public InternalRow get() {
    return currentRow;
  }

  @Override
  public void close() throws IOException {
    if (fileGroupReader != null) {
      fileGroupReader.close();
    }
  }

  /**
   * Initializes the {@link HoodieFileGroupReader} and obtains the row iterator.
   *
   * <p>Follows the pattern in
   * {@code HoodieFileGroupReaderBasedFileFormat.buildReaderWithPartitionValues()}.
   */
  private void initialize() {
    Configuration hadoopConf = new Configuration(serializableConf.value());
    HadoopStorageConfiguration storageConf = new HadoopStorageConfiguration(hadoopConf);

    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf)
        .setBasePath(partition.getTablePath())
        .build();

    SparkAdapter sparkAdapter = SparkAdapterSupport$.MODULE$.sparkAdapter();
    SQLConf sqlConf = SQLConf.get();

    // Build non-vectorized Parquet reader (MOR merging is row-based)
    scala.collection.immutable.Map<String, String> readerOptions =
        scala.collection.immutable.Map$.MODULE$.<String, String>empty()
            .$plus(new Tuple2<>(FileFormat.OPTION_RETURNING_BATCH(), "false"));
    SparkColumnarFileReader baseFileReader =
        sparkAdapter.createParquetFileReader(false, sqlConf, readerOptions, hadoopConf);

    // Build reader context with empty filters (filter pushdown comes in PR 2)
    List<Filter> emptyFilters = Collections.emptyList();
    scala.collection.Seq<Filter> scalaFilters =
        JavaConverters.asScalaBufferConverter(emptyFilters).asScala().toSeq();
    SparkFileFormatInternalRowReaderContext readerContext =
        new SparkFileFormatInternalRowReaderContext(
            baseFileReader, scalaFilters, scalaFilters, storageConf, metaClient.getTableConfig());

    // Convert StructType schemas to HoodieSchema
    String qualifiedName = HoodieSchemaUtils.getRecordQualifiedName(
        metaClient.getTableConfig().getTableName());
    HoodieSchema dataSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
        tableSchema, qualifiedName);
    HoodieSchema requestedSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
        requiredSchema, qualifiedName);

    // Build properties: table config + user options
    TypedProperties props = metaClient.getTableConfig().getProps();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      props.setProperty(entry.getKey(), entry.getValue());
    }

    // Determine whether to use record position based merging
    boolean shouldUseRecordPosition = HoodieSparkUtils.gteqSpark3_5()
        && Boolean.parseBoolean(properties.getOrDefault(
            HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS.key(),
            String.valueOf(HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS.defaultValue())));

    fileGroupReader = HoodieFileGroupReader.<InternalRow>newBuilder()
        .withReaderContext(readerContext)
        .withHoodieTableMetaClient(metaClient)
        .withLatestCommitTime(partition.getLatestCommitTime())
        .withFileSlice(partition.getFileSlice())
        .withDataSchema(dataSchema)
        .withRequestedSchema(requestedSchema)
        .withInternalSchema(Option.empty())
        .withProps(props)
        .withShouldUseRecordPosition(shouldUseRecordPosition)
        .build();

    try {
      this.iterator = fileGroupReader.getClosableIterator();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to initialize file group reader", e);
    }
  }
}
