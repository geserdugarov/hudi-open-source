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

import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.util.collection.ClosableIterator;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Map;

/**
 * DSv2 {@link PartitionReader} that reads a single {@link HoodieInputPartition}
 * by delegating to {@link HoodieFileGroupReader}.
 *
 * <p>Skeleton implementation — the {@code initialize()} method is a stub that
 * will be wired to {@code HoodieFileGroupReader} in a follow-up change.
 */
public class HoodiePartitionReader implements PartitionReader<InternalRow> {

  private final HoodieInputPartition partition;
  private final StructType tableSchema;
  private final StructType requiredSchema;
  private final Map<String, String> properties;

  private boolean initialized;
  private ClosableIterator<InternalRow> iterator;
  private InternalRow currentRow;
  private HoodieFileGroupReader<InternalRow> fileGroupReader;

  public HoodiePartitionReader(HoodieInputPartition partition,
                                StructType tableSchema,
                                StructType requiredSchema,
                                Map<String, String> properties) {
    this.partition = partition;
    this.tableSchema = tableSchema;
    this.requiredSchema = requiredSchema;
    this.properties = properties;
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
   * <p>TODO: Wire to HoodieFileGroupReader following the pattern in
   * {@code HoodieFileGroupReaderBasedFileFormat.buildReaderWithPartitionValues()}.
   */
  private void initialize() {
    // TODO: build HoodieTableMetaClient from tablePath + properties
    // TODO: build SparkFileFormatInternalRowReaderContext
    // TODO: build HoodieFileGroupReader via builder with fileSlice, dataSchema, requestedSchema
    // TODO: call fileGroupReader.getClosableIterator()
    throw new UnsupportedOperationException(
        "TODO: wire HoodieFileGroupReader in HoodiePartitionReader.initialize()");
  }
}
