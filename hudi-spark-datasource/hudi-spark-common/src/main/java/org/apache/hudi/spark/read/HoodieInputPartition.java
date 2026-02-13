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

import org.apache.hudi.common.model.FileSlice;

import org.apache.spark.sql.connector.read.InputPartition;

import java.io.Serializable;

/**
 * DSv2 {@link InputPartition} wrapping a single Hudi {@link FileSlice}.
 * Shipped to executors for reading.
 */
public class HoodieInputPartition implements InputPartition, Serializable {

  private static final long serialVersionUID = 1L;

  private final int index;
  private final FileSlice fileSlice;
  private final String partitionPath;
  private final String tablePath;
  private final String latestCommitTime;

  public HoodieInputPartition(int index, FileSlice fileSlice,
                               String partitionPath, String tablePath,
                               String latestCommitTime) {
    this.index = index;
    this.fileSlice = fileSlice;
    this.partitionPath = partitionPath;
    this.tablePath = tablePath;
    this.latestCommitTime = latestCommitTime;
  }

  @Override
  public String[] preferredLocations() {
    return new String[0];
  }

  public int getIndex() {
    return index;
  }

  public FileSlice getFileSlice() {
    return fileSlice;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public String getTablePath() {
    return tablePath;
  }

  public String getLatestCommitTime() {
    return latestCommitTime;
  }

  public boolean hasLogFiles() {
    return fileSlice.hasLogFiles();
  }
}
