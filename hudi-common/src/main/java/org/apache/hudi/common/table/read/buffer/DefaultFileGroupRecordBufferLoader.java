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

package org.apache.hudi.common.table.read.buffer;

import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieMergeSortLogRecordScanner;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.read.BaseFileUpdateCallback;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.table.read.InputSplit;
import org.apache.hudi.common.table.read.ReaderParameters;
import org.apache.hudi.common.table.read.UpdateProcessor;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.storage.HoodieStorage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Default implementation of {@link FileGroupRecordBufferLoader} that initializes a buffer based on the reader parameters.
 *
 * @param <T> the engine specific record type
 */
class DefaultFileGroupRecordBufferLoader<T> extends LogScanningRecordBufferLoader implements FileGroupRecordBufferLoader<T> {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultFileGroupRecordBufferLoader.class);
  private static final DefaultFileGroupRecordBufferLoader INSTANCE = new DefaultFileGroupRecordBufferLoader<>();

  // RFC-81 config keys. The canonical definitions live in HoodieCompactionConfig
  // (hudi-client-common); we reference them by literal key/default here to avoid pulling that
  // module into hudi-common.
  static final String COMPACTION_WITH_MERGE_SORT_ENABLE_KEY = "hoodie.compaction.with.merge.sort.enable";
  static final String COMPACTION_LOG_STREAMING_READ_BUFFER_KEY = "hoodie.compaction.log.streaming.read.buffer";
  static final long COMPACTION_LOG_STREAMING_READ_BUFFER_DEFAULT = 10L * 1024 * 1024;

  static <T> DefaultFileGroupRecordBufferLoader<T> getInstance() {
    return INSTANCE;
  }

  private DefaultFileGroupRecordBufferLoader() {
  }

  @Override
  public Pair<HoodieFileGroupRecordBuffer<T>, List<String>> getRecordBuffer(HoodieReaderContext<T> readerContext,
                                                                            HoodieStorage storage,
                                                                            InputSplit inputSplit,
                                                                            List<String> orderingFieldNames,
                                                                            HoodieTableMetaClient hoodieTableMetaClient,
                                                                            TypedProperties props,
                                                                            ReaderParameters readerParameters,
                                                                            HoodieReadStats readStats,
                                                                            Option<BaseFileUpdateCallback<T>> fileGroupUpdateCallback) {
    boolean isSkipMerge = ConfigUtils.getStringWithAltKeys(props, HoodieReaderConfig.MERGE_TYPE, true).equalsIgnoreCase(HoodieReaderConfig.REALTIME_SKIP_MERGE);
    Option<PartialUpdateMode> partialUpdateModeOpt = hoodieTableMetaClient.getTableConfig().getPartialUpdateMode();
    UpdateProcessor<T> updateProcessor = UpdateProcessor.create(readStats, readerContext, readerParameters.emitDeletes(), fileGroupUpdateCallback, props);

    // RFC-81: when the merge-sort path is enabled and every log block is ordered, build a
    // streaming, spill-free buffer that bypasses HoodieMergedLogRecordReader entirely. The probe
    // is a header-only walk over the log files, comparable in cost to the first traversal the
    // legacy reader does. If anything disqualifies the file group, fall through to the existing
    // spill-backed path.
    Option<MergeSortFileGroupRecordBuffer<T>> mergeSortBuffer = tryBuildMergeSortBuffer(
        readerContext, storage, inputSplit, props, readerParameters, isSkipMerge,
        orderingFieldNames, updateProcessor);
    if (mergeSortBuffer.isPresent()) {
      MergeSortFileGroupRecordBuffer<T> buffer = mergeSortBuffer.get();
      return Pair.of(buffer, buffer.getValidBlockInstants());
    }

    FileGroupRecordBuffer<T> recordBuffer;
    if (isSkipMerge) {
      recordBuffer = new UnmergedFileGroupRecordBuffer<>(
          readerContext, hoodieTableMetaClient, readerContext.getMergeMode(), partialUpdateModeOpt, props, readStats);
    } else if (readerParameters.sortOutputs()) {
      recordBuffer = new SortedKeyBasedFileGroupRecordBuffer<>(
          readerContext, hoodieTableMetaClient, readerContext.getMergeMode(), partialUpdateModeOpt, props, orderingFieldNames, updateProcessor);
    } else if (readerParameters.useRecordPosition() && inputSplit.getBaseFileOption().isPresent()) {
      recordBuffer = new PositionBasedFileGroupRecordBuffer<>(
          readerContext, hoodieTableMetaClient, readerContext.getMergeMode(), partialUpdateModeOpt, inputSplit.getBaseFileOption().get().getCommitTime(), props,
          orderingFieldNames, updateProcessor);
    } else {
      recordBuffer = new KeyBasedFileGroupRecordBuffer<>(
          readerContext, hoodieTableMetaClient, readerContext.getMergeMode(), partialUpdateModeOpt, props, orderingFieldNames, updateProcessor);
    }
    return Pair.of(recordBuffer, scanLogFiles(readerContext, storage, inputSplit, hoodieTableMetaClient, props,
        readerParameters, readStats, recordBuffer));
  }

  /**
   * Probes the log files and, when every prerequisite holds, returns a streaming merge-sort
   * buffer. Returns {@link Option#empty()} when the merge-sort path is disabled or any
   * disqualifying condition is detected — in that case the caller must use the legacy
   * spill-backed buffer flow.
   * <p>
   * Disqualifying conditions (any one forces a fallback):
   * <ul>
   *   <li>{@code hoodie.compaction.with.merge.sort.enable} is {@code false} (default).</li>
   *   <li>The reader is configured for {@code REALTIME_SKIP_MERGE} or sorted-output modes
   *       (those buffer flavors have specialized semantics not yet covered by merge-sort).</li>
   *   <li>The file group has no log files (no merging needed at all).</li>
   *   <li>Any log block lacks {@code IS_ORDERED=true} — see
   *       {@link HoodieMergeSortLogRecordScanner#shouldUseMergeSort}.</li>
   *   <li>Any non-data block is present (delete, command, corrupt). The current merge-sort
   *       implementation streams data blocks only; richer block types fall back to the legacy
   *       path.</li>
   *   <li>The table requires schema evolution (an internal schema is configured).</li>
   * </ul>
   */
  private Option<MergeSortFileGroupRecordBuffer<T>> tryBuildMergeSortBuffer(
      HoodieReaderContext<T> readerContext,
      HoodieStorage storage,
      InputSplit inputSplit,
      TypedProperties props,
      ReaderParameters readerParameters,
      boolean isSkipMerge,
      List<String> orderingFieldNames,
      UpdateProcessor<T> updateProcessor) {
    boolean configEnabled = props.getBoolean(COMPACTION_WITH_MERGE_SORT_ENABLE_KEY, false);
    if (!configEnabled) {
      return Option.empty();
    }
    if (isSkipMerge || readerParameters.sortOutputs()) {
      LOG.debug("Merge-sort path skipped: incompatible buffer mode (skipMerge={}, sortOutputs={})",
          isSkipMerge, readerParameters.sortOutputs());
      return Option.empty();
    }
    List<HoodieLogFile> logFiles = inputSplit.getLogFiles();
    if (logFiles == null || logFiles.isEmpty()) {
      return Option.empty();
    }
    InternalSchema internalSchema = readerContext.getSchemaHandler().getInternalSchema();
    if (internalSchema != null && !internalSchema.isEmptySchema()) {
      LOG.debug("Merge-sort path skipped: internal schema evolution requires the legacy reader");
      return Option.empty();
    }
    Option<HoodieRecordMerger> mergerOpt = readerContext.getRecordMerger();
    if (mergerOpt.isEmpty()) {
      return Option.empty();
    }

    Set<String> validInstants = new LinkedHashSet<>();
    List<HoodieDataBlock> orderedBlocks = new ArrayList<>();
    HoodieSchema readerSchema = readerContext.getSchemaHandler().getRequiredSchema();
    try {
      for (HoodieLogFile logFile : logFiles) {
        try (HoodieLogFormat.Reader reader = HoodieLogFormat.newReader(storage, logFile, readerSchema)) {
          while (reader.hasNext()) {
            HoodieLogBlock block = reader.next();
            if (!block.isOrdered()) {
              LOG.debug("Merge-sort path skipped: block of type {} lacks IS_ORDERED",
                  block.getBlockType());
              return Option.empty();
            }
            if (!(block instanceof HoodieDataBlock)) {
              // Delete, command, and corrupt blocks are routed to the legacy reader for now.
              LOG.debug("Merge-sort path skipped: non-data block ({}) requires the legacy reader",
                  block.getBlockType());
              return Option.empty();
            }
            orderedBlocks.add((HoodieDataBlock) block);
            String instantTime = block.getLogBlockHeader().get(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME);
            if (instantTime != null) {
              validInstants.add(instantTime);
            }
          }
        }
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to probe log blocks for merge-sort eligibility", e);
    }

    long perStreamBufferBytes = props.getLong(COMPACTION_LOG_STREAMING_READ_BUFFER_KEY,
        COMPACTION_LOG_STREAMING_READ_BUFFER_DEFAULT);
    String[] orderingFields = orderingFieldNames.toArray(new String[0]);
    DeleteContext deleteContext = readerContext.getSchemaHandler().getDeleteContext().withReaderSchema(readerSchema);

    LOG.info("Selected merge-sort buffer for file group: {} ordered data block(s), per-stream buffer={} bytes",
        orderedBlocks.size(), perStreamBufferBytes);
    MergeSortFileGroupRecordBuffer<T> buffer = new MergeSortFileGroupRecordBuffer<>(
        readerContext,
        orderedBlocks,
        new ArrayList<>(validInstants),
        mergerOpt.get(),
        readerSchema,
        orderingFields,
        deleteContext,
        updateProcessor,
        props,
        perStreamBufferBytes);
    return Option.of(buffer);
  }

  /**
   * Convenience hook for tests that want to ask the loader the same question without driving a
   * full read: returns {@code true} iff the supplied props would cause the merge-sort branch to
   * be selected for the supplied set of blocks.
   */
  static boolean wouldSelectMergeSort(TypedProperties props, Iterable<HoodieLogBlock> blocks) {
    boolean configEnabled = props.getBoolean(COMPACTION_WITH_MERGE_SORT_ENABLE_KEY, false);
    return HoodieMergeSortLogRecordScanner.shouldUseMergeSort(configEnabled, blocks);
  }
}
