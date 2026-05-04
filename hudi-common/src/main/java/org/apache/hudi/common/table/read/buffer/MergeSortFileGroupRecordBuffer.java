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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.log.HoodieMergeSortLogRecordScanner;
import org.apache.hudi.common.table.log.KeySpec;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.table.read.UpdateProcessor;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Streaming, spill-free implementation of {@link HoodieFileGroupRecordBuffer} backed by
 * {@link HoodieMergeSortLogRecordScanner}.
 * <p>
 * Selected by {@link DefaultFileGroupRecordBufferLoader} when the RFC-81 prerequisites hold —
 * {@code hoodie.compaction.with.merge.sort.enable} is set, every log block in the file group
 * carries {@code IS_ORDERED=true}, and the file group has no command/corrupt/delete blocks. The
 * loader pre-collects the ordered data blocks and hands them to this buffer; the merge-sort
 * scanner is assembled lazily on the first {@link #setBaseFileIterator}, treating the base file
 * as the oldest stream.
 * <p>
 * Unlike {@link FileGroupRecordBuffer}, this class does <strong>not</strong> declare or allocate
 * an {@link org.apache.hudi.common.util.collection.ExternalSpillableMap}; the merge runs in
 * heap-bounded streaming memory. The {@link #processDataBlock}, {@link #processNextDataRecord},
 * {@link #processDeleteBlock}, and {@link #processNextDeletedRecord} entry points are not
 * exercised on this path (the legacy {@link org.apache.hudi.common.table.log.HoodieMergedLogRecordReader}
 * is bypassed) and throw {@link UnsupportedOperationException} to surface accidental misuse.
 *
 * @param <T> engine-specific record type.
 */
public class MergeSortFileGroupRecordBuffer<T> implements HoodieFileGroupRecordBuffer<T> {

  private static final Logger LOG = LoggerFactory.getLogger(MergeSortFileGroupRecordBuffer.class);

  private final HoodieReaderContext<T> readerContext;
  private final List<HoodieDataBlock> orderedDataBlocks;
  private final List<String> validBlockInstants;
  private final HoodieRecordMerger recordMerger;
  private final HoodieSchema readerSchema;
  private final String[] orderingFields;
  private final DeleteContext deleteContext;
  private final UpdateProcessor<T> updateProcessor;
  private final TypedProperties props;
  private final long perStreamBufferBytes;

  private ClosableIterator<T> baseFileIterator;
  private HoodieMergeSortLogRecordScanner<BufferedRecord<T>> scanner;
  private ClosableIterator<BufferedRecord<T>> mergedIterator;
  private BufferedRecord<T> nextRecord;
  private long totalLogRecords;
  private boolean assembled;

  public MergeSortFileGroupRecordBuffer(HoodieReaderContext<T> readerContext,
                                        List<HoodieDataBlock> orderedDataBlocks,
                                        List<String> validBlockInstants,
                                        HoodieRecordMerger recordMerger,
                                        HoodieSchema readerSchema,
                                        String[] orderingFields,
                                        DeleteContext deleteContext,
                                        UpdateProcessor<T> updateProcessor,
                                        TypedProperties props,
                                        long perStreamBufferBytes) {
    this.readerContext = readerContext;
    this.orderedDataBlocks = orderedDataBlocks;
    this.validBlockInstants = validBlockInstants;
    this.recordMerger = recordMerger;
    this.readerSchema = readerSchema;
    this.orderingFields = orderingFields;
    this.deleteContext = deleteContext;
    this.updateProcessor = updateProcessor;
    this.props = props;
    this.perStreamBufferBytes = perStreamBufferBytes;
  }

  @Override
  public BufferType getBufferType() {
    return BufferType.KEY_BASED_MERGE;
  }

  // ---------------------------------------------------------------------------
  // The merge-sort path bypasses HoodieMergedLogRecordReader; the legacy fine-grained entry
  // points are intentionally unsupported so that accidental routing through the spill-map flow
  // surfaces immediately rather than silently corrupting output.
  // ---------------------------------------------------------------------------

  @Override
  public void processDataBlock(HoodieDataBlock dataBlock, Option<KeySpec> keySpecOpt) {
    throw new UnsupportedOperationException(
        "MergeSortFileGroupRecordBuffer is populated by the loader; processDataBlock is not used");
  }

  @Override
  public void processNextDataRecord(BufferedRecord<T> record, Serializable index) {
    throw new UnsupportedOperationException(
        "MergeSortFileGroupRecordBuffer streams records directly; processNextDataRecord is not used");
  }

  @Override
  public void processDeleteBlock(HoodieDeleteBlock deleteBlock) {
    throw new UnsupportedOperationException(
        "MergeSortFileGroupRecordBuffer rejects delete blocks; loader must fall back when present");
  }

  @Override
  public void processNextDeletedRecord(DeleteRecord record, Serializable index) {
    throw new UnsupportedOperationException(
        "MergeSortFileGroupRecordBuffer rejects delete records; loader must fall back when present");
  }

  @Override
  public boolean containsLogRecord(String recordKey) {
    return false;
  }

  @Override
  public int size() {
    return orderedDataBlocks.size();
  }

  @Override
  public long getTotalLogRecords() {
    return totalLogRecords;
  }

  @Override
  public Map<Serializable, BufferedRecord<T>> getLogRecords() {
    return Collections.emptyMap();
  }

  @Override
  public ClosableIterator<BufferedRecord<T>> getLogRecordIterator() {
    if (!assembled) {
      assemble();
    }
    return mergedIterator;
  }

  public List<String> getValidBlockInstants() {
    return validBlockInstants;
  }

  @Override
  public void setBaseFileIterator(ClosableIterator<T> baseFileIterator) {
    this.baseFileIterator = baseFileIterator;
  }

  @Override
  public boolean hasNext() throws IOException {
    if (nextRecord != null) {
      return true;
    }
    if (!assembled) {
      assemble();
    }
    while (mergedIterator.hasNext()) {
      BufferedRecord<T> rec = mergedIterator.next();
      BufferedRecord<T> processed = updateProcessor.processUpdate(
          rec.getRecordKey(), null, rec, rec.isDelete());
      if (processed != null) {
        nextRecord = processed;
        return true;
      }
    }
    return false;
  }

  @Override
  public BufferedRecord<T> next() {
    BufferedRecord<T> r = nextRecord;
    nextRecord = null;
    return r;
  }

  @Override
  public void close() {
    try {
      if (mergedIterator != null) {
        mergedIterator.close();
      } else if (scanner != null) {
        scanner.close();
      }
    } finally {
      mergedIterator = null;
      scanner = null;
    }
  }

  /**
   * Builds the merge-sort scanner from the pre-collected blocks plus the (now-known) base file
   * iterator. Streams are added oldest-to-newest: base file first, then each ordered data block
   * in append order.
   */
  private void assemble() {
    if (assembled) {
      return;
    }
    HoodieMergeSortLogRecordScanner.Builder<BufferedRecord<T>> builder =
        HoodieMergeSortLogRecordScanner.<T>newBuilderForBufferedRecords()
            .withBufferBytes(perStreamBufferBytes)
            .withMergeFn((older, newer) -> {
              try {
                BufferedRecord<T> combined = recordMerger.merge(
                    older, newer, readerContext.getRecordContext(), props);
                totalLogRecords++;
                return combined;
              } catch (IOException e) {
                throw new HoodieIOException("Failed to merge buffered records", e);
              }
            });

    if (baseFileIterator != null) {
      builder.addStream("base", asBufferedRecordStream(baseFileIterator, readerSchema, false));
    }
    for (int i = 0; i < orderedDataBlocks.size(); i++) {
      HoodieDataBlock block = orderedDataBlocks.get(i);
      ClosableIterator<T> blockIter = block.getEngineRecordIterator(readerContext);
      builder.addStream("log-block-" + i,
          asBufferedRecordStream(blockIter, block.getSchema(), true));
    }

    this.scanner = builder.build();
    this.mergedIterator = scanner.iterator();
    this.assembled = true;
    LOG.info("Assembled streaming merge-sort buffer with {} log block stream(s){}",
        orderedDataBlocks.size(),
        baseFileIterator != null ? " plus base file" : "");
  }

  private ClosableIterator<BufferedRecord<T>> asBufferedRecordStream(ClosableIterator<T> source,
                                                                     HoodieSchema schema,
                                                                     boolean checkDeleteMarker) {
    RecordContext<T> recordContext = readerContext.getRecordContext();
    List<String> orderingFieldsList = orderingFields.length == 0
        ? Collections.emptyList()
        : java.util.Arrays.asList(orderingFields);
    return new ClosableIterator<BufferedRecord<T>>() {
      @Override
      public boolean hasNext() {
        return source.hasNext();
      }

      @Override
      public BufferedRecord<T> next() {
        T raw = source.next();
        boolean isDelete = checkDeleteMarker && recordContext.isDeleteRecord(raw, deleteContext);
        return BufferedRecords.fromEngineRecord(raw, schema, recordContext, orderingFieldsList, isDelete);
      }

      @Override
      public void close() {
        source.close();
      }
    };
  }
}
