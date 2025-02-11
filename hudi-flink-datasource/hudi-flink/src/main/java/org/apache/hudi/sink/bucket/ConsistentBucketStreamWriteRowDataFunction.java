package org.apache.hudi.sink.bucket;

import org.apache.hudi.client.model.HoodieFlinkRecord;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.RowType;

public class ConsistentBucketStreamWriteRowDataFunction<T extends HoodieFlinkRecord> extends ConsistentBucketStreamWriteFunction<T> {

  private final RowType rowType;

  ConsistentBucketStreamWriteRowDataFunction(Configuration config, RowType rowType) {
    super(config);
    this.rowType = rowType;
  }

  // TODO: implement
}
