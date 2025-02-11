/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.bucket;

import org.apache.hudi.client.model.HoodieFlinkRecord;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.ConsistentHashingNode;
import org.apache.hudi.common.util.StringUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/**
 * The function to tag each incoming {@link HoodieFlinkRecord} with a location of a file based on consistent bucket index.
 */
public class ConsistentBucketAssignRowDataFunction<I extends HoodieFlinkRecord, O extends HoodieFlinkRecord>
    extends ConsistentBucketAssignFunction<I, O> {

  public ConsistentBucketAssignRowDataFunction(Configuration conf) {
    super(conf);
  }

  @Override
  public void processElement(I income, Context context, Collector<O> collector) {
    String recordKey = income.getRecordKey();
    String partition = income.getPartitionPath();

    final ConsistentHashingNode node = getBucketIdentifier(partition).getBucket(recordKey, indexKeyFields);
    Preconditions.checkArgument(
        StringUtils.nonEmpty(node.getFileIdPrefix()),
        "Consistent hashing node has no file group, partition: " + partition + ", meta: "
            + partitionToIdentifier.get(partition).getMetadata().getFilename() + ", record_key: " + recordKey);

    income.setInstantTime("U");
    income.setFileId(FSUtils.createNewFileId(node.getFileIdPrefix(), 0));
    collector.collect((O) income);
  }
}
