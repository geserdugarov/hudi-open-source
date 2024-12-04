<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# RFC-84: Optimized SerDe of `DataStream` in Flink operators

## Proposers

- @geserdugarov

## Approvers

- @danny0405
- @xiarixiaoyao
- @yuzhaojing

## Status: Design in progress

JIRA: [HUDI-8799](https://issues.apache.org/jira/browse/HUDI-8799)

## Abstract

Currently, in the scenario when Flink writes into Hudi, the first step is row data conversion into Avro record, which is used for key generation, 
and passed to the following operators as part of `HoodieRecord`. Kryo serializer is used to serialize/deserialize those records. 
And as it mentioned in the [claim for this RFC](https://github.com/apache/hudi/pull/12550), Kryo serde costs are huge, which is unacceptable for stream processing.

This RFC suggests ...

## Background

...

Introduce any much background context which is relevant or necessary to understand the feature and design choices.

## Implementation

1. We need to preserve `RowDataToHoodieFunction` operators, which supports all kind of key generators. Incoming row data should be converted to `HoodieRowData`, not to Avro records.
2. To test assumption #1 we could start with bulk insert pipeline, where `BucketBulkInsertWriterHelper::rowWithFileId` returns `GenericRowData`.
Switch return to `HoodieRowData`, and make sure that there is no Kryo in bulk insert scenario when `HoodieRowData` is used.
3. For some cases it would be possible to use `RowDataKeyGen` directly on `RowData`, instead of converting `RowData` to Avro using `RowDataToAvroConverters`, 
and passing result to `HoodieAvroKeyGeneratorFactory`. We could add switch with key generator type check.
4. If there would be no use of `RowData` to Avro converters, how to support additional functionality with timezones? Refactor everything related?
5. It's possible to reuse part of `HoodieRowDataCreateHandle`, but we need to separate data preparation and writing into files first in this case.

Describe the new thing you want to do in appropriate detail, how it fits into the project architecture.
Provide a detailed description of how you intend to implement this feature.This may be fairly extensive and have large subsections of its own.
Or it may be a few sentences. Use judgement based on the scope of the change.

## Rollout/Adoption Plan

- What impact (if any) will there be on existing users?
- If we are changing behavior how will we phase out the older behavior?
- If we need special migration tools, describe them here.
- When will we remove the existing behavior

## Test Plan

Describe in few sentences how the RFC will be tested. How will we know that the implementation works as expected? How will we know nothing broke?.

