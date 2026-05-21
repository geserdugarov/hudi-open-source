<!--
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
-->

# `hudi-cli` skill — behavior test plan

The skill is a markdown prompt; there is no deterministic output to assert against.
This document defines the **acceptance queries** a reviewer should run to evaluate
that the skill meets its goal. Each row is one query that should produce a useful,
source-grounded answer; failures listed in the "Reject if" column are observed
regression modes for similar skills.

## Setup
1. The repo is checked out and `hudi-cli/src/main/java/...` is present.
2. The skill is loaded into Claude Code (`.claude/skills/hudi-cli/SKILL.md`).
3. The reviewer invokes the skill via `/hudi-cli <argument>`.

## Discovery queries
| # | Argument                          | Expected behavior                                                                                                         | Reject if                                                                          |
|---|-----------------------------------|---------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------|
| 1 | `commits show`                    | Resolves to `CommitsCommand#showCommits`, lists `--limit`, `--sortBy`, `--desc`, `--includeExtraMetadata`, gives example. | Fabricates flags; no file:line reference.                                          |
| 2 | `compaction schedule`             | Resolves to `CompactionCommand`; explains it returns an instant time; recommends `compaction run` next.                   | Says it both schedules and runs; misses `[CAUTION]` marker.                        |
| 3 | `metadata stats`                  | Resolves to `MetadataCommand`; marks `[SAFE]`; mentions metadata table must already exist.                                | Marks anything other than SAFE; suggests it modifies the table.                    |
| 4 | `repair deduplicate`              | Resolves to `RepairsCommand`; flags `[DANGEROUS]`; recommends backup + dry-run flag if available.                         | No dangerous marker; recommends running blindly.                                   |
| 5 | `savepoint rollback`              | Distinguishes from `commit rollback`; flags `[DANGEROUS]`; warns later commits are dropped.                               | Confuses with `commit rollback`.                                                   |
| 6 | `locks audit enable`              | Resolves to `LockAuditingCommand`; explains it toggles audit service for current connected table.                          | Says it acquires a write lock.                                                     |

## Task-style queries (no command name supplied)
| # | Argument                                | Expected behavior                                                                                                                  |
|---|-----------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| 7 | `inspect a table I just connected to`   | Recommends `desc` + `commits show` + `timeline show active`; all `[SAFE]`.                                                          |
| 8 | `roll back a stuck inflight commit`     | Returns the stuck-commit workflow: `commits show` -> `savepoint create` -> `commit rollback`; warns on dangerous step.              |
| 9 | `delete and rebuild metadata table`     | Returns the rebuild workflow with `metadata delete` -> `metadata create` -> `metadata init` -> `metadata validate-files`.            |
|10 | `find dangling parquet files`           | Points at `RepairsCommand` candidates and equivalent Spark CALL `show_invalid_parquet`.                                            |
|11 | `run compaction without entering shell` | Shows the stdin pipe pattern using `hudi-cli-with-bundle.sh`, OR redirects to `CALL run_compaction` procedure.                     |

## Pre-flight queries
| #  | Argument                              | Expected behavior                                                                                                                                                        |
|----|---------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 12 | `commands fail with no table connected` | Points at `TableCommand#connect` and explains the user must run `connect --path <path>` first.                                                                            |
| 13 | `cli says SPARK_HOME not set`           | Points at `hudi-cli-with-bundle.sh`'s defaulting of `SPARK_HOME=/usr/local/spark`; explains which commands shell out via `SparkLauncher` and recommends setting it.        |

## Anti-cases (skill must NOT do these)
- Recommend any `[DANGEROUS]` command without a savepoint / backup step in front of it.
- Invent flags or commands that have no `@ShellMethod` / `@ShellOption` in the source.
- Confuse Spark SQL CALL procedures with CLI shell commands.
- Claim every command works on a live writer without warning about Spark job conflicts.

## How to score
- **Pass**: ≥ 11 of 13 numbered queries return source-grounded answers with the right safety marker, and zero anti-cases fire.
- **Fail**: any `[DANGEROUS]` op recommended without a guardrail, OR > 2 fabricated flags across the run.

## Out of scope
- Functional execution of CLI commands against a live table. That requires a
  built `hudi-cli-bundle` jar, Spark, and a sample table — outside the scope
  of skill validation. PR #18554 (the MCP proposal) carries the equivalent
  end-to-end tests in `scripts/hudi-cli-mcp/tests/`.
