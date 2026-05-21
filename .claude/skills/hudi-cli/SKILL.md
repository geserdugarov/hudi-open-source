---
name: hudi-cli
description: Find, explain, or run a Hudi CLI (Spring-Shell) command. Use when asking how to inspect, repair, or operate a Hudi table from the `hudi-cli` shell — e.g. "commits show", "compaction run", "savepoint create", "metadata stats", "repair deduplicate".
user-invocable: true
disable-model-invocation: true
allowed-tools:
  - Read
  - Grep
  - Glob
argument-hint: "[command or task e.g. 'commits show', 'compaction schedule', 'metadata stats', 'repair corrupted clean files']"
---

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

# Hudi CLI Operator Helper

User's input: **$ARGUMENTS**

## Instructions

You are helping a user discover and operate the Hudi CLI shell. Do NOT rely on memory — every command claim must be grounded in source code in this repo. Many CLI operations are mutating; clearly mark safety.

**Execution model:** this skill is *advisory*. It is granted only `Read`,
`Grep`, and `Glob` so it can read source under `hudi-cli/` to ground
recommendations. It does NOT pre-approve `Bash`, so any CLI invocation,
backup command, or `mvn` build shown below is for the user to run in their
own shell after reviewing it. This is deliberate — many Hudi CLI commands
are `[DANGEROUS]`, and the human-in-the-loop step is part of the safety
posture. If a calling agent decides it needs `Bash`, that approval should
be granted explicitly per-invocation, not by the skill.

### Where the CLI lives in the repo
- Entry point: `hudi-cli/src/main/java/org/apache/hudi/cli/Main.java` (Spring Boot + Spring Shell)
- Command classes: `hudi-cli/src/main/java/org/apache/hudi/cli/commands/` — every public method tagged with `@ShellMethod` is a CLI command
- Connection state: `hudi-cli/src/main/java/org/apache/hudi/cli/HoodieCLI.java`
- Launcher: `packaging/hudi-cli-bundle/hudi-cli-with-bundle.sh`
- Bundle module: `packaging/hudi-cli-bundle/`

### How to launch the CLI
The launcher (`packaging/hudi-cli-bundle/hudi-cli-with-bundle.sh`) requires
**both** `CLI_BUNDLE_JAR` (from `packaging/hudi-cli-bundle/target/`) and
`SPARK_BUNDLE_JAR` (from `packaging/hudi-spark-bundle/target/`). It will exit
with `Make sure to generate both the hudi-cli-bundle.jar and hudi-spark-bundle.jar`
if either is missing.

```bash
# Build both bundles in one shot (-am pulls in everything they depend on):
mvn install -pl packaging/hudi-cli-bundle,packaging/hudi-spark-bundle -am -DskipTests

export SPARK_HOME=/path/to/spark
./packaging/hudi-cli-bundle/hudi-cli-with-bundle.sh
# At the `hudi->` prompt:
hudi-> connect --path /data/my_table
hudi-> commits show --limit 10
```

If only one bundle is built (or jars live elsewhere), point at them explicitly:
```bash
export CLI_BUNDLE_JAR=/path/to/hudi-cli-bundle-X.Y.Z.jar
export SPARK_BUNDLE_JAR=/path/to/hudi-spark<X.Y>-bundle_<scala>-X.Y.Z.jar
export SPARK_HOME=/path/to/spark
./packaging/hudi-cli-bundle/hudi-cli-with-bundle.sh
```

The shell is interactive. To run a single command non-interactively pipe it to stdin:
```bash
echo -e "connect --path /data/my_table\ncommits show --limit 5\nexit\n" \
  | ./packaging/hudi-cli-bundle/hudi-cli-with-bundle.sh
```

### Discovering commands

Each command class lives under `hudi-cli/src/main/java/org/apache/hudi/cli/commands/`. The command key, parameters, and behaviour come from the source — always read the class:

1. **List candidates**: grep `@ShellMethod` in the commands directory for keys matching the user's topic.
2. **Read the method**: the `@ShellMethod(key = "...", value = "...")` annotation gives the command name and one-line description; `@ShellOption(...)` annotations on parameters give flag names, defaults, and required/optional status.
3. **Trace what it does**: follow the body — many delegate to client APIs (`SparkRDDWriteClient`, `HoodieTableMetaClient`) or shell out to `SparkLauncher`. Note any side effects.

### Command-class -> topic map (for fast lookup)

| Topic                       | Class                                                                   |
|-----------------------------|-------------------------------------------------------------------------|
| Table connect / desc / type | `TableCommand.java`                                                     |
| Commits / compare / sync    | `CommitsCommand.java`                                                   |
| Archived commits / archival | `ArchivedCommitsCommand.java`                                           |
| Compaction                  | `CompactionCommand.java`                                                |
| Clustering                  | `ClusteringCommand.java`                                                |
| Cleans                      | `CleansCommand.java`                                                    |
| Savepoints                  | `SavepointsCommand.java`                                                |
| Restores                    | `RestoresCommand.java`                                                  |
| Rollbacks                   | `RollbacksCommand.java`                                                 |
| Markers                     | `MarkersCommand.java`                                                   |
| Repair                      | `RepairsCommand.java`                                                   |
| Metadata table              | `MetadataCommand.java`                                                  |
| Timeline                    | `TimelineCommand.java`                                                  |
| Stats (file size, write amp)| `StatsCommand.java`                                                     |
| File system view            | `FileSystemViewCommand.java`                                            |
| Log files                   | `HoodieLogFileCommand.java`                                             |
| Diff (file/partition)       | `DiffCommand.java`                                                      |
| Bootstrap                   | `BootstrapCommand.java`                                                 |
| Export instants             | `ExportCommand.java`                                                    |
| Lock auditing               | `LockAuditingCommand.java`                                              |
| Sync validate               | `HoodieSyncValidateCommand.java`                                        |
| Upgrade / downgrade         | `UpgradeOrDowngradeCommand.java`                                        |
| Spark env                   | `SparkEnvCommand.java`                                                  |
| Temp views                  | `TempViewCommand.java`                                                  |
| Kerberos                    | `KerberosAuthenticationCommand.java`                                    |
| Utils (loadClass)           | `UtilsCommand.java`                                                     |

### Pre-flight (almost every command needs this)
Most commands require an active connection. Ensure the user has run:
```
connect --path <table_path>
```
If the user reports "no table connected" or `HoodieCLI.tableMetadata` is null, point them at `TableCommand#connect`.

### Safety classification
Mark every recommended command. Heuristic from the source:

- `[SAFE]` — read-only listing/inspection. Examples: `commits show`, `compactions show all`, `cleans show`, `metadata stats`, `metadata list-files`, `show fsview latest`, `stats filesizes`, `stats wa`, `desc`, `fetch table schema`, `show rollbacks`, `show restores`, `timeline show active`.
- `[CAUTION]` — schedules work, runs Spark jobs, or rewrites table state but is recoverable. Examples: `compaction schedule`, `compaction run`, `compaction scheduleAndExecute`, `clustering schedule`, `clustering run`, `cleans run`, `savepoint create`, `savepoint delete`, `trigger archival`, `metadata create`, `metadata init`, `repair addpartitionmeta`, `repair migrate-partition-meta`, `marker delete`.
- `[DANGEROUS]` — can lose data or break the table. Examples: `commit rollback`, `savepoint rollback` (restores the table to a savepoint and drops later commits), `repair overwrite-hoodie-props`, `repair deduplicate`, `metadata delete`, `metadata delete-record-index`, `compaction unschedule`, `compaction unscheduleFileId`, `compaction repair`, `table change-table-type`, `table delete-configs`, `upgrade table`, `downgrade table`, `rename partition`, `repair deprecated partition`.

When recommending a `[DANGEROUS]` command:
1. Always recommend a backup of `.hoodie/` first.
2. Always recommend a savepoint at the latest known-good commit if the table is readable.
3. Prefer the dry-run / show flavour first (e.g. `compaction validate` before `compaction repair`).

### Workflow templates

**Inspect a table:**
```
connect --path <path>
desc
commits show --limit 10
timeline show active
```

**Schedule + run a compaction:**
```
connect --path <path>
compactions show all                       # [SAFE]
compaction schedule                        # [CAUTION] returns instant time
compaction run --compactionInstant <ts>    # [CAUTION] runs Spark job
compactions show all                       # verify completed
```
(or single-shot: `compaction scheduleAndExecute`)

**Rollback a stuck commit:**
```bash
# 1. Backup .hoodie/ (run from a regular shell, NOT the hudi-> prompt).
#    Adjust scheme for gs://, abfs://, hdfs://, or local cp -r as needed.
aws s3 cp --recursive s3://my-bucket/my_table/.hoodie \
  s3://my-bucket/backups/my_table/.hoodie.$(date +%Y%m%dT%H%M%S)
```
```
# 2. Then in hudi-cli:
connect --path <path>
commits show --limit 20                    # find stuck instant
savepoint create --commit <last_good_ts>   # [CAUTION] in-table safety net
commit rollback --commit <stuck_ts>        # [DANGEROUS] verify first
commits show --limit 5                     # verify rolled back
```

**Rebuild metadata table:**
```bash
# 1. Backup .hoodie/ first; metadata delete is irreversible.
aws s3 cp --recursive s3://my-bucket/my_table/.hoodie \
  s3://my-bucket/backups/my_table/.hoodie.$(date +%Y%m%dT%H%M%S)
```
```
# 2. Then in hudi-cli:
connect --path <path>
metadata stats                             # [SAFE] confirm broken
savepoint create --commit <last_good_ts>   # [CAUTION] in-table safety net
metadata delete                            # [DANGEROUS] removes MDT
metadata create                            # [CAUTION] rebuild
metadata init                              # [CAUTION] backfill from commits
metadata validate-files                    # verify
```

**Repair dangling partition metadata (dry-run-then-real):**
```
connect --path <path>
repair addpartitionmeta --dryrun true      # [SAFE]
repair addpartitionmeta --dryrun false     # [CAUTION]
```

### Output format

When the user asks about a command:
1. **Command** — exact key as registered in `@ShellMethod`, with file:line
2. **Purpose** — one sentence from the `@ShellMethod` value plus what the body actually does
3. **Parameters** — table of `--flag`, type, default, required/optional (read from `@ShellOption`)
4. **Safety** — `[SAFE]` / `[CAUTION]` / `[DANGEROUS]`
5. **Pre-conditions** — connection required? metadata table required? Spark env (`set --conf` for `--sparkMaster`/`--sparkMemory`) required?
6. **Example** — copy-pasteable invocation
7. **Verification** — read-only command(s) to confirm the result
8. **Equivalent Spark SQL CALL procedure** if one exists (point at `hudi-spark-datasource/hudi-spark/src/main/scala/org/apache/spark/sql/hudi/command/procedures/`)

When the user describes a task without a command name, list the 2–3 candidate commands ranked by fit, and recommend one.

### When to escalate / not use the CLI
- **Live, multi-writer table** — CLI runs Spark jobs that may conflict with active writers. Pause writers first or use the procedure equivalents inside an existing Spark session.
- **Cloud-only environment without Spark** — many CLI commands shell out to `SparkLauncher` (see `SparkMain.java`). Without `SPARK_HOME` and the spark bundle they will fail. Suggest the Spark SQL CALL procedure equivalents instead (see the `hudi-procedures` skill).
- **Need programmatic / scriptable access** — the interactive shell is awkward; suggest piping commands via stdin or using the equivalent CALL procedures from a SQL script.
