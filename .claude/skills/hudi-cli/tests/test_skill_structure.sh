#!/usr/bin/env bash
#
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
# Structural tests for the hudi-cli skill.
#
# These do NOT require a running Hudi CLI. They verify that:
#   1. SKILL.md parses (frontmatter present, required keys set)
#   2. Required sections exist
#   3. Every command-class file path referenced in the skill actually exists
#      under hudi-cli/src/main/java/org/apache/hudi/cli/commands/
#   4. The launcher script the skill points at exists
#
# Usage: bash test_skill_structure.sh
# Exits 0 on success, non-zero on first failure.

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SKILL_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
SKILL_FILE="${SKILL_DIR}/SKILL.md"
REPO_ROOT="$(cd "${SKILL_DIR}/../../.." && pwd)"
COMMANDS_DIR="${REPO_ROOT}/hudi-cli/src/main/java/org/apache/hudi/cli/commands"
LAUNCHER="${REPO_ROOT}/packaging/hudi-cli-bundle/hudi-cli-with-bundle.sh"

PASS=0
FAIL=0

assert() {
  local msg="$1"; shift
  if "$@"; then
    echo "PASS: ${msg}"
    PASS=$((PASS + 1))
  else
    echo "FAIL: ${msg}"
    FAIL=$((FAIL + 1))
  fi
}

skill_has_line() {
  grep -qE "$1" "${SKILL_FILE}"
}

# 1. SKILL.md exists and is non-empty.
assert "SKILL.md exists" test -s "${SKILL_FILE}"

# 2. Frontmatter present and bounded by --- markers.
assert "frontmatter opens with ---" \
  bash -c "head -1 '${SKILL_FILE}' | grep -qx -- '---'"
assert "frontmatter contains 'name: hudi-cli'" \
  skill_has_line '^name: hudi-cli$'
assert "frontmatter contains description" \
  skill_has_line '^description: '
assert "frontmatter contains user-invocable" \
  skill_has_line '^user-invocable: '
assert "frontmatter contains allowed-tools (scalar or list)" \
  skill_has_line '^allowed-tools:( |$)'
assert "frontmatter contains argument-hint" \
  skill_has_line '^argument-hint: '

# 3. Required body sections.
for section in \
  "## Instructions" \
  "### Where the CLI lives in the repo" \
  "### How to launch the CLI" \
  "### Discovering commands" \
  "### Safety classification" \
  "### Workflow templates" \
  "### Output format"; do
  assert "section present: ${section}" \
    skill_has_line "^${section}\$"
done

# 4. Every CLI command-class referenced in the skill table exists in source.
COMMAND_CLASSES=(
  TableCommand.java
  CommitsCommand.java
  ArchivedCommitsCommand.java
  CompactionCommand.java
  ClusteringCommand.java
  CleansCommand.java
  SavepointsCommand.java
  RestoresCommand.java
  RollbacksCommand.java
  MarkersCommand.java
  RepairsCommand.java
  MetadataCommand.java
  TimelineCommand.java
  StatsCommand.java
  FileSystemViewCommand.java
  HoodieLogFileCommand.java
  DiffCommand.java
  BootstrapCommand.java
  ExportCommand.java
  LockAuditingCommand.java
  HoodieSyncValidateCommand.java
  UpgradeOrDowngradeCommand.java
  SparkEnvCommand.java
  TempViewCommand.java
  KerberosAuthenticationCommand.java
  UtilsCommand.java
)
for cls in "${COMMAND_CLASSES[@]}"; do
  assert "command class exists in source: ${cls}" \
    test -f "${COMMANDS_DIR}/${cls}"
  assert "skill references ${cls}" \
    skill_has_line "${cls}"
done

# 5. Launcher script exists.
assert "launcher script exists" test -f "${LAUNCHER}"
assert "skill references the launcher script" \
  skill_has_line 'hudi-cli-with-bundle\.sh'

# 6. Safety vocabulary appears (skill must distinguish safe vs dangerous ops).
for marker in 'SAFE' 'CAUTION' 'DANGEROUS'; do
  assert "skill uses safety marker: ${marker}" \
    skill_has_line "\\[${marker}\\]"
done

# 7. Skill points at the canonical entry-point class.
assert "skill references HoodieCLI.java (connection state)" \
  skill_has_line 'HoodieCLI\.java'
assert "skill references Main.java entry point" \
  skill_has_line 'Main\.java'

# 8. \$ARGUMENTS placeholder is present so the skill receives user input.
assert 'skill uses $ARGUMENTS placeholder' \
  skill_has_line '\$ARGUMENTS'

echo
echo "----"
echo "passed: ${PASS}"
echo "failed: ${FAIL}"
[ "${FAIL}" -eq 0 ]
