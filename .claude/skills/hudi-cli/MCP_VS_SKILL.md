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

# MCP server vs. skill: comparing two ways to surface Hudi CLI to an AI assistant

This doc compares two proposals for letting an AI assistant operate the Hudi CLI:

- **MCP server** — `apache/hudi#18554` (`scripts/hudi-cli-mcp/`): a Python
  FastMCP server that wraps the Hudi CLI as ~40 typed tools, with a
  token-based confirmation flow for write ops.
- **Skill** — this directory (`.claude/skills/hudi-cli/SKILL.md`): a
  markdown prompt grounded in the source tree, with safety classification,
  workflow templates, and pre-flight checks.

The point isn't to pick a winner; the two ship at different layers and the
right answer is "both, for different audiences." The aim of this doc is to
give a structured rubric so the community can decide what to merge, what
to defer, and how to keep them aligned.

## Side-by-side summary

| Aspect                          | MCP server (#18554)                                                                            | Skill (this repo)                                                                  |
|---------------------------------|------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------|
| Artifact                        | Python package + 40 `@mcp.tool()` definitions                                                  | One `SKILL.md` (223 lines) + tests + this doc                                     |
| Repo footprint                  | 28 files, ~3.2k LOC additive, new directory `scripts/hudi-cli-mcp/`                            | 4 files, ~40 KB total under `.claude/skills/hudi-cli/` (only ~13 KB is loaded)     |
| Runtime                         | Python 3.10+, Java 8/11, Spark 3.x, hudi bundles, FastMCP server process                       | None beyond the assistant; user runs the CLI themselves                            |
| Transport                       | stdio MCP                                                                                      | n/a — prompt context only                                                          |
| Where execution happens         | Server-side: spawns the Hudi CLI in a subprocess and parses output                             | Client-side: human or assistant runs the CLI; the skill just guides them           |
| Tool surface                    | ~40 typed tools (connect, execute_hudi_command, table_health_check, rollback_commit, …)        | One natural-language entry, dispatches to any of 98 `@ShellMethod` keys           |
| Safety model                    | Risk levels (LOW / MEDIUM / HIGH) + token-based two-step confirmation (5-min TTL, single-use)   | `[SAFE]` / `[CAUTION]` / `[DANGEROUS]` labels + savepoint / dry-run guardrails    |
| State                           | Persistent: `SessionManager` + `SafetyManager` singletons; outlives a single LLM call          | Stateless: each invocation re-reads the source                                     |
| Output shape                    | Structured JSON (parsed from CLI ASCII tables via `parser.py`)                                  | Free-form prose with code fences                                                   |
| Authoritative source            | The MCP wrapper (re-implements safety, drift risk vs. CLI source)                               | The Hudi source files themselves (no drift; skill says "read this class")          |
| Discovery                       | Tool list shows tools and their JSON schemas                                                    | Grep `@ShellMethod` in `hudi-cli/.../commands/`                                     |
| Hands-off automation            | Yes — agent can call tools without a human in the loop (modulo confirmation tokens)             | No — needs a human or wrapping agent to actually invoke the shell                  |
| Multi-turn workflows            | First-class (workflow tools, confirmation tokens, batched commands)                              | Skill describes the workflow; the assistant has to drive it                        |

## Functionality

### What the MCP server can do that the skill cannot
- **Actually execute** commands without involving the human shell. Critical for
  fully autonomous agents (e.g. scheduled health checks, ticket-driven repairs).
- **Return structured data** — `parser.py` turns CLI FlipTable output into JSON,
  which downstream tools can chain together.
- **Enforce a confirmation step** at the protocol level: a destructive op
  returns a token + dry-run summary, the agent must surface it to a human, and
  only then does `confirm_operation(token=…)` execute. The skill recommends the
  same pattern but cannot enforce it.
- **Batch commands** to amortise JVM start-up (`execute_hudi_commands`). The
  skill points at the same trick (piping multiple commands to stdin) but the
  caller has to assemble it.

### What the skill can do that the MCP server cannot
- **Stay in sync with new commands automatically.** The MCP server hard-codes
  ~40 wrappers; new `@ShellMethod` entries do not appear until someone updates
  the Python file. The skill says "grep `@ShellMethod` in this directory," so
  the moment a command lands in source, the skill answers about it.
- **Explain *why*.** The skill is a prompt that walks the user through the
  source — class, method, flags, side effects, equivalent CALL procedure — for
  learning, code review, and triage. The MCP server is opaque about
  implementation details by design.
- **Run anywhere the assistant runs.** No Spark, no JVM, no Python venv, no
  per-host MCP install. Onboarding cost is "git pull."
- **Cover advisory tasks** — "what's safe to run while writers are active?",
  "should I prefer `compaction repair` or `repair_corrupted_clean_files`?".
  The MCP server answers these only via the LLM; the skill is the LLM's
  reference material.

## Usability

| Persona                                    | Better fit | Why                                                                                    |
|--------------------------------------------|------------|----------------------------------------------------------------------------------------|
| Hudi committer learning a new code area    | Skill      | Wants source pointers, not abstracted tool calls.                                      |
| Oncall engineer triaging a paged table     | Either     | Skill wins if Spark/CLI not yet installed; MCP wins if a server is already running.    |
| Agent doing scheduled health checks        | MCP        | Needs autonomous execution + structured output for downstream rules.                   |
| User in a sandboxed laptop with no Spark   | Skill      | MCP server can't even start without `SPARK_HOME` + bundle jars.                        |
| Consumer of stable API for tooling         | MCP        | JSON output + versioned tool surface beats free-form prose.                            |
| Writer of new CLI commands                 | Skill      | Skill auto-covers their addition; MCP needs a separate PR per command.                 |

Onboarding cost (rough):
- **Skill:** drop the directory in, restart Claude Code. < 1 minute.
- **MCP:** install Python venv, build hudi-cli-bundle + hudi-spark-bundle,
  set 3+ env vars, register the server in `claude_desktop_config.json` /
  `~/.claude/mcp.json`, restart the client. ~30 minutes plus build time
  for a fresh checkout.

## Token usage

Rough estimates (1 token ≈ 4 chars of English). Numbers measured against
the artifacts in this directory and the file list in apache/hudi#18554.

**Measurement method**

- Skill bootstrap = `wc -c SKILL.md` ÷ 4. Only `SKILL.md` is loaded by the
  skill loader; the sibling `MCP_VS_SKILL.md`, `tests/test_plan.md`, and
  `tests/test_skill_structure.sh` are reference material for humans, not
  loaded into the model.
- MCP bootstrap = ~40 tools × ~150 tokens each. 150 tokens is a typical
  serialized tool schema (name + description + JSON schema for params)
  for the moderately-typed tools in #18554; small read tools come out
  closer to 80, write tools with risk-level annotations and confirmation
  semantics closer to 250.
- Per-action numbers are typical-case round-trip costs (input + output)
  excluding the bootstrap.

**Current artifacts** (re-measure with `wc -c` after any edit; numbers
below were taken at the commit that introduces this paragraph)

- `SKILL.md`: **13,075 bytes ≈ 3.27k tokens** — only this file is loaded
  into the model when the skill is invoked.
- `MCP_VS_SKILL.md`: ~15.8 KB — reference material for humans, not loaded
  by the skill loader.
- `tests/test_plan.md`: ~6.6 KB — same.
- `tests/test_skill_structure.sh`: ~4.7 KB — same.
- Total directory: **~40 KB** (only ~13 KB / ~3.3k tokens is the resident
  model cost).

The earlier numbers in this doc (1.4k tokens, 2.78k tokens, "~10 KB" total)
were stale; reviewers should re-run `wc -c .claude/skills/hudi-cli/SKILL.md`
if SKILL.md is edited again and update the table below.

| Scenario                                                      | MCP                                                                                                | Skill                                                                                                |
|---------------------------------------------------------------|----------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| Bootstrap cost per session (loaded into the model)            | ~40 tool definitions × ~150 tokens each ≈ **6k tokens** of always-resident schema                   | Loaded only when the skill is invoked. **~3.3k tokens** for SKILL.md.                                 |
| Per-action cost: "show me last 10 commits"                    | One tool call (`execute_hudi_command`) + a JSON result of ~40 rows ≈ **300–800 tokens** round trip | One assistant turn: read 1–2 source files (~300 tokens read budget) + ~250 token response.           |
| Per-action cost: "rollback commit X (dangerous)"              | Tool call -> token returned (~150 tokens) -> human confirms -> second call (~150 tokens). **~400 tokens.** | Skill emits a workflow + warnings (~400 tokens). User executes the CLI themselves (no LLM cost).      |
| Per-action cost: agent retries on a bad flag                  | ~600–1200 tokens. The MCP returns a tool error; the LLM has to debug from JSON.                    | ~400 tokens. The LLM re-reads the `@ShellOption` list and corrects.                                  |
| Cost when the answer doesn't need execution (e.g. "explain")  | Still pays the bootstrap cost                                                                      | Pays only the load + response                                                                         |

Net: the **MCP wins per-action when the work is repeated, structured execution**
(the bootstrap cost amortises across many tool calls in one session, and JSON
results are denser than ASCII tables). The **skill wins when the session is
short or advisory**, because the MCP's always-loaded tool schemas are pure
overhead in those cases. The skill's bootstrap is now larger than originally
estimated, but it is still smaller than the MCP's because (a) the MCP
schema is always-resident across the whole session even for non-execution
queries, while the skill is loaded only on `/hudi-cli`, and (b) the gap is
~2.7k tokens (6k MCP − 3.3k skill).

A good rule of thumb (with the corrected bootstrap costs):
- ≤ 4 CLI ops in the session → skill is cheaper.
- ≥ 7 CLI ops in the session → MCP amortises and is cheaper.
- 4–7 ops or non-trivial branching workflows → roughly a wash; pick on
  output-shape needs (structured JSON vs. prose) instead.

Note: these breakevens assume a single Claude Code session. If you keep an
MCP server registered across many sessions, the bootstrap cost is paid every
time a session starts, so the effective MCP cost in advisory-heavy workflows
is meaningfully higher than the per-session math suggests.

## Risks

| Risk                                       | MCP                                                                  | Skill                                                                |
|--------------------------------------------|----------------------------------------------------------------------|----------------------------------------------------------------------|
| Drift from CLI source                      | High (separate Python wrapper)                                       | Low (points at source by file:line)                                  |
| Maintenance burden                         | Medium (~3.2k LOC, end-to-end tests)                                 | Low (~10 KB markdown)                                                |
| Footgun (destructive op without guardrail) | Mitigated by token confirmation, but bypassable by an autonomous agent | Mitigated by `[DANGEROUS]` markers and savepoint workflow templates |
| Supply chain                               | Adds Python + FastMCP runtime dependency                             | None                                                                 |
| LLM hallucination on flags                 | Lower — tool schema is the spec                                      | Higher unless the LLM actually reads the source (which the skill instructs) |

## Recommendation

These should ship as **complementary**, not as alternatives:

- Merge the **skill** as the default entry point: zero-cost, always in sync,
  good enough for the >50 % of users whose interaction is advisory (explain,
  triage, draft a workflow).
- Keep the **MCP server** for users who want autonomous, structured-output
  execution from agents — and have the runtime to host it.

To prevent them from drifting:
- Have the MCP wrapper's tool list and the skill's command-class table
  reference the same enumeration: a generated table of `@ShellMethod` keys.
  A short Maven plugin or `mvn enforcer` check that fails CI if a new
  `@ShellMethod` is added without being mentioned in the skill or the MCP
  server is the cheapest way to keep both honest.
- Cross-link them: the skill's "When to escalate" section points at the
  MCP for autonomous use cases; the MCP `README` points at the skill for
  conceptual help.

## Open questions for the issue thread

1. Is the project comfortable adding a Python runtime dependency (MCP) to
   support AI workflows? If not, the skill is the only viable path.
2. Should the MCP server be moved out of `scripts/` to its own module so it
   can be released independently of the main bundles?
3. Is there appetite for an annotation processor that emits a JSON manifest
   of every `@ShellMethod` at build time? Both proposals would benefit.
