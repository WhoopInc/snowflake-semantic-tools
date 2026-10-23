---
name: sst-core
description: |-
  Use first for any task in a project that publishes with Snowflake Semantic Tools (SST):
  which `sst` command does what, which ones connect to Snowflake, write to it, or cost
  credits, the safe edit-validate-plan-apply loop, exit codes, and reading `-o agent` and `-o json`.
  Routes to the other sst-* skills for authoring, diagnostics, agents and evals,
  troubleshooting, migration, and bug reports. Do NOT use for dbt modeling questions that
  do not involve SST, or for SST's own source code.
---
# Working in an SST project

SST compiles a dbt project's semantic YAML into Snowflake semantic views, Cortex Agents,
CoWork profiles, skills, and CoCo Desktop profiles, then publishes them. This kit was installed by SST
@SST_VERSION@; its documentation is at @SST_DOCS@/index.md, and the terms are defined in
@SST_DOCS@/glossary.md.

If the user is new to SST or to dbt, or asks where to start, use `sst-getting-started` first,
and explain each SST term the first time you use it.

## Ground rules

1. **Never publish without being asked.** `sst apply`, `sst drop`, and anything else marked
   *writes* below changes Snowflake. Run them only when the user asks for that command, and
   show the plan first.
2. **Never spend credits without being asked.** `sst eval` runs agents in Snowflake; `sst test`
   only reads what was published.
   To check a change offline, use `sst validate --no-snowflake-syntax-check` and `sst compile`.
3. **Stay offline by default.** `sst validate` connects unless told not to, and so do
   `sst debug`, `sst plan`, and `sst diff`. Use the offline forms in the table unless the user
   wants a connected check. With `authenticator: externalbrowser` in `profiles.yml`, a
   connecting command opens a browser and waits for the sign-in with no time limit, so in a
   shell nobody watches it never returns: ask the user to run it in their own terminal.
4. **Facts come from the installed CLI, not from memory.** Run `sst <command> --help` for
   flags and `sst explain <CODE>` for any diagnostic. Both work offline and match the SST
   that is installed.
5. **Read `-o agent`; parse `-o json`.** When a coding agent runs `sst` with stdout piped and no `--output` set, it
   prints the `agent` format on its own (its first line ends `output=agent(detected ...)`);
   `-o agent` (a global option, before the command name) asks for it anywhere. Its first line
   is the verdict (`sst <command>: <OUTCOME> exit=1 errors=2 ...`), then one `E`/`W` line per
   finding with a `fix:` line under it, then the last line, `next:`. When you need a field
   that is not there, use `-o json`: one JSON object on stdout with `exit_code`,
   `diagnostics`, `summary`, `data`, and `next_steps`. Never scrape `table` output.
6. **Never baseline a diagnostic to make a run pass** unless the user asks; fix it instead.
   See the `sst-fix-diagnostics` skill.

## Commands by risk

| Command | Risk | Notes |
|---|---|---|
| `sst explain` | offline | Explains one diagnostic code; needs no project. |
| `sst compile` | offline | Writes `sst_target/manifest.json`; `--emit-ddl <dir>` writes the DDL to read. |
| `sst list` | offline | Lists compiled artifacts and their cached status. |
| `sst lineage` | offline | What a model, member or artifact reads and what reads it; `--state <dir>` marks changes. Use it before an edit to see what it reaches, and `--select model:<name>+` to plan only that. |
| `sst format` | offline, edits files | `--check` exits 2 when a file would change; run it after editing SST YAML. |
| `sst init` | offline, edits files | Scaffolds `sst_config.yml`; never overwrites a file. |
| `sst clean` | offline, edits files | Removes `sst_target` only. |
| `sst migrate refs` | offline, edits files | Dry run by default; `--write` rewrites references and moves old settings under `config:`. |
| `sst migrate split` | offline, edits files | Dry run by default; `--write` writes one file per listed member and removes the list. |
| `sst docs` | offline, edits files | Regenerates SST's own reference pages; for SST maintainers, not projects. |
| `sst baseline show` | offline | Lists the baseline file's entries. |
| `sst baseline add` | offline, edits files | Writes `.sst/baseline.json`; `--target` connects for a code only a connection finds, or with `--all-warnings`. Ask before using it. |
| `sst baseline prune` | offline, edits files | Removes stale entries; `--target` connects only to judge an entry only a connection to it found. |
| `sst baseline renew` | offline, edits files | Re-dates the baseline; needs `--reason`. Ask before using it. |
| `sst agent-kit install` | offline, edits files | Installs these skills. |
| `sst agent-kit upgrade` | offline, edits files | Brings these skills to the installed SST's kit. |
| `sst agent-kit status` | offline | `--check` exits 2 when the kit is out of date. |
| `sst agent-kit remove` | offline, edits files | Removes these skills. |
| `sst validate` | reads Snowflake | Offline with `--no-snowflake-syntax-check`. |
| `sst debug` | reads Snowflake | Offline with `--no-connect`: shows the config, profile, and target SST resolved. |
| `sst diff` | reads Snowflake | Offline only when `--from` and `--to` are both `local` or saved plans. |
| `sst plan` | reads Snowflake | Never writes; exits 2 when there are changes. Saves `sst_target/plan.json`. |
| `sst enrich` | reads Snowflake, edits files | Reads the warehouse into model YAML; `--include synonyms` calls Cortex, which costs credits. |
| `sst test` | reads Snowflake | Smoke-probes what `sst apply` published; never writes and runs no agent. |
| `sst eval` | billed | Runs agents against their evals; `--capture-baseline` writes the eval baseline. Only when asked. |
| `sst ask` | billed | Runs one agent on one question, on scratch copies of your edits; publishes nothing. Always pass the question: without one it opens a full-screen conversation for a person. |
| `sst apply` | writes Snowflake | Publishes. Only when asked, and from a reviewed plan. |
| `sst drop` | writes Snowflake | Break-glass: drops one object SST owns. Only when asked. |
| `sst state recover` | writes Snowflake | Records lost state entries from ownership markers; writes no object. Only when asked. |
| `sst state convert` | writes Snowflake | Rebuilds the state and lock tables as hybrid or standard tables. Only when asked. |
| `sst state unlock` | writes Snowflake | Releases the run lock of one named run that will never end. Only when asked, once the user confirmed that run is gone. |

## The loop

Every change goes through the same steps. The first three are offline.

```bash
sst format                                  # canonical YAML after an edit
sst validate --no-snowflake-syntax-check    # every offline rule
sst compile --emit-ddl target/ddl           # the exact DDL each view publishes
```

When the user wants a connected check or a release:

```bash
sst validate                                # also has Snowflake compile each expression
sst plan                                    # what would change; exit 2 = changes pending
sst apply --plan sst_target/plan.json --yes # only when the user asks to publish
sst test                                    # queries what was published
```

`sst plan` and `sst apply` read the manifest `sst compile` writes, so compile first.

## Before a change: what does it reach?

```bash
sst lineage model:orders                  # what reads the model, down to agents and evals
sst plan --select model:orders+           # plan only the model and what is built on it
```

Use `sst lineage` before editing a model or member the user did not write, and tell them what
else the change reaches. Selectors and `+` are in @SST_DOCS@/guides/lineage.md.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success; for `plan`, nothing to change. |
| 1 | Errors were reported, or an apply, a smoke probe, an eval gate, or a check failed. |
| 2 | Differences: `plan` has changes, `diff` found a difference, a `--check` would rewrite. |
| 3 | The command line is invalid. |
| 4 | The project, its configuration, or a saved plan cannot be used. |
| 5 | Snowflake could not be reached, rejected the credential, or refused a privilege. |
| 130 | Interrupted, or a confirmation was declined. |

## Where things live

- `sst_config.yml` at the project root: SST's settings. `sst debug --no-connect` shows the
  file and target that were resolved.
- `profiles.yml`: the same targets dbt uses.
- The semantic models directory (`project.semantic_models_dir`, default `semantic_models`):
  views under `semantic_views/`, and metrics, relationships, filters, verified queries, and
  custom instructions anywhere below it, usually one item per file under its singular root.
- Column metadata: each dbt model's YAML, under `config.meta.sst`.
- `agents/`, `cowork_profiles/`, `tools/`, `skills/`, `plugins/`, `coco_desktop_profiles/`: agents and what SST publishes for
  them. The `skills/` folder there is published to Snowflake. These kit skills are not, and
  must stay out of it.
- `sst_target/`: build output. Never edit it, and never commit it.
- `.sst/`: the diagnostic baseline and this kit's lock. Commit both.

## Other skills in this kit

- `sst-getting-started`: what SST and its pieces are, the dbt basics, a first project, and
  which guide to read.
- `sst-semantic-authoring`: writing views, metrics, relationships, filters, verified queries.
- `sst-fix-diagnostics`: reading and fixing what `sst validate` reports.
- `sst-agents-and-evals`: agents, tools, published skills, and evals.
- `sst-troubleshoot`: dbt parse failures, profiles, connections, wrong targets.
- `sst-migrate-from-0-3`: moving a 0.3 project to this SST.
- `sst-report-issue`: when SST itself looks wrong, and the user wants to report it.
