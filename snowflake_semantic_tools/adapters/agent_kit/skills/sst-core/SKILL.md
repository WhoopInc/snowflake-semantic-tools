---
name: sst-core
description: |-
  Use first for any task in a project that publishes with Snowflake Semantic Tools (SST):
  which `sst` command does what, which ones connect to Snowflake, write to it, or cost
  credits, the safe edit-validate-plan-apply loop, exit codes, and reading `--output json`.
  Routes to the other sst-* skills for authoring, diagnostics, agents and evals,
  troubleshooting, migration, and bug reports. Do NOT use for dbt modelling questions that
  do not involve SST, or for SST's own source code.
---
# Working in an SST project

SST compiles a dbt project's semantic YAML into Snowflake semantic views, Cortex Agents,
skills, and CoCo Desktop profiles, then publishes them. This kit was installed by SST
@SST_VERSION@; its documentation is at @SST_DOCS@.

## Ground rules

1. **Never publish without being asked.** `sst apply`, `sst drop`, and anything else marked
   *writes* below changes Snowflake. Run them only when the user asks for that command, and
   show the plan first.
2. **Never spend credits without being asked.** `sst test --suite evals` runs agents in
   Snowflake, and `sst test` with no `--suite` runs that suite too once an eval compiles.
   Prefer `sst test --suite golden`, which is offline.
3. **Stay offline by default.** `sst validate` connects unless told not to, and so do
   `sst debug`, `sst plan`, and `sst diff`. Use the offline forms in the table unless the user
   wants a connected check. With `authenticator: externalbrowser` in `profiles.yml`, a
   connecting command opens a browser and waits for the sign-in with no time limit, so in a
   shell nobody watches it never returns: ask the user to run it in their own terminal.
4. **Facts come from the installed CLI, not from memory.** Run `sst <command> --help` for
   flags and `sst explain <CODE>` for any diagnostic. Both work offline and match the SST
   that is installed.
5. **Parse JSON, not text.** Add `-o json` (a global option, before the command name) when you
   need to read the result: one JSON object on stdout with `exit_code`, `diagnostics`,
   `summary`, and `data`.
6. **Never baseline a diagnostic to make a run pass** unless the user asks; fix it instead.
   See the `sst-fix-diagnostics` skill.

## Commands by risk

| Command | Risk | Notes |
|---|---|---|
| `sst explain` | offline | Explains one diagnostic code; needs no project. |
| `sst compile` | offline | Writes `target/sst/manifest.json`; `--emit-ddl <dir>` writes the DDL to read. |
| `sst list` | offline | Lists compiled artifacts and their cached status. |
| `sst format` | offline, edits files | `--check` exits 2 when a file would change; run it after editing SST YAML. |
| `sst init` | offline, edits files | Scaffolds `sst_config.yml`; never overwrites a file. |
| `sst clean` | offline, edits files | Removes `target/sst` only. |
| `sst migrate refs` | offline, edits files | Dry run by default; `--write` rewrites references and moves old settings under `config:`. |
| `sst migrate split` | offline, edits files | Dry run by default; `--write` writes one file per listed member and removes the list. |
| `sst docs` | offline, edits files | Regenerates SST's own reference pages; for SST maintainers, not projects. |
| `sst baseline show` | offline | Lists the baseline file's entries. |
| `sst baseline add` | offline, edits files | Writes `.sst/baseline.json`; `--target` connects. Ask before using it. |
| `sst baseline prune` | offline, edits files | Removes stale entries; `--target` connects. |
| `sst baseline renew` | offline, edits files | Re-dates the baseline; needs `--reason`. Ask before using it. |
| `sst agent-kit install` | offline, edits files | Installs these skills. |
| `sst agent-kit upgrade` | offline, edits files | Brings these skills to the installed SST's kit. |
| `sst agent-kit status` | offline | `--check` exits 2 when the kit is out of date. |
| `sst agent-kit remove` | offline, edits files | Removes these skills. |
| `sst validate` | reads Snowflake | Offline with `--no-snowflake-syntax-check`. |
| `sst debug` | reads Snowflake | Offline with `--no-connect`: shows the config, profile, and target SST resolved. |
| `sst diff` | reads Snowflake | Offline only when `--from` and `--to` are both `local` or saved plans. |
| `sst plan` | reads Snowflake | Never writes; exits 2 when there are changes. Saves `target/sst/plan.json`. |
| `sst enrich` | reads Snowflake, edits files | Reads the warehouse into model YAML; `--include synonyms` calls Cortex, which costs credits. |
| `sst test` | billed | Golden is offline, smoke reads, evals run agents. Always pass `--suite`. |
| `sst apply` | writes Snowflake | Publishes. Only when asked, and from a reviewed plan. |
| `sst drop` | writes Snowflake | Break-glass: drops one object SST owns. Only when asked. |

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
sst apply --plan target/sst/plan.json --yes # only when the user asks to publish
sst test --suite smoke                      # queries what was published
```

`sst plan` and `sst apply` read the manifest `sst compile` writes, so compile first.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success; for `plan`, nothing to change. |
| 1 | Errors were reported, or an apply, a suite, or a check failed. |
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
  custom instructions anywhere below it.
- Column metadata: each dbt model's YAML, under `config.meta.sst`.
- `agents/`, `tools/`, `skills/`, `plugins/`, `profiles/`: agents and what SST publishes for
  them. The `skills/` folder there is published to Snowflake. These kit skills are not, and
  must stay out of it.
- `target/sst/`: build output. Never edit it, and never commit it.
- `.sst/`: the diagnostic baseline and this kit's lock. Commit both.

## Other skills in this kit

- `sst-semantic-authoring`: writing views, metrics, relationships, filters, verified queries.
- `sst-fix-diagnostics`: reading and fixing what `sst validate` reports.
- `sst-agents-and-evals`: agents, tools, published skills, and evals.
- `sst-troubleshoot`: dbt parse failures, profiles, connections, wrong targets.
- `sst-migrate-from-0-3`: moving a 0.3 project to this SST.
- `sst-report-issue`: when SST itself looks wrong, and the user wants to report it.
