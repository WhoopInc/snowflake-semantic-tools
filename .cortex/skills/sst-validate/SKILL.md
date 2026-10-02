---
name: sst-validate
description: "Run sst validate and sst compile --emit-ddl against an SST project and interpret the diagnostics. Use when: validating a project, explaining an SST-XXXnnn diagnostic, inspecting rendered DDL, checking references or relationships. Triggers: validate, sst validate, sst compile, emit-ddl, diagnostic, error code, check models, relationship errors."
---

# SST Validate

Run `sst validate` against a project, interpret its diagnostics, and inspect what compiles with `sst compile --emit-ddl`. Both run offline unless stated otherwise.

## Prerequisites

- SST 1.x on PATH (`sst --version`). In the SST repo, use `poetry run sst` (the `sst-test` skill covers the environment choice).
- The project root holds `sst_config.yml` and `profiles.yml`; SST never reads `~/.dbt/profiles.yml`. A dbt project also holds `dbt_project.yml`, and SST runs `dbt parse` itself unless `--manifest` names a manifest.
- Never print `profiles.yml` or the environment variables behind it. `sst debug` shows the resolved target without credentials.

## Workflow

### Step 1: Locate the project

**⚠️ MANDATORY STOPPING POINT**: Ask the user for the project directory. Do NOT assume a path. For an engine check, offer the in-repo reference project, `tests/fixtures/reference_project`.

### Step 2: Check what the target resolves to

```bash
sst debug --project-dir <project> [--target <target>]
```

This prints the profile, target, database, schema, state table, and authentication method. It connects only with `--test-connection`.

### Step 3: Validate offline

```bash
sst validate --project-dir <project> --no-snowflake-syntax-check
```

- Add `--manifest <path>` to skip `dbt parse`. For the reference project, from the repo root: `--manifest tests/fixtures/reference_project_manifest.json --no-strict` (its config is strict and its one warning, `SST-VAL528`, is deliberate).
- `validation.snowflake_syntax_check` defaults to true, so without the flag `validate` connects and compiles each expression against Snowflake. Run it that way only when the user asks for connected checks.

### Step 4: Interpret results

- **Exit codes**: `0` no errors; `1` errors, including warnings promoted by `--strict` or `validation.strict: true`; `3` invalid command line; `4` the project or its configuration cannot be used; `5` Snowflake could not be reached.
- **Human output**: one `<severity>[<code>]: <message>` line per diagnostic on stderr; a passing run ends with `validated N artifact(s): X errors, Y warnings`.
- **JSON**: `--output json` prints one envelope. Each entry in `diagnostics` carries `code`, `severity`, `message`, `location`, `suggestion`, and `help_url`; `summary` counts each severity and the warnings `promoted` to errors.
- **Severities**: an error blocks; a warning blocks only under strict mode; an info never blocks.
- **Fixes**: look up every code in `docs/reference/error-codes.md`, and symptoms in `docs/guides/troubleshooting.md`. Some errors stop other checks on the same artifact, so re-run after each fix.

### Step 5: Inspect the rendered output

```bash
sst compile --project-dir <project> --emit-ddl "$(mktemp -d)"
```

- One file per artifact: `.sql` for semantic views and tools, `.json` for agents, skills, plugins, and profiles, `.yaml` for evals. `--print-ddl` prints to stdout instead, and `--select <name>` narrows to one artifact.
- `compile` also writes the SST manifest to `<project>/target/sst/manifest.json`. `list` reads it, and `plan`, `apply`, and the smoke and eval suites refuse to run unless it was compiled for the same `--target`.
- `compile` never connects to Snowflake.

### Step 6: A 0.3 project

`SST-REF034` and `SST-REF035` mean 0.3 `table()` / `column()` references. Run the codemod as a dry run first (exit `2` while rewrites are pending):

```bash
sst migrate refs --project-dir <project>
```

**⚠️ MANDATORY STOPPING POINT**: `--write` rewrites the user's files in place. Show the report and get approval before running `sst migrate refs --project-dir <project> --write`. The rest of the upgrade is in `docs/guides/migrating-from-0.3.md`.

## Common First Checks

- **`dbt parse failed`**: `profiles.yml` is not in the project root, or `dbt deps` has not run. Alternatively pass `--manifest`.
- **`SST-PRT007`**: the dbt manifest's schema version is not the one SST supports.
- **`SST-MEM005`**: a metric, relationship, filter, or verified query attaches to no view; compare its `tables:` with the views' `tables:`.
- **`SST-VAL116` / `SST-VAL209`**: more than one join path; add `using_relationships:` to the metrics that cross it.
- **`SST-CFG047`**: a `project.*_dir` key names a directory that does not exist.

## Key Files

- SST repo root: resolve with `git rev-parse --show-toplevel`
- Diagnostic registry: `snowflake_semantic_tools/domain/diagnostics/` (the codes are in `specs/`, one module per code area); find where a code is raised with `grep -rn "SST-VAL116" snowflake_semantic_tools/`
- Validation use case: `snowflake_semantic_tools/app/validate.py`
- Configuration schema: `snowflake_semantic_tools/domain/model/config_schema/keys.py`
- Loaders: `snowflake_semantic_tools/adapters/yaml/` and `snowflake_semantic_tools/adapters/dbt/manifest.py`

## Stopping Points

- ✋ Step 1: Before running anything (ask for the project directory)
- ✋ Step 6: Before `sst migrate refs --write` (it rewrites files)

## Output

Validation summary with error, warning, and info counts; each diagnostic with its code, what it means, and the fix from the error code reference; and, when compiled, where the rendered files were written.
