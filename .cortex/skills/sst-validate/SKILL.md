---
name: sst-validate
description: "Run sst validate and sst compile --emit-ddl against an SST project and interpret the diagnostics. Use when: validating a project, explaining an SST-XXXnnn diagnostic, inspecting rendered DDL, checking references or relationships. Triggers: validate, sst validate, sst compile, emit-ddl, diagnostic, error code, check models, relationship errors."
---

# SST Validate

Run `sst validate` against a project, interpret its diagnostics, and inspect what compiles with `sst compile --emit-ddl`. Both run offline unless stated otherwise.

## Prerequisites

- SST 1.x on PATH (`sst --version`). In the SST repo, use `poetry run sst` (the `sst-test` skill covers the environment choice).
- The project root holds `sst_config.yml`; a dbt project also holds `dbt_project.yml`, and SST runs `dbt parse` itself unless `--manifest` names a manifest. SST reads the first `profiles.yml` it finds in `--profiles-dir` (or `$SST_PROFILES_DIR`), `$DBT_PROFILES_DIR`, the project root, then `~/.dbt/`.
- Never print `profiles.yml` or the environment variables behind it. `sst debug --no-connect` shows the resolved target without credentials.

## Workflow

### Step 1: Locate the project

**⚠️ MANDATORY STOPPING POINT**: Ask the user for the project directory. Do NOT assume a path. For an engine check, offer the in-repo reference project, `tests/fixtures/reference_project`.

### Step 2: Check what the target resolves to

```bash
sst debug --project-dir <project> --no-connect [--target <target>]
```

This prints the versions, the configuration file, the `profiles.yml` it chose, the dbt manifest, and the target's profile, database, schema, state table, and authentication method. Without `--no-connect`, `debug` also tests the connection (exit 5 when it fails); run it that way only when the user asks.

### Step 3: Validate offline

```bash
sst validate --project-dir <project> --no-snowflake-syntax-check
```

- Add `--manifest <path>` to skip `dbt parse`. For the reference project, from the repo root: `--manifest tests/fixtures/reference_project_manifest.json --no-strict` (its config is strict, and its five warnings are expected; see the `sst-e2e-test` skill's `references/expected-outputs.md`).
- `validation.snowflake_syntax_check` defaults to true, so without the flag `validate` connects and compiles each expression against Snowflake; `--verify-schema` connects too, to look up the columns each view reads. Run either only when the user asks for connected checks.
- A baseline of known warnings (`.sst/baseline.json`, written by `sst baseline add`) is applied when it exists; `--no-baseline` shows everything as declared.

### Step 4: Interpret results

- **Exit codes**: `0` no errors; `1` errors, including warnings promoted by `--strict` or `validation.strict: true`; `3` invalid command line; `4` the project or its configuration cannot be used; `5` Snowflake could not be reached, rejected the credential, refused a privilege, or did not finish in time (the diagnostic code says which).
- **Human output**: each diagnostic on stderr as `[<file>:<line>:<col>: ]<severity>[<code>]: <message>`, followed by `help:` (the suggestion) and `docs:` lines; a promoted warning reads `error[<code>] (promoted from warning)`. stderr ends with the counts, for example `0 errors; 6 warnings; 51 info; 51 info not shown (--show-info)`: infos, baselined diagnostics, and cascades are counted, not shown, unless `--show-info`, `--show-baselined`, or `--show-cascade` is given (on `validate`, `--show-info` also adds an info per semantic-model file naming the type that owns it). A run without errors also prints `validated N artifact(s): X errors, Y warnings` on stdout, with `(N baselined)` when the baseline held any.
- **JSON**: `--output json` prints one envelope on stdout. Each entry in `diagnostics` carries `code`, `severity`, `message`, `location`, `suggestion`, and `help_url`, among others; `summary` counts `error`, `warning`, and `info`, the warnings `promoted` to errors, the `baselined` ones, and the `suppressed_cascade` ones.
- **Severities**: an error blocks; a warning blocks only under strict mode; an info never blocks.
- **Fixes**: `sst explain <code>` prints a code's meaning offline; look up every code in `docs/reference/error-codes.md`, and symptoms in `docs/guides/troubleshooting.md`. Some errors stop other checks on the same artifact, so re-run after each fix.

### Step 5: Inspect the rendered output

```bash
sst compile --project-dir <project> --emit-ddl "$(mktemp -d)"
```

- It prints `compiled N artifact(s); manifest <path>`, then `wrote N file(s) to <dir>`. One file per artifact: `.sql` for semantic views and tools, `.json` for agents, skills, plugins, and profiles, `.yaml` for evals. `--select <name>` narrows the report and the files to those artifacts (the manifest still holds everything), and `--emit-agent-spec <dir>` writes each agent's specification on its own.
- `compile` also writes the SST manifest to `<project>/target/sst/manifest.json`. `list` reads it, and `plan`, `apply`, and the smoke and eval suites refuse to run unless it is current and was compiled for the same `--target` (`SST-MAN001`, `SST-MAN006`, or `compiled SST manifest is stale`).
- `compile` never connects to Snowflake.

### Step 6: A 0.3 project

`SST-REF034` and `SST-REF035` mean 0.3 `table()` / `column()` references. Run the codemod as a dry run first (exit `2` while rewrites are pending):

```bash
sst migrate refs --project-dir <project>
```

**⚠️ MANDATORY STOPPING POINT**: `--write` rewrites the user's files in place. Show the report and get approval before running `sst migrate refs --project-dir <project> --write`. The rest of the upgrade is in `docs/guides/migrating-from-0.3.md`.

## Common First Checks

- **`SST-DBT026` / `SST-DBT027` / `SST-DBT028`**: SST could not get a manifest from dbt: `packages.yml` declares packages `dbt deps` has not installed, no runnable `dbt` is on PATH, or `dbt parse` exited non-zero (dbt's own output comes first; often no `profiles.yml` was found, see Prerequisites). Alternatively pass `--manifest`.
- **`SST-DBT017`**: the dbt manifest's schema version is not one SST reads; `--allow-unsupported-manifest-schema` reads it for one run.
- **`SST-MEM005`** (warning): a metric, relationship, filter, or verified query attaches to no view; compare its `tables:` with the views' `tables:`.
- **`SST-VAL116`** (error) / **`SST-VAL209`** (warning): more than one join path; add `using_relationships:` to the metrics that cross it.
- **`SST-CFG047`**: a `project.*_dir` key names a directory that does not exist.
- **`SST-CFG034`**: `--strict`/`--no-strict` disagrees with `validation.strict`; the flag wins.

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
