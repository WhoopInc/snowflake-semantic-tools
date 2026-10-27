# Environment Setup Guide

## Prerequisites

- Python 3.11–3.13 and Poetry
- The SST repo, with dependencies installed by `poetry install` from its root
- Nothing else for the offline phases: no Snowflake account, credentials, or dbt
- For the connected phase only: a dbt + SST project whose `profiles.yml` has a target pointing at a scratch schema, dbt on PATH (or `poetry install --extras dbt`), and a role that can create what the plan creates

## Locating the SST Repo

The skill runs from the root of the `snowflake-semantic-tools` repository: the directory with `pyproject.toml` and `snowflake_semantic_tools/`. `git rev-parse --show-toplevel` finds it.

## Installing SST (Development Mode)

```bash
cd <SST_REPO>
poetry install
poetry run sst --version   # must print the version in snowflake_semantic_tools/_version.py
```

If the user keeps SST in another environment (a conda env or virtualenv with this checkout installed in editable mode and the dev dependencies from `pyproject.toml`), ask which one and run the same commands there without `poetry run`. Do not assume an environment name or installation path.

## The Reference Project

| Property | Value |
|----------|-------|
| Path | `tests/fixtures/reference_project/` |
| dbt manifest | `tests/fixtures/reference_project_manifest.json` (dbt 1.11, manifest schema v12) |
| Artifacts | 16, of all eight types |
| Goldens | `tests/golden/expected/{ddl,agent,tool,cowork_profile,eval,skill,plugin,coco_profile}/` |
| Targets | `dev` (default, the golden target), `prod`, `ci`: no credentials, offline only (`ci` is in no tool `relations:` map, so compiling for it stops with `SST-REF018`); `verify`: reads `SST_TEST_SNOWFLAKE_*` |

It is vendored in this repository, so there is nothing to clone.

## Known Gotchas

1. **Where `profiles.yml` is found.** SST looks in `--profiles-dir` (or `$SST_PROFILES_DIR`), then `$DBT_PROFILES_DIR`, then the project root, then `~/.dbt/`, and uses the first that exists; `sst debug --no-connect` lists the candidates (`profiles_candidates`) and the file it chose (`profiles_file`). The fixture keeps its own in the project root. Never print a `profiles.yml` or the environment variables that hold credentials; `sst debug` shows the resolved target without secrets.

2. **Strict fixture.** The fixture sets `validation.strict: true`, and its five artifact warnings (`SST-VAL528`, `SST-RND010`, `SST-RND013`, and `SST-CFG018` twice) are expected, so offline runs pass `--no-strict`, which itself adds `SST-CFG034`.

3. **Syntax checks connect.** `validation.snowflake_syntax_check` is true in the fixture and is the default elsewhere; pass `--no-snowflake-syntax-check` to stay offline.

4. **`sst test` does not compare goldens.** It smoke-probes what `sst apply` published, as `sst eval` runs its agents, so neither belongs in an offline phase. The goldens are compared by `poetry run pytest tests/unit/test_goldens.py` and rewritten by `poetry run python -m tests.helpers.update_goldens`, both from the repo root.

5. **Compile before plan.** `plan`, `apply`, and the smoke and eval suites read the manifest `sst compile` wrote for the same `--target`. `SST-MAN001` (none), `SST-MAN006` (another target), or `compiled SST manifest is stale; run sst compile before ...` means compile again with that target.

   `sst debug` connects unless given `--no-connect`; for the fixture's credential-less targets, a plain `sst debug` exits 5 (`SST-PRT001`).

6. **Build output.** `sst compile` writes `sst_target/` inside the project; in the fixture, git ignores it, and `sst clean --project-dir tests/fixtures/reference_project` removes it. Never run `sst migrate refs --write` on `tests/fixtures/`; use a copy.

7. **The `verify` target** reads `SST_TEST_SNOWFLAKE_ACCOUNT`, `_USER`, `_ROLE`, `_DATABASE`, `_SCHEMA`, and `_WAREHOUSE`, the same variables the live tests read (`docs/guides/testing-live.md`, template `tests/live.example.env`); only the authenticator has a default (`externalbrowser`). An unset one stops at profile load with `SST-CFG013`, exit 4. The fixture documents the target for building its dbt models in a real account (`dbt deps`, then `dbt build --target verify`). SST itself cannot compile the fixture for `verify`: the tool `relations:` maps cover only `dev` and `prod`, so compile stops with `SST-REF018` (and `SST-MAN008`, no manifest written), and `--partial` cannot go ahead either (`SST-PLN033`): some of those errors cannot be traced to an artifact to exclude.

8. **Package manager.** SST uses Poetry. Never use `uv`.
