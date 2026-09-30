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
| Artifacts | 14, of all seven types |
| Goldens | `tests/golden/expected/{ddl,agent,tool,eval,skill,plugin,profile}/` |
| Targets | `dev` (default, the golden target), `prod`, `ci`: no credentials, offline only; `verify`: reads `SST_VERIFY_*` |

It is vendored in this repository, so there is nothing to clone.

## Known Gotchas

1. **`profiles.yml` lives in the project root.** SST never reads `~/.dbt/profiles.yml`. Never print a `profiles.yml` or the environment variables that hold credentials; `sst debug` shows the resolved target without secrets.

2. **Strict fixture.** The fixture sets `validation.strict: true`, and its one warning (`SST-VAL528`) is deliberate, so offline runs pass `--no-strict`.

3. **Syntax checks connect.** `validation.snowflake_syntax_check` is true in the fixture and is the default elsewhere; pass `--no-snowflake-syntax-check` to stay offline.

4. **`--golden-dir` is resolved against `--project-dir`** unless it is absolute. From the repo root, pass `"$PWD/tests/golden/expected/ddl"`.

5. **Compile before plan.** `plan`, `apply`, and the smoke and eval suites read the manifest `sst compile` wrote for the same `--target`. `SST-MAN001` or `compiled SST manifest is stale` means compile again with that target.

6. **Build output.** `sst compile` writes `target/sst/` inside the project; in the fixture, git ignores it. Never run `sst migrate refs --write` on `tests/fixtures/`; use a copy.

7. **The `verify` target** needs `SST_VERIFY_ACCOUNT`, `SST_VERIFY_USER`, `SST_VERIFY_ROLE`, `SST_VERIFY_SCHEMA`, and `SST_VERIFY_WAREHOUSE`; the database and authenticator have defaults in the fixture's `profiles.yml`. The fixture documents it for building its dbt models in a real account (`dbt deps`, then `dbt build --target verify`). SST itself cannot compile the fixture for `verify`: the tool `relations:` maps cover only `dev` and `prod`, so compile stops with `SST-REF018`, and `--partial` refuses (`SST-PLN033`).

8. **Package manager.** SST uses Poetry. Never use `uv`.
