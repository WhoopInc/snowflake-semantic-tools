---
name: sst-test
description: "Run the SST test suite and the CI gates locally. Use when: running tests, checking coverage floors, verifying fixes, checking a branch before a PR. Triggers: test, pytest, unit test, run tests, coverage, gates, mypy, lint-imports."
---

# SST Test

Run the SST test suite and the other CI gates. Nothing here connects to Snowflake.

## Prerequisites

- Dependencies installed with Poetry from the repo root: `poetry install` (Poetry, NOT uv)
- The gates are the `run:` steps of `.github/workflows/test-and-lint.yml`. If they differ from the commands below, the workflow wins.

## Workflow

### Step 1: Choose the environment

The commands below use Poetry's environment (`poetry run ...`). Another environment works if it has this checkout installed in editable mode plus the dev dependencies from `pyproject.toml`; `test_ring_boundaries.py` needs `lint-imports` beside the interpreter that runs pytest.

**⚠️ MANDATORY STOPPING POINT**: Ask the user whether to use Poetry's environment or another one. Do NOT assume an environment name, and do NOT proceed until the user responds.

Then, from the repo root:
```bash
cd "$(git rev-parse --show-toplevel)"
poetry run sst --version    # must print the version in snowflake_semantic_tools/_version.py
```

In another environment, drop the `poetry run` prefix from every command.

### Step 2: Run the suite

```bash
poetry run pytest tests/
```

### Step 3: Targeted runs (while iterating)

```bash
poetry run pytest tests/unit/domain -q               # one ring
poetry run pytest tests/unit/test_golden_ddl.py -q   # one file
poetry run pytest tests/ -q -k "migrate_refs"        # by name
```

### Step 4: Coverage floors (branch coverage)

```bash
poetry run pytest -q --cov=snowflake_semantic_tools.domain --cov-branch --cov-report=term --cov-fail-under=100 \
  tests/unit/domain tests/unit/test_render_semantic_view.py
poetry run pytest -q --cov=snowflake_semantic_tools.app --cov-branch --cov-report=term --cov-fail-under=95 \
  tests/unit/app tests/unit/test_compile_use_case.py tests/unit/test_manifest_v1.py
poetry run pytest -q --cov=snowflake_semantic_tools.cli --cov-branch --cov-report=term --cov-fail-under=90 \
  tests/unit/cli
poetry run pytest -q -n auto --cov=snowflake_semantic_tools.adapters --cov-branch --cov-report=term --cov-fail-under=90 \
  tests/
```

### Step 5: Static gates

```bash
poetry run mypy snowflake_semantic_tools tests   # strict
poetry run ruff format --check snowflake_semantic_tools/ tests/
poetry run ruff check snowflake_semantic_tools/ tests/
poetry run lint-imports       # the six import contracts
poetry run sst docs --check   # docs/reference/*.md matches the registries
```

### On Failure

**⚠️ MANDATORY STOPPING POINT**: If anything fails, report the failures to the user with their messages before taking any corrective action. Do NOT auto-fix without approval.

What the usual failures mean:
- **Golden diff**: rendered output changed. Confirm the change is intended before any golden is edited (`tests/README.md`, Goldens).
- **`lint-imports` or `test_ring_boundaries.py`**: an import crosses a ring; the output names the broken contract.
- **`sst docs --check`**: a diagnostic, config key, CLI option, or artifact type changed; `poetry run sst docs` regenerates the pages.
- **ruff**: `poetry run ruff format snowflake_semantic_tools/ tests/` and `poetry run ruff check --fix snowflake_semantic_tools/ tests/` fix most findings; the rest name the file and line.
- **`test_import_style.py`**: a relative import, or a test importing a conftest or another test module; the message names the file and line. Write the full dotted path, and move shared test code into `tests/helpers/`.
- **`test_release_hygiene.py` / `test_public_docs.py`**: a committed file carries an environment-specific name, a home-directory path, a planning identifier, or a broken docs link; the message names the file and line.

## Test Structure

```
tests/
  unit/domain/        # pure ring
  unit/app/           # use cases over in-memory ports
  unit/adapters/      # YAML, dbt manifest, connector, files, config
  unit/cli/           # commands through click's CliRunner, one module per command
  unit/test_golden_*.py
  contract/           # adapters against their ports
  fixtures/           # reference_project (+ its dbt manifest), v1_dialect
  golden/expected/    # ddl, agent, tool, eval, skill, plugin, profile
  helpers/            # shared test support, imported as tests.helpers.<module>
```

`tests/README.md` has the details.

## Key Conventions

- Test files: `test_*.py`; test functions: `test_*`
- The suite is offline: no Snowflake connection, and the reference project compiles from `tests/fixtures/reference_project_manifest.json`
- Tests use real in-memory ports and `monkeypatch`; there is no mocking library and there are no pytest markers
- CLI tests invoke `snowflake_semantic_tools.cli.main:cli` through `CliRunner`

## Stopping Points

- ✋ Step 1: Before running anything (ask which environment to use)
- ✋ On Failure: After test or gate failures (report before taking action)

## Output

A summary per gate (pass/fail), the test counts from `pytest tests/` (passed, failed, errors), coverage per ring against its floor, and the error messages for any failure.
