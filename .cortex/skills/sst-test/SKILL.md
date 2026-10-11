---
name: sst-test
description: "Run the SST test suite and the CI gates locally. Use when: running tests, checking coverage floors, verifying fixes, checking a branch before a PR. Triggers: test, pytest, unit test, run tests, coverage, gates, mypy, lint-imports."
---

# SST Test

Run the SST test suite and the other CI gates. Nothing here connects to Snowflake.

## Prerequisites

- Dependencies installed with Poetry from the repo root: `poetry install` (Poetry, NOT uv)
- The gates are the `run:` steps of `.github/workflows/test-and-lint.yml`, also listed in `CONTRIBUTING.md` (Running the Gates). If they differ from the commands below, the workflow wins.

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
poetry run pytest tests/ -n auto
```

Tests marked `live` connect to Snowflake and are deselected by default; never add `-m live` unless the user asks (they need an account configured as `docs/guides/testing-live.md` describes). Every test has a 120-second timeout.

### Step 3: Targeted runs (while iterating)

```bash
poetry run pytest tests/unit/domain -q               # one ring
poetry run pytest tests/unit/test_golden_ddl.py -q   # one file
poetry run pytest tests/ -q -k "migrate_refs"        # by name
poetry run pytest tests/codes -q                     # the diagnostic-code guards
```

### Step 4: Coverage (branch coverage)

```bash
# The whole suite with coverage, the per-layer ratchet against tests/coverage_baseline.json,
# and the adapters floor, read from that same run
poetry run pytest tests/ -n auto --cov=snowflake_semantic_tools --cov-branch --cov-report=json:coverage.json
poetry run python -m tests.helpers.coverage_ratchet check --coverage-json coverage.json
poetry run coverage report --include='snowflake_semantic_tools/adapters/*' --fail-under=90 --skip-covered

# Floors, each measured on its own test paths
poetry run pytest -q --cov=snowflake_semantic_tools.domain --cov-branch --cov-report=term --cov-fail-under=100 \
  tests/unit/domain tests/unit/test_render_semantic_view.py
poetry run pytest -q --cov=snowflake_semantic_tools.app --cov-branch --cov-report=term --cov-fail-under=95 \
  tests/unit/app tests/unit/test_compile_use_case.py tests/unit/test_manifest_v1.py
poetry run pytest -q --cov=snowflake_semantic_tools.cli --cov-branch --cov-report=term --cov-fail-under=90 \
  tests/unit/cli
```

### Step 5: Static and contract gates

```bash
poetry run pytest tests/unit/cli/test_exit_codes.py                    # the exit-code contract
poetry run sst --project-dir tests/fixtures/reference_project format --check  # fixture YAML is canonical; exit 2 if not
poetry run mypy snowflake_semantic_tools tests   # strict
poetry run ruff format --check snowflake_semantic_tools/ tests/
poetry run ruff check snowflake_semantic_tools/ tests/
poetry run lint-imports       # the seven import contracts in pyproject.toml
poetry run sst docs --check   # docs/reference/*.md matches the registries
poetry run bandit -r snowflake_semantic_tools -ll
```

CI also runs `pip-audit --skip-editable` over the locked dependencies, which needs network access to a vulnerability database, and semgrep in a separate job with the rules vendored in `.semgrep/` (`semgrep scan --config .semgrep/ --error --metrics off snowflake_semantic_tools`). Run them only if the user asks; `CONTRIBUTING.md` says how semgrep is installed.

### On Failure

**⚠️ MANDATORY STOPPING POINT**: If anything fails, report the failures to the user with their messages before taking any corrective action. Do NOT auto-fix without approval.

What the usual failures mean:
- **Golden diff**: rendered output changed. Confirm the change is intended before any golden is rewritten, then rewrite with `sst test --suite golden ... --update-golden`, never by hand (`tests/README.md`, Goldens).
- **`lint-imports` or `test_ring_boundaries.py`**: an import crosses a ring; the output names the broken contract.
- **`sst docs --check`** (`out of date: <path>`): a diagnostic, config key, CLI option, or artifact type changed; `poetry run sst docs` regenerates the pages.
- **`format --check` on the fixture**: `poetry run sst --project-dir tests/fixtures/reference_project format` rewrites its YAML into canonical form.
- **`coverage_ratchet check`**: a layer's line or branch coverage fell below `tests/coverage_baseline.json`; add tests rather than lowering the number.
- **`tests/codes/`**: a diagnostic code is registered without a catalog row, is never emitted, or lacks its `test_<code>_fires` / `test_<code>_silent` pair; `tests/codes/README.md` says how to add one. Its allowlists only shrink.
- **ruff**: `poetry run ruff format snowflake_semantic_tools/ tests/` and `poetry run ruff check --fix snowflake_semantic_tools/ tests/` fix most findings; the rest name the file and line.
- **`test_import_style.py`**: a relative import, or a test importing a conftest or another test module; the message names the file and line. Write the full dotted path, and move shared test code into `tests/helpers/`.
- **`test_structure.py` / `test_docstrings.py`**: a module or function exceeds a size budget or breaks a docstring rule (`CONTRIBUTING.md`, Docstrings and comments); there are no exceptions.
- **`test_release_hygiene.py` / `test_public_docs.py`**: a committed file carries an environment-specific name, a home-directory path, a planning identifier, or a broken docs link; the message names the file and line.
- **`NetworkAccessRefused`**: a test outside the `live` marker tried to reach the network; fake the call with the doubles in `tests/helpers/`.

## Test Structure

```
tests/
  unit/domain/        # pure ring
  unit/app/           # use cases over in-memory ports
  unit/adapters/      # YAML, dbt manifest, connector, files, config
  unit/cli/           # commands through click's CliRunner, one module per command
  unit/test_golden_*.py
  codes/              # diagnostic-code guards: one fires/silent pair per code
  contract/           # adapters against their ports, offline
  contract_live/      # live: the real connector against the same contracts
  integration/        # live: apply in a real account
  e2e/                # the CLI end to end: the offline negative corpus, and the live lifecycle
  fixtures/           # reference_project (+ its dbt manifest), v1_dialect, negative_overlays, scoped_views
  golden/expected/    # ddl, agent, tool, eval, skill, plugin, profile, enrich
  helpers/            # shared test support, imported as tests.helpers.<module>
```

`tests/README.md` has the details.

## Key Conventions

- Test files: `test_*.py`; test functions: `test_*`
- The default suite is offline: `tests/conftest.py` refuses any network connection except loopback and Unix sockets, and the reference project compiles from `tests/fixtures/reference_project_manifest.json`
- Two markers: `live` (connects to Snowflake; deselected by default) and `e2e` (drives the `sst` CLI in a subprocess over a whole project)
- Tests use real in-memory ports (`tests/helpers/app_ports.py`, `tests/helpers/snowflake_fake/`) and `monkeypatch`; there is no mocking library
- CLI tests invoke `snowflake_semantic_tools.cli.main:cli` through `CliRunner`

## Stopping Points

- ✋ Step 1: Before running anything (ask which environment to use)
- ✋ On Failure: After test or gate failures (report before taking action)

## Output

A summary per gate (pass/fail), the test counts from `pytest tests/` (passed, failed, errors, skipped), coverage per ring against its floor and the ratchet, and the error messages for any failure.
