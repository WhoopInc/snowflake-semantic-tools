# Contributing to Snowflake Semantic Tools

Thank you for your interest in contributing to SST! This document outlines the process and guidelines.

## Maintainers

The maintainers are listed in [CODEOWNERS](CODEOWNERS), and GitHub requests their review on every pull request.

**All external contributions must be reviewed and approved by a maintainer before merging.**

## How to Contribute

### Reporting Issues

We use GitHub issue templates to help structure your report. When you [create a new issue](https://github.com/WhoopInc/snowflake-semantic-tools/issues/new/choose), you'll be able to choose from:

- **Bug Report** - Report unexpected behavior or errors
- **Feature Request** - Suggest new features or enhancements
- **Documentation Issue** - Report missing or unclear documentation

#### What Makes a Great Issue?

1. **A clear problem statement.** "`sst validate` reports `SST-REF001` for a model that `dbt ls` lists" helps; "SST doesn't work" does not.
2. **Expected versus actual behavior**, with output copied verbatim. Every diagnostic prints its code; `--output json` prints each one with its location and suggestion.
3. **Minimal reproduction steps**: the files involved, the exact command, and its exit code.
4. **Environment**: `sst --version`, `python --version`, `dbt --version` if the project uses dbt, the operating system, and the authentication method `sst debug` reports.
5. **Nothing private**: remove account, user, database, and schema names and every credential before posting.

**Example:**

> **Title**: [Bug]: `sst validate` reports SST-REF001 for a model dbt knows
>
> **Expected**: `{{ ref('orders') }}` in a view's `tables:` resolves; `dbt ls --select orders` lists the model.
>
> **Actual**: `error[SST-REF001]: {{ ref('orders') }} is not a model in the dbt manifest`, exit code 1.
>
> **Steps to Reproduce**: 1. Add the view in `semantic_models/semantic_views/sales.yml`. 2. Run `sst validate --no-snowflake-syntax-check`.
>
> **Environment**: SST 1.0.0, Python 3.12, dbt 1.11, macOS 15, externalbrowser

#### Before Submitting

- [ ] Search existing issues to avoid duplicates
- [ ] Use the appropriate issue template
- [ ] Say which version the issue affects: 1.0, or 0.3.x
- [ ] Provide clear, concise descriptions

### Proposing Changes

**Before starting work:**
1. Open an issue describing the problem/feature
2. Wait for maintainer feedback before implementing
3. Discuss approach and get alignment

**This prevents wasted effort on changes that won't be accepted.**

### Pull Request Process

1. **Fork the repository** and branch from the line you are changing: the 1.0 development branch, or for a 0.3.x fix the maintenance branch (see [Maintaining 0.3.x](#maintaining-03x))
2. **Make your changes** following the architecture and code style below
3. **Add tests** for new behavior
4. **Update documentation**, and regenerate the reference pages if needed
5. **Run the gates** and ensure they all pass
6. **Submit a pull request** with:
   - Clear description of changes
   - Link to related issue (`Closes #<number>`)
   - Gate results showing everything passes

**Pull requests will only be merged after maintainer review and approval.**

## Development Setup

### Prerequisites

- Python 3.11–3.13
- Poetry for dependency management
- Nothing else for the test suite: it needs no Snowflake account and no dbt install

### Installation

```bash
git clone https://github.com/WhoopInc/snowflake-semantic-tools.git
cd snowflake-semantic-tools

poetry install                  # add --extras dbt to run sst against a dbt project of your own
poetry run pre-commit install   # optional: the CI gates and file checks on every commit

poetry run sst --version
```

### Changing dependencies

`poetry.lock` must match `pyproject.toml`. After adding, removing, or re-pinning a dependency in
`pyproject.toml`, regenerate the lock and commit both files together:

```bash
poetry lock
```

CI checks this first (`poetry check --lock`) and fails with that instruction when the lock is out of
date. The lock on this branch predates the dev dependencies added for the hardening work (property
tests, parallel runs, mutation testing, security scans, the per-test timeout), so it must be
regenerated, on a machine that can reach PyPI, before CI can install.

### Running the Gates

These are the checks `.github/workflows/test-and-lint.yml` runs on every pull request. A release
tag runs the same workflow before it builds anything (`publish-to-pypi.yml` calls it). None of them
needs a secret or a Snowflake account. `.github/workflows/nightly.yml` adds mutation testing over
`domain/` and the property tests under the deeper `nightly` Hypothesis profile, both offline and
report-only.

```bash
# The exit-code contract, reported as its own step
poetry run pytest tests/unit/cli/test_exit_codes.py

# The reference project's semantic-layer YAML is in canonical form (exit 2 when not)
poetry run sst --project-dir tests/fixtures/reference_project format --check

# The suite, with coverage over the whole of it, then the per-layer ratchet against
# tests/coverage_baseline.json: line and branch coverage per layer may rise, never fall
poetry run pytest tests/ -n auto --cov=snowflake_semantic_tools --cov-branch --cov-report=json:coverage.json
poetry run python -m tests.helpers.coverage_ratchet check --coverage-json coverage.json
poetry run coverage report --include='snowflake_semantic_tools/adapters/*' --fail-under=90

# Branch-coverage floors, each measured on its own test paths
poetry run pytest -q --cov=snowflake_semantic_tools.domain --cov-branch --cov-fail-under=100 \
  tests/unit/domain tests/unit/test_render_semantic_view.py
poetry run pytest -q --cov=snowflake_semantic_tools.app --cov-branch --cov-fail-under=95 \
  tests/unit/app tests/unit/test_compile_use_case.py tests/unit/test_manifest_v1.py
poetry run pytest -q --cov=snowflake_semantic_tools.cli --cov-branch --cov-fail-under=90 \
  tests/unit/cli

poetry run mypy snowflake_semantic_tools tests   # strict
poetry run ruff format --check snowflake_semantic_tools/ tests/
poetry run ruff check snowflake_semantic_tools/ tests/
poetry run lint-imports         # ring boundaries
poetry run sst docs --check     # generated reference pages are current
poetry run bandit -r snowflake_semantic_tools -ll

# semgrep runs in its own job, installed outside the project's environment from a hash-locked
# requirements file, with the rules vendored in .semgrep/ (its README says how to refresh them)
pip install --require-hashes --only-binary :all: -r .github/requirements/semgrep.txt  # linux x86_64
semgrep scan --config .semgrep/ --error --metrics off snowflake_semantic_tools
```

The requirements file pins wheels for the CI runner (linux x86_64, CPython 3.11); elsewhere,
`pip install semgrep==1.179.0` gives the same scanner.

A semgrep false positive is silenced on its line with `# nosemgrep: <rule-id>` and a comment
saying why.

No test outside the `live` marker may reach the network. `tests/conftest.py` installs a guard for
the whole session (`tests/helpers/network_guard.py`) that patches `socket.socket.connect`,
`socket.socket.connect_ex`, `socket.create_connection` and `socket.getaddrinfo`: a Unix-domain
socket and a loopback address (`127.0.0.0/8`, `::1`, `localhost`) pass, so pytest-xdist and a
test's own local server work, and anything else raises `NetworkAccessRefused`. A refusal the code
under test catches and swallows still fails the test. A test that needs the network either fakes
the call with the doubles in `tests/helpers/` or is marked `live`, which lifts the guard from the
setup of its fixtures to its teardown. The guard holds in the pytest process only: a subprocess a
test starts, such as the end-to-end layer's `run_sst`, is not covered.

A coverage number that rises can be locked in with `coverage_ratchet raise`, which only ever moves
a number up. Lowering one is an edit to `tests/coverage_baseline.json` in the pull request that
needs it, where a reviewer sees it.

Every test has a 120-second timeout (`pytest-timeout`, configured in `pyproject.toml`), so a test
that deadlocks fails with every thread's stack rather than hanging the run. A test also fails if a
thread it started is still running after it returns.

### Tests against Snowflake

Optional. Tests marked `live` connect to a real account and are deselected by default, and no
contribution needs them: the fast gate above is the whole required gate, and it runs offline with no
secrets. They are for maintainers who have a Snowflake account of their own. To run them locally,
point them at an account with a key-pair user and a role that may create schemas in one scratch
database:

```bash
export SST_TEST_SNOWFLAKE_ACCOUNT=... SST_TEST_SNOWFLAKE_USER=... SST_TEST_SNOWFLAKE_ROLE=...
export SST_TEST_SNOWFLAKE_WAREHOUSE=... SST_TEST_SNOWFLAKE_DATABASE=...
export SST_TEST_SNOWFLAKE_PRIVATE_KEY_PATH=~/.ssh/sst_test.p8
poetry run pytest -m live tests/contract_live tests/integration tests/e2e
```

Each run creates its own `SST_IT_<UTC timestamp>_<run>_<worker>` schemas and drops them when it
ends; `python -m tests.helpers.sweep_scratch --older-than-hours 6` drops any a killed run left
behind, and never touches a schema without SST's marker comment. Without an account the tests skip;
with `--require-snowflake` they fail instead. Setting `SST_TEST_SNOWFLAKE_GRANTEE_ROLE` to a role
the test role may grant to adds the check that replacing a view keeps its grants.

The same checks can run in GitHub Actions, in this repository or a fork, through
`.github/workflows/slow.yml`. Nothing triggers it automatically and nothing waits on it: start it by
hand (Actions, "Snowflake checks (manual)", Run workflow) and pick a suite -- `live`, `sweep`,
`recording` or `evals`. It reads your account from these repository secrets, which you supply:

| Secret | Value |
|--------|-------|
| `SNOWFLAKE_ACCOUNT` | account identifier |
| `SNOWFLAKE_USER` | a key-pair user |
| `SNOWFLAKE_PRIVATE_KEY` | that user's unencrypted PKCS#8 private key, as PEM text |
| `SNOWFLAKE_ROLE` | a role that may create and drop schemas in `SNOWFLAKE_DATABASE` |
| `SNOWFLAKE_WAREHOUSE` | the warehouse the tests run in |
| `SNOWFLAKE_DATABASE` | a database set aside for test scratch schemas |
| `SNOWFLAKE_SCHEMA` | the schema the `evals` suite deploys to (`evals` only) |
| `SNOWFLAKE_GRANTEE_ROLE` | optional; enables the grant-preservation check |

When the secrets are not set the run skips with a notice and succeeds; it never fails for missing
credentials.

[tests/README.md](tests/README.md) describes the suite, the reference project, and the goldens.

## Architecture

The CLI is the only interface: the package exports nothing but `__version__`, so new behavior is a command or an option. The package root holds `__init__.py`, `_version.py`, and four rings:

```text
snowflake_semantic_tools/
├── cli/        # click commands: the composition root that wires adapters into use cases
├── app/        # use cases: compile, validate, plan, apply, the test suites, migrate refs
├── adapters/   # the edges: YAML, the dbt manifest, the Snowflake connector, files, config, profile
└── domain/     # pure: the pipeline's logic by stage, its data types, and the ports adapters implement
```

`domain/` is laid out by stage, following the pipeline parse → resolve → validate → plan → apply:

```text
domain/
├── model/        # data types only: artifacts, members, identifiers, the artifact registry, lifecycle values
├── diagnostics/  # the diagnostic machinery, with every code in specs/, one module per code area
├── parse/        # scanners: the template dialect, and the paths a skill's Markdown names
├── resolve/      # template calls, member attachment, and eval name templates
├── validate/     # every offline check, one module per subject
├── render/       # DDL, agent and tool specs, eval payloads, skill bundles, and Desktop profiles
├── plan/         # what apply must change, and in what order
├── state/        # the manifest, the applied state, and saved plans
├── enrich/       # the rules `sst enrich` derives column metadata by
├── migrate/      # the `sst migrate refs` rewrite
├── sql/          # typed SQL: the one way SST builds statement text
└── ports/        # the protocols adapters implement; ports/snowflake/ holds one module per role
```

A use case depends on the narrowest port it uses: a single role such as `ExecutionPort`, or a
small protocol combining the roles it calls. `SnowflakePort`, the union of every role, is for
the composition root in `cli/`.

Imports run one way, `cli` → (`app` | `adapters`) → `domain`:

- `app` and `adapters` are independent: a use case receives adapters through the ports in `domain/ports/`, and an adapter never calls a use case.
- `domain` does no I/O and reads no clock, environment, or randomness, so rendering is a pure function of its input and goldens can compare bytes.
- `app` never imports an SDK or touches the terminal (`yaml`, `click`, and `snowflake` are forbidden there).
- `adapters` may use domain models and ports, but not `domain/render`.
- The adapter subpackages `yaml`, `dbt`, `snowflake`, and `fs` never import one another, so each can be replaced and tested alone; a top-level adapter module such as `project_source.py` composes them.
- Only `adapters/yaml` (SST's own files) and `adapters/dbt` (dbt's) import `yaml`; everything else reaches YAML through them.

`poetry run lint-imports` enforces these as six contracts in `pyproject.toml`, and `tests/unit/test_ring_boundaries.py` proves that each contract rejects a crossing and that the package root holds nothing else.

### Diagnostics

Every problem SST reports is a diagnostic registered in `snowflake_semantic_tools/domain/diagnostics/specs/`, in the module for its code area (VAL is split by hundreds, `val_metric` for 1xx and so on): a stable code (`SST-`, a three-letter subsystem, and a number, such as `SST-VAL116`), a severity, a message template, and a suggestion that says how to fix it. Build one with `D("SST-...", ...)`. A new problem gets a new code; codes are never reused, even after the diagnostic that used one is removed.

### Generated Reference Pages

`docs/reference/*.md` is rendered from the engine's registries: the diagnostics, the configuration schema (`snowflake_semantic_tools/domain/model/config_schema/keys.py`, where every `sst_config.yml` key is declared), the CLI's commands and options, and the artifact registry (`snowflake_semantic_tools/domain/model/registry.py`). After changing any of them, run `poetry run sst docs` and commit the pages it rewrites; CI fails on `sst docs --check` otherwise.

### Version

`snowflake_semantic_tools/_version.py` holds the one version string, and it must equal `version` in `pyproject.toml`; a test asserts it. Change both together.

## Code Style

- **Formatting and linting**: ruff (line length 120)
- **Imports**: absolute, naming each module by its full dotted path: `from snowflake_semantic_tools.domain.model.dbt import DbtCatalog`, never `from ..model.dbt import DbtCatalog`. A search for a module's name then finds every module that imports it. Tests share code only through `tests/helpers/`, imported as `tests.helpers.<module>`, never from a conftest or another test module. ruff sorts imports, and `tests/unit/test_import_style.py` enforces both rules.
- **Type hints**: mypy runs in strict mode over the package and the tests, so every function is annotated
- **Error messages**: a diagnostic's suggestion is actionable — it says what is wrong and how to fix it
- **Tests**: real in-memory ports and pytest's `monkeypatch`, no mocking library
- **Size**: a module holds at most 800 lines and a function at most 100, with a decision count of at most 25; a function defined inside another holds at most 25 lines. Aim well below: about 500 lines per module and 60 per function. `tests/unit/test_structure.py` enforces the limits.

```bash
# Format code and sort imports
poetry run ruff format snowflake_semantic_tools/ tests/
poetry run ruff check --fix snowflake_semantic_tools/ tests/
```

### Docstrings and comments

Annotations already say what types flow where, so a docstring never restates them. It says what the code is for and what a caller or maintainer must know that the signature cannot show: invariants, sentinels, ordering, failure behavior, and the diagnostics it reports. `tests/unit/test_docstrings.py` checks the rules below.

1. **Every module** opens with what it owns and, where it matters, what it must not do.
2. **Every public class** has a summary line, then its invariants. A dataclass documents, in an `Attributes:` section, each field whose meaning the name and type do not settle: a sentinel (`None` versus `()` versus `""`), a unit, casefolding, or "overridden by".
3. **Every public function and method** opens with a one-line summary in the imperative, at most 100 characters, ending in a period, and a blank line before anything more. Add a section only when it carries information:
   - `Args:` for a parameter that is not obvious from its name and type;
   - `Returns:` when the value has structure or sentinels;
   - `Raises:` for every exception that escapes;
   - `Diagnostics:` for every `SST-` code the function reports, one per line. The test checks each code is registered, so the registry and the code that raises each entry stay linked.
4. **A `Protocol` method documents the contract**: what it promises, what it returns on failure, whether it raises, and whether it is idempotent. An implementation that overrides a documented method of a base class in this package inherits that docstring and adds one only to say how it differs.
5. **A private function** needs a docstring once it is 25 lines or longer, scores 10 or more on the decision count, or depends on an ordering, poisoning, or casefolding rule a reader could break.
6. **Comments say why, not what.** Mark ordering dependencies where they bite (for example, "runs after the view checks: poisoning reads their diagnostics"). Never cite a design document or planning identifier.
7. **A renderer** shows an `Example:` of input and output when the grammar it produces is not obvious.
8. **Use the shared vocabulary** (key, subject, origin, fingerprint, poisoned, ownership marker) in the sense [docs/concepts.md](docs/concepts.md#terms-used-in-the-code) defines.

A protocol method and a validator, written to these rules:

```python
def execute_script(self, statements: Sequence[str]) -> ExecResult:
    """Run statements in order on one session, stopping at the first failure.

    Never raises for a SQL error; the result carries it.

    Args:
        statements: Complete statements, each executed separately (never split on ``;``).

    Returns:
        ``ok=True`` with one query id per statement, or ``ok=False`` with the error and the
        ids of the statements that already ran, which the caller must treat as a partial write.
    """


def window_diagnostics(metric: MetricDef, models: Mapping[str, DbtModel]) -> list[Diagnostic]:
    """Check a window-function metric against rules Snowflake reports only at query time.

    Diagnostics:
        SST-VAL102: the window appears inside a derived metric.
        SST-VAL126: the window applies to a raw column, not a metric or an aggregate.
    """
```

The package meets every rule above, and `tests/unit/test_docstrings.py` allows no exception: document what you write.

## Code Review Criteria

Pull requests will be evaluated on:

- **Functionality**: Does it solve the stated problem?
- **Tests**: Are the changes covered, with the coverage floors intact?
- **Documentation**: Are the guides updated and the reference pages regenerated?
- **Code quality**: Do all gates pass, ring boundaries included?
- **Compatibility**: Exit codes, the `--output json` envelope, and diagnostic codes are contracts that CI pipelines depend on; changing what one means is a breaking change
- **Performance**: No significant performance regressions

## What We're Looking For

**Priority areas for contribution:**
- Bug fixes with reproduction steps
- Clearer diagnostics and suggestions
- New validation rules, each with its own diagnostic code
- Documentation improvements
- Performance optimizations
- Test coverage improvements

**Not currently accepting:**
- Major architectural changes (discuss first)
- Breaking changes (discuss first)
- Features without clear use cases

## Maintaining 0.3.x

SST 1.0 is a new major version. SST 0.3 lives at the tag `v0.3.1`. Maintenance fixes for 0.3.x are made on a branch cut from that tag (for example `release/0.3.x`), released as 0.3.x patch versions, and never merged into 1.0.

## Questions?

- Open an issue for questions
- Mention a maintainer from [CODEOWNERS](CODEOWNERS) for urgent matters
- Check the [documentation](docs/index.md)
- Review existing issues and pull requests

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.

---

Thank you for helping improve Snowflake Semantic Tools!
