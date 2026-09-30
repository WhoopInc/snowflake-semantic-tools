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
poetry run pre-commit install   # optional: black, isort, and file checks on every commit

poetry run sst --version
```

### Running the Gates

These are the checks `.github/workflows/test-and-lint.yml` runs on every pull request:

```bash
poetry run pytest tests/

# Branch-coverage floors, each measured on its own test paths
poetry run pytest -q --cov=snowflake_semantic_tools.domain --cov-branch --cov-fail-under=100 \
  tests/unit/domain tests/unit/test_render_semantic_view.py
poetry run pytest -q --cov=snowflake_semantic_tools.app --cov-branch --cov-fail-under=95 \
  tests/unit/app tests/unit/test_compile_use_case.py tests/unit/test_manifest_v1.py
poetry run pytest -q --cov=snowflake_semantic_tools.cli --cov-branch --cov-fail-under=90 \
  tests/unit/test_cli_v1.py tests/unit/test_cli_m2.py tests/unit/test_cli_m5.py

poetry run mypy snowflake_semantic_tools
poetry run black --check snowflake_semantic_tools/
poetry run isort --check snowflake_semantic_tools/
poetry run lint-imports         # ring boundaries
poetry run sst docs --check     # generated reference pages are current
```

[tests/README.md](tests/README.md) describes the suite, the reference project, and the goldens.

## Architecture

The CLI is the only interface: the package exports nothing but `__version__`, so new behavior is a command or an option. The package root holds `__init__.py`, `_version.py`, and four rings:

```text
snowflake_semantic_tools/
├── cli/        # click commands: the composition root that wires adapters into use cases
├── app/        # use cases: compile, validate, plan, apply, the test suites, migrate refs
├── adapters/   # the edges: YAML, the dbt manifest, the Snowflake connector, files, config, profile
└── domain/     # pure: model, render, resolve, plan, state, and the ports adapters implement
```

Imports run one way, `cli` → (`app` | `adapters`) → `domain`:

- `app` and `adapters` are independent: a use case receives adapters through the ports in `domain/ports/`, and an adapter never calls a use case.
- `domain` does no I/O and reads no clock, environment, or randomness, so rendering is a pure function of its input and goldens can compare bytes.
- `app` never imports an SDK or touches the terminal (`yaml`, `click`, and `snowflake` are forbidden there).
- `adapters` may use domain models and ports, but not `domain/render`.

`poetry run lint-imports` enforces these as four contracts in `pyproject.toml`, and `tests/unit/test_ring_boundaries.py` proves that each contract rejects a crossing and that the package root holds nothing else.

### Diagnostics

Every problem SST reports is a diagnostic registered in `snowflake_semantic_tools/domain/model/diagnostic.py`: a stable code (`SST-`, a three-letter subsystem, and a number, such as `SST-VAL116`), a severity, a message template, and a suggestion that says how to fix it. Build one with `D("SST-...", ...)`. A new problem gets a new code; codes are never reused, even after the diagnostic that used one is removed.

### Generated Reference Pages

`docs/reference/*.md` is rendered from the engine's registries: the diagnostics, the configuration schema (`snowflake_semantic_tools/domain/model/config_schema.py`, where every `sst_config.yml` key is declared), the CLI's commands and options, and the artifact registry (`snowflake_semantic_tools/domain/model/registry.py`). After changing any of them, run `poetry run sst docs` and commit the pages it rewrites; CI fails on `sst docs --check` otherwise.

### Version

`snowflake_semantic_tools/_version.py` holds the one version string, and it must equal `version` in `pyproject.toml`; a test asserts it. Change both together.

## Code Style

- **Formatting**: Black (line length 120)
- **Imports**: isort with black profile
- **Type hints**: mypy runs with `disallow_untyped_defs`, so every function is annotated
- **Error messages**: a diagnostic's suggestion is actionable — it says what is wrong and how to fix it
- **Tests**: real in-memory ports and pytest's `monkeypatch`, no mocking library

```bash
# Format code
poetry run black snowflake_semantic_tools/

# Sort imports
poetry run isort snowflake_semantic_tools/
```

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
