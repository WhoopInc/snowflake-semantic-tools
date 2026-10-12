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

- Python 3.11–3.13 (`pyproject.toml` declares `>=3.11,<3.14`; CI runs 3.11)
- Poetry 2 for dependency management (CI pins 2.2.1)
- Nothing else for the test suite: it needs no Snowflake account and no dbt install

Any interpreter in that range works. To use a conda environment, create it with the Python you
want (`conda create -n sst python=3.11`), activate it, and point Poetry at it with
`poetry env use "$(which python)"`; Poetry then builds its virtualenv from that interpreter.
Conda is optional: CI uses a plain Poetry virtualenv.

### Installation

```bash
git clone https://github.com/WhoopInc/snowflake-semantic-tools.git
cd snowflake-semantic-tools

poetry install                  # the package and the dev group; add --extras dbt to run sst against a dbt project of your own
poetry run pre-commit install   # optional: a fast subset of the gates on every commit (see below)

poetry run sst --version
```

### Changing dependencies

`poetry.lock` must match `pyproject.toml`. After adding, removing, or re-pinning a dependency in
`pyproject.toml`, regenerate the lock and commit both files together:

```bash
poetry lock
```

CI checks this first (`poetry check --lock`) and fails with that instruction when the lock is out of
date. A new dependency also needs a row, with its license, in [DEPENDENCIES.md](DEPENDENCIES.md).

### Running the Gates

These are the checks `.github/workflows/test-and-lint.yml` runs on every pull request. A release
tag runs the same workflow before it builds anything (`publish-to-pypi.yml` calls it). None of them
needs a secret or a Snowflake account. `.github/workflows/nightly.yml` adds mutation testing over
`domain/` and the property tests under the deeper `nightly` Hypothesis profile, both offline and
report-only.

In the order CI runs them:

```bash
# poetry.lock matches pyproject.toml
poetry check --lock

# The exit-code contract, reported as its own step
poetry run pytest tests/unit/cli/test_exit_codes.py

# The reference project's semantic-layer YAML is in canonical form (exit 2 when not)
poetry run sst --project-dir tests/fixtures/reference_project format --check

# The whole suite in parallel, with coverage over the whole of it, then the per-layer ratchet
# against tests/coverage_baseline.json: line and branch coverage per layer may rise, never fall
poetry run pytest tests/ -v -n auto --cov=snowflake_semantic_tools --cov-branch \
  --cov-report=term --cov-report=json:coverage.json
poetry run python -m tests.helpers.coverage_ratchet check --coverage-json coverage.json
poetry run coverage report --include='snowflake_semantic_tools/adapters/*' --fail-under=90 --skip-covered

# Branch-coverage floors, each measured serially on its own test paths
poetry run pytest -q --cov=snowflake_semantic_tools.domain --cov-branch --cov-report=term --cov-fail-under=100 \
  tests/unit/domain tests/unit/test_render_semantic_view.py
poetry run pytest -q --cov=snowflake_semantic_tools.app --cov-branch --cov-report=term --cov-fail-under=95 \
  tests/unit/app tests/unit/test_compile_use_case.py tests/unit/test_manifest_v1.py
poetry run pytest -q --cov=snowflake_semantic_tools.cli --cov-branch --cov-report=term --cov-fail-under=90 \
  tests/unit/cli

poetry run mypy snowflake_semantic_tools tests   # strict = true in pyproject.toml
poetry run ruff format --check snowflake_semantic_tools/ tests/
poetry run ruff check snowflake_semantic_tools/ tests/
poetry run lint-imports         # the seven ring-boundary contracts
poetry run sst docs --check     # generated reference pages are current (exit 2 when not)
poetry run bandit -r snowflake_semantic_tools -ll
poetry run pip-audit --skip-editable --progress-spinner off   # needs to reach the vulnerability database

# semgrep runs in its own job, installed outside the project's environment from a hash-locked
# requirements file, with the rules vendored in .semgrep/ (its README says how to refresh them)
pip install --require-hashes --only-binary :all: -r .github/requirements/semgrep.txt  # linux x86_64
semgrep scan --config .semgrep/ --error --metrics off --disable-version-check snowflake_semantic_tools
```

The ratchet and the three floors are separate checks: the ratchet compares each layer over the
whole suite with its recorded baseline, and each floor measures one layer from its own tests
only, so a layer cannot borrow coverage from tests written for another.

The requirements file pins wheels for the CI runner (linux x86_64, CPython 3.11); elsewhere,
`pip install semgrep==1.179.0` gives the same scanner.

A semgrep false positive is silenced on its line with `# nosemgrep: <rule-id>` and a comment
saying why.

`.pre-commit-config.yaml` runs a fast subset on each commit, from the project's own virtualenv
(every hook is `language: system`, so the tools are the versions `poetry.lock` pins): ruff format
`--check` and ruff check on the staged files, mypy, `lint-imports`, `sst docs --check`, and
`pytest -q -n auto tests/unit/domain tests/codes`, plus whitespace, end-of-file, YAML, and
large-file checks. It is not the gate: run the whole list above before you open a pull request.
`poetry run pre-commit run --all-files` runs every hook over the tree.

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
thread it started is still running two seconds after it returns.

### Testing

The offline suite is every test the gate requires: `poetry run pytest tests/ -n auto` runs it with no
account and no secrets, and no test in it can reach the network or read your own Snowflake or dbt
configuration. Goldens are rewritten by the golden suite with `--update-golden`, never by hand;
[tests/README.md](tests/README.md#goldens) gives the exact command, and a pull request that changes a
golden says why.

Tests marked `live` connect to a real account and are deselected by default. No contribution needs
them; they are for maintainers with an account of their own. Their account comes only from
configuration -- `SST_TEST_SNOWFLAKE_*` variables, a git-ignored `tests/live.local.env` (copy
`tests/live.example.env`), or a named connection in `~/.snowflake` -- and they sign in by key pair
or by an authenticator such as `externalbrowser`:

```bash
cp tests/live.example.env tests/live.local.env   # then fill in your own account
poetry run pytest -m live tests/contract_live tests/integration tests/e2e
```

Each run writes only in `SST_IT_<UTC timestamp>_<run>_<worker>` schemas it creates in the
configured database, and drops them when it ends. Without an account the tests skip with a message
naming what to set; with `--require-snowflake` they fail instead. The same checks run by hand in
GitHub Actions through `.github/workflows/slow.yml`, from repository secrets you supply.

[Testing SST](docs/guides/testing-live.md) explains every key, the privileges the test role needs,
the safety guarantees, and the manual workflow's secrets.

[tests/README.md](tests/README.md) describes the suite, the reference project, and the goldens.

### Benchmarking

`tests/helpers/bench.py` runs `sst` commands against a project, each as its own process
`--runs` times with `--output json`, and reports where their time went from the envelope's
`invocation.timings`: startup (the interpreter, imports, and the `load` phase, dbt included),
Snowflake (statements, their summed and wall-clock time, logins), the rest, and each phase the
engine names. It imports only the standard library and runs whichever `sst` is on `PATH`
(or `--sst`). Benchmark a change by running it before and after against the same scratch target:

```bash
python tests/helpers/bench.py --project-dir ../my-project --target dev --runs 3 \
  --commands compile,validate-offline,plan --out before.json
python tests/helpers/bench.py --project-dir ../my-project --target dev --runs 3 \
  --commands compile,validate-offline,plan --out after.json
python tests/helpers/bench.py --compare before.json after.json
python tests/helpers/bench.py --table after.json
```

The commands are `compile`, `validate-offline`, `validate` (with the Snowflake syntax check),
`plan`, `apply-noop` and `smoke`; `--append` adds to an existing results file. `apply-noop`
runs `sst apply --yes`, which publishes whatever the plan holds, so point it only at a scratch
target whose plan is already applied.

A run fails when its command exits with a status the benchmark does not expect, times out, prints
no envelope, or prints its envelope on stderr rather than stdout: that breaks the `--output json`
contract, so the run is left out of the medians and the table ends with a `CONTRACT BROKEN` line
naming the command. The benchmark exits 1 when any run failed.

## Architecture

Why the engine is shaped this way, with diagrams, is in [docs/design/](docs/design/README.md); start
with [architecture.md](docs/design/architecture.md). This section is the rules a change must keep.

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
├── ports/        # the protocols adapters implement; ports/snowflake/ holds one module per role
└── file_names.py # the one file name each artifact name maps to
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
- Only `adapters/yaml` (SST's own files), `adapters/dbt` (dbt's), and `adapters/bounded_yaml.py` (the depth-bounded loader both parse through) import `yaml`; everything else reaches YAML through them.
- Only `adapters/roundtrip.py` and the `yaml` and `dbt` adapters edit YAML in place with `ruamel`.

`poetry run lint-imports` enforces these as seven contracts in `pyproject.toml`, and `tests/unit/test_ring_boundaries.py` proves that each contract rejects a crossing and that the package root holds nothing else:

| Contract | Rule |
|----------|------|
| Rings are layered: cli above (app \| adapters) above domain | `cli` → (`app` \| `adapters`) → `domain`; `app` and `adapters` independent |
| domain is pure -- no I/O, no clock, no environment, no SDKs | `domain` imports none of `yaml`, `click`, `snowflake`, `os`, `pathlib`, `datetime`, `time`, `random`, `tempfile`, `subprocess`, `socket`, `logging` |
| app imports domain only -- no SDKs, no terminal | `app` imports none of `yaml`, `click`, `snowflake` |
| adapters use domain ports and models, not domain renderers | `adapters` never imports `domain.render` |
| adapter subpackages are independent | `adapters.yaml`, `.dbt`, `.snowflake`, `.fs` never import one another |
| only the roundtrip, dbt and yaml adapters edit YAML | nothing else imports `ruamel` |
| only the yaml and dbt adapters parse YAML | `cli` and the other adapters never import `yaml` directly |

### Diagnostics

Every problem SST reports is a diagnostic registered in `snowflake_semantic_tools/domain/diagnostics/specs/`, in the module for its code area (VAL is split by hundreds, `val_metric` for 1xx and so on; the package docstring lists the bands): a stable code (`SST-`, a three-letter subsystem, and a number, such as `SST-VAL116`), a severity, a message template, and a suggestion that says how to fix it. Build one with `D("SST-...", ...)`. A new problem gets a new code; codes are never reused, even after the diagnostic that used one is removed.

#### Adding a diagnostic code

1. **Register it.** Add a `spec(...)` entry to `SPECS` in the module for its area, kept sorted by code: the code, its `Severity`, a title, a message template, and an actionable suggestion (or `None`), plus `condition=` (when it is raised, which `sst explain` and the error reference print) and, where it helps, `note=`. Pass `demotable=False` for an error no setting may downgrade. Take the next free number; a number in `specs/retired.py` is burned. The registry is built through `domain/diagnostics/integrity.py` at import, which refuses a malformed code (SST-REG012), a duplicate (SST-REG002), a retired number (SST-REG017), a template placeholder outside the shared `PLACEHOLDERS` vocabulary (SST-REG016), a 900-band code that is not an error, or in INT and REG not a non-demotable one (SST-REG014), and `internal_detail` outside INT and SNO (SST-REG019).
2. **Raise it** with `D("SST-XXXnnn", subject=..., <placeholder>=...)` from a module outside `specs/`, and list it under `Diagnostics:` in the docstring of the function that reports it. `D` reports SST-INT901 instead when a template placeholder is not supplied.
3. **Say where it is first detectable.** A condition that needs no connection is `local`, the default. Pass `precheck=Precheck.OBSERVE` when it needs a read of Snowflake, or `precheck=Precheck.RUNTIME` when only a running statement shows it; the coverage reference prints it. The registry entry is the code's whole contract, and `sst docs` renders it, so agree the severity, demotability, title, template, and suggestion with a maintainer in the issue.
4. **Test it both ways.** Add `tests/codes/<area>/test_sst_<area><nnn>.py` with `test_sst_<area><nnn>_fires` (the smallest input that reports it, pinning severity, message, and subject) and `test_sst_<area><nnn>_silent` (the nearest legitimate input), copying `tests/codes/cfg/test_sst_cfg003.py`. `test_codes_tested.py` requires the pair and `test_codes_emitted.py` requires the code to be named outside `specs/`. The allowlists under `tests/codes/allowlist/` are empty and only shrink: never add an entry to make a guard pass.
5. **Regenerate the reference.** `poetry run sst docs` rewrites `docs/reference/error-codes.md` and `docs/reference/coverage.md`; commit them, then check `poetry run sst explain SST-XXXnnn` reads well.

#### Retiring a diagnostic code

A code whose condition can no longer arise, or that another code already reports, is retired, never deleted and reused. Remove its `spec(...)` entry and its fires/silent pair; add the code with the release to `RETIRED_CODES` in `specs/retired.py`; and add a `TOMBSTONES` row in `specs/aliases.py` with its old title, the code that took its condition over (or `None`), and why it was retired. The registry then refuses the number (SST-REG017), `sst explain` answers for it, and the error reference lists it under "Retired numbers".

### Generated Reference Pages

`docs/reference/*.md` is rendered from the engine's registries: the diagnostics, the configuration schema (`snowflake_semantic_tools/domain/model/config_schema/keys.py`, where every `sst_config.yml` key is declared), the CLI's commands and options, and the artifact registry (`snowflake_semantic_tools/domain/model/registry.py`). After changing any of them, run `poetry run sst docs` and commit the pages it rewrites; CI fails on `sst docs --check` otherwise.

### Adding a CLI option

Each command lives in its own module under `snowflake_semantic_tools/cli/commands/`. An option is declared with `click.option(...)` and **no `help=`**: once every command is registered, `cli/help_text.py`'s `document_options` gives each flag its one description from `_OPTION_HELP`, or from `_COMMAND_OPTION_HELP[("sst <command>", "--flag")]` where the flag means something narrower on one command, so `--help` and `docs/reference/cli.md` say the same thing everywhere. An option several commands share is a factory in `cli/options.py`; a global option belongs in `cli/globals.py`, and every command lists it under "Global options".

- Say in the help text what the default is and where else the value comes from, in the order it is resolved: the flag, then its `$SST_*` variable (`envvar=`), then the `sst_config.yml` key, then the default. A setting that has both a flag and a config key resolves in `cli/settings.py`; a new key is declared in `domain/model/config_schema/keys.py`.
- `tests/unit/cli/test_docs.py` fails for an option without help text. An exit code the option can produce must be one its command documents: `tests/unit/cli/test_exit_codes.py` holds each command's codes and a scenario per flag that changes them.
- A removed flag stays as a hidden option that refuses with what replaces it, as `cli/commands/enrich.py` does for the 0.3 flags.
- Run `poetry run sst docs` to regenerate `docs/reference/cli.md` (and `config.md` for a new key), and commit the pages.

### Exit codes

A pipeline branches on the exit code without reading the output, so each code has one meaning, declared in `cli/exit_codes.py` and rendered into the CLI reference:

| Code | Meaning |
|---:|---|
| 0 | The run succeeded; for `sst plan`, nothing to change. |
| 1 | The project was read and something in it is wrong: an error diagnostic, a failed apply, test suite, or check, a statement Snowflake rejected, a file SST could not write, or an internal error. |
| 2 | Declared and actual state differ: `plan`, `diff`, `enrich --check`, `format --check`, `docs --check`, or `migrate refs` found a difference. |
| 3 | The command line is refused. |
| 4 | The project, its configuration, or a saved plan cannot be used. |
| 5 | Snowflake would not cooperate: a failed login or dropped connection (SST-PRT001), a refused privilege, or a missed deadline. |
| 130 | The run was interrupted (Ctrl-C, SIGTERM, SIGHUP) or a confirmation was declined. |

- **2 reports a difference; it is not a failure.** A command that asserts never returns it: `validate` and `test` exit 1 when an assertion fails, a golden mismatch included. `--no-detailed-exitcode` turns 2 into 0 on `plan`, `diff`, `enrich`, `format`, and `docs`.
- **Usage errors exit 3, not click's default 2**, so 2 always means a difference. `cli/group.py` sets the exit code of click's usage errors to 3 and reports each as a diagnostic; a bad value read from an environment variable is a configuration error, exit 4, since the command line itself was fine.
- **1, 4, and 5 are split by how far the run got,** which tells the reader what to fix: 4, the setup (the run could not use the project, its configuration, or a saved plan); 1, the project; 5, the account (retry, or fix the grants). A manifest SST could not obtain is 4 even when `dbt parse` ran and failed (SST-DBT028): no model was read, and 5 is for Snowflake alone. `cli/runner.py` maps each exception to its code in one place, and `snowflake_failure` decides between 1 and 5 for a Snowflake error.
- **An interrupt is 130**, never an internal error, from any point in the run.

`tests/unit/cli/test_exit_codes.py` holds the contract: `EXIT_CODES` lists the codes each command may return, every case is a real invocation, and CI runs it as its own step. A new failure path lands in an existing code; changing what a code means is a breaking change.

### Building SQL

Every statement SST sends to Snowflake is an `Sql` value from `snowflake_semantic_tools.domain.sql`. `Sql` is sealed: constructing one outside that package raises `TypeError`. A statement is composed with `sql("<constant template>", name=<Sql part>, ...)`, where each part comes from a constructor that quotes or refuses its input (`ident`, `qname`, `literal`, `number`, `keyword` from a closed vocabulary, `datatype`, `dollar_quoted`, `guard_expression`, and the rest of the package's exports), and `join` takes a constant separator. The connector turns an `Sql` into text only at the driver boundary (`Sql.for_driver`).

`tests/unit/security/test_sql_construction.py` refuses, anywhere else in the package, an f-string, `%` format, `.format()`, `+`, or `join` whose constant text reads as SQL, a direct `Sql(...)` call, and an `sql()` or `join()` whose template is not a string constant; semgrep's vendored SQL-injection rules scan the same code. A new statement shape is a new template, never string concatenation.

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
6. **Comments say why, not what.** Mark ordering dependencies where they bite (for example, "runs after the view checks: poisoning reads their diagnostics"). Never cite a planning identifier or a document outside this repository; a comment may point to a page under `docs/design/`, as the code sites that follow Snowflake's server rather than its docs do.
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
