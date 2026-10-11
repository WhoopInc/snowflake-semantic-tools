# Test Suite for Snowflake Semantic Tools

The whole suite runs offline. Nothing connects to Snowflake: the Snowflake port is replaced by real in-memory implementations, and the reference project compiles from a vendored dbt manifest instead of running `dbt parse`.

## Running Tests

```bash
poetry run pytest tests/ -n auto                   # everything offline, in parallel
poetry run pytest tests/unit/domain -q             # one ring
poetry run pytest tests/unit/test_golden_ddl.py -q # one file
poetry run pytest tests/ -q -k migrate_refs        # by name
```

Run with the dev dependencies installed (`poetry install`): `test_ring_boundaries.py` calls the `lint-imports` script beside the running interpreter. The coverage floors and the other CI gates are in [CONTRIBUTING.md](../CONTRIBUTING.md#running-the-gates). Two markers: `live` tests connect to Snowflake and are deselected by default (`-m live` runs them; see [CONTRIBUTING.md](../CONTRIBUTING.md#testing)), and `e2e` tests drive the `sst` CLI in a subprocess over a whole project. Every other test runs offline under `pytest tests/`.

## Structure

```text
tests/
├── unit/
│   ├── domain/                  # the pure ring (coverage floor 100%)
│   ├── app/                     # use cases over in-memory ports
│   ├── adapters/                # YAML loading, dbt manifest, connector, files, config, eval state
│   ├── cli/                     # commands through click's CliRunner, one module per command
│   ├── security/                # statement binding, as the connector formats it, offline
│   ├── test_golden_*.py         # rendered DDL, agents, tools, evals, and enrich against the goldens
│   ├── test_ring_boundaries.py  # each import-linter contract rejects a crossing
│   ├── test_structure.py        # size and complexity budgets, with no exceptions
│   ├── test_docstrings.py       # docstring rules, with no exceptions
│   ├── test_import_style.py     # absolute imports; tests share code only through tests.helpers
│   ├── test_dialect_compat.py   # the 0.3 dialect corpus and `sst migrate refs`
│   ├── test_public_docs.py      # links, examples, and planning identifiers in the docs
│   ├── test_release_hygiene.py  # nothing environment-specific in committed files
│   ├── test_network_guard.py, test_environment_isolation.py  # what offline tests cannot reach
│   ├── test_live_config.py, test_live_tooling.py, test_ci_tools.py  # live and CI tooling, offline
│   └── test_*.py                # compile use case, manifest, renderer
├── codes/                       # diagnostic-code guards: emitted, tested (README.md)
│   ├── allowlist/               # shrink-only allowlists
│   └── <area>/test_<code>.py    # a code's fires/silent pair
├── contract/                    # adapters against their ports: offline Snowflake ports, connector helpers, file stores, profile
├── contract_live/               # live: the real connector against the same port contract, in a scratch schema
├── integration/                 # live: apply in a real account; marker, metric query, re-plan, grants
├── e2e/                         # the CLI end to end: the offline negative corpus, and the live lifecycle through smoke
├── fixtures/
│   ├── reference_project/                # a dbt + SST project that uses every artifact type
│   ├── reference_project_manifest.json   # its dbt manifest, so it compiles offline
│   ├── scoped_views/                     # two include- and exclude-mode views, copied into a project copy
│   ├── v1_dialect/                       # the 0.3 dialect corpus for `sst migrate refs`
│   └── negative_overlays/                # one bad file per case, laid over the reference project; expected.json
├── golden/
│   ├── README.md
│   └── expected/{ddl,agent,tool,eval,skill,plugin,profile,enrich}/
└── helpers/                     # shared test support, imported as tests.helpers.<module>:
                                 #   snowflake_fake/ -- the one in-memory Snowflake: every port, and the driver
                                 #   app_ports.py, clocks.py -- the in-memory state store and the clock
                                 #   eval_state_store.py, golden_store.py -- more ports
                                 #   project_inputs.py, projects.py, manifests.py -- project inputs and manifests
                                 #   artifact_builders.py, compile_builders.py, eval_builders.py -- test values
                                 #   cli_projects.py -- the reference fixture, project copies, CLI invocations
                                 #   code_metrics.py, structure_rules.py, docstring_rules.py, import_rules.py -- the gates
                                 #   code_guards.py -- the diagnostic-code guards and the catalog projection
                                 #   coverage_ratchet.py -- the per-layer coverage ratchet CI runs
                                 #   live_config.py, live_snowflake.py, live_project.py -- the live account and scratch schemas
                                 #   sweep_scratch.py, session_recorder.py -- live scripts the manual workflow runs
                                 #   run_recorded_*.py -- run `sst` against a recorded Snowflake observation
```

## The Reference Project

`fixtures/reference_project/` publishes 14 artifacts: three semantic views, a tool, three agents, an eval, three skills, a plugin, and two CoCo Desktop profiles. Its default `dev` target is the golden target; `dev`, `prod`, and `ci` carry no credentials and cannot connect. From the repository root:

```bash
P=tests/fixtures/reference_project
M=tests/fixtures/reference_project_manifest.json
poetry run sst validate --project-dir $P --manifest $M --no-strict --no-snowflake-syntax-check
poetry run sst compile --project-dir $P --manifest $M --emit-ddl "$(mktemp -d)"
poetry run sst test --suite golden --project-dir $P --manifest $M --golden-dir "$PWD/tests/golden/expected/ddl"
```

- The fixture sets `validation.strict: true` and carries deliberate warnings (`SST-VAL528`, `SST-RND010`, `SST-RND013`, `SST-CFG018`), so a clean offline run passes `--no-strict`, which itself reports `SST-CFG034` for disagreeing with the config.
- The fixture sets `validation.snowflake_syntax_check: true`, so an offline `validate` passes `--no-snowflake-syntax-check`.
- A relative `--golden-dir` resolves against `--project-dir`, so pass an absolute path.
- `sst compile` writes `target/sst/` inside the fixture; git ignores it.

`fixtures/v1_dialect/` is written in the 0.3 dialect. `expected/converted/` is what `sst migrate refs --write` must turn it into; once its 0.3 key spellings are renamed as well, it renders `expected/ddl/`. Run `--write` only on a copy.

## Goldens

`golden/expected/` holds the exact expected output of the reference project under `dev`, one directory per artifact type, and two DDL goldens for the views in `fixtures/scoped_views/`. The `test_golden_*.py` tests and `sst test --suite golden` compare the text exactly, after trailing whitespace and the commit in a `GIT_<commit>` name are normalised. A DDL golden starts with a `--` provenance header that is not compared; see [golden/README.md](golden/README.md).

When a change to rendered output is intended, run the golden suite to see the diff, then rewrite the goldens with the same command plus `--update-golden` (refused when `$CI` is set; a DDL golden keeps its header), review the diff, and say why in the pull request. The scoped-view goldens are rewritten the same way from a copy of the reference project with `fixtures/scoped_views/semantic_views.yml` at `semantic_models/semantic_views/scoped/`. Never edit a golden by hand, except the `enrich` goldens, which `sst test` does not write; `unit/test_golden_enrich.py` shows their diff. An unexplained golden change is a regression.

## Coverage and the code guards

CI holds each layer to a coverage floor and to the ratchet in `coverage_baseline.json`, whose line and branch numbers per layer may rise but never fall; `python -m tests.helpers.coverage_ratchet raise --coverage-json coverage.json` locks in a rise. [CONTRIBUTING.md](../CONTRIBUTING.md#running-the-gates) gives the commands. `codes/` requires every registered diagnostic code to be raised and tested both ways; its allowlists are empty, and only shrink ([codes/README.md](codes/README.md)).

## Live Tests

Tests marked `live` connect to a Snowflake account that only configuration names: `SST_TEST_SNOWFLAKE_*`, the git-ignored `live.local.env` (template: `live.example.env`), or a named connection. `SST_TEST_RUN_ID` names the run in its scratch schemas. [docs/guides/testing-live.md](../docs/guides/testing-live.md) explains how to configure and run them, and what each workflow under `.github/workflows/` runs.

## Writing Tests

- Put a test beside the ring it exercises: `unit/domain`, `unit/app`, `unit/adapters`, or `unit/cli/test_<command>.py` for command behavior.
- No test outside the `live` marker reaches the network: `conftest.py` installs `tests/helpers/network_guard.py` for the session, which lets only Unix sockets and loopback through. Domain tests are stricter still; `unit/domain/conftest.py` refuses loopback too.
- Code that more than one test module uses goes in `tests/helpers/`, imported as `tests.helpers.<module>`. Never import a conftest or another test module: pytest has already imported it under a name of its own, so the import loads a second copy.
- Application tests use real in-memory ports (`tests/helpers/snowflake_fake/`, `tests/helpers/app_ports.py`), not mocks. There is no mocking library; use pytest's `monkeypatch` for the rest. Test doubles live under `tests/`, never in the package, so they do not ship.
- CLI tests run the reference project through `CliRunner` with `--manifest`, or build a small project in `tmp_path`.
- A bug fix starts with a test that fails without the fix.
- Guard any path or collection a test depends on, as `test_fixture_and_goldens_are_present` does, so a wrong path cannot pass vacuously.
- A new diagnostic, config key, or CLI option also changes a generated page: run `poetry run sst docs`.

## Structure, docstring, and import gates

`test_structure.py` and `test_docstrings.py` hold all package code to the size budgets and docstring rules in [CONTRIBUTING.md](../CONTRIBUTING.md#docstrings-and-comments), and `test_import_style.py` holds the package and the tests to the import rules under Code Style there. The package meets them today and the gates allow no exception, so a new module or function that breaks one fails the suite with its location and measure.
