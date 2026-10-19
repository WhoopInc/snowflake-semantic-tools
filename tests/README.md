# Test Suite for Snowflake Semantic Tools

The whole suite runs offline. Nothing connects to Snowflake: the Snowflake port is replaced by real in-memory implementations, and the reference project compiles from a vendored dbt manifest instead of running `dbt parse`.

## Running Tests

```bash
poetry run pytest tests/ -n auto                   # everything offline, in parallel
poetry run pytest tests/unit/domain -q             # one ring
poetry run pytest tests/unit/test_golden_ddl.py -q # one file
poetry run pytest tests/ -q -k migrate_refs        # by name
poetry run pytest tests/contract/test_dbt_manifest_contract.py --dbt-manifest <manifest.json>  # also a manifest dbt just wrote
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
│   ├── test_goldens.py          # every artifact of the reference project against its golden
│   ├── test_golden_*.py         # rendered DDL, agents, tools, evals, and enrich against the goldens
│   ├── test_ring_boundaries.py  # each import-linter contract rejects a crossing
│   ├── test_structure.py        # size and complexity budgets, with no exceptions
│   ├── test_docstrings.py       # docstring rules, with no exceptions
│   ├── test_import_style.py     # absolute imports; tests share code only through tests.helpers
│   ├── test_dialect_compat.py   # the 0.3 dialect corpus and `sst migrate split` / `refs`
│   ├── test_migrate_round_trip.py  # the pre-split reference project migrated to the goldens
│   ├── test_public_docs.py      # links, examples, and planning identifiers in the docs
│   ├── test_release_hygiene.py  # nothing environment-specific in committed files
│   ├── test_network_guard.py, test_environment_isolation.py  # what offline tests cannot reach
│   ├── test_live_config.py, test_live_tooling.py, test_ci_tools.py  # live and CI tooling, offline
│   └── test_*.py                # compile use case, manifest, renderer
├── codes/                       # diagnostic-code guards: emitted, tested (README.md)
│   ├── allowlist/               # shrink-only allowlists
│   └── <area>/test_<code>.py    # a code's fires/silent pair
├── contract/                    # adapters against their ports: offline Snowflake ports, connector helpers, file stores, profile, the dbt manifest
├── contract_live/               # live: the real connector against the same port contract, in a scratch schema
├── integration/                 # live: apply in a real account; marker, metric query, re-plan, grants
├── e2e/                         # the CLI end to end: the offline negative corpus, and the live lifecycle through smoke
├── fixtures/
│   ├── reference_project/                # a dbt + SST project that uses every artifact type
│   ├── reference_project_manifest.json   # its dbt manifest, so it compiles offline
│   ├── dbt_manifests/                    # full manifests of it, one per schema version and dbt release
│   ├── scoped_views/                     # two include- and exclude-mode views, copied into a project copy
│   ├── v1_dialect/                       # the 0.3 dialect corpus for `sst migrate split` / `refs`
│   ├── pre_split_reference/              # the reference project's files before `sst migrate split` / `refs`
│   ├── ask/stream.sse                    # a Cortex Agents event stream, every event type once, as `sst ask` reads it
│   └── negative_overlays/                # one broken change per case, laid over the reference project; expected.json
├── golden/
│   ├── README.md
│   └── expected/{ddl,agent,tool,eval,skill,plugin,profile,enrich}/
└── helpers/                     # shared test support, imported as tests.helpers.<module>:
                                 #   snowflake_fake/ -- the one in-memory Snowflake: every port, and the driver
                                 #   app_ports.py, clocks.py -- the in-memory state store and the clock
                                 #   eval_state_store.py, golden_store.py -- more ports
                                 #   goldens.py, update_goldens.py -- golden comparison and rewrite
                                 #   project_inputs.py, projects.py, manifests.py -- project inputs and manifests
                                 #   manifest_capture.py -- scrubs a manifest `dbt parse` wrote into dbt_manifests/
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
poetry run pytest tests/unit/test_goldens.py   # every artifact against its golden
```

- The fixture sets `validation.strict: true` and carries deliberate warnings (`SST-VAL528`, `SST-RND010`, `SST-RND013`, `SST-CFG018`), so a clean offline run passes `--no-strict`, which itself reports `SST-CFG034` for disagreeing with the config.
- The fixture sets `validation.snowflake_syntax_check: true`, so an offline `validate` passes `--no-snowflake-syntax-check`.
- `sst compile` writes `target/sst/` inside the fixture; git ignores it.

The reference project is a conformance fixture, not a template to start a project from. It holds every artifact type and many of their options at once so one copy can exercise the whole engine, and it carries deliberate warnings, folder routes and stand-ins a real project would not. A change to it is a change to what the tests assert: the goldens, the negative overlays and the CLI tests all read it, so keep it as small as that coverage allows.

`fixtures/v1_dialect/` is written in the 0.3 dialect. `expected/converted/` is what `sst migrate split --write` then `sst migrate refs --write` must turn it into, one file per member; once its 0.3 key spellings are renamed as well, it renders `expected/ddl/`. Run `--write` only on a copy.

`fixtures/pre_split_reference/` holds the reference project's files as they were written before `sst migrate split` and `sst migrate refs`: member lists, tool groups, settings outside `config:`, bare references, and the old `skills:` blocks. `tests.helpers.reference_project.pre_split_copy` lays them over a copy of the reference project, and `test_migrate_round_trip.py` migrates that copy, in either order, to a project that renders the committed goldens.

## Goldens

`golden/expected/` holds the exact expected output of the reference project under `dev`, one directory per artifact type, and two DDL goldens for the views in `fixtures/scoped_views/`. `unit/test_goldens.py` compiles the reference project (from the vendored manifest, or with `SST_GOLDEN_PROJECT` naming a copy whose dbt packages are installed, with no manifest so SST runs `dbt parse`) and compares every artifact's payloads with its golden; it and the other `test_golden_*.py` tests compare the text exactly, after trailing whitespace and the commit in a `GIT_<commit>` name are normalised. A DDL golden starts with a `--` provenance header that is not compared; see [golden/README.md](golden/README.md).

When a change to rendered output is intended, run `poetry run pytest tests/unit/test_goldens.py` to see the diff, then rewrite the goldens from the repository root with `poetry run python -m tests.helpers.update_goldens` (refused when `$CI` is set to anything but empty, `0`, `false` or `no`; a DDL golden keeps its header), review the diff, and say why in the pull request. It rewrites the two scoped-view DDL goldens too, from a copy with `fixtures/scoped_views/semantic_views.yml` at `semantic_models/semantic_views/scoped/`, and prints a `wrote <path>` line per golden and `N golden file(s) written for 16 artifact(s)`. Never edit a golden by hand, except the `enrich` goldens, which `update_goldens` does not write; `unit/test_golden_enrich.py` shows their diff. An unexplained golden change is a regression.

## Coverage and the code guards

CI holds each layer to a coverage floor and to the ratchet in `coverage_baseline.json`, whose line and branch numbers per layer may rise but never fall; `python -m tests.helpers.coverage_ratchet raise --coverage-json coverage.json` locks in a rise. [CONTRIBUTING.md](../CONTRIBUTING.md#running-the-gates) gives the commands. `codes/` requires every registered diagnostic code to be raised and tested both ways; its allowlists are empty, and only shrink ([codes/README.md](codes/README.md)).

## Live Tests

Tests marked `live` connect to a Snowflake account that only configuration names: `SST_TEST_SNOWFLAKE_*`, the git-ignored `live.local.env` (template: `live.example.env`), or a named connection. `SST_TEST_RUN_ID` names the run in its scratch schemas. [docs/guides/testing-live.md](../docs/guides/testing-live.md) explains how to configure and run them, and what each workflow under `.github/workflows/` runs.

## What the tests promise

A test is worth what it cannot be fooled by. Each layer proves one kind of claim, and the rules on what it may not do are what make the claim hold.

- **Domain** (`unit/domain`): the pipeline's logic, as pure functions. The domain does no I/O and reads no clock, environment, or randomness (the ring contracts in [CONTRIBUTING.md](../CONTRIBUTING.md#architecture)), so its tests build values and compare results. `unit/domain/conftest.py` refuses every connection, loopback included. A failure here is a logic bug, never the machine it ran on.
- **Application** (`unit/app`): use cases over real in-memory ports, never mocks; the dev dependencies hold no mocking library. A fake is working code with state, so a test asserts what the use case left behind, not which calls it made, and a refactor that keeps the behaviour keeps the test green.
- **Contract** (`contract/`): the offline Snowflake doubles and the file adapters held to their ports' contracts. `contract_live/` holds the real connector to the same results; if the doubles promise something the connector does not, every application test that leans on them is suspect, and that layer is how it shows.
- **CLI** (`unit/cli`): commands through `CliRunner`, asserting each command's exit codes, its JSON envelope, and what it prints.
- **Live** (`contract_live/`, `integration/`, `e2e/test_live_lifecycle.py`): the only tests marked `live`, so the only ones that may connect, and only when `-m live` selects them and an account is configured. Without one they skip, or fail under `--require-snowflake`, so a gate that could not connect is never green. Every other test runs under the network guard, which fails a test even when the code under test swallowed the refusal.

Coverage is measured per layer because one global number lets a well-tested layer hide an untested one. The domain, application, and CLI floors are each measured from that layer's own tests, so `app` cannot pass on lines the CLI tests happened to run; the adapters floor and the ratchet read the whole suite's run, and the ratchet holds each layer's line and branch coverage to its baseline, which may rise and never fall. The domain, being pure, is held to every line and branch.

A failing test fails the run. The suite has no retry plugin, and what it does instead makes a flaky test visible rather than lucky: CI's Hypothesis profile is deterministic, so a property failure reproduces on rerun; a test that deadlocks fails at 120 seconds with every thread's stack; a test whose thread outlives it fails; and every offline test starts from an empty home directory and none of the `SST_*` variables `sst` reads.

Some claims cannot be proven offline. The goldens prove SST renders the intended text; only Snowflake can say it accepts it. The live layers prove, in scratch schemas:

- the real connector meets the port contract the doubles meet, including that a statement Snowflake refuses fails with text the signature table recognises, and that state round-trips and the run lock admits one run;
- `apply` publishes a view that carries its ownership marker and answers a metric query, and a plan after it has nothing left to do;
- replacing the view keeps the grants on it (when a grantee role is configured);
- the CLI sequence `validate` with the Snowflake syntax check, `plan`, `apply`, `plan` again, and the smoke suite each exits as documented.

Agent evaluation is the evals suite, run by the manual workflow against a published agent, not by pytest. [docs/guides/testing-live.md](../docs/guides/testing-live.md) says how to configure and run each of these.

Drawn from: [conftest.py](conftest.py), [unit/domain/conftest.py](unit/domain/conftest.py), [helpers/network_guard.py](helpers/network_guard.py), [helpers/coverage_ratchet.py](helpers/coverage_ratchet.py), [contract_live/test_connector_contract.py](contract_live/test_connector_contract.py), [integration/test_apply_live.py](integration/test_apply_live.py), [e2e/test_live_lifecycle.py](e2e/test_live_lifecycle.py), [pyproject.toml](../pyproject.toml), [test-and-lint.yml](../.github/workflows/test-and-lint.yml).

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
