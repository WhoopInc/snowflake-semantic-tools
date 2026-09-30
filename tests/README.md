# Test Suite for Snowflake Semantic Tools

The whole suite runs offline. Nothing connects to Snowflake: the Snowflake port is replaced by real in-memory implementations, and the reference project compiles from a vendored dbt manifest instead of running `dbt parse`.

## Running Tests

```bash
poetry run pytest tests/                           # everything
poetry run pytest tests/unit/domain -q             # one ring
poetry run pytest tests/unit/test_golden_ddl.py -q # one file
poetry run pytest tests/ -q -k migrate_refs        # by name
```

Run with the dev dependencies installed (`poetry install`): `test_ring_boundaries.py` calls the `lint-imports` script beside the running interpreter. The coverage floors and the other CI gates are in [CONTRIBUTING.md](../CONTRIBUTING.md#running-the-gates). There are no pytest markers; `pytest tests/` runs every test.

## Structure

```text
tests/
├── unit/
│   ├── domain/                  # the pure ring (coverage floor 100%)
│   ├── app/                     # use cases over in-memory ports (conftest.py, helpers.py)
│   ├── adapters/                # YAML loading, dbt manifest, connector, files, config, eval state
│   ├── test_cli_*.py            # commands through click's CliRunner
│   ├── test_golden_*.py         # rendered DDL, agents, tools, and evals against the goldens
│   ├── test_ring_boundaries.py  # each import-linter contract rejects a crossing
│   ├── test_dialect_compat.py   # the 0.3 dialect corpus and `sst migrate refs`
│   ├── test_public_docs.py      # links, examples, and planning identifiers in the docs
│   ├── test_release_hygiene.py  # nothing environment-specific in committed files
│   └── test_*.py                # compile use case, manifest, renderer
├── contract/                    # adapters against their ports: offline Snowflake ports, connector helpers, file stores, profile
├── fixtures/
│   ├── reference_project/                # a dbt + SST project that uses every artifact type
│   ├── reference_project_manifest.json   # its dbt manifest, so it compiles offline
│   └── v1_dialect/                       # the 0.3 dialect corpus for `sst migrate refs`
├── golden/
│   ├── README.md
│   └── expected/{ddl,agent,tool,eval,skill,plugin,profile}/
└── helpers/                     # scripts that run `sst` against a recorded Snowflake observation
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

- The fixture sets `validation.strict: true`, and its one warning (`SST-VAL528`) is deliberate, so a clean offline run passes `--no-strict`.
- A relative `--golden-dir` resolves against `--project-dir`, so pass an absolute path.
- `sst compile` writes `target/sst/` inside the fixture; git ignores it.

`fixtures/v1_dialect/` is written in the 0.3 dialect. `expected/converted/` is what `sst migrate refs --write` must turn it into; once its 0.3 key spellings are renamed as well, it renders `expected/ddl/`. Run `--write` only on a copy.

## Goldens

`golden/expected/` holds the exact expected output of the reference project under `dev`, one directory per artifact type. The `test_golden_*.py` tests and `sst test --suite golden` compare bytes. A DDL golden starts with a `--` provenance header that is not compared; see [golden/README.md](golden/README.md).

There is no update switch. When a change to rendered output is intended, run the golden suite to see the diff, edit the golden by hand (keeping a DDL golden's header), and say why in the pull request. An unexplained golden change is a regression.

## Writing Tests

- Put a test beside the ring it exercises: `unit/domain`, `unit/app`, `unit/adapters`, or a `test_cli_*.py` file for command behavior.
- Domain tests stay pure; `unit/domain/conftest.py` refuses network access.
- Application tests use real in-memory ports (`unit/app/conftest.py`, `snowflake_semantic_tools/adapters/snowflake/memory.py`), not mocks. There is no mocking library; use pytest's `monkeypatch` for the rest.
- CLI tests run the reference project through `CliRunner` with `--manifest`, or build a small project in `tmp_path`.
- A bug fix starts with a test that fails without the fix.
- Guard any path or collection a test depends on, as `test_fixture_and_goldens_are_present` does, so a wrong path cannot pass vacuously.
- A new diagnostic, config key, or CLI option also changes a generated page: run `poetry run sst docs`.
