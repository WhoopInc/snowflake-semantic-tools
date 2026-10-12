# Testing SST

The test suite has two halves. The offline suite, with a `dbt parse` of the reference project
that needs no warehouse, is the whole required gate: it runs on every pull request, needs no
account and no secrets, and the suite cannot reach the network. The live suite is
optional: it runs the real Snowflake connector, `sst apply`, and the CLI end to end against an
account you configure, and only maintainers with an account of their own need it.

This page is about running the suite. [CONTRIBUTING.md](../../CONTRIBUTING.md#running-the-gates)
lists every gate, and [tests/README.md](../../tests/README.md) describes the reference project and
the goldens.

## What CI runs

| Workflow | When | What |
|---|---|---|
| `test-and-lint.yml` | every pull request (the `pull_request` trigger only), each push to `main`, and every release | The required gate, offline and with no secrets: the exit-code conformance tests, `format --check` on the reference project, the whole offline suite with coverage, the per-layer coverage floors and ratchet, strict mypy, ruff, import-linter, `sst docs --check`, bandit, pip-audit, and semgrep; and, in its `dbt` job, a real `dbt parse` of the reference project with dbt 1.11 and with 1.12, the golden suite over what it wrote, and the dbt manifest contract test on that manifest; and, in its `wheel` job, the built wheel installed alone into a venv outside the checkout, running validate, compile and the golden suite on a copy of the reference project. |
| `nightly.yml` | 02:00 UTC daily, or by hand | Mutation testing over `domain/` and the suite under the deeper `nightly` Hypothesis profile. Offline and report-only: it blocks nothing. |
| `slow.yml` | by hand only | The checks that connect to Snowflake; see [the manual workflow](#the-manual-workflow). No pull request and no workflow waits on it; a maintainer runs it [before each release](#when-the-live-suites-run). |
| `publish-to-pypi.yml` | a `v*.*.*` tag | Runs `test-and-lint.yml` itself, checks the tag matches `pyproject.toml`, builds, and publishes through PyPI's trusted publishing; the publish job holds the only token and runs no project code. |

The coverage floors are measured per layer, as line and branch coverage together: `domain` 100%
and `app` 95% on their own tests, `cli` 90% on `tests/unit/cli`, and `adapters` 90% over the
whole suite. On top
of them, `python -m tests.helpers.coverage_ratchet check --coverage-json coverage.json` holds
each layer's line and branch coverage over the whole suite to `tests/coverage_baseline.json`
(each side floored to one decimal place): a number may rise, never fall. `coverage_ratchet
raise` locks a rise in; lowering a number is an edit to the baseline file, in review.

## Offline tests

```bash
poetry run pytest tests/ -n auto
```

`pyproject.toml` deselects the `live` marker by default, so this runs everything else. Every
offline test:

- runs against the reference project in `tests/fixtures/reference_project/`, whose `dev`, `prod`
  and `ci` targets name a placeholder account (`sst_reference`) and databases (`SST_REF_DEV`,
  ...) and carry no credential, so they cannot connect;
- reads Snowflake through the fakes and the recorded session in `tests/helpers/`, never through a
  connection;
- runs with an empty home directory, none of the `SST_*` variables `sst` reads, no
  `SST_TEST_SNOWFLAKE_*` variable and no local live configuration, so nothing on your machine
  changes what it sees;
- fails if it opens a network connection, even one the code under test catches
  (`tests/helpers/network_guard.py`);
- fails if a thread it started is still running after it returns, and fails rather than hangs
  once it has run for 120 seconds (`pytest-timeout`).

Property tests run under the Hypothesis profile `HYPOTHESIS_PROFILE` names: `dev` (the default,
random), `ci` (deterministic, so a failure reproduces on rerun), or `nightly` (deeper).

A change to rendered output is compared byte for byte with the goldens under
`tests/golden/expected/`; when the change is intended, rewrite them with the engine itself and
review the diff:

```bash
P=tests/fixtures/reference_project
M=tests/fixtures/reference_project_manifest.json
poetry run sst test --suite golden --update-golden --project-dir $P --manifest $M \
  --golden-dir "$PWD/tests/golden/expected/ddl"
```

`--update-golden` is refused whenever `$CI` is set. The `enrich` goldens are the exception to
the rule: `sst test` does not write them; see [tests/golden/README.md](../../tests/golden/README.md).

## Live tests

Tests marked `live` connect. They live in `tests/contract_live/` (the real connector against the
port contract the fakes satisfy), `tests/integration/` (apply in a real account) and `tests/e2e/`
(the CLI from `validate` to the smoke suite).

### Configure an account

Nothing in the repository names an account: every value comes from configuration, in this order,
highest first.

1. **The environment**: `SST_TEST_SNOWFLAKE_<KEY>` variables.
2. **A local file**: `tests/live.local.env`, which git ignores. Start from the committed template:

   ```bash
   cp tests/live.example.env tests/live.local.env
   ```

   Each line is `SST_TEST_SNOWFLAKE_<KEY>=value`; blank lines and `#` comments are skipped, an
   `export ` prefix and quotes are allowed, and a line for an unknown key is refused. The file is
   also shell, so `set -a; . tests/live.local.env; set +a` exports the same values for dbt and
   for the reference project's `verify` target.
3. **A named connection**: with `SST_TEST_SNOWFLAKE_CONNECTION=<name>`, the tests read that
   connection from the Snowflake driver's own files under `$SNOWFLAKE_HOME` (default
   `~/.snowflake`): `[<name>]` in `connections.toml`, else `[connections.<name>]` in `config.toml`.
   It supplies the account, user, role, warehouse, database, authenticator and private key file
   (`private_key_file` or `private_key_path`) when the sources above leave them unset, ignores a
   `schema` it holds, and passes its other driver settings, such as
   `client_store_temporary_credential`, to the test process's connection as they are.

| Key (`SST_TEST_SNOWFLAKE_` + ) | Required | Meaning |
|---|---|---|
| `ACCOUNT` | yes | Account identifier, such as `myorg-myaccount`. |
| `USER` | yes | The user the tests sign in as. |
| `ROLE` | yes | The role they run with; see [privileges](#privileges). |
| `WAREHOUSE` | yes | The warehouse their statements run in. |
| `DATABASE` | yes | The database the live layers create their scratch schemas in. |
| `SCHEMA` | evals, `verify` | The reference project's own schema on the `verify` target: its dbt models and SST's state table. The live layers never write to it. |
| `PRIVATE_KEY_PATH` | one of three | An unencrypted PKCS#8 private key; signs in by key pair. A leading `~` is expanded. |
| `AUTHENTICATOR` | one of three | Signs in without a key, such as `externalbrowser` (SSO) or `oauth_authorization_code`. Ignored when a key path is set. |
| `CONNECTION` | one of three | A named connection, read as above. |
| `GRANTEE_ROLE` | no | A role the test role may grant `SELECT` to; adds the check that replacing a view keeps its grants. A test-harness grant only; see [privileges](#privileges). |

The first five must resolve from some source, and so must one way to sign in: a key path (set
directly or as the connection's `private_key_file`) or an authenticator. A password is never
used. A configuration that is started but incomplete fails every live test with the keys that
are missing named; one that is absent skips them.

Key pair is the most predictable way to run the suite. The integration and end-to-end layers run
`sst` in subprocesses that sign in through a generated dbt profile, and a profile carries only the
authenticator, not a named connection's other driver settings: with a browser-based
authenticator each subprocess signs in on its own, which can open a browser per `sst` command
unless the driver finds a cached credential.

### Run them

```bash
poetry run pytest -m live tests/contract_live tests/integration tests/e2e
```

- Without any configuration the tests are skipped with a message naming what to set.
- They run only when selected with `-m live`: a run that clears `addopts`, or names a live file
  without the marker, skips them even when an account is configured.
- `--require-snowflake` fails them instead, so a gate that could not connect is never green.
- `-n 4` runs them in parallel; each worker works in schemas of its own.
- `SST_TEST_RUN_ID` names the run in its schemas, so a teardown can drop exactly them
  (`sweep_scratch --run`); without it, a run is named after its start time and process.
- `pytest -m live --collect-only -q` lists them without connecting.

### What they write, and where

Every live run creates its own schemas in `DATABASE`, named
`SST_IT_<UTC timestamp>_<run>_<worker><label>` and commented
`sst test suite scratch schema, dropped by its run or by the sweep`, and drops them when the
session ends. Everything the tests create -- tables, semantic views, SST's state table -- is
inside those schemas, and the `sst` runs they start publish only there, because the generated
profile targets the scratch schema. The helpers enforce it:

- `scratch_scope` is the one way a live helper names a schema to write to, and it refuses any
  name without the `SST_IT_` prefix, and the configured `SCHEMA` itself;
- creating or dropping a schema whose name does not parse as a scratch schema is refused before
  any statement is sent;
- `python -m tests.helpers.sweep_scratch --older-than-hours 6` (or `--run <id>`) drops only
  schemas with both the prefix and the marker comment, so a schema anything else created is out
  of reach. `--dry-run` reports what it would drop.

`python -m tests.helpers.session_recorder --out capture.json` publishes the small live project in
the schema `SST_IT_RECORDING`, which it replaces and drops, captures what Snowflake shows of it
with every per-run value replaced by a placeholder, and compares the capture with the committed
recording, `tests/fixtures/recordings/live_project.json` (`--recording` names another). It writes
the capture to `--out` every time. No recording is committed until a first capture: when there is
none, the capture is written there as well, creating it, and the script exits `0`; review it and
commit it. After that, it exits `1` on a difference.

### Privileges

The test role needs:

| On | Privilege | For |
|---|---|---|
| the warehouse | `USAGE` | every statement |
| `DATABASE` | `USAGE`, `CREATE SCHEMA` | the scratch schemas, which the role then owns and drops |
| `GRANTEE_ROLE` (optional) | the right to grant `SELECT` on objects the test role owns | the grant-preservation check |

Nothing account-wide is needed.

The `GRANTEE_ROLE` grant is the test harness's own. Only the `live` suite reads the variable, and
only `tests/integration/test_apply_live.py` grants to it: `GRANT SELECT` on a view in its own
scratch schema, so that it can check that `sst apply` keeps the grant when it replaces the view.
SST itself never reads `GRANTEE_ROLE` and never issues a grant of its own; the only grants apply
runs are the ones it replays, for an object that cannot copy its grants, that the object already
held before the replace. The offline gate and every other job run without it.

For the evals suite and the `verify` target, the role also needs to use Cortex agents in your
account, and to create the reference project's objects where its `sst_config.yml` routes them:

- the dbt models and SST's state table in `DATABASE.SCHEMA`;
- semantic views, the search service, agents, evals and skills in `DATABASE.JAFFLE`,
  `DATABASE.CORE` and `DATABASE.OPS`.

Apply creates none of those schemas (a missing one is `SST-PLN010`), and SST never creates a tag
object, so the schemas and the tags under `tags:` must already exist. The `apply.agent_spec_stage`
stage need not: apply creates it when it is absent, which needs `CREATE STAGE` on its schema
(`SST-PLN008` otherwise).

The evals suite is `sst test --suite evals --target verify` on the reference project. Like the
smoke suite, it reads the manifest `sst compile` wrote for that target (`SST-MAN001` without one)
and runs against the agents already published there, and it holds the target's run lock while it
runs. So the order is: build the dbt models in `verify`, compile and apply against the manifest
that build wrote, then run the suite; the committed `tests/fixtures/reference_project_manifest.json`
is dbt's for `dev`, and its refs name `SST_REF_DEV`. Apply with `--no-strict`: the fixture keeps
warnings on purpose, and a connected check reports each object it references but does not
publish as `SST-VAL531` when it is absent, which the fixture's `diagnostics.severity_overrides`
demotes to a warning. Those `verify` relations name stand-ins under
`SST_REF_VERIFY`, because a relation is a literal name and cannot follow `DATABASE`; no eval
question reaches them. The fixture's
`verify` target signs in with `SST_TEST_SNOWFLAKE_AUTHENTICATOR` (default `externalbrowser`) and
reads no key path; to sign in by key pair, point `--profiles-dir` at a profile of your own, as the
manual workflow does.

## The manual workflow

`.github/workflows/slow.yml` runs the same checks in GitHub Actions, in this repository or a
fork. Nothing triggers it but a manual run (Actions, "Snowflake checks (manual)", Run workflow),
and nothing waits on it. Pick a suite:

| Suite | Runs |
|---|---|
| `live` | `pytest -m live --require-snowflake -n 4` over the three live layers, then drops the run's scratch schemas even when a test failed |
| `sweep` | `sweep_scratch --older-than-hours` with the `older_than_hours` input (default `6`), for schemas a cancelled job left |
| `recording` | `session_recorder`, uploading the capture as an artifact either way; the first capture passes and is the one to commit |
| `evals` | on the reference project's `verify` target, with a key-pair profile written for the run: `dbt build`, `sst compile`, `sst apply --no-strict`, then `sst test --suite evals`; uploads `evals.json`. What it publishes stays in place for the next run. |

A second run of the same suite on the same ref cancels the first. Supply these repository
secrets; the workflow maps them to `SST_TEST_SNOWFLAKE_*`, sets `SST_TEST_RUN_ID` to the
workflow run, and signs in by key pair:

| Secret | Value |
|--------|-------|
| `SNOWFLAKE_ACCOUNT` | account identifier |
| `SNOWFLAKE_USER` | a key-pair user |
| `SNOWFLAKE_PRIVATE_KEY` | that user's unencrypted PKCS#8 private key, as PEM text |
| `SNOWFLAKE_ROLE` | a role with the [privileges](#privileges) above |
| `SNOWFLAKE_WAREHOUSE` | the warehouse the tests run in |
| `SNOWFLAKE_DATABASE` | the database the scratch schemas are created in |
| `SNOWFLAKE_SCHEMA` | the reference project's schema (`evals` only) |
| `SNOWFLAKE_GRANTEE_ROLE` | optional, `live` only; enables the grant-preservation check, a test-harness grant |

Without the secrets the run skips with a notice and succeeds. The key is written outside the
checkout, readable only by the runner user, and removed by the last step.

## When the live suites run

At release time, and otherwise whenever a maintainer chooses. The offline gate proves SST still
relies on the Snowflake behaviours it was written against; only a live run proves Snowflake still
has them, so the release checklist in [CONTRIBUTING.md](../../CONTRIBUTING.md#releasing) runs the
`live` suite and the `recording` suite before every tag, and commits the fresh capture as the
recording. The committed recording is therefore what Snowflake showed at the last release, and a
later capture that differs names what moved since.

Nothing runs them on a schedule, and no workflow waits on them: a run needs an account's
secrets, which a fork or a contributor without an account does not have. `evals`, which publishes
lasting objects and spends warehouse time, and `sweep` stay manual, run when they are needed.
