# Testing SST

The test suite has two halves. The offline suite is the whole required gate: it runs on every
pull request, needs no account and no secrets, and cannot reach the network. The live suite is
optional: it runs the real Snowflake connector, `sst apply`, and the CLI end to end against an
account you configure, and only maintainers with an account of their own need it.

This page is about running the suite. [CONTRIBUTING.md](../../CONTRIBUTING.md#running-the-gates)
lists every gate, and [tests/README.md](../../tests/README.md) describes the reference project and
the goldens.

## Offline tests

```bash
poetry run pytest tests/ -n auto
```

`pyproject.toml` deselects the `live` marker by default, so this runs everything else. Every
offline test:

- runs against the reference project in `tests/fixtures/reference_project/`, whose `dev`, `prod`
  and `ci` targets name placeholder accounts (`sst_reference`, `SST_REF_DEV`, ...) and carry no
  credential, so they cannot connect;
- reads Snowflake through the fakes and the recorded session in `tests/helpers/`, never through a
  connection;
- runs with an empty home directory, none of the `SST_*` variables `sst` reads, no
  `SST_TEST_SNOWFLAKE_*` variable and no local live configuration, so nothing on your machine
  changes what it sees;
- fails if it opens a network connection, even one the code under test catches
  (`tests/helpers/network_guard.py`).

A change to rendered output is compared byte for byte with the goldens under
`tests/golden/expected/`; when the change is intended, rewrite them with the engine itself and
review the diff:

```bash
P=tests/fixtures/reference_project
M=tests/fixtures/reference_project_manifest.json
poetry run sst test --suite golden --update-golden --project-dir $P --manifest $M \
  --golden-dir "$PWD/tests/golden/expected/ddl"
```

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
   It supplies the account, user, role, warehouse and authenticator when the sources above leave
   them unset, and passes its other driver settings, such as
   `client_store_temporary_credential`, to the test process's connection as they are.

| Key (`SST_TEST_SNOWFLAKE_` + ) | Required | Meaning |
|---|---|---|
| `ACCOUNT` | yes | Account identifier, such as `myorg-myaccount`. |
| `USER` | yes | The user the tests sign in as. |
| `ROLE` | yes | The role they run with; see [privileges](#privileges). |
| `WAREHOUSE` | yes | The warehouse their statements run in. |
| `DATABASE` | yes | The database the live layers create their scratch schemas in. |
| `SCHEMA` | evals, `verify` | The reference project's own schema, where `sst test --suite evals` and the fixture's `verify` target deploy. The live layers never write to it. |
| `PRIVATE_KEY_PATH` | one of three | An unencrypted PKCS#8 private key; signs in by key pair. A leading `~` is expanded. |
| `AUTHENTICATOR` | one of three | Signs in without a key, such as `externalbrowser` (SSO) or `oauth_authorization_code`. Ignored when a key path is set. |
| `CONNECTION` | one of three | A named connection, read as above. |
| `GRANTEE_ROLE` | no | A role the test role may grant `SELECT` to; adds the check that replacing a view keeps its grants. |

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
- `--require-snowflake` fails them instead, so a gate that could not connect is never green.
- `-n 4` runs them in parallel; each worker works in schemas of its own.
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

`python -m tests.helpers.session_recorder --out capture.json` re-captures the recorded session the
offline tests replay, in the schema `SST_IT_RECORDING`, which it replaces and drops.

### Privileges

The test role needs:

| On | Privilege | For |
|---|---|---|
| the warehouse | `USAGE` | every statement |
| `DATABASE` | `USAGE`, `CREATE SCHEMA` | the scratch schemas, which the role then owns and drops |
| `GRANTEE_ROLE` (optional) | the right to grant `SELECT` on objects the test role owns | the grant-preservation check |

Nothing account-wide is needed. For the evals suite and the `verify` target, the role also needs
to create the reference project's objects in `DATABASE.SCHEMA` -- tables, semantic views and
agents -- and to use Cortex agents in your account.

## The manual workflow

`.github/workflows/slow.yml` runs the same checks in GitHub Actions, in this repository or a
fork. Nothing triggers it but a manual run (Actions, "Snowflake checks (manual)", Run workflow),
and nothing waits on it. Pick a suite -- `live`, `sweep`, `recording` or `evals` -- and supply
these repository secrets; the workflow maps them to `SST_TEST_SNOWFLAKE_*` and signs in by key
pair:

| Secret | Value |
|--------|-------|
| `SNOWFLAKE_ACCOUNT` | account identifier |
| `SNOWFLAKE_USER` | a key-pair user |
| `SNOWFLAKE_PRIVATE_KEY` | that user's unencrypted PKCS#8 private key, as PEM text |
| `SNOWFLAKE_ROLE` | a role with the [privileges](#privileges) above |
| `SNOWFLAKE_WAREHOUSE` | the warehouse the tests run in |
| `SNOWFLAKE_DATABASE` | the database the scratch schemas are created in |
| `SNOWFLAKE_SCHEMA` | the reference project's schema (`evals` only) |
| `SNOWFLAKE_GRANTEE_ROLE` | optional; enables the grant-preservation check |

Without the secrets the run skips with a notice and succeeds. The key is written outside the
checkout, readable only by the runner user, and removed by the last step.
