# Configuration

SST reads two files from the project root, the directory that holds
`dbt_project.yml`:

- **`sst_config.yml`** says what SST publishes and where;
- **`profiles.yml`** holds the connection targets, in dbt's format.

The [configuration reference](../reference/config.md) lists every key, its type,
and its default; it is generated from the schema SST validates against. This
page explains how the pieces fit.

## How keys are validated

Every key in `sst_config.yml` is checked against that schema when a command
starts:

- an unknown key is a warning (`SST-CFG003`) that names the closest known key;
  a misspelled top-level block is an error (`SST-CFG007`), because every setting
  under it would otherwise be ignored;
- a value of the wrong type or outside its range is an error (`SST-CFG004`,
  `SST-CFG008`), and so is a missing required key (`SST-CFG006`);
- a removed key is an error that says what replaced it;
- an *inert* key, one SST 0.3 read and 1.0 does not, is accepted with an info
  diagnostic (`SST-CFG044`) so an existing file keeps loading.

## Locations and targets

Every artifact type has a block, and its `+database` / `+schema` keys decide
where it publishes. Values can use three substitutions from the selected
target:

```yaml
semantic_views:
  +database: "{{ target.database }}"
  +schema: "{{ target.schema }}"

tools:
  +warehouse: "{{ target.warehouse }}"
```

`--target` picks the target from `profiles.yml`, so one configuration publishes
to development and production from the same authored files. A key that is not
set falls back to the target's own database, schema, or warehouse.

Keys that start with `+` set defaults. Under `semantic_views:`, an unprefixed
key names a folder under `semantic_models_dir/semantic_views/`, and its `+` keys
apply to the views in that folder. A route to a folder that does not exist is
an error (`SST-CFG041`), so a mistyped route cannot quietly route nothing.

## Blocks

| Block | What it controls |
|---|---|
| `project` | Where each artifact type is authored. A directory key that is set must name a directory that exists (`SST-CFG047`); an unset one falls back to its default, which may be absent. |
| `validation` | `strict` promotes warnings to errors; `snowflake_syntax_check` compiles expressions against Snowflake. |
| `vars` | Values `{{ var() }}` substitutes into expressions. |
| `tags` | Tags `{{ tag() }}` resolves, by `fqn:` or by `default_prefix`. |
| `state` | Where the state table lives (default `SST_STATE` in the target schema). |
| `semantic_views`, `tools`, `agents` | Location and defaults for each type. |
| `evals` | Defaults for evaluations; never a location, because evals publish into their agent's schema. |
| `skills` | Skill, plugin, and profile publishing; see [skills](skills.md) and [plugins and profiles](plugins-and-profiles.md). |
| `apply` | The stages apply uploads agent specs and eval configs through. |
| `snowflake` | Allowlists, such as the orchestration models agents may name. |

## Projects without dbt

A project that publishes only skills, plugins, and profiles does not need dbt.
Without a `dbt_project.yml`, SST reads no dbt manifest and takes its
`profiles.yml` profile from `project.target_profile`:

```yaml
project:
  target_profile: skills
skills:
  catalog:
    +bundle_stage: SKILL_BUNDLE_SRC
```

Semantic views, tools, agents, and evals are built from dbt models, so their
blocks are an error in such a project (`SST-CFG046`).

## Authentication

SST connects with the fields of the selected `profiles.yml` output, read the way
dbt-snowflake reads them. `{{ env_var('NAME') }}` and
`{{ env_var('NAME', 'default') }}` are rendered anywhere in a value, so
`"svc_{{ env_var('SUFFIX') }}"` works; any other template, including a filter
such as `as_number`, is an error (`SST-CFG049`). Numbers and booleans can be
written plainly or as strings.

| Method | Fields |
|---|---|
| Browser SSO | `authenticator: externalbrowser` |
| Key pair, file | `private_key_path` (or `private_key_file`), and `private_key_passphrase` for an encrypted key |
| Key pair, inline | `private_key` as PEM or base64 DER, and `private_key_passphrase` for an encrypted key |
| OAuth access token | `token`; `authenticator: oauth` is implied |
| Password | `password` |

Every output also needs `account` and `user`, and `database` and `schema` are
required: they are where SST publishes by default and keeps its state.
`role`, `warehouse`, `query_tag`, `host`, `port`, `protocol`, `insecure_mode`,
`client_session_keep_alive`, and `connect_timeout` are used when set.

- dbt settings with no meaning for one connection -- `threads`,
  `connect_retries`, `retry_on_database_errors`, `retry_all`,
  `reuse_connections` -- are ignored, and never rendered, so an unset variable
  there cannot stop a run.
- Any other field is a warning (`SST-CFG048`), so a misspelled credential field
  is reported instead of dropped silently.
- `oauth_client_id` and `oauth_client_secret` ask dbt to exchange a refresh
  token; SST does not, so they are an error (`SST-CFG050`). Supply an access
  token in `token` instead. `private_key` together with a key file is an error
  too, and a key that cannot be read is reported without echoing it.
- `sst debug` shows the method SST will use, never the credential.

Because SST passes the project directory to `dbt parse` as `--profiles-dir`,
`profiles.yml` must be in the project root. Commit it with every secret behind
`env_var()`, or generate it in CI.

## Useful commands

```bash
sst debug                     # the profile, target, and locations SST resolved
sst debug --test-connection   # also open a session and report its role
sst validate --no-snowflake-syntax-check   # fully offline
```
