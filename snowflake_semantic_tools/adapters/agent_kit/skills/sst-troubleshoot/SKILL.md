---
name: sst-troubleshoot
description: |-
  Use when SST cannot get started or acts on the wrong thing: `dbt parse` fails, no
  `profiles.yml` or `sst_config.yml` is found, the manifest is stale or of another schema,
  sign-in hangs or is refused, a privilege is missing, SST publishes to the wrong database
  or schema, a plan shows unexpected changes, an apply is refused, or a run lock is held. Do
  NOT use for validation findings in the semantic YAML (use sst-fix-diagnostics).
---
# Troubleshooting SST

The full guide is @SST_DOCS@/guides/troubleshooting.md. Start with the offline view of what
SST resolved:

```bash
sst debug --no-connect
```

It prints the SST and dbt versions, every configuration path it checked, where the dbt
manifest comes from, and the resolved target: profile, role, warehouse, database, schema,
state table, and how it signs in. It never prints a credential.

## `dbt parse` fails before anything else runs

SST runs `dbt parse` itself, with the directory of the first `profiles.yml` it finds as
`--profiles-dir`. dbt's own output is printed above `SST-DBT028`.

- **No `profiles.yml` is found** (`SST-CFG009`, which lists every place searched): looked for
  in `--profiles-dir`, `$SST_PROFILES_DIR`, `$DBT_PROFILES_DIR`, the project root, and
  `~/.dbt`, in that order.
- **Packages are not installed** (`SST-DBT026`): run `dbt deps`.
- **The manifest's schema is not one SST supports** (`SST-DBT017`): use a dbt version that
  writes manifest schema v12.
- **The manifest passed with `--manifest` is older than the models** (`SST-DBT005`):
  regenerate it, or drop `--manifest` and let SST run `dbt parse`.

## No configuration is found

`sst_config.yml` is read from the project root only (`--project-dir`, else
`$SST_PROJECT_DIR`), or from the file `--config` names; parent folders are not searched.
`SST-CFG001` means none was found; `SST-CFG005` means both `sst_config.yml` and
`sst_config.yaml` exist.

## Sign-in hangs, or is refused

- **It never returns.** With `authenticator: externalbrowser`, a connecting command waits for
  a browser sign-in with no time limit; in a shell nobody watches, that is for ever. Press
  Ctrl-C (exit 130), use the offline forms (`sst debug --no-connect`,
  `sst validate --no-snowflake-syntax-check`), and let the user run connected commands in
  their own terminal.
- **`SST-PRT002`**: Snowflake rejected the credential. **`SST-PRT001`**: the connection failed
  for another reason, with the driver's message. Both exit 5.
- **`SST-CFG048`** to **`SST-CFG051`**: a `profiles.yml` field SST does not read, a template
  other than `env_var()`, an unsupported authentication setup, or `insecure_mode: true`.

## The wrong database or schema

- `sst debug --no-connect` shows the database and schema the target resolves to;
  `--target <name>` shows another target.
- Check `+database` and `+schema` for the artifact type, and any folder route, in
  `sst_config.yml`.

## Snowflake says an object does not exist or is not authorized

Snowflake answers the same for an object that is absent and one the role cannot see. Check
that it was built or applied on this target, that the target resolves where you looked, and
that the role has `USAGE` on the database and schema. `SST-PRT004` names a missing privilege.

## `plan` shows changes when nothing changed

Someone changed the object in Snowflake, the dbt project changed a column, description, or
type the views use, or the plan ran against another target.

## `plan` stops at an object SST does not manage

`SST-PLN024`: the object exists and SST did not publish it, so SST will not change it. A
person must drop it by hand or rename the artifact. Never drop it without the user's say.

## `apply` refuses the plan, or a lock is held

- A saved plan is applied only while the project, the target, and Snowflake still match it.
  Run `sst plan` again and apply the new plan.
- **`SST-APL011`**: another run holds the target's lock. Wait for it. Only once the lock has
  expired (30 minutes without renewal) does `--break-stale-lock` take it over (`SST-APL010`);
  check first that the run it names is really gone.
- **`SST-PRT109`**: `apply` and `sst baseline add --all-warnings` ask before writing, and off
  a terminal they need `--yes`. Do not add `--yes` unless the user asked for the write.
