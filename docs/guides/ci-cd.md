# CI/CD

The commands a person runs locally are the ones CI runs: `validate` and `plan`
on every pull request, `apply` from a reviewed plan on merge. This page shows
the shape of each job and the pieces of SST's output that are built for
automation.

## On every pull request

```bash
sst validate --strict
sst compile --emit-ddl target/ddl/
sst plan --target prod --output json > plan.json || test $? -eq 2
```

- `validate --strict` fails on any warning. Leave `--strict` off to fail on
  errors only.
- `compile --emit-ddl` writes each semantic view's DDL, a readable artifact to
  attach to the review.
- `plan` reads production and reports what merging would change without writing
  anything. It exits `2` when there are changes, so a pipeline that should
  accept a pending change treats `2` as success; any other non-zero exit is a
  failure.

If the project commits golden files, `sst test --suite golden` compares the
rendered DDL and specs with them offline.

## On merge

```bash
sst plan --target prod
sst apply --target prod --plan target/sst/plan.json --yes
sst test --suite smoke --target prod
```

`apply` executes exactly the plan it is given and refuses a plan that no longer
matches the project, so it cannot publish something no one planned. Keep the
`plan` and `apply` in one job, or pass `target/sst/plan.json` between jobs as an
artifact.

`apply` holds a lock on the target's state while it runs, so a second apply
against the same target stops with `SST-APL011` instead of interleaving with
the first. If a run died and left its lock behind, `--break-stale-lock` takes it
over.

## Example: GitHub Actions

```yaml
name: semantic layer
on:
  pull_request:
  push:
    branches: [main]

jobs:
  sst:
    runs-on: ubuntu-latest
    env:
      SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
      SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
      SNOWFLAKE_PRIVATE_KEY_PATH: ${{ runner.temp }}/snowflake_key.p8
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: python -m pip install "snowflake-semantic-tools[dbt]"
      - run: echo "${{ secrets.SNOWFLAKE_PRIVATE_KEY }}" > "$SNOWFLAKE_PRIVATE_KEY_PATH"
      - run: dbt deps
      - run: sst validate --strict
      - name: plan
        run: sst plan --target prod || test $? -eq 2
      - name: apply
        if: github.event_name == 'push'
        run: sst apply --target prod --plan target/sst/plan.json --yes
```

The `prod` output in `profiles.yml` reads the key path with
`private_key_file: "{{ env_var('SNOWFLAKE_PRIVATE_KEY_PATH') }}"`.

## JSON output

Every command accepts `--output json` and then prints exactly one JSON object on
stdout:

```json
{
  "tool": "sst",
  "sst_version": "1.0.0",
  "schema_version": 2,
  "command": "plan",
  "status": "changes",
  "exit_code": 2,
  "invocation": {"argv": ["sst", "plan"], "target": "prod", "project_dir": "...", "config_file": "...", "started_at": "...", "duration_s": 4.2},
  "diagnostics": [],
  "summary": {"error": 0, "warning": 0, "info": 3, "promoted": 0, "suppressed_cascade": 0, "baselined": 0},
  "data": {}
}
```

- `status` is `ok`, `changes`, or `error`, and `exit_code` is the process exit
  code.
- Each diagnostic carries `code`, `severity`, `message`, `location`, `suggestion`,
  and `help_url`, a link to its entry in the
  [error code reference](../reference/error-codes.md).
- `data` holds the command's result: the planned changes for `plan`, the
  outcomes for `apply`, the artifacts for `compile`.

`schema_version` changes only when a field is removed or changes meaning; new
fields can appear in the same version.

## Exit codes

| Code | Meaning |
|---:|---|
| 0 | Success. For `plan`, nothing to change. |
| 1 | Errors were reported, or an apply or test failed. |
| 2 | `plan` found changes. |
| 3 | The command line is invalid. |
| 4 | The project or its configuration cannot be used. |
| 5 | Snowflake could not be reached. |

The [CLI reference](../reference/cli.md#exit-codes) lists every code.
