# CI/CD

The commands a person runs locally are the ones CI runs: `validate`, `compile`
and `plan` on every pull request, `apply` from a reviewed plan on merge. This
page shows the shape of each job and the pieces of SST's output that are built
for automation.

## On every pull request

```bash
sst validate --strict
sst compile --target prod --emit-ddl target/ddl/
sst plan --target prod --output json > plan.json || test $? -eq 2
```

- `validate --strict` fails on any warning. Leave `--strict` off to fail on
  errors only.
- `compile` writes the SST manifest `plan` and `apply` read, for the same target;
  `--emit-ddl` also writes each semantic view's DDL, a readable artifact to
  attach to the review.
- `plan` reads production and reports what merging would change without writing
  anything. It exits `2` when there are changes, so a pipeline that should
  accept a pending change treats `2` as success; any other non-zero exit is a
  failure.

If the project commits golden files, `sst test --suite golden` compares the
rendered DDL and specs with them offline.

## On merge

```bash
sst compile --target prod
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
      - run: sst compile --target prod
      - name: plan
        run: sst plan --target prod || test $? -eq 2
      - name: apply
        if: github.event_name == 'push'
        run: sst apply --target prod --plan target/sst/plan.json --yes
```

The `prod` output in `profiles.yml` reads the key path with
`private_key_path: "{{ env_var('SNOWFLAKE_PRIVATE_KEY_PATH') }}"`, the field dbt
reads too; see [authentication](configuration.md#authentication).

The `pull_request` trigger gives a fork's pull request no secrets, so its connected steps cannot
authenticate. Keep it that way: a project's YAML is SQL that runs with the job's role, so never
move a credentialed step to `pull_request_target`; see [Security](security.md#trust-model).

## Template: a consumer project's deploy workflow

> **A template for the repository that holds your dbt project.** It is not this repository's CI,
> and nothing here runs it. The project that publishes to production owns its deploys: copy the
> file to `.github/workflows/semantic-layer.yml` there and adjust the names.

It validates and plans every pull request from the repository's own branches, and on each merge
to `main` plans again and applies exactly that saved plan to `prod`. It signs in by key pair from
repository secrets: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_ROLE`,
`SNOWFLAKE_WAREHOUSE`, and `SNOWFLAKE_PRIVATE_KEY`, the user's PKCS#8 private key as PEM text.
The `prod` output in `profiles.yml` reads them:

```yaml
my_project:
  target: dev
  outputs:
    prod:
      type: snowflake
      authenticator: snowflake_jwt
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      private_key_path: "{{ env_var('SNOWFLAKE_PRIVATE_KEY_PATH') }}"
      role: "{{ env_var('SNOWFLAKE_ROLE') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE') }}"
      database: ANALYTICS_DB
      schema: SEMANTIC
      threads: 4
```

```yaml
name: semantic layer

on:
  pull_request:
  push:
    branches: [main]

permissions:
  contents: read

env:
  SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
  SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
  SNOWFLAKE_ROLE: ${{ secrets.SNOWFLAKE_ROLE }}
  SNOWFLAKE_WAREHOUSE: ${{ secrets.SNOWFLAKE_WAREHOUSE }}

jobs:
  plan:
    name: Validate and plan
    # A fork's pull request gets no secrets, so it cannot plan; review it before running this.
    if: github.event_name == 'pull_request' && github.event.pull_request.head.repo.full_name == github.repository
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262 # v4.4.0
        with:
          persist-credentials: false
      - uses: actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065 # v5.6.0
        with:
          python-version: "3.12"
      - run: python -m pip install "snowflake-semantic-tools[dbt]==1.0.0"
      - name: Write the private key
        env:
          PRIVATE_KEY: ${{ secrets.SNOWFLAKE_PRIVATE_KEY }}
        run: |
          umask 077
          printf '%s\n' "$PRIVATE_KEY" > "$RUNNER_TEMP/snowflake_key.p8"
          echo "SNOWFLAKE_PRIVATE_KEY_PATH=$RUNNER_TEMP/snowflake_key.p8" >> "$GITHUB_ENV"
      - run: dbt deps
      - run: sst validate --strict --target prod
      - run: sst compile --target prod --emit-ddl target/ddl/
      - name: Plan (exit 2 means there are changes to review)
        run: sst plan --target prod --output json > plan.json || test $? -eq 2
      - uses: actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02 # v4.6.2
        with:
          name: sst-plan
          path: |
            plan.json
            target/ddl/
      - if: always()
        run: rm -f "$RUNNER_TEMP/snowflake_key.p8"

  apply:
    name: Apply to prod
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    # Add required reviewers to this environment to approve each deploy by hand.
    environment: production
    # One deploy at a time, in merge order. Never cancel one in progress: an apply stopped part
    # way can leave changes half published and its run lock behind.
    concurrency:
      group: sst-apply-prod
      cancel-in-progress: false
    steps:
      - uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262 # v4.4.0
        with:
          persist-credentials: false
      - uses: actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065 # v5.6.0
        with:
          python-version: "3.12"
      - run: python -m pip install "snowflake-semantic-tools[dbt]==1.0.0"
      - name: Write the private key
        env:
          PRIVATE_KEY: ${{ secrets.SNOWFLAKE_PRIVATE_KEY }}
        run: |
          umask 077
          printf '%s\n' "$PRIVATE_KEY" > "$RUNNER_TEMP/snowflake_key.p8"
          echo "SNOWFLAKE_PRIVATE_KEY_PATH=$RUNNER_TEMP/snowflake_key.p8" >> "$GITHUB_ENV"
      - run: dbt deps
      - run: sst compile --target prod
      - name: Plan, saving it to target/sst/plan.json
        run: sst plan --target prod || test $? -eq 2
      - name: Apply exactly the saved plan
        run: sst apply --target prod --plan target/sst/plan.json --yes
      - run: sst test --suite smoke --target prod
      - if: always()
        run: rm -f "$RUNNER_TEMP/snowflake_key.p8"
```

- **The plan on merge is the one applied.** `apply --plan` executes that saved plan and refuses
  it if the project or the target's state changed after it was written, so nothing is published
  that no plan listed. The pull request's `plan.json` is for review; it is not applied, because
  `main` may have moved by the time the branch merges.
- **The run lock backs up the concurrency group.** The group queues this workflow's deploys;
  `apply` also holds a lock in the target's state table while it runs, so an apply started
  anywhere else -- another workflow, a laptop -- stops with `SST-APL011` rather than interleave.
  If a killed run left its lock behind, `--break-stale-lock` takes over a lock whose run no
  longer exists.
- **Pin what runs.** Keep SST at an exact version and each action at a commit SHA, and let a
  dependency bot propose updates.
- **Prune is a separate decision.** The template never passes `--prune`; add it only once the
  plan's drops are part of the review (see [Security](security.md#operating-sst)).

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
