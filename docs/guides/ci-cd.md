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

- `validate --strict` promotes every warning to an error. `--no-strict` fails
  on errors only; with neither flag, `validation.strict` in `sst_config.yml`
  decides. `validate` connects when `validation.snowflake_syntax_check` is true,
  to compile expressions against Snowflake; `--no-snowflake-syntax-check` keeps
  it offline.
- `compile` writes the SST manifest `plan` and `apply` read, for the same target;
  `--emit-ddl` also writes each artifact's rendered payload (a semantic view's
  DDL, an agent's specification), a readable artifact to attach to the review.
  Each file sits in a folder named for its artifact's type, as
  `<dir>/semantic_view/<name>.sql` or `<dir>/agent/<name>.json`, since a name
  need only be unique within its type.
- `plan` reads production and reports what merging would change without writing
  anything. It exits `2` when there are changes, so a pipeline that should
  accept a pending change treats `2` as success; any other non-zero exit is a
  failure. `--no-detailed-exitcode` makes it exit `0` with changes pending
  instead. It also saves the plan to `target/sst/plan.json` (`--plan-out` to put
  it elsewhere).

### Reviewing what an update changes

Under each update, `sst plan` shows what it changes member by member: the live
object as Snowflake holds it, against what SST would publish. Plan only updates
an object that still matches what SST last wrote, so the live side is exactly
what apply replaces.

```
  ~ update  semantic_view:sales  SALES_DB.SEMANTIC_VIEWS.SALES  metric orders.total_revenue
      ~ metric orders.total_revenue
          comment:
            - Revenue before tax.
            + Revenue before tax, in dollars.
```

The members are a semantic view's tables, relationships, facts, dimensions,
filters, metrics, verified queries and instructions; an agent's instructions,
sample questions, tools and skills, from the version it serves; a procedure's,
function's or search service's settings, comment and body; and a skill's,
plugin's or profile's files and an eval's config and questions. A list shows the
items it loses and gains, text shows the lines that change, and sample values
are counted rather than listed. What Snowflake's DESCRIBE has not been seen to
report (a view's variables, a window metric's expression, `USING`) is not
compared, so an update that changes only those says
`definition (no member-level difference)`.

The terminal cuts a long diff short; every plan writes all of it, as a unified
diff an editor or a pull request colours red and green, to
`target/sst/plan.<target>.diff` (`--diff-out` to put it elsewhere). Paste it
into the pull request, or upload it as an artifact. JSON reports each change's
members as `diff`, with `diff_status` saying whether it was compared. Each
update costs one read (a DESCRIBE, the agent's versions, or one GET per staged
file); `--no-diff` skips them. A read the role may not make leaves the update
uncompared, with an info note (`SST-PLN041`), and plans as before. Either way,
apply records a fingerprint per member in state, so an update that was not read
still names the members that changed since the last apply, without their
values: `diff_status` is then `from_state` (or `unavailable` for a refused
read) and `changed_members` lists them, as the table and the diff file do. `apply`
shows the same diff when it asks to confirm; with `--yes` it shows nothing, so
it reads nothing.

Before promoting, `sst diff --from dev --to prod` compares the two targets the
same way. Each target's physical names are first replaced by what they stand
for, so two targets that publish the same project to different databases
agree; only a real difference, such as one target's metric edited by hand or a
warehouse configured per target, is listed.

### What a change reaches

The compiled manifest records the project's lineage: dbt's graph as the target
reads it (models, seeds, snapshots, sources and exposures), what each view member
reads, and what each artifact reads, a procedure's or function's body included
when it names its tables with `{{ ref() }}`. `sst lineage NODE` shows one node of
it, offline:

```bash
sst lineage model:orders --downstream            # every member and artifact orders feeds
sst lineage tool:order_status --upstream          # the models the procedure's body reads
sst lineage metric:total_revenue --state prod-run/ # and which of them changed since then
```

NODE is an artifact key, a member key such as `metric:<name>`, a dbt node
(`model:`, `seed:`, `snapshot:`, `test:`, `exposure:`, or
`source:<source>.<table>`), `tool_ref:<name>` for a tool member another team
owns, or a bare name. Each direction prints as a tree, a node under the one it
was reached from, and a subtree reached twice is shown once. `--depth` bounds
the walk, `--state` marks each artifact and member that changed since another
run's manifest, and `--no-manifest` traces the project as it compiles now;
`-o plain` prints one `<depth> <key>` line per node (upstream negative), and
`-o json` the nodes and the edges between them.

Where the walk meets someone else's work, the node says so rather than ending
silently: a source is a `(raw source)`; a model of another dbt package that
reads nothing here is `(defined in another project; upstream continues there)`;
a tool member another team owns is `(owned by another team)`; and a procedure
or function whose body names no `ref()` is `(reads: unknown; body names no
ref())`, since SST cannot see what a literal name reads.

Every `--select` and `--exclude` takes dbt's graph operators over the same
graph, so a pull request can plan only what a model change reaches:

```bash
sst plan --target prod --select model:orders+          # what orders feeds
sst plan --target prod --select +agent:support_agent   # what the agent reads
sst plan --target prod --select 2+metric:total_revenue+1
```

`+X` adds what X reads, `X+` what reads X, and a number bounds the steps.
dbt nodes, members and `tool_ref:` name no artifact on their own, so they are
refused outside the operators. `sst enrich --select +model:orders`
walks dbt's graph alone, to enrich a model and those it is built from.

### Recording what an update replaces

`sst plan --capture-prior` also reads the current definition of each live object
a planned artifact names, so a reviewer can see what an update replaces and
restore it afterwards. `--full` prints it under each update, JSON reports it as
each change's `prior_definition`, and the observation the plan records in
`target/sst/observation.<target>.json` keeps it as each object's `definition`.
What is read depends on how the kind is published:

| Kind | Prior definition | To restore it |
|---|---|---|
| Semantic view | What `GET_DDL` prints, with names fully qualified, rewritten into the form SST renders: the trailing clauses in the order the server accepts (`MAX_STALENESS` after the AI clauses) and `COPY GRANTS` added, which `GET_DDL` drops; needs REFERENCES or OWNERSHIP | Run it as recorded. If it starts with a `-- SST did not put this definition ...` comment, `GET_DDL` printed a shape SST does not rewrite and the text is as returned: add `COPY GRANTS` and move `MAX_STALENESS` after the AI clauses before running it, or the restore drops the view's grants or is refused |
| Procedure, function | What `GET_DDL` prints, with names fully qualified (addressed by their argument types), with `COPY GRANTS` added after the argument list, which `GET_DDL` drops; needs REFERENCES or OWNERSHIP | Run it as recorded; `COPY GRANTS` keeps the routine's grants. If it starts with a `-- SST did not add COPY GRANTS ...` comment, the text is as returned: add `COPY GRANTS` after the argument list before running it, or the restore revokes every grant on the routine |
| Cortex search service | What `GET_DDL` prints, with names fully qualified, followed by one `GRANT` per explicit grant plan read on the service: a search service takes no `COPY GRANTS`, so replacing it revokes them; needs REFERENCES or OWNERSHIP | Run it as recorded, the `GRANT` statements included. Under `--no-grants` the grants are not read, and a closing comment says to grant them again by hand |
| Agent | The version it serves now (the one carrying the agent's alias, else the default, else the last committed), its specification as indented JSON, and the `ALTER AGENT` statements that point the alias and the default version back at it | An update adds a version and keeps every earlier one, so run those statements; never re-create the agent, which would drop its versions and grants |
| Stage tool | None: apply only creates a stage when it is absent | Nothing to restore |
| Skill, plugin, profile, eval | None: each publishes content-addressed versions, trees, tables, and datasets beside the earlier ones; a profile's row and an eval's staged configuration are rewritten in place and not captured | Re-publish the earlier source |

A definition that cannot be read, for want of a privilege or because the agent
lists no version, is a warning (`SST-PLN037`), and that change's
`prior_definition` is null; the plan is otherwise the same as without the flag.

### Rollback

Running an agent's captured statements rolls it back: its alias and default
version point at the version it served before. The rollback holds until the
next apply. The next `sst plan` sees that the alias or the default version is no
longer on the newest version holding the project's spec, and plans the agent as
an update with reason `version_drift` (`SST-PLN102`); applying it rolls forward,
pointing them back at that version without adding one. To keep the rollback,
do not apply until it should end, or change the source back and apply that,
which publishes the earlier spec as a new version.

## On merge

```bash
sst compile --target prod
sst plan --target prod
sst apply --target prod --plan target/sst/plan.json --yes
sst test --suite smoke --target prod
```

`apply` executes exactly the plan it is given. It plans the target again and
refuses the saved plan, exiting `4`, if it was made for another target, from
another manifest, or from a different reading of Snowflake, so it cannot
publish something no one planned. Keep the `plan` and `apply` in one job, or
pass `target/sst/plan.json` between jobs as an artifact. Pass `--yes` in a
pipeline: it is required with `--output json` or `--prune`, and without it
`apply` stops to ask before it writes.

`apply` holds a run lock in the target's state table while it runs, so a second
apply against the same target stops with `SST-APL011` instead of interleaving
with the first; `sst test --suite evals` and `sst drop` take the same lock. A
running apply extends its lock every five minutes, and the lock expires 30
minutes after its last extension. If a run died and left its lock behind,
`--break-stale-lock` takes it over once it has expired, reporting `SST-APL010`;
a lock that has not expired is refused all the same.

## Known warnings: the baseline

A project that adopts `--strict` with warnings it cannot fix yet records them in
a baseline instead of turning strict off:

```bash
sst baseline add <CODE>                 # every current instance of one warning
sst baseline add <CODE> --target prod   # what a connected validate of prod reports
```

Commit the file, `.sst/baseline.json`. A diagnostic it holds no longer fails a
run, strict or not; it is counted under `baselined` in the summary and hidden
unless `--show-baselined`. An entry is keyed on the code, the artifact, the file
and the message's values, never the line, so a reformat keeps it while a new
instance, or a renamed artifact, is not covered. Errors and non-demotable
warnings cannot be baselined. The file expires
(`--expires-in`, 180 days by default): a run past its expiry fails with
`SST-CFG039` until `sst baseline renew --reason ...`. Its `expires_on` is a
`YYYY-MM-DD` date, as `sst baseline` writes it; any other spelling, such as
`2026-9-1`, is refused when the file is read (`SST-PRT009`). `sst baseline prune`
removes the entries nothing matches any more, and `--no-baseline` ignores the
file for one run.

## Template: a consumer project's deploy workflow

> **A template for the repository that holds your dbt project.** It is not this repository's CI,
> and nothing here runs it. The project that publishes to production owns its deploys: copy the
> file to `.github/workflows/semantic-layer.yml` there and adjust the names.

It validates and plans every pull request from the repository's own branches, and on each merge
to `main` plans again and applies exactly that saved plan to `prod`. It signs in by key pair from
repository secrets: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_ROLE`,
`SNOWFLAKE_WAREHOUSE`, and `SNOWFLAKE_PRIVATE_KEY`, the user's unencrypted PKCS#8 private key as
PEM text. Each job writes the key to a file outside the checkout and exports its path as
`SNOWFLAKE_PRIVATE_KEY_PATH`; the `prod` output in `profiles.yml` reads them all, through
`private_key_path` for the key, the field dbt reads too (see
[authentication](configuration.md#authentication)):

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
  it, exiting `4`, if the manifest or the target's state changed after it was written, so
  nothing is published that no plan listed. The pull request's `plan.json` is for review; it is
  not applied, because `main` may have moved by the time the branch merges.
- **A fork's pull request runs nothing credentialed.** The `pull_request` trigger gives it no
  secrets, and the `plan` job's condition skips it, `sst validate` included: with
  `validation.snowflake_syntax_check: true`, validate connects too. A project's YAML is SQL that
  runs with the job's role, so never move a credentialed step to `pull_request_target`; see
  [Security](security.md#trust-model). To check a fork's change offline, add a job with no
  Snowflake secrets that runs `sst validate --no-snowflake-syntax-check` and `sst compile`.
  Give it the profile's non-secret values, not empty ones: the target's
  warehouse, for one, is compiled into an agent tool that declares none of its own.
- **The run lock backs up the concurrency group.** The group queues this workflow's deploys;
  `apply` also holds a lock in the target's state table while it runs, so an apply started
  anywhere else -- another workflow, a laptop -- stops with `SST-APL011` rather than interleave.
  If a killed run left its lock behind, it expires 30 minutes after the run last extended it,
  and from then on `--break-stale-lock` takes it over.
- **Pin what runs.** Keep SST at an exact version and each action at a commit SHA, and let a
  dependency bot propose updates.
- **Prune is a separate decision.** The template never passes `--prune`; add it only once the
  plan's drops are part of the review (see [Security](security.md#operating-sst)).

## JSON output

Every command accepts `--output json` (or `-o json`, or `SST_OUTPUT=json`) and then
prints exactly one JSON object on stdout, on one line with its keys sorted; it is
shown indented here:

```json
{
  "tool": "sst",
  "sst_version": "1.0.0",
  "schema_version": 2,
  "command": "plan",
  "status": "changes",
  "exit_code": 2,
  "invocation": {"argv": ["sst", "plan"], "target": "prod", "project_dir": "...", "config_file": "...", "overrides": [], "started_at": "...", "duration_s": 4.2,
    "timings": {"wall_s": 4.2, "phases": {"load": 1.1, "observe": 2.4}, "snowflake": {"queries": 12, "client_s": 3.1, "max_s": 0.6, "busy_s": 2.6, "connects": 1, "connect_s": 0.4}, "dbt_runs": 0, "dbt_s": 0.0}},
  "diagnostics": [],
  "summary": {"error": 0, "warning": 0, "info": 3, "promoted": 0, "suppressed_cascade": 0, "baselined": 0},
  "data": {},
  "next_steps": ["show the user this plan; `sst apply` changes Snowflake, so run it only when they ask"]
}
```

- `status` is `ok`, `changes`, or `error`, and `exit_code` is the process exit
  code.
- `invocation.overrides` names each guarantee the run switched off:
  `--allow-stale-manifest`, `--allow-unsupported-manifest-schema`, or
  `--no-baseline`.
- `invocation.timings` says where the run's time went, in seconds as the client
  observed them: `wall_s` (the run, as `duration_s`); `phases`, the seconds of
  each step the command names (`load`, the configuration and the compile with
  any `dbt parse`; `validate`; `state_read`, `composites` (the agents' and
  evals' own reads), `observe`, `classify` for `plan` and `apply`; `lock`,
  `publish`, `state_write` for `apply`; `smoke`; `disconnect`, closing the
  parallel sessions), a phase
  absent when the run did not reach it; `snowflake`, with `queries` (statements
  sent, failed ones included), `client_s` (their summed time, which statements
  on parallel sessions push past the wall clock), `max_s` (the slowest),
  `busy_s` (the wall time during which any statement or login was in flight),
  `connects` and `connect_s` (logins); and `dbt_runs` and `dbt_s` (dbt
  processes SST started). The names a phase can take may grow; read them as
  diagnostics for a slow run, not as a contract on what a run does.
- Each diagnostic carries `code`, `severity` (and `declared_severity`, with
  `promoted_from` or `demoted_from` when they differ), `message`, `location`,
  `suggestion`, `baselined`, a stable `fingerprint`, and `help_url`, a link to
  its entry in the [error code reference](../reference/error-codes.md).
  `internal_detail` is the diagnostic's `detail` value, the same text its message
  shows and `params.detail` holds, for an internal error
  (`SST-INT001`) or an unrecognised Snowflake refusal (`SST-SNO001`), and null for
  every other code.
- `summary` counts a baselined diagnostic under `baselined` and under no
  severity.
- `data` holds the command's result: the planned changes for `plan`, the
  outcomes for `apply`, the artifacts for `compile`.
- `next_steps` is what to do next, first step first; it never tells the reader
  to run `apply`, `drop`, or a baseline or state write on its own.
- With `--output json`, no progress line is printed on stderr, so a long
  `sst test --suite evals` stays silent until its envelope.
- An envelope that would carry a credential is replaced by one reporting
  `SST-PRT012`, and the run exits `1`.

`schema_version` changes only when a field is removed or changes meaning; new
fields can appear in the same version.

## Progress and colour in a CI log

With the default output, a run on a terminal shows one live progress line on
stderr after its first second. A CI runner has no terminal, so `sst` prints
progress there only when `$CI` is set, as GitHub Actions, GitLab CI and most
runners set it: a line as each slow phase ends, a heartbeat every 15 seconds
while one runs, and, for a command that connects, the account and role it
connected as. Library log records stay below `--log-level`, which defaults to
`warn`; `--log-level info` shows the Snowflake connector's request logs again.
Colour is off without a terminal; `--color always` (or `SST_COLOR=always`, or
`FORCE_COLOR=1`) keeps it for a log viewer that renders ANSI.

A coding agent that runs `sst` in a pipeline step gets `--output agent` on its
own when it sets its marker variable; set `SST_OUTPUT` to choose the format
for every step.

## Exit codes

| Code | Meaning |
|---:|---|
| 0 | Success. For `plan`, nothing to change: every artifact's live object carries the marker state records, and the properties plan compares (see [Concepts](../concepts.md#how-plan-decides-each-change)) are as apply read them back. Anything else about an object, such as its grants, is not compared. |
| 1 | Errors were reported, or an apply, a test suite, or a check failed. |
| 2 | Declared and actual state differ: `plan` found changes, `diff` found the two states differ, or `format --check`, `enrich --check`, `docs --check`, `migrate refs`, or `migrate split` found something to rewrite. |
| 3 | The command line is invalid. |
| 4 | The project, its configuration, or a saved plan cannot be used. |
| 5 | Snowflake could not be reached, rejected the credential, refused a privilege, or did not finish in time. The diagnostic code says which: a refused privilege (`SST-PRT004`, or an `SST-SNO` privilege code) needs a grant, not a retry. |
| 130 | The run was interrupted: Ctrl-C, `SIGTERM` (a cancelled job), or `SIGHUP`, or a prompt was declined. Any command can return it. `apply` records what finished and releases its lock first. |

A pipeline that runs `sst format --check` gates on canonical YAML the same way
it gates `plan`: `2` means a file would be rewritten. The
[CLI reference](../reference/cli.md#exit-codes) lists every code, and which
commands can return it.
