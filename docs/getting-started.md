# Getting started

This guide takes a dbt project to a published Snowflake semantic view. The
commands it uses -- `validate`, `plan`, `apply` -- are the same ones every later
change goes through.

## Requirements

- Python 3.11, 3.12, or 3.13.
- A dbt project on the Snowflake adapter whose `dbt parse` writes manifest
  schema v12: dbt Core 1.10 to 1.13, or dbt v2 2.0. Every change to SST runs
  `dbt parse` with each of them on its reference project and checks the
  manifest each writes; SST reports `SST-DBT017` for any other manifest schema.
- A Snowflake role that can create semantic views in the schema you publish to,
  select from the tables those views read, and create tables in the state
  schema (SST's state table and its lock table). [Security](guides/security.md#operating-sst)
  lists the optional grants.

Everything up to `sst compile` in step 7 runs offline; `sst plan`, `sst apply`,
and `sst test --suite smoke` connect.

## 1. Install

```bash
python -m pip install snowflake-semantic-tools
sst --version
```

SST runs whichever `dbt` is on `PATH`. If the environment has none, install
`snowflake-semantic-tools[dbt]` instead, which adds dbt Core 1.x with the
Snowflake adapter. For dbt v2, install it yourself beside SST (`pip install
dbt`, or `dbt-oss`); see [Using dbt v2](design/dbt-compatibility.md#using-dbt-v2).

## 2. Put `profiles.yml` in the project

Keep `profiles.yml` **in the project root**, the directory that holds
`dbt_project.yml`. SST takes the first `profiles.yml` it finds in
`--profiles-dir` (or `$SST_PROFILES_DIR`), `$DBT_PROFILES_DIR`, the project
root, then `~/.dbt`, and passes that directory to `dbt parse` as
`--profiles-dir`, so dbt and SST read the same targets. Keep secrets out of the
file with `env_var()`:

```yaml
jaffle_shop:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      authenticator: externalbrowser
      role: ANALYTICS_DEV
      warehouse: ANALYTICS_WH
      database: ANALYTICS_DEV
      schema: SEMANTIC
```

The profile name must match `profile:` in `dbt_project.yml`. The target's
`database` and `schema` are where SST publishes by default and where it keeps
its state table. See [Configuration](guides/configuration.md#authentication) for
the other authentication methods.

## 3. Scaffold

```bash
sst init
sst debug --no-connect
```

`sst init` writes an `sst_config.yml` that sets the validation, enrichment,
governance, apply, and state keys to their defaults,
and a `semantic_models/semantic_views/` directory. It never overwrites a file
that exists; run interactively, it asks for the semantic models directory first
(`--skip-prompts` takes the default). `sst debug` prints the configuration file,
profile, target, database, schema, and state table SST resolved:

```text
target:
  profile: jaffle_shop
  name: dev
  profiles_file: profiles.yml
  role: ANALYTICS_DEV
  warehouse: ANALYTICS_WH
  database: ANALYTICS_DEV
  schema: SEMANTIC
  state_table: ANALYTICS_DEV.SEMANTIC.SST_STATE
  authentication: externalbrowser
connection:
  tested: False
```

Without `--no-connect` it also opens a Snowflake session, and exits `5` when
that fails.

## 4. Describe columns in dbt

Semantic views are built from dbt models, and the column metadata lives in the
model YAML under `config.meta.sst`:

```yaml
models:
  - name: orders
    description: One row per order.
    config:
      meta:
        sst:
          primary_key: [order_id]
    columns:
      - name: order_id
        description: Surrogate key for the order.
        data_type: VARCHAR
        data_tests: [unique, not_null]
        config:
          meta:
            sst:
              column_type: dimension
              synonyms: [order number]
      - name: order_total
        description: Order value in cents, including tax.
        data_type: NUMBER(38,0)
        config:
          meta:
            sst:
              column_type: fact
      - name: ordered_at
        description: When the order was placed.
        data_type: TIMESTAMP_NTZ
        config:
          meta:
            sst:
              column_type: time_dimension
```

`column_type` is `dimension`, `fact`, or `time_dimension`. Every column a view
exposes needs a `data_type` (`SST-VAL309` otherwise), and typing every column
also stands in for a dbt contract (`SST-DBT024`). Give each one a description:
it is what Cortex Analyst reads to choose a column. The tests on the key keep
the untested-model warnings (`SST-DBT023`, `SST-VAL324`) away. Once the models
are built, `sst enrich` can fill the column and data types from the warehouse
instead; see [Enriching models](guides/enrich.md).

## 5. Write a semantic view and a metric

```yaml
# semantic_models/semantic_views/sales.yml
semantic_view:
  name: sales
  description: Orders and their value. Use for order counts and revenue.
  tables:
    - "{{ ref('orders') }}"
```

```yaml
# semantic_models/metrics/order_count.yml
metric:
  name: order_count
  description: Number of distinct orders placed.
  expr: "COUNT(DISTINCT {{ ref('orders', 'order_id') }})"
```

Each file holds one item under its type's singular root. A list under the plural
root (`semantic_views:`, `snowflake_metrics:`) is also read, for several items in
one file. Any YAML file under `semantic_models/` is read, and each member is
recognised by its root key; only views must live under `semantic_views/`. A metric's tables
are the models its expression names with `ref()`, and it joins every view whose
tables include all of them, so `order_count` lands in `sales` without the view
naming it.
[Concepts](concepts.md#association-how-members-find-views) explains this
association rule.

## 6. Validate

```bash
sst validate --no-snowflake-syntax-check
```

```text
0 errors; 0 warnings; 7 info; 7 info not shown (--show-info)
validated 1 artifact(s): 0 errors, 0 warnings
```

`validate` runs `dbt parse`, loads every artifact, and checks references, types,
and the rules each artifact type carries. By default
(`validation.snowflake_syntax_check: true`) it also connects and has Snowflake
compile each metric, dimension, and filter expression, every column typed as the
target's tables have it, and each verified query; `--no-snowflake-syntax-check`
stays offline and reports each skipped check as an info (`SST-VAL020`, shown with
`--show-info`).
If the project declares dbt packages, run `dbt deps` first (`SST-DBT026`).

Each diagnostic prints its code and a `docs:` link to its entry in the
[error code reference](reference/error-codes.md); with `--output json` the link
is the diagnostic's `help_url`. `sst explain <code>` prints the same entry
offline.

## 7. Compile and plan

```bash
sst compile
sst list
sst plan
```

```text
compiled 1 artifact(s); manifest sst_target/manifest.json
semantic_view:sales pending ANALYTICS_DEV.SEMANTIC.SALES 38edd49c...
```

`compile` renders every artifact and writes the SST manifest to
`sst_target/manifest.json`, offline. `sst_target/` is SST's build directory,
beside dbt's `target/` and fixed as dbt's is: add `sst_target/` to the
project's `.gitignore`, and `sst clean` removes it. `sst list` shows what it holds, and
`sst compile --emit-ddl <dir>` writes the `CREATE SEMANTIC VIEW` statement to
read. `plan` connects: it validates and compiles the project again, and refuses
to go on when `sst_target/manifest.json` was compiled for another target
(`SST-MAN006`) or no longer matches the project (run `sst compile` again). It
then reads what is live in Snowflake and saves the difference to
`sst_target/plan.json`. It never writes to Snowflake. It exits `2` when there
are changes and `0` when there are none, so CI can tell the two apart.

## 8. Apply

```bash
sst apply --plan sst_target/plan.json --yes
```

`apply` executes exactly the saved plan, and refuses one that no longer matches
the project; `--yes` skips the confirmation prompt. It holds the target's run
lock while it writes, so two applies never interleave, and records what it
published in the state table, so the next `plan` compares against what SST
actually wrote.

## 9. Check it

```bash
sst plan          # exits 0: nothing left to change
sst test --suite smoke
```

The smoke suite queries each published view, its metrics, and its verified
queries, and reports any that fail.

## Next

- [Concepts](concepts.md): artifacts, members, references, and the
  `validate` / `plan` / `apply` pipeline.
- [Semantic views](guides/semantic-views.md): relationships, filters, verified queries.
- [Configuration](guides/configuration.md): folder routes, an item's own `config:`,
  and the [precedence](guides/configuration.md#precedence) every setting resolves by.
- [Agents](guides/agents.md), [evals](guides/evals.md), [skills](guides/skills.md),
  and [plugins and profiles](guides/plugins-and-coco-desktop-profiles.md).
- [CI/CD](guides/ci-cd.md): the same four commands in a pipeline.
- Coming from SST 0.3: [Migrating from 0.3](guides/migrating-from-0.3.md).
