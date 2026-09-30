# Getting started

This guide takes a dbt project to a published Snowflake semantic view. The
commands it uses -- `validate`, `plan`, `apply` -- are the same ones every later
change goes through.

## Requirements

- Python 3.11, 3.12, or 3.13.
- A dbt project on the Snowflake adapter whose `dbt parse` writes manifest
  schema v12. SST is tested with dbt 1.11 and 1.12 and reports `SST-PRT007` for
  any other manifest schema.
- A Snowflake role that can create semantic views in the schema you publish to,
  and create a table for SST's state.

## 1. Install

```bash
python -m pip install snowflake-semantic-tools
sst --version
```

Install `snowflake-semantic-tools[dbt]` instead if the environment does not
already have `dbt-snowflake`.

## 2. Put `profiles.yml` in the project

SST reads connection targets from `profiles.yml` **in the project root**, the
directory that holds `dbt_project.yml`, and passes that directory to `dbt parse`
as `--profiles-dir`. Keep secrets out of the file with `env_var()`:

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
sst debug
```

`sst init` writes a minimal `sst_config.yml` and a `semantic_models/semantic_views/`
directory, and never overwrites a file that exists. `sst debug` prints the
profile, target, database, schema, and state table SST resolved. Add
`--test-connection` to also open a Snowflake session and report its role.

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
        config:
          meta:
            sst:
              column_type: dimension
              synonyms: [order number]
      - name: order_total
        description: Order value in cents, including tax.
        config:
          meta:
            sst:
              column_type: fact
      - name: ordered_at
        description: When the order was placed.
        config:
          meta:
            sst:
              column_type: time_dimension
```

`column_type` is `dimension`, `fact`, or `time_dimension`. Give every column a
view exposes a description: it is what Cortex Analyst reads to choose a column.

## 5. Write a semantic view and a metric

```yaml
# semantic_models/semantic_views/sales.yml
semantic_views:
  - name: sales
    description: Orders and their value. Use for order counts and revenue.
    tables:
      - "{{ ref('orders') }}"
```

```yaml
# semantic_models/metrics/orders.yml
snowflake_metrics:
  - name: order_count
    description: Number of distinct orders placed.
    tables:
      - orders
    expr: "COUNT(DISTINCT {{ ref('orders', 'order_id') }})"
```

A metric joins every view whose tables include all of the metric's `tables:`,
so `order_count` lands in `sales` without the view naming it. [Concepts](concepts.md)
explains this association rule.

## 6. Validate

```bash
sst validate
```

`validate` runs `dbt parse`, loads every artifact, and checks references, types,
and the rules each artifact type carries. With a connection it also compiles
each expression against Snowflake; pass `--no-snowflake-syntax-check` to stay
offline. Each diagnostic prints its code; with `--output json` it also carries
a `help_url` into the [error code reference](reference/error-codes.md).

## 7. Compile and plan

```bash
sst compile
sst plan
```

`compile` renders every artifact and writes the SST manifest to
`target/sst/manifest.json`. `plan` reads that manifest, reads what is live in
Snowflake, and saves the difference to `target/sst/plan.json`; it refuses a
manifest compiled for another target or from an older project. It never writes
to Snowflake. It exits `2` when there are changes and `0` when there are none,
so CI can tell the two apart.

## 8. Apply

```bash
sst apply --plan target/sst/plan.json --yes
```

`apply` executes exactly the saved plan, and refuses one that no longer matches
the project. It records what it published in the state table, so the next
`plan` compares against what SST actually wrote.

## 9. Check it

```bash
sst plan          # exits 0: nothing left to change
sst test --suite smoke
```

The smoke suite queries each published view, its metrics, and its verified
queries, and reports any that fail.

## Next

- [Concepts](concepts.md): artifacts, members, references, and the compile pipeline.
- [Semantic views](guides/semantic-views.md): relationships, filters, verified queries.
- [Agents](guides/agents.md), [evals](guides/evals.md), [skills](guides/skills.md),
  and [plugins and profiles](guides/plugins-and-profiles.md).
- [CI/CD](guides/ci-cd.md): the same four commands in a pipeline.
- Coming from SST 0.3: [Migrating from 0.3](guides/migrating-from-0.3.md).
