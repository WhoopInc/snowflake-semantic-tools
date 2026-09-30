# `models/`: the dbt layer of the reference fixture

The dbt models behind the fixture's semantic views. SST never reads these files
directly: it reads dbt's compiled `target/manifest.json`. The per-model files point
here for what is true of the whole directory.

| File | Holds |
|---|---|
| `marts/<model>.sql` | the model's SQL |
| `marts/<model>.yml` | that model's `description`, `config.meta.sst` and `columns` |
| `staging/_sources.yml` | the `sources:` declaration, which belongs to no single model |
| `staging/stg_orders.sql`, `.yml` | the one staging model |

Every model has its own `.sql`. A `models:` entry with no matching SQL is a patch
with nothing to patch: dbt reports `Did not find matching node for patch`, the model
never reaches the manifest, and every `ref()` to it fails.

dbt reads `.md` files under `models/` for doc blocks, so keep Jinja tags out of this
file.

## The seam

The dbt model is the source of truth for columns. A semantic view names its tables,
never their columns; every column with a `config.meta.sst.column_type` becomes a
dimension or fact of each view that lists its model, carrying its synonyms, sample
values and `is_enum` with it.

## The domain

A jaffle shop: toasted sandwiches sold from a handful of locations. It was chosen
because readers already understand it, and its grain gives each join and metric shape
the fixture exercises a natural home. Its data is the few rows per table in
`../seeds/`.

## One-line descriptions

A column's `description` becomes its member's `COMMENT` in the rendered DDL, and the
repo's DDL goldens (`tests/golden/expected/ddl/`) hold one member per line. So every
column a semantic view reads has a one-line description: a plain statement about the
column, which Cortex Analyst reads. Explanations for people go in `#` comments. No
view lists `product_docs` or `stg_orders`, so their descriptions can run longer.

## The data rule

- SQL carries no data values: every literal is structural (a cast, a default or a
  boundary).
- `sample_values` are hand-authored synthetic values; nothing is collected from a
  warehouse.
- No sample value is identifier-shaped or a person's name. `customer_name` has none;
  `product_name` and `location_name` do, because they name things, not people.
- Placeholders such as `nan` or `null` are not sample values (`SST-VAL316`).

## Column metadata

| Key | Columns | Where |
|---|---:|---|
| `column_type` | 37 of 37 | every column of the seven marts a view reads |
| `synonyms` | 32 | every column of the six populated marts |
| `sample_values` | 22 | those six, except identifiers and `customer_name` |
| `is_enum` | 17 | dimensions only |
| `meta.sst.data_type` | 5 | `supplies` only |

The marts show three shapes:

- **Six are fully populated.**
- **`order_items` columns carry only `column_type`.** Its members render as name,
  expression and `COMMENT`, with no `WITH SYNONYMS`, `SAMPLE_VALUES` or `IS_ENUM`.
  It is the model that shows absent metadata is omitted, not emitted empty.
- **`product_docs` carries no `meta.sst`.** It backs the `menu_docs_search` Cortex
  Search service and no semantic view lists it, so its columns are not members and
  `SST-VAL308` (a column a view reads must declare `column_type`) does not apply.

The 15 columns without `sample_values` are the 12 identifiers, `customer_name`, and
the two non-identifier columns of `order_items`.

Timestamps are authored as `dimension`; `time_dimension` is also accepted and renders
the same way. Each timestamp outside `order_items` has two sample values, and in
`pricing_periods` each sampled `effective_end_at` is the next period's
`effective_start_at`, because the periods are half-open.

`is_enum: true` marks a closed set and requires `sample_values` (`SST-VAL314`);
`false`, on identifiers and name columns, says the set is open. Facts and timestamps
omit the key because it does not apply to them.

Six marts enforce a dbt contract, so their columns declare native `data_type`.
`supplies` has no contract and declares `meta.sst.data_type` instead, covering the
fallback. A column with neither is `SST-VAL309`; if both are set and disagree, dbt's
type wins and SST reports `SST-DBT004`.

## Model-level `config.meta.sst`

It holds only the grain: `primary_key` on the seven marts a view reads, and
`unique_keys` on `customers` alone. A model has one grain, so every view that reads
it gets the same keys: `products` renders `PRIMARY KEY (PRODUCT_ID)` in both
`jaffle_menu` and `jaffle_minimal`, though only `jaffle_menu` gives it a
`table_config`. Key columns must exist (`SST-VAL310`) and may not appear in both
lists (`SST-VAL223`).

| Not here | Why |
|---|---|
| table-level `synonyms` | set per view, under `table_config.<model>.synonyms` in `semantic_views.yml`, since two views may call one table different things |
| `cortex_searchable` | not a 1.0 key; an unknown `meta.sst` key on a model a view reads is `SST-PRS004` |
| `database`, `schema` | rejected (`SST-DBT030`): SST reads each relation's location from the manifest, after dbt has applied `+database` and `+schema` |
