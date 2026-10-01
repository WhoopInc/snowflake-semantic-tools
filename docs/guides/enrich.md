# Enriching models

`sst enrich` fills the column metadata a semantic view needs from what the warehouse holds,
and writes it into the dbt model YAML in place. It reads each model's relation for its columns
and types, and fills what the YAML leaves out. It never writes to Snowflake.

```bash
sst enrich models/marts                          # column types and data types
sst enrich --select model:orders --include all   # everything, for one model
sst enrich --include sample-values --check       # would anything change? exit 2 if so
sst enrich --include synonyms --dry-run          # show each change as a diff
```

## Components

`--include` names what a run fills; it is repeatable and takes comma-separated lists. Without
it, a run fills `column-types` and `data-types`, which need no row data and no model call.

| Component | Writes under `meta.sst` | From |
|---|---|---|
| `column-types` | `column_type`: `fact` for a numeric column, else `dimension` | the relation's type; a key is always a dimension |
| `data-types` | `data_type`, as INFORMATION_SCHEMA names it (`TEXT`, `NUMBER`, `TIMESTAMP_NTZ`) | the relation |
| `sample-values` | `sample_values`, and `is_enum` beside them on a dimension | the column's most frequent distinct values |
| `enums` | `is_enum` for values already written; includes `sample-values` | the same query |
| `column-synonyms` | `synonyms` | a Cortex model |
| `table-synonyms` | `synonyms` in each semantic view's `table_config` for the model | a Cortex model |

`synonyms` names both synonym components, and `all` names every component.

A key is a column the model declares in `primary_key` or `unique_keys`, or one a `unique`,
`unique_combination_of_columns`, or `relationships` test names, so a numeric identifier is not
summed as a fact. `time_dimension` is never derived; write it by hand where you want one.

## What it keeps

Enrich fills only what is missing. A value already written is kept unless `--force` names its
component; forcing a component includes it. `sample_values: []` counts as written, and
`synonyms: []` as missing.

- **Columns come from the warehouse.** A column the relation has and the YAML does not
  describe is added, in the relation's order. A column the YAML describes and the relation
  lacks is reported (`SST-VAL325`) and left in place.
- **dbt's contract type wins.** A column with dbt's own `data_type` gets no `meta.sst.data_type`.
  A written `meta.sst.data_type` the relation contradicts is reported (`SST-VAL327`), not
  changed, unless `--force data-types`.
- **Excluded columns are left alone.** A column with `meta.sst.exclude: true` is not read or
  written.
- **PII is never sampled.** A column carrying `pii_tags` in its meta, of any category, is not
  sampled, and its values are never sent to Cortex. Sample values still on such a column are
  reported (`SST-VAL328`) when the run reads row data.

## Sample values and enums

A sampled column's distinct non-null values are read most frequent first, ties in text order,
so the same data always gives the same values. When the column has no more distinct values
than `enrichment.distinct_limit` and every one is usable, all of them are written with
`is_enum: true`. Otherwise the most frequent `sample_values_display_limit` are written with
`is_enum: false`, which tells validation the list is a sample (`SST-VAL315` is not reported).
A fact gets sample values and never `is_enum`.

A value is never written when it is blank, a missing-value placeholder such as `nan`, longer
than 500 characters, spread over several lines, or holds template syntax; a column with such a
value is not an enum. Only scalar types are sampled: text, numbers, booleans, dates, times, and
timestamps.

Set `enrichment.allow_sample_value_collection: false` to forbid reading row data in a project:
a run that includes `sample-values` or `enums` then stops before reading anything
(`SST-CFG038`), and no setting can demote that error.

## Synonyms

Synonyms are written by `enrichment.synonym_model` through `AI_COMPLETE`, at temperature 0,
with structured output, inside the account. A prompt describes up to 50 columns: their names,
types, descriptions, and up to five example values from columns without `pii_tags`. Each
synonym is cleaned before it is written: whitespace collapsed, quotes and template syntax
refused, and any synonym that repeats the column's own name, or another column's name or
synonym, dropped. At most `enrichment.synonym_max_count` are kept.

A table's synonyms are generated once per model and written into every semantic view that
uses the model and has none for it, each cleaned against that view's other tables.

## Selecting models

`PATH` arguments, relative to the project, select the models whose SQL or YAML file is under
them. `--select` and `--exclude` take `model:<name>`, a bare name, or a glob such as
`model:fct_*`, and repeat. Only the project's own models are enriched, never an installed
package's; an ephemeral model has no relation to read (`SST-DBT031`).

`--database` and `--schema` read every relation from another database or schema, such as a
production clone, while the YAML stays where it is.

## Writing files

Every edit is computed in memory before any file is written. The YAML is edited in place:
comments, key order, quoting, and the file's indentation are kept, and only the keys enrich
sets change. A new `meta.sst` block goes under `config.meta.sst`; an existing one is edited
where it is. A file whose layout cannot be reproduced exactly, usually because it mixes
indentation styles, is reported (`SST-PRS125`) and written in one consistent style; review the
diff with `--dry-run`.

| Flag | Effect |
|---|---|
| `--check` | Write nothing; exit `2` when a file would change. `--no-detailed-exitcode` makes it `0`. |
| `--dry-run` | Write nothing; print each change as a unified diff. |
| `--fail-fast` | Stop at the first model that fails, and write nothing. |

A model whose relation is missing or not visible (`SST-SNO030`), or whose sampling or Cortex
call fails (`SST-SNO031`), gains nothing; the others are still written, and the run exits `1`.
A connection, transient, or privilege failure stops the run with exit `5`.

## Configuration

```yaml
enrichment:
  distinct_limit: 25                 # distinct values sampled; no more than this is an enum
  sample_values_display_limit: 10    # values written for a column that is not an enum
  synonym_model: mistral-large2      # Cortex model for synonyms
  synonym_max_count: 4               # synonyms per column and per table
  allow_sample_value_collection: true
```

See the [configuration reference](../reference/config.md) for each key's bounds. An error in
this block, such as a limit out of bounds, stops a run before it reads anything; enrich reports
the block's warnings and leaves the rest of the file to `sst validate`.

## Coming from 0.3

| 0.3 | 1.0 |
|---|---|
| `sst enrich models/x` | `sst enrich models/x` |
| `--models a,b` / `-m` | `--select model:a --select model:b` |
| `--column-types`, `--data-types`, `--sample-values`, `--detect-enums` | `--include column-types,data-types,sample-values,enums` |
| `--table-synonyms`, `--column-synonyms`, `--synonyms` | `--include table-synonyms`, `column-synonyms`, `synonyms` |
| `--all` | `--include all` |
| `--force-synonyms`, `--force-column-types`, `--force-data-types`, `--force-all` | `--force synonyms`, `column-types`, `data-types`, `all` |
| `-d`, `-s`, `-t` | `--database`, `--schema`, `--target` |
| `--include-sources`, `--sources-only`, `--source` | not supported: 1.0 enriches dbt models only |
| `--allow-non-prod` | not needed: enrich reads the manifest of the target you pass |

Each removed flag is refused with what replaces it. Two behaviours differ: `time_dimension` is
no longer derived, and table synonyms are written into the semantic views' `table_config`
rather than the model YAML.
