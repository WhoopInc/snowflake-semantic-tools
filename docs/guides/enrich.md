# Enriching models

`sst enrich` fills the column metadata a semantic view needs from what the warehouse holds,
and writes it into the dbt model YAML in place. It reads each model's relation for its columns
and types, and fills what the YAML leaves out. It never writes to Snowflake, and it never
writes a description: a model's and its columns' descriptions are only read, into the synonym
prompts.

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
| `table-synonyms` | the model's own `synonyms`, which every view that holds it renders | a Cortex model |

`synonyms` names both synonym components, and `all` names every component. Names are matched
without case; an unknown one is refused with exit `3`.

A key is a column the model declares in `primary_key` or `unique_keys`, or one a `unique`,
`unique_combination_of_columns`, or `relationships` test names, so a numeric identifier is not
summed as a fact. `time_dimension` is never derived; write it by hand where you want one.

## What it keeps

Enrich fills only what is missing. A value already written is kept unless `--force` names its
component; forcing a component includes it. `sample_values: []` counts as written, and
`synonyms: []` as missing.

- **Columns come from the warehouse.** A column the relation has and the YAML does not
  describe is added, in the relation's order, when the run gives it a value. A column the YAML
  describes and the relation lacks is reported (`SST-VAL325`) and left in place.
- **Names match without case.** A YAML entry describes the relation column of the same name
  in any case. An added column is written lowercase when Snowflake stores its name unquoted
  (`ORDER_ID` becomes `order_id`), and exactly as stored when it was created quoted
  (`"MixedCase"`). Two relation columns that differ only in case share one entry.
- **`is_enum` is enrich's to write.** A hand-written `is_enum: true` on a column whose values
  the run would write, when the data shows more values than an enum holds, is reported
  (`SST-VAL317`) and kept, and the column's values are not written, unless `--force enums`.
- **Template syntax is never written.** dbt renders the YAML as Jinja, so a column whose name,
  or a value enrich would write for it, holds `{{`, `{%` or `{#` gets nothing, and is reported
  (`SST-SNO031`).
- **dbt's contract type wins.** A column with dbt's own `data_type` gets no `meta.sst.data_type`.
  A written `meta.sst.data_type` the relation contradicts is reported (`SST-VAL327`), not
  changed, unless `--force data-types`.
- **Excluded columns are left alone.** A column with `meta.sst.exclude: true` is not read or
  written.
- **Sensitive columns are never sampled.** A column carrying `pii_tags` in its meta, of any
  category, or `meta.sst.governance_tags`, or, when the run reads column tags, a sensitive tag
  in Snowflake (see [Governance tags](#governance-tags)), is not sampled, and its values are
  never sent to Cortex; its name, type and description still are when it is asked synonyms for.
  The Snowflake tag is checked on the relation's column, so a column the YAML does not describe
  yet is covered too. Sample values still on such a column are reported (`SST-VAL328`) when the
  run reads row data, and are never published: a sensitive column renders without
  `SAMPLE_VALUES` or `IS_ENUM`.

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

## Governance tags

A column can be marked sensitive in Snowflake instead of, or as well as, in its YAML. Name the
tags that count, and turn reading on:

```yaml
governance:
  sensitive_tags:
    - TAG_DB.PRIVACY.*                 # every tag in that schema
    - SNOWFLAKE.CORE.PRIVACY_CATEGORY
  read_column_tags: true
```

Each entry is a three-part tag name, or a database and schema followed by `.*`. An unquoted part
matches without case and a quoted one exactly; an entry that is neither is an error
(`SST-CFG057`) that no setting lowers, and stops the run before it reads anything. Nothing is
read until `read_column_tags` is true and some tag is named.

With both set, enrich reads `TAG_REFERENCES_ALL_COLUMNS` once per relation, after its columns.
Every tag it lists counts, whatever its level and apply method: one set on the column, and one
set on the table, schema or database that every column inherits, whether applied by hand,
propagated, or set by classification. A read that fails fails the model (`SST-SNO031`, "reading
column tags"): nothing is sampled or written for it, so no column the read would have protected
is read. A refused privilege stops the whole run, as for any other read. What a role that cannot
see a tag is answered has not been measured; the read fails closed only when Snowflake returns
an error ([Snowflake behavior](../design/snowflake-behavior.md#tag_references_all_columns-lists-inherited-tags-on-every-column)).

The sensitive tags found are written back to each column as `meta.sst.governance_tags`, sorted,
each named as Snowflake stores it (upper case unless the tag was created quoted), so review sees
them and an offline `compile` or `validate` treats the column as sensitive. The list is rewritten
to exactly what the run read: a tag Snowflake no longer lists is removed, and the key with it
when none is left. Enrich never writes `meta.pii_tags`, and never sets or unsets a tag in
Snowflake. A column the YAML does not describe gains an entry only to record its tags. The
connected `sst validate` reads the same tags and warns of each one a column's YAML records
neither as `pii_tags` nor in `meta.sst.governance_tags` (`SST-VAL338`).

SST computes no lineage. Snowflake's documentation describes tag propagation, set on each tag
with `PROPAGATE`, as what carries a column's tag to the views and tables built from it; SST does
not set it, and without it a downstream column carries only the tags applied to it or inherited
from its own table, schema or database.

## Synonyms

Before a run that fills synonyms reads anything, it asks the synonym model for a single
token. A model `AI_COMPLETE` refuses, such as an embedding model, a misspelt name, or a legacy
model the account never used, stops the run with `SST-SNO032`, and nothing is written; without
the check, every model would read the warehouse and then fail its synonyms. A model that
answers but that `SHOW CORTEX BASE MODELS` lists as legacy, past end-of-life, in preview,
`INTERNAL`, or not at all, is reported with `SST-SNO033` and the run goes on. Connected
`sst validate` reports the same lifecycle with `SST-VAL557`, without asking the model anything.
The default, `claude-sonnet-4-5`, is the GA model with the widest cross-region availability.

Synonyms are written by `enrichment.synonym_model` through `AI_COMPLETE`, at temperature 0,
with structured output, inside the account. A prompt describes up to 50 columns, and a wider
model is asked in batches: each column's name, type, description, and up to five example
values, each cut to 40 characters, from the values sampled in the run or already written,
never from a sensitive column. Each synonym is cleaned before it is written:

- whitespace is collapsed;
- one holding a quote, a control character or template syntax, or longer than 100
  characters, is refused and reported as a warning (`SST-PRS030`, at most
  `synonym_max_count` reports per column or table);
- one that repeats the column's own name (underscores read as spaces), another column's name
  or synonym, or an earlier synonym is dropped without a report.

At most `enrichment.synonym_max_count` are kept. A model's synonyms are a proposal: review
them in the diff before merging (see [Security](security.md#trust-model)).

A table's synonyms are generated once per model and written into the model's own `meta.sst`
when it has none, cleaned against the names and synonyms of every other table of each view that
uses it; `--force table-synonyms` replaces the ones it has.

## Selecting models

`PATH` arguments, relative to the project directory, select the models whose SQL or YAML file
is under them; a `PATH` outside the project is refused. `--select` and `--exclude` take
`model:<name>`, a bare name, or a glob such as `model:fct_*`, matched without case, and
repeat: one model or glob per flag, and take dbt's graph operators, as in
`+model:orders` for the models it is built from. A comma (`SST-PRT101`), a directory, or another kind of
selector such as `source:` (`SST-PRT105`) is refused with exit `3`.

Only the project's own models are enriched, never an installed package's. A name without a
wildcard that the manifest does not know is an error (`SST-DBT002`); an ephemeral model has no
relation to read (`SST-DBT031`, a warning). A selection that matches no model at all stops
with exit `4`.

`--database` and `--schema` read every relation from another database or schema, such as a
production clone, while the YAML stays where it is.

## Production targets

The YAML enrich writes is shared by every target, so it reads from production. A target whose
name has no `prod`, `production` or `prd` part, split on `_`, `-` and `.`, is refused with
exit 3 (`SST-PRT100`) before anything is read; pass `--target` for a production target, or
`--allow-non-prod` to enrich from this one anyway.

```bash
sst enrich --target prod models/marts
sst enrich --allow-non-prod models/marts         # from the default target, deliberately
```

## Writing files

Every edit is computed in memory before any file is written. The YAML is edited in place:
comments, key order, quoting, and the file's indentation are kept, and only the keys enrich
sets change. A new `meta.sst` block goes under `config.meta.sst`; an existing one is edited
where it is, a bare `meta.sst` winning over `config.meta.sst`. A model with no YAML file gets
one beside its SQL (`models/orders.sql` gains `models/orders.yml`). Files are written only
inside the project, never through a symbolic link. A file whose layout cannot be reproduced
exactly, usually because it mixes indentation styles, is reported (`SST-PRS125`) and written
in one consistent style; review the diff with `--dry-run`.

| Flag | Effect |
|---|---|
| `--check` | Write nothing; exit `2` when a file would change. `--no-detailed-exitcode` makes it `0`. |
| `--dry-run` | Write nothing; print each change as a unified diff. |
| `--fail-fast` | Stop at the first model that fails, and write nothing. |

A synonym model that refuses the test completion (`SST-SNO032`) stops the run before any read.
A model whose relation is missing or not visible (`SST-SNO030`), or whose sampling or Cortex
call fails or answers in another shape (`SST-SNO031`), gains nothing; the others are still
written, and the run exits `1`. A failure no model would get past (the connection or login
failing, a statement timing out or the connection dropping, a refused privilege:
`SST-PRT001` to `SST-PRT004`) stops the run with exit `5`, and nothing is written.

| Exit | When |
|---|---|
| `0` | Done, written or nothing to write. |
| `1` | An error was reported: a model failed, a name matched no model, `SST-CFG038`, or a file could not be written (`SST-PRT008`). |
| `2` | `--check` found a file that would change. |
| `3` | The command line is refused, including a target that is not production-like. |
| `4` | The project, its manifest or its configuration cannot be read, or the selection matches no model. |
| `5` | Snowflake could not be reached, rejected the credential, refused a privilege, or did not finish in time. |

The run prints one line per model (`orders: 2 columns added, column-types 9, data-types 9`,
or `unchanged`, or `failed`), then each file it wrote or would write (its diff under
`--dry-run`), then a count of files and models. With `--output json`, `data` holds
`components`, `forced`, `stopped`, `written`, one `models` entry per model (`status`
`enriched`, `unchanged` or `failed`; the columns `added`; how many columns each component
`filled`; the views given `table_synonyms`), and one `files` entry per file (`changed`,
`created`, `reformatted`).

## Offline and connected

Enrich reads the dbt manifest, running `dbt parse` unless `--manifest` names one, and then
the warehouse through the target's `profiles.yml` output. The session opens at the first
warehouse read, so a run refused before it (a target that is not production-like, an error in
the `enrichment:` block, `SST-CFG038`, a selection that matches nothing) never connects.
`--check` and `--dry-run` write nothing but read the warehouse and call Cortex like any other
run.

## Configuration

```yaml
enrichment:
  distinct_limit: 25                 # distinct values sampled; no more than this is an enum
  sample_values_display_limit: 10    # values written for a column that is not an enum
  synonym_model: claude-sonnet-4-5   # Cortex model for synonyms
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
| `--exclude dir1,dir2` (directories) | `--exclude` takes model names or globs, one per flag; pass only the `PATH`s you want |
| `--column-types`, `--data-types`, `--sample-values`, `--detect-enums` | `--include column-types,data-types,sample-values,enums` |
| `--table-synonyms`, `--column-synonyms`, `--synonyms` | `--include table-synonyms`, `column-synonyms`, `synonyms` |
| `--all` | `--include all` |
| `--force-synonyms`, `--force-column-types`, `--force-data-types`, `--force-all` | `--force synonyms`, `column-types`, `data-types`, `all` |
| `-d`, `-s` | `--database`, `--schema`; `-t` is still `--target` |
| `--manifest` | the global `--manifest` |
| `--verbose` / `-v` | the global `--verbose`; every change is reported without it, and `--output json` gives the full report |
| `--include-sources`, `--sources-only`, `--source` | not supported: 1.0 enriches dbt models only |
| `--allow-non-prod` | `--allow-non-prod`: still needed for a target that is not production-like |

Each removed flag is refused with exit `3` and what replaces it. Some behaviors differ:
`time_dimension` is no longer derived; and 0.3's `enrichment.generate_synonyms` and `generate_column_synonyms` keys are not
read (`SST-CFG003`): synonyms are generated only when `--include` names them.
