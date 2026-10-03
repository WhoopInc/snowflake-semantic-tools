# Migrating from SST 0.3

SST 1.0 keeps the shape of 0.3's authoring format: the same six YAML root keys,
`config.meta.sst` column metadata in dbt, and `sst_config.yml`. It reads only the
1.0 spellings, though. Every 0.3 spelling is an error that names its replacement,
so nothing written for 0.3 is read differently or silently dropped. Most of the
mechanical work is done by one command; the rest is renaming keys.

## The short version

1. Install 1.0 on a branch and put `profiles.yml` in the project root.
2. Run `sst migrate refs`, review the report, then `sst migrate refs --write`.
3. Run `sst validate` and rename the keys it reports (`SST-PRS020`, `SST-DBT032`,
   `SST-CFG043`).
4. Point publishing at a new schema, then `sst compile`, `sst plan` and
   `sst apply`.
5. Replace `sst deploy` and friends in CI with `sst compile`, `sst plan` and
   `sst apply`.

## Commands

| SST 0.3 | SST 1.0 |
|---|---|
| `sst deploy`, `sst generate` | `sst compile`, `sst plan`, then `sst apply --plan target/sst/plan.json --yes` |
| `sst generate --dry-run` | `sst plan`, or `sst compile --emit-ddl <dir>` for DDL files |
| `sst diff` | `sst plan`, which exits `2` when there are changes |
| `sst drop` | `sst plan --prune`, then `sst apply --prune` |
| `--models` / `-m` | `--select`, with `--exclude` |
| `sst extract` | no replacement: 1.0 compiles from the dbt manifest and records what it published in its state table |
| `sst enrich` | `sst enrich`, with components named by `--include` and `--force`; see [Enriching models](enrich.md#coming-from-03) |
| `sst format`, `sst migrate-meta` | not part of 1.0; author column metadata in the dbt YAML |
| `sst validate`, `sst compile`, `sst init`, `sst debug`, `sst list`, `sst clean` | still present, with new options |
| `import snowflake_semantic_tools...` | no Python API: the package exposes only `__version__`, and the `sst` command is the interface |

`sst plan` and `sst apply` read the manifest `sst compile` writes, and refuse
without one (`SST-MAN001`). The [CLI reference](../reference/cli.md) lists every
1.0 command and flag.

## `profiles.yml` moves into the project

SST 1.0 reads `profiles.yml` from the project root and runs `dbt parse` with
that directory as `--profiles-dir`. It does not look in `~/.dbt/`. Commit the file
with every secret behind `{{ env_var() }}`, or write it in CI; see
[Configuration](configuration.md#authentication).

`sst_config.yml` is read from the project root only: 1.0 does not search parent
directories, accept `.sst_config.yml` or `sst_config.yaml`, or fall back to a
file in the home directory.

## `sst migrate refs`

0.3 accepted two global functions that 1.0 rejects: `{{ table('x') }}` and
`{{ column('x', 'y') }}` (`SST-REF034`, `SST-REF035`). `sst migrate refs`
rewrites them, rewrites `{{ ref('x') }}` relationship endpoints to the bare model
name (`SST-REF045`), and labels boolean filters, in place and without touching
anything else in the file:

| 0.3 | 1.0 |
|---|---|
| `- "{{ table('orders') }}"` in a `tables:` list | `- "{{ ref('orders') }}"` |
| `left_table: "{{ table('orders') }}"` or `"{{ ref('orders') }}"` | `left_table: orders` |
| `{{ column('orders', 'order_id') }}` | `{{ ref('orders', 'order_id') }}` |
| a boolean filter with no `labels:` | the same filter with `labels: [filter]` |

```bash
sst migrate refs            # report only: exit 2 when rewrites are pending
sst migrate refs --write    # rewrite in place
sst migrate refs            # exit 0: nothing left to do
```

A `table()` call anywhere else, such as inside a description, is reported with
its file and line and left alone, and the command exits `1` until it is fixed by
hand. Running the command twice changes nothing the second time.

## Spellings that changed

1.0 does not read these 0.3 spellings. Each is an error (`SST-PRS020`) that names
the 1.0 key, and `sst migrate refs` does not rename them: they are short edits
that deserve review.

| 0.3 | 1.0 |
|---|---|
| `sql_generation` | `ai_sql_generation` |
| `question_categorization` | `ai_question_categorization` |
| `relationship_columns` pairs | `relationship_conditions`, one `{{ ref('a', 'x') }} = {{ ref('b', 'y') }}` string per pair |
| `visibility` on a metric | `access_modifier: public_access` or `private_access` |
| `non_additive_by` on a metric | `non_additive_dimensions` |
| `order` / `nulls` on a non-additive entry | `sort_direction: ascending \| descending` / `null_order: first \| last` |
| `column` / `direction` on a window `order_by` entry | `ref` / `sort_direction` |

In the dbt YAML, `meta.sst.primary_key` is a list of columns and
`meta.sst.unique_keys` a list of column lists. The 0.3 forms (a column name, a
comma-separated string, a flat list of names) are errors (`SST-DBT032`) and are
not read.

A metric's `window:` block keeps 0.3's structure with 1.0 spellings, and `expr`
is now the window function itself, applied to a metric or an aggregate; the
[semantic views guide](semantic-views.md#window-function-metrics) shows the
shape. A 0.3 `window_function:` key on a non-additive entry was never read; the
entry's `sort_direction` says which snapshot counts.

## Keys 1.0 does not read

Every key SST does not read is reported, so a setting cannot look as though it
takes effect when it does not:

- an unknown key on a semantic view, metric, filter, relationship, verified
  query, or custom instruction, or under a dbt model's `meta.sst`, is a warning
  (`SST-PRS004`), and an error under `--strict`;
- a `sst_config.yml` key only 0.3 read is a removed key, an error that says what
  replaced it (`SST-CFG043`): `deploy:` (now `apply:`),
  every `generation` key but `generation.threads`, `defer`, `validation.exclude_dirs`,
  and `apply.fail_fast` (now the `--fail-fast` flag);
- `generation.threads` is still read: it is how many Snowflake sessions `plan`,
  `apply` and `test` work on at once, unless `--threads` or `SST_THREADS` says;
- a key reserved for a later release is an error until SST reads it
  (`SST-CFG044`).

The [configuration reference](../reference/config.md) lists the unsupported and
removed keys.

## Checks 0.3 did not make

Run `sst validate` after the codemod. Each of these can surface a problem that
was shipping silently:

- **Unknown references are errors.** 0.3 turned an unresolvable `ref()` into an
  upper-cased name and published it.
- **Empty `tables:` is an error.** 0.3 treated an empty list inconsistently,
  sometimes as every view.
- **Ambiguous join paths are errors** for the metrics that cross them
  (`SST-VAL116`); add `using_relationships:`.
- **`validation.strict: true` is enforced.** In 0.3 the key had no effect on
  deploys. Turning it on promotes every accumulated warning at once, so read the
  warnings first.
- **`validation.snowflake_syntax_check` defaults to `true`,** so `validate`
  compiles expressions against Snowflake unless you pass
  `--no-snowflake-syntax-check` or set the key to `false`.

## Publishing over a 0.3 deployment

SST 1.0 changes only objects it published itself, and objects 0.3 published do
not carry 1.0's ownership record. The first `plan` against a schema 0.3 wrote to
therefore reports each existing view as unmanaged (`SST-PLN024`) and will not
apply over it.

Publish 1.0 into a new schema instead: change `semantic_views.+schema` (and the
other types' `+schema`), apply, and move consumers to the new objects. When
nothing reads the 0.3 objects any more, drop them yourself. Running both
versions side by side for a while also gives you a direct comparison of the
rendered views.

## Output for tools

- `--output json` prints one envelope with `schema_version: 2`; see
  [CI/CD](ci-cd.md#json-output).
- Diagnostic codes changed, and 0.3's codes are not mapped: tooling that matched
  a 0.3 code should match the 1.0 code in the
  [error code reference](../reference/error-codes.md) instead.
- The SST manifest is written to `target/sst/manifest.json`.
