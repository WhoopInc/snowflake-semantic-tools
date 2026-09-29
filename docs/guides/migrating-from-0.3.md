# Migrating from SST 0.3

SST 1.0 keeps the 0.3 authoring format: the same six YAML root keys, the same
`config.meta.sst` column metadata in dbt, and the same `sst_config.yml`. What
changes is the command surface, a handful of spellings, and several checks that
0.3 skipped. Most of the mechanical work is done by one command.

## The short version

1. Install 1.0 on a branch and put `profiles.yml` in the project root.
2. Run `sst migrate refs`, review the report, then `sst migrate refs --write`.
3. Run `sst validate` and fix what it reports.
4. Point publishing at a new schema, then `sst plan` and `sst apply`.
5. Replace `sst deploy` and friends in CI with `sst plan` and `sst apply`.

## Commands

| SST 0.3 | SST 1.0 |
|---|---|
| `sst deploy`, `sst generate` | `sst plan`, then `sst apply --plan target/sst/plan.json --yes` |
| `sst generate --dry-run` | `sst plan`, or `sst compile --emit-ddl <dir>` for DDL files |
| `sst diff` | `sst plan`, which exits `2` when there are changes |
| `sst drop` | `sst plan --prune`, then `sst apply --prune` |
| `--models` / `-m` | `--select`, with `--exclude` |
| `sst extract` | no replacement: 1.0 compiles from the dbt manifest and records what it published in its state table |
| `sst enrich`, `sst format`, `sst migrate-meta` | not part of 1.0; author column metadata in the dbt YAML |
| `sst validate`, `sst compile`, `sst init`, `sst debug`, `sst list`, `sst clean` | still present, with new options |

The [CLI reference](../reference/cli.md) lists every 1.0 command and flag.

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
rewrites them, and labels boolean filters, in place and without touching
anything else in the file:

| 0.3 | 1.0 |
|---|---|
| `- "{{ table('orders') }}"` in a `tables:` list | `- "{{ ref('orders') }}"` |
| `left_table: "{{ table('orders') }}"` | `left_table: orders` |
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

These 0.3 spellings are still read, with a deprecation warning (`SST-PRS020`),
for one release. Setting both spellings on one entry is an error (`SST-PRS121`).

| 0.3 | 1.0 |
|---|---|
| `sql_generation` | `ai_sql_generation` |
| `question_categorization` | `ai_question_categorization` |
| `relationship_columns` | `relationship_conditions` |
| `deploy:` in `sst_config.yml` | `apply:` (`SST-CFG045`) |

`sst migrate refs` does not rename these; they are one-line edits that deserve
review.

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
- **Configuration keys are checked.** Unknown keys warn, removed keys are errors,
  and keys only 0.3 read are accepted with an info diagnostic. The
  [configuration reference](../reference/config.md#removed-keys) lists every
  removed key and what replaced it.

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
- Diagnostic codes changed. The
  [error code reference](../reference/error-codes.md#codes-from-sst-03) maps
  every 0.3 code to its 1.0 codes.
- The SST manifest is written to `target/sst/manifest.json`.
