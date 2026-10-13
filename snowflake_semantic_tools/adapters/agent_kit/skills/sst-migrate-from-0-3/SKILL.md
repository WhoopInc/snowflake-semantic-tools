---
name: sst-migrate-from-0-3
description: |-
  Use when moving a project written for SST 0.3 to this SST: replacing `sst deploy`,
  `sst generate`, and `sst extract`, running `sst migrate refs`, renaming the 0.3 keys
  `sst validate` reports, and publishing alongside a 0.3 deployment. Do NOT use for a
  project already on 1.0 (use sst-core), or for fixing 1.0 diagnostics unrelated to 0.3
  spellings (use sst-fix-diagnostics).
---
# Migrating from SST 0.3

The full guide is @SST_DOCS@/guides/migrating-from-0.3.md. The authoring format keeps its
shape: the same six YAML root keys, `config.meta.sst` in dbt, and `sst_config.yml`. Every
0.3 spelling is an error that names its replacement, so nothing written for 0.3 publishes
until it is migrated.

## The order

1. Work on a branch, with this SST installed.
2. Rewrite references, reviewing the report before writing:

   ```bash
   sst migrate refs            # report only: exit 2 when rewrites are pending
   sst migrate refs --write    # rewrite in place
   sst migrate refs            # exit 0: nothing left
   ```

   It turns `{{ table('x') }}` into `{{ ref('x') }}` and `{{ column('x', 'y') }}` into
   `{{ ref('x', 'y') }}`, makes relationship endpoints bare model names, and labels boolean
   filters. A `table()` call where no rewrite is safe, such as in a description, is printed
   as `file:line:column` and exits 1 until fixed by hand.
3. Rename what `sst validate --no-snowflake-syntax-check` reports, one code at a time,
   with `sst explain <CODE>` for each:

   | 0.3 | This SST | Code |
   |---|---|---|
   | `sql_generation`, `question_categorization` | `ai_sql_generation`, `ai_question_categorization` | `SST-VAL012` |
   | `visibility` on a metric | `access_modifier: public_access` or `private_access` | `SST-VAL122` |
   | `relationship_columns`, `left_column`, `right_column` | `relationship_conditions` | `SST-PRS021` |
   | `non_additive_by`, `order`, `nulls`, window `column` / `direction` | `non_additive_dimensions`, `sort_direction`, `null_order`, `ref` | `SST-PRS020` |
   | `relationship_type`, `join_type` | delete them | `SST-VAL211` |
   | `primary_key` as a string | a list of columns | `SST-DBT032` |
   | `deploy:` in `sst_config.yml` | `apply:` | `SST-CFG200` |
   | a key only 0.3 read | what the message names | `SST-CFG043` |

4. Read the warnings before turning on `validation.strict: true`: 0.3 never enforced it,
   so it promotes every accumulated warning at once.
5. Publish into a **new schema**: objects 0.3 published carry no 1.0 ownership record, so
   the first plan over them reports each as unmanaged (`SST-PLN024`) and will not apply.
   Change `semantic_views.+schema` (and the other types'), then `sst compile` and
   `sst plan`; applying is the user's call. The 0.3 objects are dropped by hand once nothing
   reads them.
6. In CI, replace `sst deploy` and `sst generate` with `sst compile`, `sst plan`, then
   `sst apply --plan target/sst/plan.json --yes`.

## Commands that changed

- `sst deploy`, `sst generate`, `sst extract`, and `sst migrate-meta` no longer exist
  (exit 3).
- `--format json` is now the global `--output json`, and `-v` means `--verbose`.
- `--views`, `--models` become `--select` and `--exclude`; `--skip-validation` becomes
  `--no-validate` on `plan` and `apply`.
- `sst drop` drops exactly one object SST owns, named in full, with `--type`, `--target`,
  and `--yes`:
  `sst drop <db>.<schema>.<name> --type semantic_view --target <target> --yes`.
  Never run it without the user's say.
- A 0.3 diagnostic code still explains: `sst explain` names the codes it became.
