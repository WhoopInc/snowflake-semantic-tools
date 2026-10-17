---
name: sst-migrate-from-0-3
description: |-
  Use when moving a project written for SST 0.3 to this SST: replacing `sst deploy`,
  `sst generate`, and `sst extract`, running `sst migrate split` and `sst migrate refs`, renaming the 0.3 keys
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
2. Split each member list into one file per member, reviewing the report before writing:

   ```bash
   sst migrate split           # report only: exit 2 when a split is pending
   sst migrate split --write   # write <name>.yml per member, then remove the list
   ```

   It also moves tool group members to `tools/<group>/<name>.yml` (a group's `immutable`
   becomes a `tools.<group>.+immutable` route) and moves `skills.catalog`, `skills.stage`
   and `skills.extensions` in `sst_config.yml` to the `skills:` root and the `plugins:`,
   `profiles:` and `extensions:` blocks. It never overwrites a file: one in the way leaves
   that list whole and exits 1.
3. Rewrite references, reviewing the report before writing (either command may run first):

   ```bash
   sst migrate refs            # report only: exit 2 when rewrites are pending
   sst migrate refs --write    # rewrite in place
   sst migrate refs            # exit 0: nothing left
   ```

   It writes every bare reference as its template call (`relationship()`, `metric()`,
   `tag()`, `skill()`, `file()`, `tool()` with one argument), moves old item settings under
   `config:`, writes tool columns as refs, writes a view's `columns` / `exclude_columns`
   entries `x.y` (or a bare `y` in a view of one model) as `{{ ref('x', 'y') }}` and renames
   its `dimensions:` to `columns:`, and turns `{{ table('x') }}` into `{{ ref('x') }}` and `{{ column('x', 'y') }}` into
   `{{ ref('x', 'y') }}`, writes relationship endpoints as `{{ ref('x') }}`, drops members'
   `tables:` and `derived:`, turns `sql_file:` into `sql: "{{ file('...') }}"`, and labels
   boolean filters. It does not rewrite verified-query SQL: change each table it reads by name
   to `{{ ref('<model>') }}` (`SST-VAL414`). A `table()` call where no rewrite is safe, such as in a description, is printed
   as `file:line:column` and exits 1 until fixed by hand.
4. Rename what `sst validate --no-snowflake-syntax-check` reports, one code at a time,
   with `sst explain <CODE>` for each:

   | 0.3 | This SST | Code |
   |---|---|---|
   | `sql_generation`, `question_categorization` | `ai_sql_generation`, `ai_question_categorization` | `SST-VAL012` |
   | `visibility` on a metric | `access_modifier: public_access` or `private_access` | `SST-VAL122` |
   | `relationship_columns`, `left_column`, `right_column` | `relationship_conditions` | `SST-PRS021` |
   | `non_additive_by`, `order`, `nulls`, window `column` / `direction` | `non_additive_dimensions`, `sort_direction`, `null_order`, `ref` | `SST-PRS020` |
   | `relationship_type`, `join_type` | delete them | `SST-VAL211` |
   | `primary_key` as a string | a list of columns | `SST-DBT032` |
   | a view's `table_config` | the model's `meta.sst.synonyms`, `distinct_range` and `description` | `SST-PRS126` |
   | `deploy:` in `sst_config.yml` | `apply:` | `SST-CFG200` |
   | a key only 0.3 read | what the message names | `SST-CFG043` |

5. Read the warnings before turning on `validation.strict: true`: 0.3 never enforced it,
   so it promotes every accumulated warning at once. A metric 0.3 silently left out of a
   view is now reported: one that attaches to no view is `SST-MEM005` (with `SST-MEM010`),
   a warning; a kept metric whose `using_relationships` the view removes or lacks is
   `SST-VAL332`, one reading a column the scope removes `SST-VAL318`, and one built on a
   metric the scope removes `SST-VAL336`, all errors. A filter or verified query reading a
   removed column is left out with `SST-VAL337` (info).
6. Publish into a **new schema**: objects 0.3 published carry no 1.0 ownership record, so
   the first plan over them reports each as unmanaged (`SST-PLN024`) and will not apply.
   Change `semantic_views.+schema` (and the other types'), then `sst compile` and
   `sst plan`; applying is the user's call. The 0.3 objects are dropped by hand once nothing
   reads them.
7. In CI, replace `sst deploy` and `sst generate` with `sst compile`, `sst plan`, then
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
