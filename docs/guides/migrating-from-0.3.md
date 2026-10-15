# Migrating from SST 0.3

SST 1.0 keeps the shape of 0.3's authoring format: the same six YAML root keys,
`config.meta.sst` column metadata in dbt, and `sst_config.yml`. Every 0.3
spelling is an error that names its replacement, so a project written for 0.3
cannot publish under 1.0 until it is migrated, and nothing written for 0.3
passes review looking as though it still takes effect. Most of the mechanical
work is done by two commands; the rest is renaming keys.

The same two commands carry a project written for an earlier 1.0 pre-release --
tool groups, settings outside `config:`, two-argument `tool()` calls -- to the
current forms.

## The short version

1. Install 1.0 on a branch.
2. Run `sst migrate split`, review the report, then `sst migrate split --write`.
3. Run `sst migrate refs`, review the report, then `sst migrate refs --write`.
4. Run `sst validate` and rename the keys it reports (`SST-PRS020`, `SST-PRS021`,
   `SST-VAL012`, `SST-VAL122`, `SST-VAL211`, `SST-DBT032`, `SST-CFG043`,
   `SST-CFG200`).
5. Point publishing at a new schema, then `sst compile`, `sst plan` and
   `sst apply`.
6. Replace `sst deploy` and `sst generate` in CI with `sst compile`, `sst plan`
   and `sst apply`.

## Commands

| SST 0.3 | SST 1.0 |
|---|---|
| `sst deploy`, `sst generate` | `sst compile`, `sst plan`, then `sst apply --plan target/sst/plan.json --yes` |
| `sst generate --dry-run` | `sst plan`, with `--sql-out <dir>` for the statements, or `sst compile --emit-ddl <dir>` for DDL offline |
| `--only-modified` | `--select state:modified --state <dir>` on `plan` and `apply` |
| `--skip-validation` | `--no-validate` on `plan` and `apply` |
| `--defer-target`, `--no-defer` | unchanged, on `validate`, `compile`, `plan` and `apply`, with the `defer:` keys |
| `--db`, `--schema` (where views publish) | `+database` and `+schema` in `sst_config.yml` |
| `--views` / `-v`, `--view` | `--select`; `-v` is now `--verbose` |
| `--models` / `-m` | `--select`, with `--exclude` |
| `--format json` / `-f` | `--output json` / `-o json` |
| `sst diff` | `sst diff`, which compares `local` (the compiled manifest), a dbt target, or a saved plan, and exits `2` when they differ; `sst plan` also exits `2` with changes pending |
| `sst drop <view>` | `sst drop <db>.<schema>.<name> --type semantic_view --target <target> --yes`: drops one object SST owns |
| `sst drop --prune` | `sst plan --prune`, then `sst apply --prune` |
| `sst format` | `sst format`, whose `--check` exits `2` rather than `1` when a file would change |
| `sst validate --exclude <dirs>` | `validation.exclude_dirs`; `--exclude` now takes artifact selectors |
| `sst extract`, `--extract-to-snowflake` | no replacement: 1.0 compiles from the dbt manifest and records what it published in its state table |
| `sst enrich` | `sst enrich`, with components named by `--include` and `--force`; see [Enriching models](enrich.md#coming-from-03) |
| `sst migrate-meta` | not part of 1.0; author column metadata in the dbt YAML |
| `sst validate`, `sst compile`, `sst init`, `sst debug`, `sst list`, `sst clean` | still present, with new options |
| `import snowflake_semantic_tools...` | no Python API: the package exposes only `__version__`, and the `sst` command is the interface |

`sst deploy`, `sst generate`, `sst extract` and `sst migrate-meta` are unknown
commands (exit `3`). The short options `-m`, `-a`, `-s`, `-d` and `-f`, which
0.3 bound, are reserved: each is refused with exit `3` and the flag to write
instead. `sst plan` and `sst apply` read the manifest `sst compile` writes, and
refuse without one (`SST-MAN001`). New in 1.0 are `sst migrate split`,
`sst migrate refs`, `sst baseline`, `sst test`, `sst explain` and `sst docs`. The
[CLI reference](../reference/cli.md) lists every 1.0 command and flag.

## Where `profiles.yml` and `sst_config.yml` are found

SST 1.0 reads the first `profiles.yml` it finds in `--profiles-dir` (or
`$SST_PROFILES_DIR`), `$DBT_PROFILES_DIR`, the project root, then `~/.dbt/`, and
runs `dbt parse` with that file's directory as `--profiles-dir`, so dbt and SST
read the same file. With none in any of them a run stops (`SST-CFG009`). See
[Configuration](configuration.md#authentication) for the fields it reads.

`sst_config.yml` is read from the project root only (`--project-dir`, else
`$SST_PROJECT_DIR`), or from the file `--config` or `$SST_CONFIG` names. 1.0
does not search parent directories, accept `.sst_config.yml`, or fall back to a
file in the home directory. `sst_config.yaml` is accepted; both spellings in one
project is an error (`SST-CFG005`).

## `sst migrate split` and `sst migrate refs`

The two commands are independent: each reads the files as they are and leaves
alone what the other changes, so they may run in either order, and the guide
runs `split` first. Each is a dry run until `--write`, exits `2` while it has
something to do and `0` once it has nothing, and exits `1` for anything it will
not change by itself, named on stderr. Neither needs a manifest or a
connection, and neither edits dbt YAML.

## `sst migrate split`

1.0 writes one member per file: `metric:`, `filter:`, `relationship:`,
`verified_query:`, `custom_instruction:`, `semantic_view:` and `tool:` each hold
one mapping with its `name:`. Lists under `snowflake_metrics:` and the other
list roots stay valid, and `sst migrate split` writes each listed member to
`<same folder>/<name>.yml` under the singular root, then removes the list:

- Each member keeps its comments: those above it, back to the member before,
  open its file, and the list file's own header opens the first one.
  Indentation is re-based to two spaces under the root key.
- A file holding anything besides member lists is left whole and named.
- A file it would write that already exists, unless it is the list itself,
  leaves that list whole: nothing is overwritten. So does a member another
  list writes to the same file.

Tool groups (`tools: [{group: <g>, define: [...], reference: [...]}]`) are gone.
Each group's members move to `<tools folder>/<g>/<name>.yml`, so a
`tools.<g>:` route in `sst_config.yml` routes that folder unchanged:

| A tool group's | becomes |
|---|---|
| `define:` member | a `tool:` file whose member writes no `relations:` |
| `reference:` member | a `tool:` file keeping its `relations:` |
| `owner:` | each member's `meta.owner`, unless it names its own |
| `description:` | a comment opening each member's file |
| `immutable: true` | a `tools.<g>: {+immutable: true}` route, or each member's `config.immutable` when the folder already holds other tool files |

A member's `{{ file() }}` path is re-based on its new folder.

In `sst_config.yml` it moves what left `skills:`:

| Earlier | Now |
|---|---|
| `skills.catalog.+database`, `+schema`, `+bundle_stage` | the same keys on `skills:`, and a new `plugins:` block with them and the root's `+certified` and `+version_prefix`, when the project has plugins |
| `skills.stage` | the top-level `profiles:` block |
| `skills.extensions` | the top-level `extensions:` block |
| `+flatten`, `+prune_deleted`, `+auto_compress`, `+layout`, `skills.catalog.+registry_table` | removed: their values were fixed |
| a routed block's `+key: null` | removed: an absent key is unset, and `null` is `SST-CFG054` |

A block that already exists at the top level is not overwritten; the move is
reported for a hand edit.

```bash
sst migrate split            # report only: exit 2 when a split is pending
sst migrate split --write    # write the new files, then remove the lists
```

## `sst migrate refs`

0.3 accepted two global functions that 1.0 rejects: `{{ table('x') }}` and
`{{ column('x', 'y') }}` (`SST-REF034`, `SST-REF035`). `sst migrate refs`
rewrites them, rewrites bare relationship endpoints to `{{ ref('x') }}`
(`SST-REF046`), drops the `tables:` and `derived:` keys SST now infers from each
member's templates, turns a verified query's `sql_file:` into a `file()` call
(`SST-PRS126`), and labels boolean filters (`SST-VAL405`), in place and without
touching anything else in the file. It also writes every other reference as
its template call and moves old item settings under `config:` (the tables
below):

| 0.3 | 1.0 |
|---|---|
| `- "{{ table('orders') }}"` in a view's `tables:` list, or in `tables: [...]` | `- "{{ ref('orders') }}"` |
| `left_table: orders`, or `"{{ table('orders') }}"` | `left_table: "{{ ref('orders') }}"` |
| `{{ column('orders', 'order_id') }}` | `{{ ref('orders', 'order_id') }}` |
| a metric's, filter's or verified query's `tables:` or `derived:` | dropped: the models its `ref()` calls name |
| `sql_file: sql/x.sql` | `sql: "{{ file('sql/x.sql') }}"` |
| a boolean filter with no `labels:` | the same filter with `labels: [filter]` |

Every field that names a member or a file takes one template call
([References](references.md)); a bare name is `SST-REF050`:

| Earlier | Now |
|---|---|
| `using_relationships: [orders_to_customers]`, a view's `relationships` / `exclude_relationships` entries | `"{{ relationship('orders_to_customers') }}"` |
| a view's `metrics` / `exclude_metrics` entries | `"{{ metric('<name>') }}"` |
| a view's or agent's tag `name: cost_center` | `name: "{{ tag('cost_center') }}"` |
| `non_additive_dimensions: [{dimension: d, table: m}]` | `[{ref: "{{ ref('m', 'd') }}"}]`, `sort_direction` and `null_order` kept; without `table:`, the one model the metric's `expr` names |
| `{{ tool('<group>', '<name>') }}`, anywhere | `{{ tool('<name>') }}` |
| an agent toolset written by bare `name: x` | the same, with `identifier: "{{ tool('x') }}"` |
| an agent plugin skill's `name: x` | `name: "{{ skill('x') }}"` |
| a defined tool member's `search_column`, `attribute_columns`, `on.search_column`, `on.attributes`, `id_column`, `title_column`, `relative_path_column` | `"{{ ref('<on model>', '<column>') }}"`; a referenced member's stay bare |
| `columns_and_descriptions: {c: {...}}`, on a tool member or a search tool | `columns: [{column: ..., ...}]`, each column as the member writes its columns |
| a tool's `body_file: tools/sql/x.sql`, read from the project root | `body: "{{ file('sql/x.sql') }}"`, relative to the tool file |
| a literal `@DB.SCHEMA.STAGE/path` on a stage member, in `stage_path` or `imports` | `"{{ tool('<stage>') }}/path"` |
| a hook's `script: run.sh` | `script: "{{ file('run.sh') }}"` |
| a command's frontmatter `skill: x` | `skill: "{{ skill('x') }}"` |
| an eval file's `agent:` | removed: its `evals/` folder says whose it is |

Settings an item wrote among its content move under its own `config:` block,
with their comments; a parent left empty goes, and one written with no value is
removed:

| Item | Earlier | Now |
|---|---|---|
| view | `enabled`, `tags`, `max_staleness` | `config.<same>` |
| agent | `alias`, `secure`, `enabled`, `tags` | `config.<same>` |
| agent | `spec.models.orchestration` | `config.orchestration_model` |
| agent | `spec.orchestration.budget.seconds` / `.tokens` | `config.budget_seconds` / `config.budget_tokens` |
| agent | `spec.orchestration.tool_not_accessible`, `spec.orchestration.capabilities.analytical_search` | `config.tool_not_accessible`, `config.analytical_search` |
| tool member | `warehouse`, `target_lag`, `embedding_model`, `execute_as` | `config.<same>` |
| tool member | `owner` | `meta.owner` |
| eval config | `agent_version`, `run.tier`, `run.retry`, `run.concurrency`, `run.baseline_runs` | `config.agent_version`, `config.eval_tier`, `config.retry`, `config.concurrency`, `config.baseline_runs` |
| eval config | `run.retention` (classes by variant) | `config.retention`: the class `run.variant` is listed under; `decision_window_days` has no successor |

A setting already set under `config:`, a `config:` written in flow style, or a
setting on the same line as an item's `- ` is reported and left for a hand
edit.

Verified-query SQL is not rewritten: a table it reads by name rather than with
`{{ ref() }}` is warned about (`SST-VAL414`) at its line, and is yours to change.

It reads every YAML file under `project.semantic_models_dir` and
`project.tools_dir`, each `agent.yml` and `evals/` file under
`project.agents_dir`, each `hook.yml` under `project.hooks_dir`, each command
under `project.commands_dir`, and the `tool()` calls of `sst_config.yml`,
skipping hidden directories; it never edits dbt YAML, and needs neither a
manifest nor a connection.

```bash
sst migrate refs            # report only: exit 2 when rewrites are pending
sst migrate refs --write    # rewrite in place
sst migrate refs            # exit 0: nothing left to do
```

The report names each file and what it would rewrite, for example
`would rewrite semantic_models/relationships/relationships.yml: 4 endpoint, 4 column`.
A `table()` call anywhere else, such as inside a description, and anything
else it will not rewrite safely -- a non-additive entry on a metric naming
several models, a column of a tool member whose `on:` names no model, a
`body_file` outside the project -- is printed to stderr as `file:line:column`
and left alone, and the command exits `1` until it is fixed by hand; `--write` still rewrites everything else. A file's line endings
are kept, and running the command twice changes nothing the second time.

## What needs a hand edit

After both commands, these are yours to review:

- **The baseline.** A baselined warning on a tool group names the group
  (`tool_group:<g>`) and its old file; it now names the member (`tool:<name>`)
  and the member's file. Run `sst baseline prune`, then add back the warnings
  you still accept.
- **Folder names.** Tool members land in a folder named for their group; rename
  the folders if another layout reads better, and move their `tools:` routes
  with them.
- **Comments.** A comment that described a list, a group or a moved key now sits
  above a member or a `config:` key; reword what no longer reads right.
- **Everything reported on stderr**, and the 0.3 spellings below.

## Spellings that changed

`sst migrate split` and `sst migrate refs` do not rename these: they are short edits that deserve
review. Each is an error that names the 1.0 key.

| 0.3 | 1.0 | Code |
|---|---|---|
| `sql_generation` on a custom instruction | `ai_sql_generation` | `SST-VAL012` |
| `question_categorization` on a custom instruction | `ai_question_categorization` | `SST-VAL012` |
| `visibility: public \| private` on a metric | `access_modifier: public_access \| private_access` | `SST-VAL122` |
| `relationship_columns`, `left_column`, `right_column` | `relationship_conditions`, one `{{ ref('a', 'x') }} = {{ ref('b', 'y') }}` string per pair | `SST-PRS021` |
| `non_additive_by` on a metric | `non_additive_dimensions` | `SST-PRS020` |
| `order` / `nulls` on a non-additive entry | `sort_direction: ascending \| descending` / `null_order: first \| last` | `SST-PRS020` |
| `column` / `direction` on a window `order_by` entry | `ref` / `sort_direction` | `SST-PRS020` |
| `relationship_type`, `join_type` on a relationship | nothing: 1.0 never renders them, so delete them | `SST-VAL211` |

The custom instruction and metric spellings are still read, so a run that
demotes `SST-VAL012` or `SST-VAL122` with `diagnostics.severity_overrides`
renders what the author meant. Written beside its 1.0 key, a 0.3 spelling is not
read, and is `SST-PRS020`. The other 0.3 spellings are not read at all.

In the dbt YAML, `meta.sst.primary_key` is a list of columns and
`meta.sst.unique_keys` a list of column lists. The 0.3 forms (a column name, a
comma-separated string, a flat list of names) are errors (`SST-DBT032`) and are
not read. A table's synonyms and `distinct_range` sit beside them, in the
model's `meta.sst`, and its COMMENT is the model's `description`; a view's
`table_config` is an error that says so (`SST-PRS126`).

A metric's `window:` block keeps 0.3's structure with 1.0 spellings, and `expr`
is now the window function itself, applied to a metric or an aggregate; the
[semantic views guide](semantic-views.md#window-function-metrics) shows the
shape. 0.3 never read a `window_function:` key on a non-additive entry, and 1.0
reports it as a key it does not read; the entry's `sort_direction` says which
snapshot counts.

## Configuration keys

- `deploy:` is the deprecated spelling of `apply:`. It is an error
  (`SST-CFG200`) and is still read as `apply:`.
- A key only 0.3 read, such as `generation.filters_to_instructions`, is a
  removed key: an error that says what replaced it (`SST-CFG043`). The
  [configuration reference](../reference/config.md#removed-keys) lists them.
- A 0.3 key 1.0 does not know, such as `logging:`, `repository:`,
  `validation.verbose` or `enrichment.generate_synonyms`, is an unknown key: a
  warning (`SST-CFG003`), and an error under `--strict`.
- These keep their 0.3 meaning: `validation.exclude_dirs`, `validation.strict`,
  `validation.snowflake_syntax_check`, `defer.target`, `defer.state_path`,
  `defer.auto_compile`, `generation.threads` (how many Snowflake sessions
  `plan`, `apply` and `test` work on at once, unless `--threads` or
  `SST_THREADS` says), `generation.view_timeout`, and the `enrichment:` limits
  and model.

In the semantic model YAML, an unknown key on a semantic view, metric, filter,
relationship, verified query, or custom instruction, or under a dbt model's
`meta.sst`, is a warning (`SST-PRS004`), and an error under `--strict`.

## Checks 0.3 did not make

Run `sst validate` after the codemod. Each of these can surface a problem that
was shipping silently:

- **Unknown references are errors.** 0.3 turned an unresolvable `ref()` into an
  upper-cased name and published it.
- **A member that names no table is an error** (`SST-MEM002`). Its tables are
  the models its `ref()` calls name; 0.3 treated an empty `tables:` list
  inconsistently, sometimes as every view.
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
- Diagnostic codes changed, and 1.0 never raises a 0.3 code. `sst explain`
  still answers for one, naming the 1.0 codes it became
  (`sst explain SST-V052` names `SST-VAL012` and `SST-PRS020`); tooling that
  matched a 0.3 code should match those, listed in the
  [error code reference](../reference/error-codes.md).
- The SST manifest is written to `target/sst/manifest.json`.
