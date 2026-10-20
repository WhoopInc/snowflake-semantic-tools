# References

Every field that names a file, or another member of the project, is written as
one template call: `{{ <function>('<name>') }}`. SST resolves the call, not dbt,
so a renamed or missing member stops the build where it is named instead of
surfacing later as something unrelated.

A reference field is read by one rule wherever it is written:

- A bare value, such as `orders_to_customers` where
  `{{ relationship('orders_to_customers') }}` belongs, is an error that names the
  function to write (`SST-REF050`).
- A call of another function is `SST-REF041`, and a function SST does not know
  is `SST-REF004`.
- Text beside the call, or a second call, is `SST-REF003`, and a template that
  does not parse is `SST-LOD004` or `SST-REF033`.
- A call with the wrong number of names is `SST-REF015`.
- A call naming nothing the project declares has its own code, given with each
  field below.

The 0.3 spellings `{{ table() }}` and `{{ column() }}` are refused with the
`ref()` to write instead (`SST-REF034`, `SST-REF035`).

## Functions

| Function | Names | Arguments |
|---|---|---|
| `ref` | a dbt model, snapshot or seed, or one of its columns | `('<model>')` or `('<model>', '<column>')` |
| `source` | a dbt source table | `('<source>', '<table>')` |
| `metric` | a metric | `('<name>')` |
| `relationship` | a relationship | `('<name>')` |
| `filter` | a filter | `('<name>')` |
| `verified_query` | a verified query | `('<name>')` |
| `custom_instructions` | a custom instructions block | `('<name>')` |
| `semantic_view` | a semantic view this project compiles | `('<name>')` |
| `agent` | an agent this project publishes | `('<name>')` |
| `tool` | a tool member under `project.tools_dir` | `('<name>')` |
| `skill` | a skill under `project.skills_dir` | `('<name>')` |
| `plugin` | a plugin under `project.plugins_dir` | `('<name>')` |
| `extension` | a consumed Cortex Extension, declared under `extensions:` | `('<name>')` |
| `command` | a command under `project.commands_dir`, by its path without `.md` | `('<path>')` |
| `hook` | a hook folder under `project.hooks_dir` | `('<name>')` |
| `mcp_server` | an MCP config folder under `project.mcp_servers_dir` | `('<name>')` |
| `eval_dataset`, `eval_config` | an eval file in the agent's own `evals/` folder, by its `name:` | `('<name>')` |
| `eval_metric` | a custom eval metric under `project.eval_metrics_dir` | `('<name>')` |
| `tag` | an entry of the `tags:` block of `sst_config.yml` | `('<name>')` |
| `var` | a value under `vars:` | `('<name>')` |
| `file` | a file, relative to the file that names it; see [Files](#files) | `('<path>')` |

## Reference fields

Each field takes exactly one call, of the function or functions listed; the
first is the one a bare value is told to use.

### Semantic models

| Field | Function | A name that resolves to nothing |
|---|---|---|
| `semantic_view.tables` | `ref`, `source` | `SST-REF001`; any other entry is `SST-REF044` |
| `semantic_view.columns` (and `exclude_columns`) | two-argument `ref` | `SST-VAL330` |
| `semantic_view.metrics` (and `exclude_metrics`) | `metric` | `SST-VAL330` |
| `semantic_view.relationships` (and `exclude_relationships`) | `relationship` | `SST-VAL330` |
| `semantic_view.custom_instructions` | `custom_instructions` | `SST-REF007` |
| `semantic_view.config.tags.name` | `tag` | `SST-REF028` |
| `relationship.left_table`, `relationship.right_table` | one-argument `ref`; anything else is `SST-REF046` | `SST-REF001` |
| `metric.using_relationships` | `relationship` | `SST-REF029` |
| `metric.non_additive_dimensions.ref` | two-argument `ref` | `SST-VAL118` |
| `verified_query.sql` | `file`, when the SQL lives in its own file | see [Files](#files) |

A `non_additive_dimensions` entry is `- ref: "{{ ref('<model>', '<column>') }}"`
with an optional `sort_direction` and `null_order`; the 0.3 keys `dimension:`
and `table:` are errors (`SST-PRS126`).

### Agents

| Field | Function | A name that resolves to nothing |
|---|---|---|
| `agent.spec.instructions` (`orchestration`, `response`) | `file`, when the text lives in its own file | see [Files](#files) |
| `agent.spec.tools.semantic_view` | `semantic_view` | `SST-REF011` |
| `agent.spec.tools.search_service` | `tool` | `SST-REF010` |
| `agent.spec.tools.identifier` | `tool`, for a generic tool and an agent toolset over an `agent` member | `SST-REF010` |
| `agent.spec.tools.agent` | `agent` | `SST-REF012` |
| `agent.spec.tools.stage_path` | an opening `tool`; see [Stage paths](#stage-paths) | `SST-REF010` |
| `agent.spec.skills.source.path` | `skill`, `plugin`, `extension` | `SST-REF032`, `SST-REF036`, `SST-REF013` |
| `agent.spec.skills.name` | `skill`, naming a member of the plugin `source.path` names | `SST-VAL840` |
| `agent.spec.skills.source.version` | `var`, when the version is not written literally | `SST-CFG029` |
| `agent.config.tags.name` | `tag` | `SST-REF028` |
| `agent.evals.dataset` | `eval_dataset` | `SST-REF048` |
| `agent.evals.config` | `eval_config` | `SST-REF048` |
| `eval_config.metrics.custom` | `eval_metric` | `SST-REF026` |

An agent toolset names another agent of this project with `agent:`, or an
`agent` tool member with `identifier:`. Its `name:` is only the tool's label,
and a toolset that writes neither key is `SST-VAL521`. An eval file names no
agent: the `evals/` folder it sits in says whose it is, and an `agent:` key is
an error (`SST-PRS126`).

### CoWork profiles

CoWork profiles are a Snowflake private preview, and so are these fields.

| Field | Function | A name that resolves to nothing |
|---|---|---|
| `cowork_profile.spec.instructions` (`orchestration`, `response`) | `file`, when the text lives in its own file | see [Files](#files) |
| `cowork_profile.spec.subagents.agent` | `agent`, or `tool` for an `agent` tool member | checked when the profile compiles |
| `cowork_profile.spec.skills.source.path` | `skill`, `plugin`, `extension` | checked when the profile compiles |
| `cowork_profile.spec.skills.name` | `skill`, naming a member of the plugin `source.path` names | checked when the profile compiles |
| `cowork_profile.spec.skills.source.version` | `var`, when the version is not written literally | checked when the profile compiles |
| `cowork_profile.config.tags.name` | `tag` | checked when the profile compiles |

A subagent names an agent this project publishes with `{{ agent('<name>') }}`,
or an `agent` tool member, for an agent published elsewhere, with
`{{ tool('<name>') }}`.

### Tools

| Field | Function | A name that resolves to nothing |
|---|---|---|
| `tool.on` | one-argument `ref` | `SST-REF001` |
| `tool.columns` (every column a defined member names) | two-argument `ref` of its `on:` model; another model is `SST-VAL622` | `SST-REF002` |
| `tool.body` | `file` | see [Files](#files) |
| `tool.imports` | an opening `tool`; see [Stage paths](#stage-paths) | `SST-REF010` |

### Skills, plugins, and profiles

| Field | Function | A name that resolves to nothing |
|---|---|---|
| `plugin.skills` | `skill` | `SST-VAL835` |
| `profile.skills` | `skill` | `SST-VAL844` |
| `profile.plugins` | `plugin` | `SST-VAL860` |
| `profile.commands` | `command` | `SST-VAL858` |
| `profile.mcp_servers` | `mcp_server` | `SST-VAL847` |
| `profile.hooks` | `hook` | `SST-VAL846` |
| `hook.script` | `file` | see [Files](#files) |
| `command.skill` (frontmatter) | `skill` | `SST-REF032` |

A command's `skill: "{{ skill('<name>') }}"` is published as the bare skill
name, `skill: <name>`, which is what Desktop reads; every other byte of the
command is published as written.

## Expressions

Some fields hold SQL or prose that may name members among other text. Each
takes only the functions listed, as many times as it needs:

| Field | Functions |
|---|---|
| `metric.expr`, `metric.window` | `ref`, `metric`, `var` |
| `filter.expr` | `ref`, `var` |
| `tool.where` | `ref`, `var`; a column `ref` renders as the bare column |
| The file a routine's `tool.body` names | one-argument `ref`, `source`, `var`; see [Routine bodies](#routine-bodies) |
| `verified_query.sql`, written inline | `ref`, `metric`, `var` |
| A custom instruction's text | `filter`, `relationship`, `verified_query` |
| Any value in `sst_config.yml` | `{{ target.database }}`, `{{ target.schema }}`, `{{ target.warehouse }}`; a `tool()` there must name a declared member (`SST-CFG017`) |

## Files

Every `{{ file('<path>') }}` is read by one resolver, whichever field it is in:
an agent's instructions, a verified query's `sql:`, a tool member's `body:`, and
a hook's `script:`.

- The path is relative to the folder of the file that writes the call, such as
  `agent.yml`, the verified query's YAML file, the tool's file, or `hook.yml`.
  `..` may climb to another folder of the project.
- An absolute path, or one that climbs out of the project, is refused
  (`SST-REF027`).
- A symbolic link is refused (`SST-PRT009`), and so is a file reached through a
  linked folder, even when the link points inside the project.
- A path that names no file is `SST-REF014`, a file that cannot be read
  `SST-LOD018`, a text file that is not UTF-8 `SST-LOD006`, and one holding only
  whitespace `SST-LOD019`. A hook's script is read as bytes, so it need not be
  text.
- A file that begins with a UTF-8 byte-order mark is refused (`SST-LOD017`),
  naming that file, as a YAML file with one is. The mark is not removed, since
  removing it would change what is published; Snowflake refuses one before a
  procedure's `BEGIN`, and before a hook's `#!` line it stops the script
  running. Many Windows editors write one by default: save the file as UTF-8
  without a BOM.
- Each file is read once. It joins its artifact's source files, so a change to
  it changes the artifact's fingerprint and shows up in `sst plan`.

## Routine bodies

A procedure's or function's body is code in another language, so SST runs no
template engine over the file `body:` names. It rewrites exactly three calls
and leaves every other character as written:

- `{{ ref('<model>') }}` and `{{ source('<source>', '<table>') }}` become the
  relation the model or source resolves to for the target being compiled,
  deferral included, written as a view's `tables:` entry writes it: unquoted
  when every part is a plain identifier, double-quoted parts otherwise. So each
  target's routine reads that target's tables, and the manifest records them as
  the tool's dbt inputs, which apply checks exist before it publishes.
- `{{ var('<name>') }}` becomes the project variable's value.
- Any other call of the dialect, such as `metric()`, is `SST-REF041`, and a
  two-argument `ref()` is `SST-REF015`. A `{{` that opens no call of the
  dialect, such as an escaped brace in a Python f-string, is left alone.

In SQL the call stands where the table name goes. In Python, JavaScript, Java or
Scala it goes inside a single-quoted string, since a rendered name may hold
double quotes: `session.table('{{ ref('orders') }}')`.

A body that still writes the three-part name of a relation a project model,
seed, snapshot or source resolves to is `SST-VAL626`, a warning naming the call
to write instead. A name no project relation has, such as another team's table,
is never reported, so a routine may read one by name.

## Characters no file may hold

The rules above for a byte-order mark and UTF-8 hold for every file SST reads:
project YAML, each `file()` sidecar, a `SKILL.md`, and a profile's `AGENTS.md`
and rules. A NUL character, or a lone UTF-16 surrogate, which a YAML escape such
as `"\0"` or `"\ud800"` can write, is refused too (`SST-LOD022`), at the file
and line holding it: Snowflake cannot store either in a string, a name, or a
body. A dbt description, synonym or sample value holding one is refused the same
way where a view uses its model. Both are refused wherever they are written, an
agent's specification included.

## Stage paths

A path on a stage the project declares as a tool member opens with that member:
`"{{ tool('weekly_exports') }}/2026/"`. The path then follows the member to
wherever its settings place it on each target, `@DB.SCHEMA.WEEKLY_EXPORTS/2026/`,
or to its relation when the member is referenced. The member must be a `stage`
(`SST-REF020`), the call must open the path (`SST-REF003`), and a referenced
stage needs a relation for the target (`SST-REF018`).

This applies to an agent search tool's `stage_path` and to a routine's
`imports:`. A stage the project does not declare is an external object, written
as its own `@<database>.<schema>.<stage>/<path>` and kept as written.

## Left literal

These fields are not references, and take no template call:

| Field | Why it stays literal |
|---|---|
| A tool member's `relations`, `secrets`, `external_access_integrations` and `packages`; warehouses; external stages | They name Snowflake objects SST does not own. They are what references resolve into, not references themselves. |
| Extension and tag FQNs in `sst_config.yml` (`extensions:`, `tags:`) | They are the declarations a `tag()` or `extension()` call resolves against. |
| A consumed extension's skill `name:` in an agent | It names a skill inside an extension SST does not build; a template there is `SST-REF008`. |
| An agent's `comment`, `profile.display_name`, and sample questions; a tool's `description`, in an agent or a tool member | They are published as written; a template there, even a `{{` alone, is `SST-REF008`. |
| dbt `meta.sst` columns | dbt owns that file and renders its own Jinja before SST reads it. |
| An agent search tool's `filter` object | It is Snowflake's own filter syntax, passed through as written. |
| Eval `tool_name` | It is a run-time trace label, matched against what the agent called. |
| View variables | An expression uses a variable by its bare name, as Snowflake does. |
| SQL and prose outside the calls they hold | Only the calls are resolved; the rest renders as written. |
| An artifact's own `name:` | It declares the name other files reference, so it is the target, not a reference. |
| `project.*_dir` | Folder paths are configuration, read before any template resolves. |
