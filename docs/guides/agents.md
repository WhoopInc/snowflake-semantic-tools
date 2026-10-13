# Agents

An agent is a Cortex Agent whose specification SST renders from `agent.yml`,
resolves against the project, and publishes as a new agent version. This guide
covers the agent file, the tools it calls, how it pins the skills it uses, and
how it is published.

## Layout

```text
agents/
  sales/                # optional grouping folder
    sales_analyst/
      agent.yml         # or agent.yaml
      instructions/
        orchestration.md
        response.md
      evals/            # optional; see the evals guide
tools/
  platform.yml          # tool groups
```

Each agent is a folder under `project.agents_dir` (default `agents`) holding
`agent.yml` or `agent.yaml`; a file directly in `agents_dir` is not read. The
agent's name comes from `name:`, not from the folder, and it publishes into
`agents.+database` / `agents.+schema`, which default to the target's database
and schema.

Agent folders may sit in grouping folders, `agents/<domain>/<agent>/agent.yml`.
An `agents.<domain>:` folder route overrides any `agents:` `+` key for the
agents below it, so `agents: {sales: {+schema: SALES_AGENTS}}` publishes every
agent under `agents/sales/` into that schema. Routes nest as folders do, an
agent's own setting still wins over every route, and a route that names no
folder is an error (`SST-CFG041`). Evals publish into their agent's schema, so
a route moves an agent's eval with it. The
[configuration guide](configuration.md#locations-and-targets) covers how routes
fold.

## The agent file

```yaml
name: sales_analyst
comment: Order and revenue questions across all locations.
profile:
  display_name: Sales Analyst
spec:
  models:
    orchestration: claude-sonnet-4-6
  orchestration:
    budget:
      seconds: 120
      tokens: 16000     # bounds orchestration only, not tool spend
  instructions:
    orchestration: "{{ file('instructions/orchestration.md') }}"
    response: "{{ file('instructions/response.md') }}"
    sample_questions:
      - question: How many orders were placed last month?
  tools:
    - type: cortex_analyst_text_to_sql
      semantic_view: "{{ semantic_view('sales') }}"
      description: |-
        Orders and revenue by customer and location. Do NOT use for product
        or line-item detail.
alias: promoted
```

`name:` is the only required key (`SST-PRS002`). A key SST does not model, in
any block of the file but `meta` and the passthrough blocks, is a warning
(`SST-PRS004`) and is not rendered; put such keys under `passthrough`. One a
few edits from a key SST does model, such as `warehosue:`, is an error
(`SST-PRS022`). An agent file that cannot be read is `SST-LOD018`. The other
top-level keys:

| Key | Meaning |
|---|---|
| `comment` | Published as the agent's comment, before SST's ownership marker. |
| `profile` | `display_name`, `avatar`, and `color`, set with `ALTER AGENT ... SET PROFILE`. Two agents with one display name is a warning (`SST-VAL549`); a color must be a plain color name or `var(--token)`, and an avatar must be in `snowflake.profile.avatar_allowlist` when that is set (`SST-VAL548`). |
| `spec` | The Cortex Agent specification, below. |
| `alias` | The version alias SST moves to each new version. |
| `secure` | Published with `ALTER AGENT ... SET SECURE`; unset takes `agents.+secure`, else false. |
| `enabled` | `false` leaves the agent out of the compiled project. Unset, it takes `agents.+enabled` as the folder routes over the agent resolve it, so `enabled: true` keeps an agent under a route that sets `+enabled: false`. |
| `deprecated` | Retires the agent; a deprecated agent that still has an alias is an error (`SST-VAL550`). |
| `tags` | A list of `{name, value}` pairs, applied with `ALTER AGENT ... SET TAG`. A tag name is an identifier or a qualified name (`SST-PRS005`) of a tag object that already exists. Unset takes `agents.+tags`. |
| `meta` | Free-form metadata. It is not published, but changing it changes the agent's fingerprint, so the next plan updates the agent. |
| `evals` | `dataset: "{{ eval_dataset('<name>') }}"` and `config: "{{ eval_config('<name>') }}"`, naming files in the agent's `evals/` folder; see the [evals guide](evals.md). |

Under `spec:`:

- `models.orchestration` must be in `snowflake.orchestration_models`
  (`SST-VAL543`), which defaults to `[auto]` alone, so a project that names a
  model lists it there. Unset, or `auto`, takes `agents.+orchestration_model`.
- `orchestration.budget.seconds` and `orchestration.budget.tokens` bound
  orchestration. Tokens spent inside tools are not counted, so the token budget
  is not a spend ceiling: an agent that sets `budget.tokens` itself is warned
  (`SST-VAL547`) unless a comment on that line, or just above it, says
  `orchestration`. A budget set through `agents.+budget_tokens` is not warned.
- `orchestration.tool_not_accessible` is `accept`, `reject`, or `legacy`
  (`SST-VAL545`).
- `orchestration.capabilities.analytical_search: true` needs a `cortex_search`
  tool (`SST-VAL546`).
- `instructions`, `tools`, and `skills` are described below.
- `passthrough` holds keys SST does not model; they are added to the rendered
  spec last, unchecked, and reported (`SST-PRS024`). A key SST renders itself
  is an error there (`SST-PRS023`).

An agent with only a `name:` is valid. Its spec holds only what the `agents:`
defaults supply, and with no tools it is a warning (`SST-RND010`).

### Instructions

`spec.instructions` is a map of `orchestration`, `response`, and
`sample_questions`. A bare string in its place, such as
`instructions: Answer briefly.`, is an error (`SST-PRS003`) and the agent does
not compile; write it as `instructions: {response: Answer briefly.}`.

- `orchestration` and `response` are each inline text or exactly one
  `{{ file('<path>') }}` call that is the whole value, which inlines a file so
  long instructions live in Markdown. Text and a template mixed in one value is
  an error (`SST-REF003`), and so is a value that is not a string
  (`SST-PRS003`).
- A `file()` path resolves against the agent's folder and must stay inside the
  project (`SST-REF027`). A missing file is `SST-REF014`, an unreadable one
  `SST-LOD018`, and one holding only whitespace `SST-LOD019`. Trailing
  whitespace is stripped.
- `sample_questions` is a list of `question:` maps; a bare string entry is an
  error (`SST-PRS118`). An eval question identical to a sample question is
  reported by the eval checks.
- Instructions that name a tool the agent does not have are warned
  (`SST-VAL535`). An Analyst tool's name is its view's name in upper case, such
  as `SALES`.

### Defaults

An agent inherits each of these from `agents:` in `sst_config.yml`, as the
folder routes over it resolve, when it does not set them itself: the
orchestration model, `budget.seconds`, `budget.tokens`, `tool_not_accessible`,
`analytical_search`, `alias`, `secure`, and `tags`. `agents.+warehouse` and
`agents.+query_timeout` are the execution environment of every Analyst, search,
and generic tool that sets none; with no `+warehouse`, they run on the target's
warehouse.

## Tools

Each `spec.tools` entry is one tool. SST splits it into the spec's `tools` and
`tool_resources`, so the resources sit with the tool that uses them.

| `type` | What it names |
|---|---|
| `cortex_analyst_text_to_sql` | `semantic_view: "{{ semantic_view('<name>') }}"`, a view this project compiles (`SST-REF011`). It takes no `name:` (`SST-VAL520`): the tool is named after the view. |
| `cortex_search` | `name:` and `search_service: "{{ tool(...) }}"`, a `cortex_search_service` tool member. Optional: `max_results`, `id_column`, `title_column`, `columns_and_descriptions`, `filter`, and `stage_path` with `relative_path_column` (both or neither, `SST-VAL522`). |
| `generic` | `name:`, `identifier: "{{ tool(...) }}"`, a `procedure` or `function` tool member, and an object `input_schema` (`SST-VAL526`). |
| `agent` | `agent: "{{ agent('<name>') }}"`, or a `name:` that names a tool member; see [delegation](#delegation). |
| `mcp` | `name:`; its resources are exactly its `passthrough:` keys. |
| `data_to_chart`, `web_search`, `code_execution` | Built-ins: no resources. Unnamed, a built-in is named after its type, and `web_search` must be named `web_search` (`SST-VAL517`). |

- Every tool needs a `description` (`SST-VAL518`); a search or generic tool
  that sets none takes its member's. The description is what the orchestrator
  reads to choose a tool, so say what the tool answers and, just as usefully,
  what it does not.
- A resolved tool name is 1 to 64 characters (`SST-VAL513`) of letters,
  digits, `_`, and `-` (`SST-PRS009`), and is unique in the agent
  (`SST-VAL514`); two names that differ only by case are a warning
  (`SST-VAL515`).
- Analyst, search, and generic tools take `warehouse` and `query_timeout`,
  rendered as their `execution_environment`. A search or generic tool with no
  warehouse takes its member's, then `agents.+warehouse`, then the target's; a
  generic tool that resolves none is an error (`SST-VAL527`).
- A key another tool type takes and this one does not is an error
  (`SST-VAL516`). A type SST does not render is an error (`SST-RND012`) unless
  `snowflake.tool_types` lists it.
- `tool_spec_passthrough` adds keys to the rendered `tool_spec`; search and
  generic tools also take `passthrough`, added to their resources last.
- A generic tool's resources are not checked by `CREATE AGENT`, so each is a
  warning (`SST-RND013`): confirm the backing object yourself.

### Tool groups

Every object a search, generic, or delegating tool calls is declared once in a
**tool group** under `project.tools_dir` and referenced with
`{{ tool('<group>', '<name>') }}`, or `{{ tool('<name>') }}` when the member's
name is unique across groups (`SST-REF010`):

```yaml
tools:
  - group: platform
    description: Search and lookups for the sales agents.
    owner: analytics-team
    define:
      - name: menu_search
        type: cortex_search_service
        description: Full-text search over menu item descriptions.
        on:
          table: "{{ ref('product_docs') }}"
          search_column: product_description
          attributes: [product_type]
    reference:
      - name: settlement_lookup
        type: procedure
        description: Looks up one settlement; owned by another team.
        relations:
          dev: DEV_DB.PAYMENTS.SETTLEMENT_LOOKUP
          prod: PROD_DB.PAYMENTS.SETTLEMENT_LOOKUP
```

- **`define:`** entries are objects SST publishes and owns: Cortex Search
  services, procedures, functions, and stages. They publish into `tools.+database`
  / `tools.+schema`, or their group's `tools.<group>` override, before any agent
  that uses them, and an agent's tool calls them there, wherever the agent
  publishes.
- **`reference:`** entries are objects someone else owns. SST never creates or
  changes them; `relations:` maps each target to the object's name. A target the
  map does not cover is an error for any agent that uses the tool
  (`SST-REF018`), so a development run cannot fall through to a production
  object.
- `immutable: true` marks a group whose objects this project must never change;
  a `define:` entry in it is an error (`SST-VAL606`).
- A member of the wrong type for the tool, such as a `generic` member behind a
  `generic` tool, is an error (`SST-REF020`).

In the agent:

```yaml
  tools:
    - type: cortex_search
      name: menu_search
      search_service: "{{ tool('platform', 'menu_search') }}"
      max_results: 5
    - type: generic
      name: settlement_lookup
      identifier: "{{ tool('platform', 'settlement_lookup') }}"
      description: Looks up one settlement by order.
      input_schema:
        type: object
        properties:
          order_id: {type: string}
        required: [order_id]
```

A search tool's id and title columns and its `columns_and_descriptions` come
from the member unless the tool sets its own. An `input_schema` property type is
`string`, `number`, `integer`, `boolean`, or `array` (`SST-PRS032`), and every
`required` name is a declared property (`SST-PRS033`).

A tool object that a `define:` entry publishes is replaced when its definition
cannot change in place. SST never grants new access, but it re-issues exactly
the explicit grants the object held before the replacement, then checks that
they are all back, so the objects an agent calls keep the access they had.

### Defining tool objects

A `define:` member renders to one object, named after the member, in
`tools.+database` / `tools.+schema`. Four `type:` values render: a
`cortex_search_service`, a `procedure`, a `function`, and a `stage`. An `agent`
or `generic` member has no renderer (`SST-RND040`), so it belongs under
`reference:`, and a type outside these six is an error (`SST-VAL603`).

Every member takes `name:` and `type:` (`SST-PRS002`), and `description:`,
which becomes the object's comment before SST's ownership marker and is the
description of a search or generic tool that calls the member and sets none. A member
other than a stage needs `on:` or `body_file:` (`SST-VAL604`). A key SST does
not read, at a tools file's root, on a group, or on a member, is a warning
(`SST-PRS004`), and an error naming the key it resembles when it is a near
miss of one (`SST-PRS022`), as in an agent file.

A `reference:` member takes `relations:` and may also carry what an agent tool
reads from it -- `description`, `warehouse`, `columns_and_descriptions`,
`id_column`, `title_column`, and `signature` -- but no key that only creating
the object needs, such as `on:`, `body_file:`, or `language:` (`SST-VAL605`).

`tools:` in `sst_config.yml` fills what a member leaves unset, after any
`tools.<group>` override for its group: `+warehouse` (else the target's
warehouse), `+target_lag`, and `+embedding_model` for a search service, and
`+warehouse` and `+execute_as` for a procedure. A function and a stage take no
defaults. A member left without a value the DDL needs does not compile: one no
default can supply, such as a routine's `language:` or `returns:`, is
`SST-PRS002`, and a warehouse or target lag no default supplied either is
`SST-RND042`.

**Search service** (`type: cortex_search_service`) renders
`CREATE OR REPLACE CORTEX SEARCH SERVICE`, whose query selects the declared
columns, then the attribute columns, then the search column, each once, from
the model `on:` names:

| Key | Type | Required | Renders to |
|---|---|---|---|
| `on.table` | `"{{ ref('<model>') }}"` | yes | the relation the query reads (`SST-VAL608`) |
| `on.search_column` | column name | yes | `ON <column>` |
| `on.attributes` | list of column names | no | `ATTRIBUTES <columns>` |
| `columns_and_descriptions` | map of column to `description`, `type`, `searchable`, `filterable` | no | columns added to the query; the searchable and filterable ones describe the agent's search tool |
| `id_column`, `title_column` | column name | no | nothing in the DDL; the agent's search tool defaults to them |
| `where` | one SQL expression | no | `WHERE <expression>` on the query (`SST-VAL418`) |
| `warehouse` | identifier | yes, defaulted | `WAREHOUSE = <warehouse>` |
| `target_lag` | string, such as `1 hour` | yes, defaulted | `TARGET_LAG = '<lag>'` |
| `embedding_model` | string | no | `EMBEDDING_MODEL = '<model>'`; unset leaves Snowflake's default |
| `refresh_mode` | `AUTO`, `FULL`, or `INCREMENTAL` | no | `REFRESH_MODE = <mode>`; unset leaves it to Snowflake (`SST-PRS013`) |

`on:` may also be the `ref()` string alone, with `search_column:` and
`attribute_columns:` beside it; written there, they win over the ones inside
`on:`. The search column and every attribute must be columns of the model
(`SST-VAL609`). The checks that follow are warnings:

- `SST-VAL617`: dbt materializes the model as a table or a view, so every dbt
  run rebuilds it and the service re-embeds everything, unless `refresh_mode`
  is `FULL`;
- `SST-VAL611`: an agent gated by a blocking eval reads the service and no
  `embedding_model` pins how it retrieves;
- under connected validation, `SST-VAL613` for a declared column the live
  service lacks, `SST-VAL618` for a source with change tracking off unless
  `refresh_mode` is `FULL`, and
  `SST-VAL619` for explicit grants a replace would have to re-issue.

```yaml
tools:
  - group: docs
    define:
      - name: policy_search
        type: cortex_search_service
        description: Search over published policy documents.
        on:
          table: "{{ ref('policy_docs') }}"
          search_column: body
          attributes: [category]
        target_lag: 1 hour
        embedding_model: snowflake-arctic-embed-m-v1.5
```

**Procedure and function** (`type: procedure`, `type: function`) render
`CREATE OR REPLACE PROCEDURE` or `FUNCTION` with `COPY GRANTS`, then set the
comment with `ALTER ... SET COMMENT`. Clauses come in a fixed order: the
signature, `COPY GRANTS`, `RETURNS`, `LANGUAGE`, `RUNTIME_VERSION`, `HANDLER`,
`PACKAGES`, `IMPORTS`, `EXTERNAL_ACCESS_INTEGRATIONS`, `SECRETS`, a procedure's
`EXECUTE AS`, and the body.

| Key | Type | Required | Renders to |
|---|---|---|---|
| `body_file` | path relative to the project | yes | the body, as `AS $$ ... $$`; it may not contain `$$` |
| `language` | `sql`, `python`, `java`, `javascript`, or `scala` | yes | `LANGUAGE` (`SST-PRS013`) |
| `returns` | a Snowflake type, or `TABLE (<column> <type>, ...)` | yes | `RETURNS` (`SST-PRS003`) |
| `signature` | list of `name:` and `type:` maps | no | the argument list, in order |
| `runtime_version` | string | for Java, Python and Scala; never rendered for SQL or JavaScript | `RUNTIME_VERSION = '<version>'` |
| `handler` | string | for Java, Python and Scala; never rendered for SQL or JavaScript | `HANDLER = '<handler>'` |
| `packages` | list of strings | no | `PACKAGES = ('<package>', ...)` |
| `imports` | list of strings | no | `IMPORTS = ('<path>', ...)` |
| `external_access_integrations` | list of identifiers | no | `EXTERNAL_ACCESS_INTEGRATIONS = (<name>, ...)` |
| `secrets` | map of variable name to three-part secret name | no | `SECRETS = ('<variable>' = <secret>, ...)`, sorted by variable |
| `execute_as` | `caller`, `owner`, or `restricted caller` | procedure only, defaulted, else `caller` | `EXECUTE AS` (`SST-PRS013`) |
| `warehouse` | identifier | for a procedure, defaulted | nothing in the DDL; the warehouse a generic tool runs the routine on when the tool sets none |

`body_file:` is read relative to the project root and may not leave it
(`SST-PRT009`); a missing file is `SST-LOD018` and an empty one `SST-LOD019`.
Each name the DDL writes must be an identifier and each secret a three-part
name (`SST-PRS005`), each type a Snowflake type (`SST-PRS003`), and a parameter
of type `OBJECT` is refused (`SST-PRS032`). A generic tool's `input_schema`
must match the signature in count, in names ignoring case, and in type
(`SST-VAL607`). A procedure that runs as `owner` and that an agent reaches is a
warning (`SST-VAL610`): it lets a conversation act with the owner's privileges.

```yaml
tools:
  - group: orders
    define:
      - name: order_status
        type: procedure
        description: Returns the status of one order.
        body_file: tools/sql/order_status.sql
        language: sql
        returns: VARCHAR
        signature:
          - name: order_id
            type: NUMBER
```

```yaml
tools:
  - group: orders
    define:
      - name: shipping_estimate
        type: function
        description: Estimates delivery days for a postcode.
        body_file: tools/python/shipping_estimate.py
        language: python
        runtime_version: "3.11"
        handler: estimate
        packages: [snowflake-snowpark-python]
        returns: NUMBER
        signature:
          - name: postcode
            type: VARCHAR
```

**Stage** (`type: stage`) takes only `name:` and `description:`. It renders
`CREATE STAGE IF NOT EXISTS` with the description as its comment, so a stage is
created when it is absent and never replaced.

```yaml
tools:
  - group: files
    define:
      - name: order_exports
        type: stage
        description: Files the export procedures write.
```

Drawn from: [tool model](../../snowflake_semantic_tools/domain/model/tool.py), [tools loader](../../snowflake_semantic_tools/adapters/yaml/tools.py), [tool renderer](../../snowflake_semantic_tools/domain/render/tool.py), [tool compile](../../snowflake_semantic_tools/app/compile/tools.py), [tool validation](../../snowflake_semantic_tools/domain/validate/tool.py), [tool diagnostics](../../snowflake_semantic_tools/domain/diagnostics/specs/val_tool.py), [configuration keys](../../snowflake_semantic_tools/domain/model/config_schema/keys.py).

### Delegation

A `type: agent` tool delegates to another agent, in one of two ways:

- `agent: "{{ agent('<name>') }}"` names an enabled agent this project
  publishes (`SST-REF012`), which then publishes first. Unnamed, the tool takes
  that agent's name.
- With no `agent:`, the tool's `name:` names a tool member, normally a
  `reference:` member of type `agent`, for an agent another project owns:

```yaml
    - type: agent
      name: partner_delivery_agent
      description: The partner's delivery agent. Answers where an order is.
```

Delegation cannot form a cycle (`SST-REF022`). Every delegating tool is a
warning (`SST-VAL528`): the tools the delegate calls are not part of this spec,
so tool-name checks and eval coverage cannot see them.

## Skills

An agent loads skills published as Cortex Extensions:

```yaml
  skills:
    - source:
        type: CORTEX_EXTENSION
        path: "{{ skill('sales-semantics') }}"
    - name: partner-glossary
      source:
        type: CORTEX_EXTENSION
        path: "{{ extension('partner-glossary') }}"
        version: VERSION$3
```

- **`{{ skill('<name>') }}`** and **`{{ plugin('<name>') }}`** name extensions
  this project publishes (`SST-REF032`, `SST-REF036`). Do not write `version:`
  (`SST-VAL838`): SST pins the version it publishes for the skill's current
  content, so the agent never points at a version that was not created. A
  `skill()` entry takes the skill's name and writes no `name:` (`SST-VAL540`);
  a `plugin()` entry may name one of the plugin's members (`SST-VAL840`), or
  omit `name:` to load them all.
- **`{{ extension('<name>') }}`** names an extension another project publishes.
  It resolves through `skills.extensions` in `sst_config.yml` (`SST-REF013`;
  one this project publishes is `SST-REF037`) and must pin a version, such as
  `VERSION$3`, or `{{ var('<name>') }}` naming a value under `vars:`; no
  version, a `var()` that `vars:` gives no value, or `LIVE` is refused
  (`SST-VAL538`), and so is `{{ var('sha_version') }}` (`SST-VAL839`).
- A `STAGE` source is an error (`SST-VAL539`): a stage path can change with no
  new version.
- Two entries that load a skill of one name are a warning (`SST-VAL541`).

Because an owned pin names a version, a plan that publishes the agent must
include the skill too (`SST-PLN030`). The [skills guide](skills.md#versions)
covers how skill versions are named.

## Publishing

`sst plan` shows each agent as a create, an update, or unchanged, and its
summary counts an agent's updates as `update`, since each adds a version. Apply
uploads the rendered spec to `@<stage>/<agent>/<commit>/agent_spec.yaml` under
`apply.agent_spec_stage` (default stage `AGENT_SPECS`, in the target's database
and schema), then:

1. When the agent is absent, `CREATE AGENT ... FROM` that folder. Snowflake
   commits `VERSION$1` and also opens a live version holding the same spec,
   which it can neither drop nor abort, so apply commits that copy while the
   default is pinned to `VERSION$1`, drops it, and sets the default back to
   `LAST`. The agent is left with exactly one version, `VERSION$1`, commented
   `git:<commit>`, and no live version.
2. When the agent exists, `ALTER AGENT ... ADD VERSION FROM` that folder,
   commented `git:<commit>`. An agent with a live version, such as one created
   before this release or edited in Snowsight, has the spec written into its
   live version instead (`MODIFY LIVE VERSION SET SPECIFICATION`), which is then
   committed with that comment, so an update always adds exactly one version.
   A draft edited outside SST is replaced, not kept.
3. `ALTER AGENT ... MODIFY VERSION "LAST" SET ALIAS = <alias>`, when the agent
   has an alias, and `SET TAG` for its tags. The new version becomes the
   default.
4. `MODIFY VERSION <alias> UNSET ALIAS` for every other alias the agent shows,
   so after apply the agent carries only the alias its file declares: changing
   `alias: promoted` to `alias: candidate` moves the agent to `CANDIDATE` and
   leaves no `PROMOTED`, and removing `alias:` removes the alias. `plan --full`
   shows the `aliases` that change.
5. `SET PROFILE`, `SET COMMENT`, and `SET SECURE`.

A statement that fails after an earlier one of the agent's ran is not retried:
re-running from the top would add the version again. The run reports a partial
write, and the next plan and apply finish it.

SST creates the stage when it is absent, as it does the skill bundle, profile,
and eval config stages: before the first upload to it, apply runs
`CREATE STAGE IF NOT EXISTS ... ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')`, once per
stage per run, whether or not the stage is in the agent's own schema, and the
plan says so (`SST-PLN035`). SST never replaces, alters, or drops the stage. The
plan checks the stage before anything is written, so a stage SST cannot create
or use blocks each agent that stages there instead of failing part-way through
apply:

- an existing stage that is not an internal stage with server-side encryption
  (`SST-PLN026`);
- an absent stage when the role lacks `CREATE STAGE` on its schema, or an
  existing one it lacks `READ` or `WRITE` on (`SST-PLN008`);
- an absent schema or database for the stage (`SST-PLN010`, `SST-PLN011`).

Once a stage exists, a later plan uses it as it is, so an apply that stopped
part-way can be planned and applied again.

An agent is created once and then updated by adding a version, never replaced,
so its grants and its version history survive every publish, and promotion or
rollback is a move of the alias. An alias is a plain identifier, always written
unquoted, so it is matched without regard to case wherever it is named later.
`LIVE`, `FIRST`, `LAST`, `DEFAULT`, and every SQL reserved word, such as
`select` or `current`, are refused (`SST-PRS025`): SST never double-quotes an
alias to rescue it, because quoting would make it case-sensitive. An alias
written quoted, or one that is a valid identifier only once quoted, such as
`pro-moted`, is refused too (`SST-PRS005`). A reserved word in a tag name is
quoted, as the upper-cased name it already resolves to.

`sst apply --temporary` instead publishes each agent as a session-scoped
`CREATE OR REPLACE TEMPORARY AGENT` from the inline spec, staging nothing; it is
refused for a production-like target. A spec over 100,000 bytes is an error
(`SST-VAL511`), and one over 80% of that a warning (`SST-VAL512`).

With connected validation on, `sst validate` and `sst plan` also look up every
object an agent calls that SST does not publish, and every extension it consumes
with the version it pins. One that does not exist, or that the role cannot see,
is an error (`SST-VAL531`): Snowflake accepts the agent anyway, and it then has
no tool or skill. `sst plan` looks up the consumed extensions and pinned
versions on every connected plan unless `--no-validate`, even with connected
validation off. For a
target whose referenced objects may be absent, demote the code to a warning with
`diagnostics.severity_overrides`; a baseline cannot hold an error. `sst test --suite smoke` describes each published agent to
confirm it exists as planned; to check what an agent answers, write an
[eval](evals.md).
