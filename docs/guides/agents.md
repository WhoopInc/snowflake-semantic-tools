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
  platform/             # folders route members, as agents/ folders route agents
    menu_search.yml     # a tool: file, one member
    sql/order_status.sql
```

Each agent is a folder under `project.agents_dir` (default `agents`) holding
`agent.yml` or `agent.yaml`; a file directly in `agents_dir` is not read, and a
folder holding both is an error (`SST-VAL801`) and publishes neither. The
agent's name comes from `name:`, not from the folder. It must be a Snowflake
identifier, such as letters, digits and underscores starting with a letter
(`SST-PRS005`, and the agent is left out); a reserved word such as `select`
(`SST-PRS031`), or a reserved prefix such as `sst_` or `snowflake_`
(`SST-PRS100`), is a warning. The agent publishes into its resolved `database`
and `schema` settings (below), which default to the target's database and
schema.

Agent folders may sit in grouping folders, `agents/<domain>/<agent>/agent.yml`.
An `agents.<domain>:` folder route overrides any `agents:` `+` key for the
agents below it, so `agents: {sales: {+schema: SALES_AGENTS}}` publishes every
agent under `agents/sales/` into that schema. Routes nest as folders do, an
agent's own `config:` still wins over every route, and a route that names no
folder is an error (`SST-CFG041`). Evals publish into their agent's schema, so
a route moves an agent's eval with it. The
[configuration guide](configuration.md#precedence) covers how settings resolve.

## The agent file

```yaml
name: sales_analyst
comment: Order and revenue questions across all locations.
profile:
  display_name: Sales Analyst
config:
  orchestration_model: claude-sonnet-4-6
  budget_seconds: 120
  budget_tokens: 16000  # bounds orchestration only, not tool spend
  alias: promoted
spec:
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
| `profile` | `display_name`, `avatar`, and `color`, set with `ALTER AGENT ... SET PROFILE`. Two agents with one display name is a warning (`SST-VAL549`); a color must be a plain color name or `var(--token)`, and an avatar must be in `snowflake.profile.avatar_allowlist` when that is set (`SST-VAL548`). Snowflake refuses a profile over 1,000 bytes of JSON, so a larger one is an error (`SST-VAL558`). |
| `config` | The agent's own settings, [below](#settings). |
| `spec` | The Cortex Agent specification, below. |
| `deprecated` | Retires the agent; a deprecated agent that still has an alias is an error (`SST-VAL550`). |
| `meta` | Free-form metadata. It is not published, but changing it changes the agent's fingerprint, so the next plan updates the agent. |
| `evals` | `dataset: "{{ eval_dataset('<name>') }}"` and `config: "{{ eval_config('<name>') }}"`, naming files in the agent's `evals/` folder; see the [evals guide](evals.md). |

Under `spec:`:

- `instructions`, `tools`, and `skills` are described below.
- `passthrough` holds keys SST does not model; they are added to the rendered
  spec last, unchecked, and reported (`SST-PRS024`). A key SST renders itself
  is an error there (`SST-PRS023`).

An agent with only a `name:` is valid. Its spec holds only what the `agents:`
settings supply, and with no tools it is a warning (`SST-RND010`).

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
- A `file()` path resolves against the folder of `agent.yml`, as every
  `file()` does (see [References](references.md#files)). An absolute path or one
  that leaves the project is `SST-REF027`, and a symbolic link, wherever it
  points, `SST-PRT009`. A missing file is `SST-REF014`, an unreadable one
  `SST-LOD018`, and one holding only whitespace `SST-LOD019`. Trailing
  whitespace is stripped, and the file joins the agent's source files.
- `sample_questions` is a list of `question:` maps; a bare string entry is an
  error (`SST-PRS118`). An eval question identical to a sample question is
  reported by the eval checks.
- Instructions that name a tool the agent does not have are warned
  (`SST-VAL535`). An Analyst tool's name is its view's name in upper case, such
  as `SALES`.

### Settings

`config:` is the only place an agent overrides the `agents:` block of
`sst_config.yml`. Its keys are exactly the block's `+` keys without the `+`, and
each resolves through one chain: the agent's own `config:`, then the closest
folder route over its file, then the block root, then SST's default. A key the
agent leaves out inherits; an explicit value, `0`, `false`, `[]`, or `auto`
among them, is a value; and a key written with no value is an error
(`SST-CFG054`). Any other key under `config:` is an error (`SST-CFG053`), and a
value is checked by the rules the `+` key follows, such as `SST-CFG008` for a
`tool_not_accessible` outside its choices, or for a `query_timeout`,
`budget_seconds` or `budget_tokens` below 1: Snowflake stores a zero or negative
one as written and runs the agent as if none were set, so SST refuses it. `sst list --long` prints each
agent's settings with where each came from.

| Key | Meaning |
|---|---|
| `database`, `schema` | Where the agent publishes. Unset, the target's. |
| `warehouse`, `query_timeout` | The execution environment of every Analyst, search, and generic tool of the agent that sets none; `query_timeout` is at least 1. With no `warehouse`, they run on the target's warehouse. |
| `orchestration_model` | Rendered as `models.orchestration`. It must be in `snowflake.orchestration_models` (`SST-VAL543`), which defaults to `[auto]` alone, so a project that names a model lists it there. The connected check runs each model in that list in a throwaway agent, so a model agents refuse fails validation (`SST-VAL551`) rather than an agent's first question. Unset, `auto`. |
| `budget_seconds`, `budget_tokens` | Rendered as `orchestration.budget`; each at least 1. Tokens spent inside tools are not counted, so the token budget is not a spend ceiling: an agent that sets `config.budget_tokens` itself is warned (`SST-VAL547`) unless a comment on that line, or just above it, says `orchestration`. A budget an agent inherits from `agents:` is not warned. |
| `tool_not_accessible` | Rendered as `orchestration.tool_not_accessible`: `accept`, `reject`, or `legacy` (`SST-VAL545`). |
| `analytical_search` | Rendered as `orchestration.capabilities.analytical_search`; `true` needs a `cortex_search` tool (`SST-VAL546`). |
| `alias` | The version alias SST moves to each new version. |
| `secure` | Published with `ALTER AGENT ... SET SECURE`; unset, false. |
| `enabled` | `false` leaves the agent out of the compiled project, so `enabled: true` keeps an agent under a route that sets `+enabled: false`. |
| `tags` | A list of `{name, value}` pairs, applied with `ALTER AGENT ... SET TAG`. Each `name` is a `{{ tag('<name>') }}` call naming an entry of the `tags:` block of `sst_config.yml`, as a view's tags are; a bare name is `SST-REF050`, an undeclared one `SST-REF028`. The tag object must already exist. A closer setting replaces the whole list. |

The keys an earlier draft read elsewhere -- `alias`, `secure`, `enabled` and
`tags` at the top level, and `spec.models` and `spec.orchestration` -- are an
error that names their `config:` key (`SST-PRS126`).

## Tools

Each `spec.tools` entry is one tool. SST splits it into the spec's `tools` and
`tool_resources`, so the resources sit with the tool that uses them.

| `type` | What it names |
|---|---|
| `cortex_analyst_text_to_sql` | `semantic_view: "{{ semantic_view('<name>') }}"`, a view this project compiles (`SST-REF011`). It takes no `name:` (`SST-VAL520`): the tool is named after the view. |
| `cortex_search` | `name:` and `search_service: "{{ tool('<name>') }}"`, a `cortex_search_service` tool member. Optional: `max_results`, `id_column`, `title_column`, `columns`, `filter`, and `stage_path` with `relative_path_column` (both or neither, `SST-VAL522`). A `stage_path` on a stage this project declares opens with its member, `"{{ tool('<stage>') }}/<path>"`; an external stage is written as it is. |
| `generic` | `name:`, `identifier: "{{ tool('<name>') }}"`, a `procedure` or `function` tool member, and an object `input_schema` (`SST-VAL526`). |
| `agent_toolset` | `agent: "{{ agent('<name>') }}"`, or `identifier: "{{ tool('<name>') }}"`, an `agent` tool member; see [agent toolsets](#agent-toolsets). |
| `data_to_chart`, `web_search`, `code_execution` | Built-ins: no resources. Unnamed, a built-in is named after its type, and `web_search` must be named `web_search` (`SST-VAL517`). |

- `type: mcp` is refused (`SST-VAL559`): Snowflake accepts it in `CREATE AGENT`,
  then every run of the agent fails with "Tool type mcp is not valid". Snowflake
  attaches an MCP server with the spec's top-level `mcp_servers:` key instead,
  which `spec.passthrough` renders:

  ```yaml
  spec:
    passthrough:
      mcp_servers:
        - server_spec:
            name: PARTNER.DELIVERY.DELIVERY_MCP
  ```

  Snowflake names each tool the server offers `<server>_<tool>`, so the
  agent's tool-name checks do not see them.
- Every tool needs a `description` (`SST-VAL518`); a search or generic tool
  that sets none takes its member's. The description is what the orchestrator
  reads to choose a tool, so say what the tool answers and, just as usefully,
  what it does not. An agent toolset is the exception: Snowflake replaces it
  with the tools it inherits before the orchestrator sees anything, so its
  description, which a member-backed toolset takes from its member, is only a
  note for readers.
- A resolved tool name is 1 to 64 characters (`SST-VAL513`) of letters,
  digits, `_`, and `-` (`SST-PRS009`), and is unique in the agent
  (`SST-VAL514`); two names that differ only by case are a warning
  (`SST-VAL515`).
- Analyst, search, and generic tools take `warehouse` and `query_timeout`,
  rendered as their `execution_environment`. A search or generic tool with no
  warehouse takes its member's own `config.warehouse`, then the agent's
  `warehouse` setting, then the target's; a generic tool that resolves none is
  an error (`SST-VAL527`). A folder route's or the `tools:` root's warehouse is
  where SST creates the object, not where an agent's tool runs.
- A key another tool type takes and this one does not is an error
  (`SST-VAL516`). A type SST does not render is an error (`SST-RND012`) unless
  `snowflake.tool_types` lists it.
- `tool_spec_passthrough` adds keys to the rendered `tool_spec`; search and
  generic tools also take `passthrough`, added to their resources last.
- A generic tool's resources are not checked by `CREATE AGENT`, so each is a
  warning (`SST-RND013`): confirm the backing object yourself. Snowflake's tool
  runtime calls the routine on a warehouse with each argument named unquoted,
  and reads one value back, so when the member declares them SST refuses what
  would fail every run: a routine that returns a table (`SST-VAL623`), a
  parameter whose name must be quoted, such as a reserved word like `select`
  (`SST-VAL624`), and a parameter of type `ARRAY`, `OBJECT`, or `VARIANT`
  (`SST-VAL625`); take such a value as `VARCHAR` and parse it with
  `PARSE_JSON`. The same routine stays valid for any other caller.

### Tool members

Every object a search, generic, or toolset tool calls is declared once as a
**tool member** under `project.tools_dir`, in any folder below it, and
referenced with `{{ tool('<name>') }}`, which takes exactly one name
(`SST-REF015`) and names a declared member (`SST-REF010`). A member name is
unique in the project, ignoring case (`SST-VAL001`), and follows the agent name
rules: a name that is not an identifier is an error (`SST-PRS005`) and the
member is not compiled, and a reserved word or prefix is a warning
(`SST-PRS031`, `SST-PRS100`). A file's root `tool:` holds
one member, the norm; its root `tools:` holds a list of them:

```yaml
# tools/platform/menu_search.yml
tool:
  name: menu_search
  type: cortex_search_service
  description: Full-text search over menu item descriptions.
  on:
    table: "{{ ref('product_docs') }}"
    search_column: "{{ ref('product_docs', 'product_description') }}"
    attributes: ["{{ ref('product_docs', 'product_type') }}"]
  meta:
    owner: analytics-team
```

```yaml
# tools/payments/lookups.yml
tools:
  - name: settlement_lookup
    type: procedure
    description: Looks up one settlement; owned by another team.
    relations:
      dev: DEV_DB.PAYMENTS.SETTLEMENT_LOOKUP
      prod: PROD_DB.PAYMENTS.SETTLEMENT_LOOKUP
```

- A member that writes **`relations:`** references an object someone else owns.
  SST never creates or changes it; `relations:` maps each target to the
  object's name. A target the map does not cover is an error for any agent that
  uses the tool (`SST-REF018`), so a development run cannot fall through to a
  production object. One object for both `dev` and `prod` is a warning
  (`SST-REF023`) unless the member is immutable.
- Any other member is **defined**: SST publishes and owns it -- a Cortex Search
  service, procedure, function, or stage -- where its settings place it, before
  any agent that uses it, and an agent's tool calls it there, wherever the
  agent publishes.
- `immutable` is a setting, not a key of the member: a `tools:` folder route,
  such as `tools: {partner: {+immutable: true}}`, or the member's own
  `config: {immutable: true}`. An immutable member must reference its object
  (`SST-VAL606`), and may name one object for every target.
- `meta:` is free-form and never read; `meta.owner` names who owns the member.
  `description:` is the member's own.
- A member of the wrong type for the tool, such as a `generic` member behind a
  `generic` tool, is an error (`SST-REF020`).

In the agent:

```yaml
  tools:
    - type: cortex_search
      name: menu_search
      search_service: "{{ tool('menu_search') }}"
      max_results: 5
    - type: generic
      name: settlement_lookup
      identifier: "{{ tool('settlement_lookup') }}"
      description: Looks up one settlement by order.
      input_schema:
        type: object
        properties:
          order_id: {type: string}
        required: [order_id]
```

A search tool's id and title columns and its `columns` come from the member
unless the tool sets its own, written as the member writes its columns (below).
An `input_schema` property type is `string`, `number`, `integer`, or `boolean`
(`SST-PRS032`): the tool runtime cannot pass an array or an object argument.
Every `required` name is a declared property (`SST-PRS033`).

A tool object that a defined member publishes is replaced when its definition
cannot change in place. SST never grants new access, but it re-issues exactly
the explicit grants the object held before the replacement, then checks that
they are all back, so the objects an agent calls keep the access they had.

### Tool settings and folders

A member's settings resolve through the one precedence chain the
[configuration guide](configuration.md#precedence) describes: its own
`config:`, then the closest `tools:` folder route over its file, then the
block root, then the built-in default. Its `config:` keys are exactly the
`tools:` block's `+` keys without the `+`:

| Key | Default | Applies to |
|---|---|---|
| `database`, `schema` | the target's | where a defined member publishes |
| `warehouse` | the target's | a defined search service's `WAREHOUSE`, and a procedure's run warehouse |
| `target_lag` | none | a defined search service (`SST-RND042` when nothing sets it) |
| `embedding_model` | Snowflake's | a defined search service |
| `execute_as` | `caller` | a defined procedure's `EXECUTE AS` |
| `immutable` | `false` | whether the member may only reference its object |

A folder route names a directory below `tools_dir`, and routes nest as folders
do, so `tools: {partner: {+immutable: true}}` covers every member under
`tools/partner/`, and `tools: {platform: {docs: {+schema: DOCS}}}` those under
`tools/platform/docs/`. A route that names no folder is an error
(`SST-CFG041`). A key outside the block's settings in `config:` is
`SST-CFG053`, and one written with no value `SST-CFG054`. `warehouse`,
`target_lag`, `embedding_model`, `execute_as`, `immutable` and `owner` written
on the member itself, and a `group:`, `define:` or `reference:` entry of a
`tools:` list, are errors that say where each is written now (`SST-PRS126`).

### Defining tool objects

A defined member renders to one object, named after the member, where its
`database` and `schema` settings place it. Four `type:` values render: a
`cortex_search_service`, a `procedure`, a `function`, and a `stage`. An `agent`
or `generic` member has no renderer (`SST-RND040`), so it references its
object, and a type outside these six is an error (`SST-VAL603`).

Every member takes `name:` and `type:` (`SST-PRS002`), and `description:`,
which becomes the object's comment before SST's ownership marker and is the
description of a search or generic tool that calls the member and sets none. A
member other than a stage needs `on:` or `body:` (`SST-VAL604`). A key SST
does not read, at a tools file's root or on a member, is a warning
(`SST-PRS004`), and an error naming the key it resembles when it is a near
miss of one (`SST-PRS022`), as in an agent file.

A member that references its object may also carry what an agent tool reads
from it -- `description`, `columns`, `id_column`, `title_column`, and
`signature` -- but no key that only creating the object needs, such as `on:`,
`body:`, or `language:` (`SST-VAL605`).

A member left without a value the DDL needs does not compile: one no setting
can supply, such as a routine's `language:` or `returns:`, is `SST-PRS002`, and
a warehouse or target lag no setting supplied either is `SST-RND042`.

**Columns are references.** A defined search service is built from its `on:`
model, so every column it names is `{{ ref('<on model>', '<column>') }}`: the
search column, each attribute, `id_column`, `title_column`,
`relative_path_column`, and each `columns:` entry's `column`. A bare column is
an error that names `ref()` (`SST-REF050`), and a ref of another model is
`SST-VAL622`. The rendered DDL and the agent's `tool_resources` still write the
bare, upper-cased column names. A referenced member is built outside dbt, so
its columns are the object's own, written bare; a template call there is
`SST-REF008`. An agent's search tool writes its own columns as its member
does.

**Search service** (`type: cortex_search_service`) renders
`CREATE OR REPLACE CORTEX SEARCH SERVICE`, whose query selects the declared
columns, then the attribute columns, then the search column, each once, from
the model `on:` names:

| Key | Type | Required | Renders to |
|---|---|---|---|
| `on.table` | `"{{ ref('<model>') }}"` | yes | the relation the query reads (`SST-VAL608`) |
| `on.search_column` | a column ref | yes | `ON <column>` |
| `on.attributes` | list of column refs | no | `ATTRIBUTES <columns>` |
| `columns` | list of `column` (a column ref), `description`, `type`, `searchable`, `filterable` | no | columns added to the query; the searchable and filterable ones describe the agent's search tool |
| `id_column`, `title_column` | a column ref | no | nothing in the DDL; the agent's search tool defaults to them |
| `where` | one SQL expression, which may hold `ref()` and `var()` calls | no | `WHERE <expression>` on the query (`SST-VAL418`); a column ref renders as the bare column |
| `refresh_mode` | `AUTO`, `FULL`, or `INCREMENTAL`, in any case | no | `REFRESH_MODE = <mode>`; unset leaves it to Snowflake. Any other value is `SST-PRS013`, an unquoted `yes` or `no` (a boolean) and an empty value among them, rather than read as unset |

`warehouse`, `target_lag` and `embedding_model` are settings (above), rendered
as `WAREHOUSE`, `TARGET_LAG` and `EMBEDDING_MODEL`. `on:` may also be the
`ref()` string alone, with `search_column:` and `attribute_columns:` beside
it; written there, they win over the ones inside `on:`. The search column and
every attribute must be columns of the model (`SST-VAL609`). The checks that
follow are warnings:

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
tool:
  name: policy_search
  type: cortex_search_service
  description: Search over published policy documents.
  on:
    table: "{{ ref('policy_docs') }}"
    search_column: "{{ ref('policy_docs', 'body') }}"
    attributes: ["{{ ref('policy_docs', 'category') }}"]
  where: "{{ ref('policy_docs', 'is_published') }}"
  config:
    target_lag: 1 hour
    embedding_model: snowflake-arctic-embed-m-v1.5
```

**Procedure and function** (`type: procedure`, `type: function`) render
`CREATE OR REPLACE PROCEDURE` or `FUNCTION` with `COPY GRANTS`, then set the
comment, the description followed by the ownership marker, with
`ALTER PROCEDURE <name>(<types>) SET COMMENT`; a routine with no arguments is
addressed as `<name>()`. Clauses come in a fixed order: the
signature, `COPY GRANTS`, `RETURNS`, `LANGUAGE`, `RUNTIME_VERSION`, `HANDLER`,
`PACKAGES`, `IMPORTS`, `EXTERNAL_ACCESS_INTEGRATIONS`, `SECRETS`, `COMMENT`
(the description on one line), a procedure's `EXECUTE AS`, and the body.
Because the description is part of the rendered DDL, an edit of only the
description changes the fingerprint, and `plan` shows the routine as an update.

| Key | Type | Required | Renders to |
|---|---|---|---|
| `body` | `"{{ file('<path>') }}"`, relative to the tool file's folder | yes | the body, as `AS $$ ... $$`, its `ref()`, `source()` and `var()` calls rendered for the target; it may not contain `$$` |
| `language` | `sql`, `python`, `java`, `javascript`, or `scala` | yes | `LANGUAGE` (`SST-PRS013`) |
| `returns` | a Snowflake type, or `TABLE (<column> <type>, ...)`, which a generic tool cannot call (`SST-VAL623`) | yes | `RETURNS` (`SST-PRS003`) |
| `signature` | list of `name:` and `type:` maps; every parameter is required, and an entry that writes `required:` is an error (`SST-PRS130`) | no | the argument list, in order |
| `runtime_version` | string | for Java, Python and Scala; never rendered for SQL or JavaScript | `RUNTIME_VERSION = '<version>'` |
| `handler` | string | for Java, Python and Scala; never rendered for SQL or JavaScript | `HANDLER = '<handler>'` |
| `packages` | list of strings | no | `PACKAGES = ('<package>', ...)` |
| `imports` | list of stage paths: `"{{ tool('<stage>') }}/<path>"` for a stage this project declares, else `@<database>.<schema>.<stage>/<path>` as written | no | `IMPORTS = ('<path>', ...)`, a member's stage at its published location |
| `external_access_integrations` | list of identifiers | no | `EXTERNAL_ACCESS_INTEGRATIONS = (<name>, ...)` |
| `secrets` | map of variable name to three-part secret name | no | `SECRETS = ('<variable>' = <secret>, ...)`, sorted by variable |

A procedure's `execute_as` (`caller`, `owner`, or `restricted caller`) and
`warehouse` are settings: `EXECUTE AS`, `caller` by default, and the warehouse
a generic tool runs the routine on when neither the tool nor the member's
`config:` names one. A bare `body:` is `SST-REF050`. Its file is read relative
to the folder of the tool file that declares it, and joins the member's source
files and fingerprint; an absolute path or one that leaves the project is
`SST-REF027`, a symbolic link `SST-PRT009`, a path naming no file `SST-REF014`,
an unreadable file `SST-LOD018`, and an empty one `SST-LOD019`. Each name the
DDL writes must be an identifier and each secret a three-part name
(`SST-PRS005`), and each type a Snowflake type (`SST-PRS003`). A generic tool's
`input_schema` must match the signature in count, in names ignoring case, and
in type, and its `required` must list every parameter, since none renders a
`DEFAULT` (`SST-VAL607`). A procedure that runs as `owner` and that an agent reaches is a
warning (`SST-VAL610`): it lets a conversation act with the owner's privileges.

The body file names the tables it reads with `{{ ref('<model>') }}` or
`{{ source('<source>', '<table>') }}`, as a view does, so each target's routine
reads that target's relations and lineage sees them; `{{ var('<name>') }}` takes a
project variable. Only those three calls are rewritten, and every other character
is published as written. In a non-SQL body the call goes inside a single-quoted
string. A literal three-part name of a project relation is `SST-VAL626`, a
warning; another team's table may be named literally. See
[Routine bodies](references.md#routine-bodies).

```yaml
# tools/orders/order_status.yml
tool:
  name: order_status
  type: procedure
  description: Returns the status of one order.
  body: "{{ file('sql/order_status.sql') }}"
  language: sql
  returns: VARCHAR
  signature:
    - name: order_id
      type: NUMBER
```

```yaml
# tools/orders/shipping_estimate.yml
tool:
  name: shipping_estimate
  type: function
  description: Estimates delivery days for a postcode.
  body: "{{ file('python/shipping_estimate.py') }}"
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
tool:
  name: order_exports
  type: stage
  description: Files the export procedures write.
```

Drawn from: [tool model](../../snowflake_semantic_tools/domain/model/tool.py), [tools loader](../../snowflake_semantic_tools/adapters/yaml/tools.py), [tool columns](../../snowflake_semantic_tools/domain/resolve/tool_column.py), [tool renderer](../../snowflake_semantic_tools/domain/render/tool.py), [tool compile](../../snowflake_semantic_tools/app/compile/tools.py), [tool validation](../../snowflake_semantic_tools/domain/validate/tool.py), [tool diagnostics](../../snowflake_semantic_tools/domain/diagnostics/specs/val_tool.py), [configuration keys](../../snowflake_semantic_tools/domain/model/config_schema/keys.py).

### Agent toolsets

A `type: agent_toolset` tool gives this agent another agent's tools. At run
time Snowflake reads the named agent's tools and their resources and adds them
to this agent's own, so the orchestrator sees one flat list; a tool of this
agent wins over an inherited tool of the same name. The other agent's
instructions, skills, and model are not inherited, and it does not run as a
separate agent. The tool renders as Snowflake's
[agent toolset](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-toolsets),
its resources the other agent's name alone (`agent_name`). It names the agent
in one of two ways:

- `agent: "{{ agent('<name>') }}"` names an enabled agent this project
  publishes (`SST-REF012`), which then publishes first. Unnamed, the tool takes
  that agent's name.
- `identifier: "{{ tool('<name>') }}"` names a referenced tool member of type
  `agent`, for an agent another project owns; a member of another type is an
  error (`SST-REF020`). The tool's `name:` is only its own label: a toolset that
  writes neither `agent:` nor `identifier:` is an error (`SST-VAL521`).

```yaml
    - type: agent_toolset
      name: partner_delivery_agent
      identifier: "{{ tool('partner_delivery_agent') }}"
```

Snowflake checks the caller's `USAGE` on the other agent on every run and, when
it is missing, drops the inherited tools without an error, so connected
validation looks up a referenced agent (`SST-VAL531`) and the
[security guide](security.md#what-people-who-use-an-agent-need) lists the
grant. Toolsets cannot form a cycle (`SST-REF022`), and Snowflake also limits
how deep they nest. Every toolset is a warning (`SST-VAL528`), one per tool:
the inherited tools are not part of this spec, so tool-name checks and eval
coverage cannot see them.

Snowflake's own `type: agent` is a different tool, a subagent the calling
client runs. SST does not render it; to use it anyway, add it to
`snowflake.tool_types`.

## Skills

An agent loads skills published as Cortex Extensions:

```yaml
  skills:
    - source:
        type: CORTEX_EXTENSION
        path: "{{ skill('sales-semantics') }}"
    - name: "{{ skill('jaffle-analytics') }}"
      source:
        type: CORTEX_EXTENSION
        path: "{{ plugin('jaffle-toolkit') }}"
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
  `skill()` entry takes the skill's name and writes no `name:` (`SST-VAL540`).
  A `plugin()` entry may name one of the plugin's members as a `skill()` call,
  `name: "{{ skill('<member>') }}"` (`SST-VAL840`); a bare member name is
  `SST-REF050`. A `plugin()` entry with no `name:` loads every member: Snowflake
  refuses a plugin entry without a name, so SST renders it as one entry per
  member, each named after it.
- **`{{ extension('<name>') }}`** names an extension another project publishes.
  It resolves through `extensions:` in `sst_config.yml` (`SST-REF013`;
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
summary counts an agent's updates as `update`. Under an update it shows what
changes in the specification, member by member, against the version the agent
serves (the one carrying its alias, else the default, else the last): each
instruction, the sample questions, each tool with its resources, and each skill.
Apply uploads the rendered spec
to `@<stage>/<agent>/<commit>/agent_spec.yaml` under `apply.agent_spec_stage`
(default stage `AGENT_SPECS`, in the target's database and schema), then:

1. When the agent is absent, `CREATE AGENT ... COMMENT = '<comment>' FROM` that
   folder: the comment carries SST's ownership marker from the first statement.
   Snowflake commits `VERSION$1`, which apply comments
   `git:<commit> sst-spec:<fingerprint>`, and also opens a live version holding
   the same spec, which apply keeps.
2. When the agent exists, `SET COMMENT` writes the marked comment first, then
   `ALTER AGENT ... ADD VERSION FROM` that folder, commented
   `git:<commit> sst-spec:<fingerprint>`. An agent with a live version, which is every agent
   SST has published since this release, has the spec written into its live
   version instead (`MODIFY LIVE VERSION SET SPECIFICATION`), which is then
   committed with that comment, so an update always adds exactly one version.
   A draft edited outside SST is replaced, not kept. Then
   `ADD LIVE VERSION FROM LAST` opens a new live version holding the version
   just committed.

   Apply leaves every agent with a live version because Snowsight's agent page
   shows the live version (as DRAFT), and a request that names no version is
   served it while the agent has one committed version. Without one, the agent
   shows, and runs with, no instructions, tools, or skills.
3. `ALTER AGENT ... MODIFY VERSION "LAST" SET ALIAS = <alias>`, when the agent
   has an alias, and `SET TAG` for its tags. The new version becomes the
   default: apply also runs `SET DEFAULT_VERSION = 'LAST'`, so a default set by
   hand, even to the version that was newest, follows the new version and every
   later one (see [version drift](#version-drift)).
4. `MODIFY VERSION <alias> UNSET ALIAS` for every other alias the agent shows,
   so after apply the agent carries only the alias its settings resolve to:
   changing `alias: promoted` to `alias: candidate` moves the agent to
   `CANDIDATE` and leaves no `PROMOTED`, and an agent whose `alias` resolves to
   none, under `config:` or `agents:`, has none. A version carries one alias,
   so when only the alias name changes, the new alias set on the version that
   carried the old one replaces it there, and no `UNSET` is run for it.
   `plan --full` shows the `aliases` that change.
5. `SET PROFILE` and `SET SECURE`.

`<fingerprint>` is the SHA-256 of the specification alone, so a plan can tell
that the agent's newest committed version already holds the spec being planned.
When it does, an update adds no version: a change to only the agent's
`comment`, `profile`, tags, `alias`, or `secure` runs `SET COMMENT`, points the
alias at that version (`MODIFY VERSION <version> SET ALIAS`), opens a live
version from it if there is none, and sets the tags, profile, and `SECURE`. An
eval baseline taken against that version stays valid. A version committed
before SST wrote the fingerprint is recognised by its specification, when the
role may read it; a secure agent's version the role cannot read gets a new
version, as before.

The ownership marker means "SST is writing this definition": it is written
before anything else, so a publish that stops part way leaves the marker state
records for it. A statement that fails after an earlier one of the agent's ran
is not retried: re-running from the top would add the version again. The run
reports a partial write with Snowflake's error (`SST-APL001`), and the next
plan and apply finish it rather than refusing it as changed out of band
(`SST-PLN014`). An apply that was killed after the new version was committed
is finished the same way, without committing another version.

### Version drift

The plan reads where the agent's alias and default version point, and whether
it has a live version. An agent whose definition is unchanged but whose alias
or default version is on another version than the newest one holding its spec,
or that has no live version, is an update with reason `version_drift`
(`SST-PLN102`): apply points them back, without adding a version, and sets the
default to `'LAST'` rather than to that version by name, so the next version
apply commits is served too. An agent with
no live version whose newest version holds another spec gets a new version.
This is how a [rollback](ci-cd.md#rollback) is rolled forward.

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
- an absent database for the stage (`SST-PLN011`), or an absent schema with
  `apply.create_schemas: false` (`SST-PLN010`); with it on, apply creates the
  schema first.

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

An agent's name, and the name of a tool member SST defines, are plain
identifiers too: one written quoted, such as `'"MixedCase"'`, is refused
(`SST-PRS005`). SST matches each by its name ignoring case, which finds the
object Snowflake created only when Snowflake stored the name upper-cased, and an
agent's name is also a folder of the stage path its specification is staged
under. A member SST only references may keep a quoted name.

`sst apply --temporary` instead publishes each agent as a session-scoped
`CREATE OR REPLACE TEMPORARY AGENT` from the inline spec, staging nothing; it is
refused for a production-like target. Only agents are temporary: the views,
tools, and skills the same run publishes are permanent. The temporary agent is
dropped when the apply's session ends, before anything else can call it, so it
checks that Snowflake accepts the spec rather than giving an agent to iterate
on; to iterate, publish to a per-developer target. An eval of an agent the run
publishes as temporary is skipped, neither published nor recorded
(`SST-APL105`). A spec over 100,000 bytes is an error
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

## Trying a change before publishing

None of these adds a version to the agent consumers use, and none records
anything in state. Try them in this order: each costs more than the one before.

### Ask the edited agent

```bash
sst ask sales_analyst                                   # a conversation, on a terminal
sst ask sales_analyst "How many orders were placed in March 2026?"
echo "Why did returns rise?" | sst ask sales_analyst - --output json
```

`sst ask` runs the agent as the project compiles now: its own edits, and each
semantic view, search service, procedure, function, skill and sub-agent it
reaches that compiles differently from what the target's state records, through
a scratch copy of each, made for the session in `agents.+scratch_schema` (else
the agent's own schema) and dropped when it ends. The agent runs over the Cortex
Agents REST API, streamed, on a thread, so a conversation carries over from one
question to the next. It takes no lock and writes no state. `--published` copies
nothing and warns of each edit the agent does not see (`SST-ASK001`).
[Asking an agent](ask.md) covers the conversation's keys, the copies and their
privileges, cleanup, and how SST reaches the API.

### Run its evals unpublished

`sst test --suite evals --unpublished` runs each of the agent's eval questions
the same way, one temporary agent per question, and scores the replies without
an evaluation run: `tool_selection_accuracy` exactly, which gates on its
threshold, and `answer_correctness` and custom metrics by an approximate judge,
which never gates. Nothing is published, locked, staged or recorded, and no
baseline is read or captured. [Unpublished eval runs](evals.md#unpublished-eval-runs)
gives the scoring rules.

### Publish to a target of your own

For the evaluation service itself, a baseline, or an edited view tried with the
agent, publish to a [per-developer target](evals.md#a-per-developer-target). An
agent may live in a personal `USER$<user>.PUBLIC` schema, but its evals may not
(`SST-VAL766`).
