---
name: sst-agents-and-evals
description: |-
  Use when writing or changing what SST publishes for Cortex Agents: `agent.yml` files,
  instructions, tools, the skills and plugins an agent loads, CoWork profiles, Cortex Code profiles, and
  agent evals (datasets, run configuration, baselines, gates). Do NOT use for semantic views
  or their members (use sst-semantic-authoring), and do not confuse the project's published
  `skills/` folder with the coding-agent skills of this kit.
---
# Agents, tools, published skills, and evals

Guides: @SST_DOCS@/guides/agents.md, @SST_DOCS@/guides/skills.md,
@SST_DOCS@/guides/plugins-and-coco-profiles.md, @SST_DOCS@/guides/evals.md, and,
for where each publishes, @SST_DOCS@/guides/configuration.md.

## Two kinds of skill

- The project's **`skills/` folder** (`project.skills_dir`) holds skills SST **publishes** to
  Snowflake as Cortex Extensions, for Cortex Agents and Cortex Code profiles to load. Each is
  a folder whose `SKILL.md` has `name` and `description` frontmatter (`SST-PRS034`), and the
  folder name must match `name` (`SST-VAL801`). The folder publishes as authored, subfolders
  and nested skill folders included; every Markdown link must name a file or folder in it
  (`SST-VAL808`), and file names may use only letters, digits, `.`, `_`, `-`, and `$`
  (`SST-VAL857`).
- **This kit** (in `.cortex/skills` or `.claude/skills`) is for the coding agent working on
  the project. It is never published, and must not be moved into `skills/`.

## Agents

Each agent is a folder under `project.agents_dir` (default `agents`) holding `agent.yml`.

```yaml
name: sales_analyst
comment: Order and revenue questions across all locations.
config:
  orchestration_model: claude-sonnet-4-6
  alias: promoted
spec:
  instructions:
    orchestration: "{{ file('instructions/orchestration.md') }}"
    response: Answer in one short paragraph.
  tools:
    - type: cortex_analyst_text_to_sql
      semantic_view: "{{ semantic_view('sales') }}"
      description: |-
        Orders and revenue by customer and location. Do NOT use for product detail.
  skills:
    - source:
        type: CORTEX_EXTENSION
        path: "{{ skill('sales-semantics') }}"
```

- `name:` is the only required key. A key SST does not model is a warning (`SST-PRS004`) and
  is not rendered.
- `config:` holds the agent's settings: the `agents:` `+` keys of `sst_config.yml` without the
  `+` (`orchestration_model`, `budget_seconds`, `budget_tokens`, `tool_not_accessible`,
  `allow_auto_model`, `analytical_search`, `alias`, `secure`, `enabled`, `tags`, `warehouse`, `query_timeout`,
  `database`, `schema`). It wins over every folder route and the block root; an absent key
  inherits, any other key is `SST-CFG053`, and a key with no value is `SST-CFG054`. Never
  write these at the top level or under `spec:` (`SST-PRS126`).
- `instructions` is a map of `orchestration`, `response`, and `sample_questions`; never a
  bare string. `{{ file('<path>') }}` inlines a Markdown file relative to the agent file's
  folder; an absolute path or one leaving the project is `SST-REF027`, a symbolic link
  `SST-PRT009`.
- A tag under `config.tags` names its tag by the `tags:` block of `sst_config.yml`:
  `- {name: "{{ tag('cost_center') }}", value: sales}`. A bare name is `SST-REF050`.
- Each tool's `description` is what the agent reads to pick it: say when to use it, and when
  not to.
- An agent draws charts without a `data_to_chart` tool. Declaring one is allowed and is
  rendered as written, but SST reports it as not needed (`SST-VAL562`, info); don't add one.
- Load a project skill with `{{ skill('<name>') }}` and write neither `name:` (`SST-VAL540`),
  which is the skill's own, nor `version:` (`SST-VAL838`): SST pins the version it publishes. An extension another project publishes
  is `{{ extension('<name>') }}` and must pin a version.
- Every apply of a changed agent adds a new agent version.

## Tool members

A search, generic, or toolset tool calls a tool member, named with one argument:
`search_service: "{{ tool('<name>') }}"` or `identifier: "{{ tool('<name>') }}"`; an
`agent_toolset` over an `agent` member writes `identifier:` too, since its `name:` is only the
tool's own label. A search tool's `stage_path` names a stage member as
`"{{ tool('<stage>') }}/<path>"`, and so does a routine's `imports:` entry; a stage SST does
not declare stays literal, `@<database>.<schema>.<stage>/<path>`. A member
is one file under `project.tools_dir`, in any folder, with a `tool:` root (a `tools:` root
holds a list of them):

```yaml
# tools/sales/menu_search.yml
tool:
  name: menu_search
  type: cortex_search_service
  description: Full-text search over menu descriptions.
  on:
    table: "{{ ref('product_docs') }}"
    search_column: "{{ ref('product_docs', 'description') }}"
  config:
    target_lag: 1 hour
  meta:
    owner: sales-analytics
```

- A member that writes `relations:` references an object another team owns; any other is
  created by SST. A member name is unique in the project (`SST-VAL001`).
- A defined member writes every column as `{{ ref('<on model>', '<column>') }}`: a bare column
  is `SST-REF050`, another model `SST-VAL622`. A referenced member writes its columns bare.
- A routine's body is `body: "{{ file('<path>') }}"`, relative to the tool file's folder. In the
  file, name each table it reads as `{{ ref('<model>') }}` or `{{ source('<source>', '<table>') }}`,
  inside a single-quoted string outside SQL, never as a literal three-part name (`SST-VAL626`), so
  every target reads its own tables. `{{ var('<name>') }}` is the one other call a body takes.
- Settings go under `config:`, the `tools:` `+` keys without the `+`: `database`, `schema`,
  `warehouse`, `target_lag`, `embedding_model`, `execute_as`, `immutable`. Never write them on
  the member itself, and never write `group:`, `define:`, or `reference:` (`SST-PRS126`).
- A `tools:` folder route such as `tools: {partner: {+immutable: true}}` covers the members
  under `tools/partner/`; an immutable member must write `relations:` (`SST-VAL606`).

## Plugins and Cortex Code profiles

A plugin (`plugins/<name>/plugin.yml`) lists its members under `skills:`, and a Cortex Code
profile (`coco_profiles/<name>/coco_profile.yml`, a Snowflake private preview) lists
`skills:` and `plugins:`, as reference calls, one per entry, never bare names (`SST-REF050`):

```yaml
# coco_profiles/sales_analyst/coco_profile.yml
description: "Sales analysts: order, revenue, and month-close questions."
model: auto
skills:
  - "{{ skill('sales-semantics') }}"
plugins:
  - "{{ plugin('sales-toolkit') }}"
```

A Cortex Code profile publishes an AGENT whose specification holds only
`models.orchestration`, `instructions.response`, and `skills`, each skill pinned to its
extension's version alias as an agent's is (`SST-VAL856` when there is none). Its folder name
is `[a-z][a-z0-9_]*` (`SST-VAL801`); `default/` publishes as `DEFAULT`, the profile a role
that sees several is given (`SST-VAL893`). `model` must be in
`snowflake.orchestration_models` (`SST-VAL894`). `hooks`, `mcp_servers`, `commands`,
`owner_team`, and `allowed_roles` are refused (`SST-VAL851`). `coco_profiles/shared/` is
reserved: its `skills:` are pinned in every profile (repeating one is `SST-VAL845`), and its
`AGENTS.md` and `rules/*.md` lead every profile's instructions, before the profile's own
`AGENTS.md`. A leftover `coco_desktop_profiles/` folder is `SST-VAL895`.

A skill (in its `SKILL.md` frontmatter), a plugin, and a Cortex Code profile set their own
settings under `config:`: the `skills:`, `plugins:`, or `coco_profiles:` `+` keys of
`sst_config.yml` without the `+`. A skill or plugin publishes as an extension only where
`bundle_stage` resolves for it; `config: {certified: true}` certifies one skill alone. Folder
routes under those blocks name folders below `skills_dir`, `plugins_dir`, and
`coco_profiles_dir` (`SST-CFG041` for one that names none).

Cortex Code lists only the AGENTs in `CORTEX_CODE.CONFIG` (`SST-VAL891` notes any other
location as a rehearsal). `coco_profiles.+database` and `+schema` default to the target's, and
the schema may not be one an agent or CoWork profile publishes to (`SST-VAL892`). SST grants
nothing: a role needs USAGE on `CORTEX_CODE`, `CORTEX_CODE.CONFIG`, the AGENT, and each
extension, and the apply role CREATE AGENT on `CORTEX_CODE.CONFIG`. Targets that share
`CORTEX_CODE.CONFIG` need their own `coco_profiles.+name_suffix` (for example
`"{{ '' if target.name == 'prod' else '_DEV' }}"`), or a plan stops on the other target's
AGENT (`SST-PLN024`).

## CoWork profiles

A CoWork profile (a Snowflake private preview) is an agent that routes each question to one of
the agents it lists as `subagents:`. Each is one folder under `project.cowork_profiles_dir`
(`cowork_profiles` by default) holding `cowork_profile.yml`; the `cowork_profiles:` block places
it, and it takes an `evals:` block as an agent does. Read @SST_DOCS@/guides/cowork-profiles.md
before writing one.

## Evals

An eval runs an agent against fixed questions and scores the answers. Its files live in the
agent's `evals/` folder, each with a `name:`, and the agent names the pair it publishes:

```yaml
evals:
  dataset: "{{ eval_dataset('sales_questions') }}"
  config: "{{ eval_config('sales_ci') }}"
```

- Never write a path there (`SST-REF050`), and write no `agent:` in the files themselves
  (`SST-PRS126`): the folder says whose eval it is. A name resolves only in this agent's `evals/` folder
  (`SST-REF048`), and is unique per kind there (`SST-VAL765`).
- An eval config's own settings go under `config:`: the `evals:` `+` keys without the `+`, such
  as `agent_version`, `eval_tier`, `retry`, `baseline_runs`, and `retention`. Written at the top
  level or under `run:` they are an error (`SST-PRS126`). Folder routes under `evals:` name the
  agent folders below `agents_dir`.
- To change an eval, add a file under a new name and point the reference at it; the old file can
  stay. Every file in the folder is checked, and only the named pair publishes.

- `sst compile` checks eval files offline, and `sst compile --emit-ddl` writes what they render.
- `sst eval` runs the agent in Snowflake, costs credits, and can take many minutes. Run it only when the user asks, against the target they name.
- A gate run compares with a captured baseline, stored in Snowflake, not in
  `.sst/baseline.json`. With none, the run fails (`SST-VAL758`). A new agent version, or a
  changed dataset or configuration, needs a new baseline (`SST-VAL759`), captured on purpose:
  `sst eval --capture-baseline --reason "<why>"`.
- `sst eval` needs the agent applied exactly as compiled: never published is `SST-APL036`,
  published from an older compile `SST-APL038`, changed in Snowflake since `SST-APL012`; and the session's
  primary role to hold the task and stage privileges (`SST-VAL727`, `SST-VAL728`).
- `sst eval --compare-models <model>,<model>` compares orchestration models on an
  eval: it commits a version of the published agent per candidate, aliased `EVAL_<MODEL>`, runs
  them beside the pinned model, and writes a report; it never moves the default or the released
  alias, and costs credits like any eval run, so run it only when the user asks. Adopting a
  winner is changing `+orchestration_model`, then `sst plan`. `--refresh-cost <compare.json>`
  fills in cost still pending an hour later, running nothing.

## Checking a change

```bash
sst format
sst validate --no-snowflake-syntax-check
sst compile --emit-agent-spec target/agents
```

`--emit-agent-spec` writes each agent's rendered specification, which is exactly what
publishes. Publishing (`sst plan`, then `sst apply`) is the user's call. The people who will
use a published agent need grants SST never makes; @SST_DOCS@/guides/security.md lists them.
To run this in a pipeline, see @SST_DOCS@/guides/ci-cd.md.

## Trying a change before publishing

When the user wants to know whether an edit makes the agent answer better, before committing
or publishing it, offer these in order, and run each only when the user asks: each runs the
agent in Snowflake and costs credits. @SST_DOCS@/guides/ask.md has the details.

1. `sst ask <agent> "<question>" --show-tools --output json`: runs the agent as it compiles
   now, once, on scratch copies (`<NAME>__SST_ASK_<id>`) of each view, search service, tool,
   skill and sub-agent it reaches that is not published as compiled; the copies and the thread
   are removed when it ends. Nothing is published or recorded, and no lock is taken. Always
   pass the question: without one, on a terminal, `sst ask` opens a full-screen conversation
   meant for a person, so suggest the user run `sst ask <agent>` in their own terminal for that.
   The copies go in `agents.+scratch_schema` (else the agent's schema); a refused copy is
   `SST-ASK008`, an unreachable tool `SST-ASK010`. `--published` copies nothing, and then
   `SST-ASK001` names each edit the agent did not see. `sst ask --cleanup` removes copies and
   threads a crashed session left.
2. `sst eval --unpublished`: runs each eval question on the same copies, publishing
   nothing and touching no baseline. `tool_selection_accuracy` is scored exactly and gates on
   its threshold (`SST-ASK007`); `answer_correctness` and custom metrics are judged
   approximately and never gate. Never pass `--capture-baseline` with it (refused).
3. A per-developer target: its own state table and object locations, never shared, with
   `sst plan`, `sst apply` and `sst eval` against it. An agent may live in a
   personal `USER$<user>.PUBLIC` schema, but its evals may not (`SST-VAL766`): Snowflake creates
   no table, dataset or stage in a personal database.
