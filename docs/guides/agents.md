# Agents

An agent is a Cortex Agent whose specification SST renders from `agent.yml`,
resolves against the project, and publishes as a new agent version. This guide
covers the agent file, the tools it calls, and how it pins the skills it uses.

## Layout

```text
agents/
  sales_analyst/
    agent.yml
    instructions/
      orchestration.md
      response.md
    evals/            # optional; see the evals guide
tools/
  platform.yml        # tool groups
```

Each agent is a folder under `project.agents_dir`; its name comes from `name:`
in `agent.yml`, and it publishes into `agents.+database` / `agents.+schema`.
Agent folders may sit in grouping folders, `agents/<domain>/<agent>/agent.yml`,
and an `agents.<domain>:` folder route overrides any `agents:` `+` key for the
agents below it, so `agents: {finance: {+schema: FINANCE_AGENTS}}` publishes
every agent under `agents/finance/` into that schema.

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
      tokens: 16000
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

- `spec:` is the Cortex Agent specification. `spec.orchestration` and
  `spec.instructions` are maps.
- `{{ file() }}` inlines a file from the agent's folder, so long instructions
  live in Markdown.
- `alias:` names the version alias SST moves to each new version. Pick a name
  that is not a SQL reserved word.
- Defaults such as the orchestration model, warehouse, query timeout, and
  budgets come from `agents:` in `sst_config.yml` and apply to every agent that
  does not set them.
- A tool's `description` is what the orchestrator reads to choose a tool; say
  what it answers and, just as usefully, what it does not.

An agent with only a `name:` is valid: it renders the smallest specification
Snowflake accepts.

## Tools

Semantic views are referenced directly with `{{ semantic_view() }}`. Every other
tool an agent calls is declared once in a **tool group** under
`project.tools_dir` and referenced with `{{ tool('<group>', '<name>') }}`:

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
      - name: settlements
        type: generic
        description: Settlement records owned by another team.
        relations:
          dev: DEV_DB.PAYMENTS.SETTLEMENTS
          prod: PROD_DB.PAYMENTS.SETTLEMENTS
```

- **`define:`** entries are objects SST publishes and owns: Cortex Search
  services, procedures, functions, and stages. They publish into `tools.+database`
  / `tools.+schema` before any agent that uses them.
- **`reference:`** entries are objects someone else owns. SST never creates or
  changes them; `relations:` maps each target to the object's name. A target the
  map does not cover is an error for any agent that uses the tool, so a
  development run cannot fall through to a production object.
- `immutable: true` marks a group whose objects this project must never change;
  a `define:` entry in it is an error.

In the agent:

```yaml
  tools:
    - type: cortex_search
      name: menu_search
      search_service: "{{ tool('platform', 'menu_search') }}"
      max_results: 5
    - type: generic
      name: settlement_lookup
      identifier: "{{ tool('platform', 'settlements') }}"
      description: Looks up one settlement by order.
      input_schema:
        type: object
        properties:
          order_id: {type: string}
        required: [order_id]
```

A `type: agent` tool delegates to another agent: `agent: "{{ agent('<name>') }}"`
for one this project publishes. Delegation cannot form a cycle.

A tool object that a `define:` entry publishes is replaced when its definition
cannot change in place. SST never grants new access, but it re-issues exactly
the explicit grants the object held before the replacement, then checks that
they are all back, so the objects an agent calls keep the access they had.

## Skills

An agent loads skills published as Cortex Extensions:

```yaml
  skills:
    - name: sales-semantics
      source:
        type: CORTEX_EXTENSION
        path: "{{ skill('sales-semantics') }}"
    - name: partner-glossary
      source:
        type: CORTEX_EXTENSION
        path: "{{ extension('partner-glossary') }}"
        version: VERSION$3
```

- **`{{ skill('<name>') }}`** and **`{{ plugin('<name>') }}`** name extensions
  this project publishes. Do not write `version:`: SST pins the version it
  publishes for the skill's current content, so the agent never points at a
  version that was not created. `name:` must be the skill's name, or one of the
  plugin's members.
- **`{{ extension('<name>') }}`** names an extension another project publishes.
  It resolves through `skills.extensions` in `sst_config.yml` and must pin a
  version; `LIVE` is refused.

The [skills guide](skills.md) covers how skill versions are named.

## Publishing

`sst plan` shows each agent as a create or an update. An agent is created once
and then updated by adding a version, never replaced, so its grants and its
version history survive every publish. After each new version SST moves the
configured alias to it. `sst test --suite smoke` describes each published agent
to confirm it exists as planned; to check what an agent answers, write an
[eval](evals.md).
