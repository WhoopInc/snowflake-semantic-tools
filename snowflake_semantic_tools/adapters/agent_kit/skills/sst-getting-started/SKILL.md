---
name: sst-getting-started
description: |-
  Use when the user is new to SST, new to dbt, or asks where to start: what SST is, what a
  semantic view, Cortex Agent, tool, published skill, plugin, CoWork profile, CoCo Desktop
  profile, or eval is, the dbt ideas SST builds on (models, ref(), the manifest, targets),
  setting up a first project, or which guide to read. Explains in plain terms and leads the
  user through the first view step by step. Do NOT use for a specific edit in a working project
  (use sst-semantic-authoring or sst-agents-and-evals), or for an error code (use
  sst-fix-diagnostics).
---
# Getting started with SST

The documentation home is @SST_DOCS@/index.md. The first-project walkthrough is
@SST_DOCS@/getting-started.md, the ideas are in @SST_DOCS@/concepts.md, and every term is
in @SST_DOCS@/glossary.md. Read those pages before explaining anything they cover, and when
this skill and them differ, trust the pages and `sst explain`.

## First, find out where the user is

Ask, or infer from what they say, before explaining anything:

- Do they use dbt already? If not, start with the dbt section below.
- Have they used SST before? A project with an `sst_config.yml` already uses SST; one whose
  scripts still call SST 0.3's deploy or generate commands is on 0.3 (use
  sst-migrate-from-0-3).
- What do they want at the end: a semantic view for Cortex Analyst, an agent people can ask
  questions, or skills for a coding agent?

Then explain only what that goal needs, one idea at a time, in plain words. Define each term
the first time you use it. Show a small example rather than the full list of options.

## What SST is

SST keeps everything a Snowflake AI agent needs as files in a dbt project, and publishes them
the way infrastructure is published: check the files offline, review a plan of exactly what
would change in Snowflake, then apply that plan. Nothing is built by clicking in a UI.

## The pieces, in plain terms

| Piece | What it is | Where it lives |
|---|---|---|
| Semantic view | A Snowflake object that describes tables in business terms -- which columns are measures, which are categories, how tables join -- so Cortex Analyst can turn a question into correct SQL. | `semantic_models/semantic_views/` |
| Member | One part of a view: a metric, relationship, filter, verified query (a question with its known-good SQL), or custom instruction. A member joins every view that has all its tables. | `semantic_models/` |
| Column metadata | Whether each column is a dimension, fact, or time dimension, plus keys and synonyms. | each dbt model's YAML, under `config.meta.sst` |
| Cortex Agent | An AI assistant in Snowflake that answers questions by choosing among its tools. | `agents/<name>/agent.yml` |
| Tool | Something an agent can call: a semantic view, a Cortex Search service, a procedure or function, or another agent. | the agent's `spec.tools`; the objects behind them under `tools/` |
| Published skill | Instructions an agent or a CoCo Desktop profile loads, published to Snowflake. Not the same as this kit, which is for the coding agent and is never published. | `skills/` |
| Plugin | A bundle of published skills. | `plugins/` |
| CoCo Desktop profile | A setup for Cortex Code Desktop: skills, a system prompt, MCP servers, and hooks. | `coco_desktop_profiles/` |
| CoWork profile | An agent that hands each question to one of the agents it lists (a Snowflake private preview). | `cowork_profiles/` |
| Eval | A fixed set of questions an agent is scored on, so a change that makes it worse is caught. | the agent's `evals/` folder |

## The dbt ideas SST builds on

SST reads a dbt project, so a user new to dbt needs these and no more:

- A **model** is a SQL file in `models/` that dbt builds into a table or view. Its YAML file
  describes it and its columns.
- **`ref('<model>')`** names a model by its project name instead of its database and schema.
  SST uses the same call: `"{{ ref('orders') }}"` is the `orders` model, and
  `"{{ ref('orders', 'order_id') }}"` is one of its columns.
- **`profiles.yml`** holds the connection **targets**, such as `dev` and `prod`. A target
  decides which database and schema SST reads from and publishes to. Keep it in the project
  root.
- The **manifest** is dbt's record of the project. SST runs `dbt parse` itself to get it; run
  `dbt deps` first when the project uses dbt packages.
- A **defer target** lets `ref()` read another target's tables (often `prod`) while SST
  publishes to yours. See @SST_DOCS@/concepts.md#targets.

## A first project, step by step

Follow @SST_DOCS@/getting-started.md; this is its outline. Every step up to `sst compile` is
offline.

```bash
python -m pip install snowflake-semantic-tools   # [dbt] adds dbt Core if none is installed
sst init                                        # writes sst_config.yml; never overwrites a file
sst debug --no-connect                          # shows the profile, target, and schema SST resolved
```

1. Pick one dbt model the user knows well. In its YAML, give each column a `description`, a
   `data_type`, and a `column_type` under `config.meta.sst`, and the model a `primary_key`.
   `sst enrich` can fill types from the warehouse, but it connects: ask first.
2. Write one view and one metric, each in its own file (sst-semantic-authoring has the
   shapes).
3. Check it offline, and read the DDL it would publish:

   ```bash
   sst validate --no-snowflake-syntax-check
   sst compile --emit-ddl target/ddl
   ```

4. Fix anything reported with sst-fix-diagnostics. `sst explain <CODE>` explains any code.
5. When the user is ready to connect, `sst plan` shows what would change in Snowflake. The
   user decides whether to run `sst apply`; never run it for them unless asked.

## Which guide to read

| The user wants to | Guide |
|---|---|
| understand the ideas | @SST_DOCS@/concepts.md, @SST_DOCS@/glossary.md |
| build views and members | @SST_DOCS@/guides/semantic-views.md |
| fill column types from the warehouse | @SST_DOCS@/guides/enrich.md |
| know how references work | @SST_DOCS@/guides/references.md |
| set where things publish | @SST_DOCS@/guides/configuration.md |
| build an agent and its tools | @SST_DOCS@/guides/agents.md |
| test an agent before publishing | @SST_DOCS@/guides/ask.md |
| score an agent | @SST_DOCS@/guides/evals.md |
| publish skills, plugins, profiles | @SST_DOCS@/guides/skills.md, @SST_DOCS@/guides/plugins-and-coco-desktop-profiles.md |
| route questions across agents | @SST_DOCS@/guides/cowork-profiles.md |
| see what a change affects | @SST_DOCS@/guides/lineage.md |
| run SST in CI | @SST_DOCS@/guides/ci-cd.md |
| know which roles and grants are needed | @SST_DOCS@/guides/security.md |
| fix a setup problem | @SST_DOCS@/guides/troubleshooting.md |
| move from SST 0.3 | @SST_DOCS@/guides/migrating-from-0.3.md |
| look up a command, key, or code | @SST_DOCS@/reference/cli.md, @SST_DOCS@/reference/config.md, @SST_DOCS@/reference/error-codes.md |

For anything else, start at @SST_DOCS@/index.md.
