# CoWork profiles

> **Snowflake private preview.** CoWork profiles are a Snowflake private
> preview (PrPr) feature, available only to accounts Snowflake has enrolled.
> See Snowflake's
> [About profiles in Snowflake CoWork](https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles)
> and
> [Set up profile routing in Snowflake CoWork](https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/set-up-cowork-profile-routing).
> Snowflake may change how profiles work before general availability, and
> SST's support follows the preview: every profile SST compiles reports
> SST-VAL870 to say so.

A **CoWork profile** decides what Snowflake CoWork looks like for one group of
users: which agents a question can reach, the instructions that pick one, and
the tools the profile answers with itself. Users never choose an agent; the
profile routes each question to the agent best suited to it.

In Snowflake a profile is an AGENT in `SNOWFLAKE_INTELLIGENCE.PROFILES` whose
specification has one `agent_router` tool. SST keeps it apart from the agents
it routes to, as the `cowork_profile` artifact type: its own folder, file, and
checks. It publishes after those agents and is staged and versioned the way an
[agent](agents.md#publishing) is.

| | Agent | CoWork profile |
|---|---|---|
| Authored as | `agents/<folder>/agent.yml` | `cowork_profiles/<folder>/cowork_profile.yml` |
| Answers with | its tools: Analyst, search, procedures, other agents | the agents it routes to, plus `web_search` and `code_execution` |
| Router | none | always one, rendered by SST |
| Lists in CoWork | when attached to the CoWork object | when published to `SNOWFLAKE_INTELLIGENCE.PROFILES` and attached |

A profile controls routing, not access: a user reaches the agents behind it
under the grants those agents already carry.

## Layout

```text
my_project/
  sst_config.yml
  agents/sales/jaffle_analyst/agent.yml
  agents/commons/store_ops_analyst/agent.yml
  cowork_profiles/
    everyone/
      cowork_profile.yml
      instructions/routing.md
```

`project.cowork_profiles_dir` names the folder (`cowork_profiles` by
default). Each profile is one folder holding `cowork_profile.yml` (or
`.yaml`, but not both).

## The profile file

```yaml
name: everyone
comment: The CoWork profile for every jaffle employee.

# Published with ALTER AGENT ... SET PROFILE: the name CoWork shows.
display:
  display_name: Jaffle
  color: orange

config:
  orchestration_model: auto

spec:
  instructions:
    orchestration: "{{ file('instructions/routing.md') }}"
    response: Say which agent answered, and pass its answer on unchanged.

  subagents:
    - agent: "{{ agent('jaffle_analyst') }}"
    - agent: "{{ agent('store_ops_analyst') }}"
      description: Store locations, staffing, and labor hours.
    - agent: "{{ tool('delivery_agent') }}"
      display_name: Delivery
      description: Where one order is, and when it will arrive.

  tools:
    - type: web_search

meta:
  owner: jaffle-analytics
```

| Key | Means |
|---|---|
| `name` | The AGENT's name, as for an agent. |
| `comment` | The AGENT comment, after SST's ownership marker. |
| `display` | `display_name`, `avatar`, and `color`, checked as an agent's `profile:` block is. Named `display` so it is not one more meaning of "profile". |
| `spec.instructions` | `orchestration`, `response`, and `sample_questions`, as an agent's are. |
| `spec.subagents` | The agents the profile routes to; at least one (SST-VAL871). |
| `spec.tools` | `web_search` or `code_execution` only (SST-VAL876): a data question belongs on an agent. |
| `spec.skills` | Skills, pinned as an agent pins them. |
| `spec.passthrough` | Keys copied into the specification unread, such as `mcp_servers` or `mcp_config`. It cannot set `tools` or `tool_resources` (SST-PRS023). |
| `config` | The profile's own `cowork_profiles:` settings, without their `+`. |

## Subagents

Each entry's `agent:` is one template call:

- `{{ agent('<name>') }}` names an agent this project publishes. The profile
  then publishes after it, and the entry takes, where it writes none of its
  own, the agent's `profile.display_name` (else its name), its comment as the
  `description`, and its sample questions. An unknown or disabled agent is
  SST-REF012; a deprecated one is SST-VAL875.
- `{{ tool('<name>') }}` names a tool member of type `agent` that another
  team publishes (see [tool members](agents.md#tool-members)). It takes no
  defaults, so the entry writes `display_name` and `description`
  (SST-VAL874). A tool member of another type is SST-VAL872.

The router reads each `description` to choose an agent, so write it as what the
agent is for. Two entries reaching one agent is SST-VAL873.

SST renders every entry with all four fields the preview's routing guide asks
for, and the profile's one router tool, which you never write:

```yaml
tools:
  - tool_spec: {type: agent_router, name: router}
  - tool_spec: {type: web_search, name: web_search}
tool_resources:
  router:
    subagents:
      - name: JAFFLE_SALES_PROD.AGENTS.JAFFLE_ANALYST
        display_name: Jaffle Analyst
        description: Answers questions about jaffle shop orders, revenue, ...
        source:
          type: DATA_AGENT
          fqn: JAFFLE_SALES_PROD.AGENTS.JAFFLE_ANALYST
          sample_questions: [{question: "What was revenue by month in 2025?"}]
```

`name` is always the agent's qualified name and equals `source.fqn`, which is
the field routing follows. `sst compile --emit-agent-spec <dir>` writes the
rendered specification to `<dir>/cowork_profile/<name>.json`.

## Where it publishes

The `cowork_profiles:` block in `sst_config.yml` places profiles, with folder
routes and an item's `config:` as for [agents](configuration.md):

```yaml
cowork_profiles:
  # Snowflake private preview (PrPr):
  # https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles
  +database: "{{ 'SNOWFLAKE_INTELLIGENCE' if target.name == 'prod' else target.database }}"
  +schema: "{{ 'PROFILES' if target.name == 'prod' else 'COWORK_PROFILES' }}"
```

CoWork lists only the profiles in `SNOWFLAKE_INTELLIGENCE.PROFILES`, a schema
the account has once, so publish there from one target. Every other target
publishes a rehearsal: an AGENT that routes the same way when it is run
directly, which CoWork does not list and SST never attaches. SST-VAL878 notes
each one.

A profile and an agent never share a schema (SST-VAL879): both are AGENT
objects, and a plan tells them apart by the schema it reads each in. An agent
placed in `SNOWFLAKE_INTELLIGENCE.PROFILES` is SST-VAL880, because CoWork
treats every agent there as a profile.

`apply.create_schemas` can create the rehearsal schema; it never creates a
database, so `SNOWFLAKE_INTELLIGENCE` must already exist, as it does once the
preview is enabled.

## Publishing

A profile publishes exactly as an agent does: `CREATE AGENT` from the
specification staged under
`@<apply.agent_spec_stage>/cowork-profiles/<name>/<commit>/agent_spec.yaml`,
then one committed version per change and a live version beside it, then its
alias, tags, display, and `SECURE`. `sst plan` shows its changes member by
member, its own properties under `cowork_profile properties`.

`apply --temporary` leaves profiles out (SST-APL106): a temporary router agent
has not been measured, and a permanent one would route to session-scoped
agents.

Dropping an agent a recorded profile routes to warns SST-PLN017, naming the
profile.

## Attaching to CoWork

> **Snowflake private preview.** Attaching follows
> [About profiles in Snowflake CoWork](https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles)
> and
> [Set up profile routing in Snowflake CoWork](https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/set-up-cowork-profile-routing).

CoWork offers a profile only once it is attached to the account's CoWork object,
the one `SHOW SNOWFLAKE INTELLIGENCES` lists. An account has one, so SST finds
it rather than reading its name from the configuration. `sst apply` attaches
each profile it publishes to `SNOWFLAKE_INTELLIGENCE.PROFILES`:

- Creating a profile ends with
  `ALTER SNOWFLAKE INTELLIGENCE <object> ADD AGENT <profile>`.
- An update attaches a profile that is not attached, in the same program.
- A profile that is otherwise unchanged but was detached by hand plans an update
  that rewrites the profile's comment, carrying SST's ownership marker, then runs that
  statement. It commits no version, stages nothing, and warns SST-PLN104.
  Strict mode leaves it a warning, since the apply it plans is the repair.
  `sst plan` shows it as

  ```text
  ~ update  cowork_profile:everyone  SNOWFLAKE_INTELLIGENCE.PROFILES.EVERYONE  not attached to CoWork; CoWork attachment; no new version
  ```

  and `--full` adds `attached: no → SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT`; plan
  JSON names the object in `attaches_to` and as the `attached` property.
- A rehearsal, published anywhere else, is never attached, and planning one reads
  nothing of the CoWork object.

A plan with a profile to attach reads `SHOW SNOWFLAKE INTELLIGENCES` and then
`SHOW AGENTS IN SNOWFLAKE INTELLIGENCE <object>`, once for the whole run. A read
Snowflake refuses is SST-PLN001, and the profile then plans as though it were
not attached to anything. An account without a CoWork object publishes the
profile unattached, with the warning SST-PLN105; a later plan attaches it once
the object exists.

Attaching needs MODIFY on the CoWork object, or its ownership, held by any role
in the session, a secondary role included. While a profile is still to be
attached, preflight reads `SHOW GRANTS ON SNOWFLAKE INTELLIGENCE <object>`, and a
change that would attach without the privilege is blocked (SST-PLN106) before
apply publishes anything.

`+attach: false`, on the block, a folder route, or a profile's `config:`, leaves
a profile unattached:

```yaml
cowork_profiles:
  +attach: false
```

Turning it off never detaches a profile that is already attached. Attaching is
not part of a profile's definition, so changing `+attach` never plans an update
of the profile by itself. Dropping a profile detaches it, so `sst drop` and
prune run no statement for the attachment.

## What SST does not do

- **Detach a profile**, or change any other attachment of the CoWork object.
  The account's attachments are shared; turning `+attach` off leaves an attached
  profile attached, for CoWork Admin to detach.
- **Set `DEFAULT_AGENT`**, or migrate an account from a Main Agent.
- **Grant access.** Users need USAGE on the `SNOWFLAKE_INTELLIGENCE` database,
  the `PROFILES` schema, and the profile itself; manage those grants where your
  other grants are managed. Attaching needs no grant on the profile, only the
  deploying role's MODIFY on the CoWork object.
- **Ask or evaluate a profile.** `sst ask` and evals run agents only.
- **Route to another profile.** A subagent is always an agent.

Where the preview's server differs from its guides, and the code that depends
on it, is in [Snowflake behaviour](../design/snowflake-behaviour.md#cowork-profiles-private-preview).
