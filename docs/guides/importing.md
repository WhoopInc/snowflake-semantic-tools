# Importing live objects

`sst import` writes objects that already live in Snowflake as project files, so SST can
publish them from then on. It reads one object, a schema's, or a database's, maps every
physical name the object holds back to what the project publishes, and reports what it would
write. With `--write` it creates the files. It never replaces a file, never writes to Snowflake,
and records nothing in state: `sst state adopt` hands an imported object to SST once its files
compile to what is live.

```bash
sst import agent ANALYTICS.AGENTS.SALES_AGENT --target prod     # one agent: report only, exit 2
sst import agent ANALYTICS.AGENTS --target prod --write          # every agent in the schema
sst import semantic_view ANALYTICS --like 'SALES%' --target prod # every matching view in the database
sst import agent ANALYTICS.AGENTS.SALES_AGENT --with-dependencies --write
sst import tool ANALYTICS.TOOLS.ORDER_STATUS --define --write   # a procedure SST will publish
sst import skill @CONFIG.SKILLS.SRC/skills/ops/check-pipeline/ --write
sst import eval EVALS.S.SALES_QUESTIONS --to agents/sales/sales_agent --write
sst import                                                       # on a terminal: asks for each part
```

## What it reads

| Type | Read with | Written as |
|---|---|---|
| `semantic_view` | `SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW` | its view file, one file per member the project does not declare, and the dbt model metadata it sets |
| `tool` | `SHOW` and `DESCRIBE` of a search service, procedure or function | a tool member that references it (`relations:`), or defines it with `--define` |
| `skill` | the version an extension serves, or a stage folder | its skill folder, each file copied byte for byte |
| `plugin` | the version a plugin extension serves | its `plugin.yml`, and each member skill the project does not publish |
| `agent` | `SHOW AGENTS`, and the specification of the version it serves | its `agent.yml`, and each long instruction as a file |
| `cowork_profile` | as an agent, its spec holding an `agent_router` tool | its `cowork_profile.yml` (Snowflake private preview) |
| `eval` | an eval input table's `INPUT_QUERY` and `GROUND_TRUTH` | a dataset in the agent folder `--to` names |

`--agent-version` reads another version of an agent or profile than the one it serves. A
three-part name reads one object; a two-part name every object of the type in the schema; a
one-part name every one in the database. `--like` and `--not-like` narrow a schema or database
by Snowflake `LIKE` patterns, case-insensitive. An object of the other agent kind in a scope is
passed over, and counted. An object carrying SST's ownership marker is not imported
(`SST-IMP003`), nor is one the project already compiles an artifact to, or a tool member
references (`SST-IMP007`): extensions carry no marker, so that is how one SST publishes is told.

## Where files go

Each object goes in the folder whose routes publish it where it is now on `--target`, so its
files compile back to the same name: a folder that already holds artifacts of the type there
comes first, then the shallowest route that places it there. When no folder does, the file
names its database and schema in `config:` (`SST-IMP014`); add a route and drop the override.
`--to` names the folder below the type's directory instead. An object goes in a folder of its
own where its type keeps one: `agents/sales/sales_agent/agent.yml`. A dependency imported with
it goes in the same folder as the object that names it, where that folder places it as its own
routes would.

An agent's settings are written into its own `config:` only where they differ from what its
folder inherits: the orchestration model, budgets, `tool_not_accessible`, `analytical_search`,
`secure`, and the version alias. A tool's warehouse and query timeout are written where they
differ from the agent's. A setting the folder gives that the live object does not have is
reported (`SST-IMP013`), since the next apply sets it.

## How names map back

Every physical name a live object holds is mapped to the project's own call, on the target:

- a semantic view to `{{ semantic_view('<name>') }}`, an agent to `{{ agent('<name>') }}`;
- a search service, procedure, function or partner agent to `{{ tool('<member>') }}`; one the
  project has no member for gets a new tool member that references it;
- a skill extension to `{{ skill('<name>') }}`, a plugin to `{{ plugin('<name>') }}`;
- a view's base table to `{{ ref('<model>') }}`, its source table to `{{ source() }}`, and each
  `TABLE.COLUMN` and metric in an expression to a `ref()` or `metric()` call.

A name the project does not compile is written as the call it would be and reported
(`SST-IMP012`); `--with-dependencies` imports it too, before the object that names it, and the
report clears. A STAGE skill source, which SST refuses (`SST-VAL539`), is written as the project's
skill of its folder's name.

## Semantic views

A 1.0 view lists only its tables; its columns come from each dbt model's `config.meta.sst`, and
its other members attach by the tables they name (see
[Semantic views](semantic-views.md#how-members-attach)). So a live view becomes:

- its view file: tables, variables, `max_staleness`, and `custom_instructions`. A view another
  view renders the same instruction text from reuses that view's blocks, so views sharing a
  table describe it alike (`SST-MEM105`); otherwise one block, named after the view, is written;
- each table's dbt model metadata: keys, synonyms and description, and each fact's and
  dimension's `column_type`, synonyms, sample values, `is_enum` and description. Only a key the
  project leaves unset is written, in the model's YAML, as `sst enrich` writes it; one the project
  sets to another value is kept, and reported (`SST-IMP011`). A PII-tagged column's sample values
  are dropped (`SST-IMP015`);
- one file per metric, relationship, labeled filter, verified query and instruction block the
  project does not declare already, beside the view file.

A typed column the live view does not have is scoped out with `columns:` or `exclude_columns:`,
the shorter. A table that is no dbt model of the manifest refuses the view (`SST-IMP010`). A fact
or dimension computed from an expression has no 1.0 form, since columns come from dbt, and is
reported (`SST-IMP013`). A member import adds attaches to every view over its tables, and those
views are named (`SST-IMP013`): review them with `sst plan`.

## Taking the object over

An imported object is still unmanaged: `sst plan` blocks it (`SST-PLN024`). Adopt it:

```bash
sst compile --target prod
sst state adopt --select agent:sales_agent --target prod
sst plan --select agent:sales_agent --target prod     # update: adopted; SST's marker is written
sst apply --select agent:sales_agent --target prod
```

`sst state adopt` plans the selected artifacts, and compares each object the plan blocks as
unmanaged with its compiled artifact, member by member, as `sst plan` shows an update. One that
matches is recorded in state as adopted, under the run lock; adopt writes no object. One that
differs is refused, naming each member and property that differs (`SST-IMP020`), unless
`--accept-differences` records it anyway, so the next apply publishes the project's definition
over the live one. An agent whose newest version already holds the compiled specification keeps
its versions and gets only its marker. Semantic views, tools, agents and CoWork profiles are
adopted; a skill, plugin or eval has no single object to compare.

The next plan updates an adopted object, which writes SST's ownership marker. An object that
changed after it was adopted is blocked instead (`SST-PLN014`).

## Access

Import reads only. Each read the role is refused is reported for that object, naming the role,
the read and Snowflake's message (`SST-IMP005`), and the other objects still import. SST never
grants a privilege: the reads above need what any session reading those objects needs, such
as `USAGE` on the database and schema and `SELECT` on an eval's input table. A secure agent's
specification is visible only to the role that owns it, so import it as that role.

## Exit codes

`0` when every object was imported, or there was nothing to import; `2` when files are pending
and `--write` was not given; `1` when an object could not be read, mapped or written; `3` when
the command line is refused; `4` when the project or target cannot be read; `5` when Snowflake
cannot be reached.
