# Design

Why SST is built the way it is. These pages are for maintainers changing the
engine and for evaluators deciding whether its approach fits their project.
How to use SST is in the [concepts](../concepts.md) and the guides under
[docs/guides](../guides/); the generated pages under
[docs/reference](../reference/) list every command, key, and code.

**Pages**

- [Decisions](decisions.md): the design choices, each with the alternative it
  rejected and what would reopen it.
- architecture.md: the rings, the pipeline, and how an artifact type plugs in.
- snowflake-behaviour.md: what Snowflake does that the engine compensates for.
- dbt-compatibility.md: the seam between SST and dbt.
- diagnostics.md: how problems are coded, ranked, and reported.

## What SST is

SST is a command-line compiler and publisher for a Snowflake semantic layer kept
as code in a dbt project. From files beside the dbt models it builds semantic
views, the tools agents call, Cortex Agents, their evaluations, skills, plugins,
and CoCo Desktop profiles. It validates them offline, saves a reviewable plan of
what would change in Snowflake, and applies exactly that plan.

The command line is the interface. The package exports nothing but its version.

Drawn from: [package root](../../snowflake_semantic_tools/__init__.py),
[artifact registry](../../snowflake_semantic_tools/domain/model/registry.py).

## Why it is shaped this way

A semantic layer is not one object. A view is queried by an agent; the agent
calls tools and loads skills; an evaluation scores the agent. Managed by
separate tools, each piece has its own idea of a project, a target, and a
reference, and the dependencies between them are visible to none of them. A
change to a view can then break an agent while every tool in the pipeline
reports success.

SST answers that with one engine for every artifact type:

- **One manifest.** A compile records every artifact with its key, fingerprint,
  publish target, dependencies, and source files, so a view and the agent that
  queries it are nodes in one graph and are published in dependency order.
- **One reference system.** Authored files name things -- a dbt model, a metric,
  a tool, a skill -- and never a database or schema. Every name resolves against
  the target being built, and a name that does not resolve is an error.
- **One validation model.** Every problem is a diagnostic with a stable code and
  a registered severity, and `validate`, `plan`, and `apply` run the same
  checks, so a project that fails one cannot be applied by another.
- **One publish path.** Every type goes through the same plan, run lock, and
  state table, and one grant policy. A type differs only in what its registry
  entry declares, its renderer, and, for a type that spans several objects, its
  lifecycle handler.

Rendering is separated from execution. The domain ring is pure: it does no I/O
and reads no clock, environment, or randomness, so a render is a function of its
input alone. "This YAML produces this SQL" is a golden test that needs no
account, and `compile --emit-ddl` writes each rendered payload without
connecting.

A successful `CREATE` is not proof that an object works. Snowflake accepts some
definitions it cannot answer queries over -- it enforces none of the
derived-metric restrictions itself -- so SST carries three further lines of
evidence. Validation encodes the rules Snowflake leaves to query time and, with
`validation.snowflake_syntax_check` on, compiles each expression against
Snowflake. The smoke suite, `sst test --suite smoke`, queries the published
views, their metrics, and their verified queries; `apply` itself runs no
probes. The eval suite scores each agent against a fixed question set and gates
a change on regressions against a captured baseline.

Drawn from: [manifest](../../snowflake_semantic_tools/domain/state/manifest.py),
[template resolver](../../snowflake_semantic_tools/domain/resolve/template.py),
[severity policy](../../snowflake_semantic_tools/domain/diagnostics/policy.py),
[plan](../../snowflake_semantic_tools/app/plan.py),
[purity guard](../../snowflake_semantic_tools/app/purity.py),
[metric restriction codes](../../snowflake_semantic_tools/domain/diagnostics/specs/val_metric.py),
[smoke suite](../../snowflake_semantic_tools/app/smoke.py),
[eval gate](../../snowflake_semantic_tools/app/evals/gate.py).

## What SST is not

- **Not a replacement for dbt.** dbt builds the tables and stays the source of
  truth for what a model is and which columns it has. SST runs `dbt parse` as a
  subprocess and reads the manifest it writes; it never imports dbt. A project
  that publishes only skills, plugins, and profiles needs no dbt at all.
- **Not an access-control tool.** SST never grants new access. The only grants
  it issues re-issue the explicit grants an object held before SST replaced it,
  where the object's type cannot carry `COPY GRANTS`; otherwise it replaces with
  `COPY GRANTS` or does not replace at all, and it verifies the grants
  afterwards. Who may use a published object is decided outside SST.
- **Not a query engine.** Snowflake executes every query. SST compiles to
  Snowflake objects and runs queries only to check them -- syntax checks,
  connected validation, smoke probes, evaluation runs -- and to read what
  `sst enrich` derives.
- **Not a UI.** It is a CLI whose JSON output is a contract for CI.
- **Not a library.** There is no Python API; new behaviour is a command or an
  option.
- **Not a manager of the account.** Databases, schemas, warehouses, and roles
  exist before SST runs. Beyond the objects it publishes, SST creates only what
  publishing needs -- the tables it records state, locks, eval baselines, and
  profiles in, and the stages it uploads to -- and only when they are absent.

Drawn from: [dbt invocation](../../snowflake_semantic_tools/adapters/dbt/invoke.py),
[dbt manifest reader](../../snowflake_semantic_tools/adapters/dbt/manifest.py),
[artifact registry](../../snowflake_semantic_tools/domain/model/registry.py),
[grant handling in apply](../../snowflake_semantic_tools/app/apply/one.py),
[upload stages](../../snowflake_semantic_tools/app/apply/stages.py),
[state table](../../snowflake_semantic_tools/adapters/snowflake/connector/state_table.py),
[eval state](../../snowflake_semantic_tools/adapters/snowflake/eval_state.py),
[package root](../../snowflake_semantic_tools/__init__.py).
