# Design decisions

The choices that shape SST, grouped by theme. Each states what the code does,
the obvious alternative it rejected and why, and, where one exists, the change
that would reopen it. The code is the authority: each theme ends with links to
the modules that implement it.

## The artifact registry

### Artifact types are registry data

**Chosen.** Each artifact type is one `ArtifactType` declaration: its root key,
publish position, the types it depends on, whether prune drops it, how its
grants survive an update, its lifecycle shape, the rule sets that validate it,
and its directory. Compile, plan, apply, and `sst docs` read the registry rather
than switching on type names. `RegistryBuilder.freeze` runs its integrity
checks in a fixed order and raises the first `SST-REG` fault, so a contradictory
registration fails at import, before any command runs. Among those checks, the
template functions resolvers handle and the types the registry declares must
agree in both directions (`SST-REG020`), with the types nothing references
named rather than inferred.

**Rejected.** A branch per type in each pipeline stage, kept consistent by
review: adding a type means finding every branch, and nothing reports a miss.

**Revisit when.** A type needs behaviour that neither a registry field nor a
lifecycle handler can hold.

### Members and artifacts are two registries

**Chosen.** Members -- facts, dimensions, metrics, relationships, filters,
verified queries, custom instructions -- are `MemberType` entries in their own
map. Only `semantic_view` may own members (`MEMBER_OWNING_TYPES`, `SST-REG022`).

**Rejected.** One registry with the publish fields left empty for members. Most
fields would mean nothing for most entries, and every reader of a publish field
would need a guard.

**Revisit when.** A second artifact type attaches parts by a rule rather than by
naming them. Widening `MEMBER_OWNING_TYPES` is that decision, taken by name.

### Two lifecycle shapes: one object, or a composite

**Chosen.** An `OBJECT` type is one Snowflake object, observed by its object
type and planned generically. A `COMPOSITE` type -- skill, plugin, profile, eval
-- spans several objects and is planned and applied by its own handler. The
registry refuses a composite that declares an object type, or is pruned or
replaced generically (`SST-REG021`).

**Rejected.** Forcing every type into one object per artifact. A skill is an
extension version plus stage files, and an eval is a table, a dataset, and a
staged configuration; none has one object to observe.

### Publish order comes from the dependency graph

**Chosen.** Each type's `ddl_position` must follow every type it depends on
(`SST-REG010`), and a plan orders its changes by dependency, then position,
then key. Apply starts a change only once every change it depends on ended, so
a view exists before the agent that queries it; only `--fail-fast` runs whole
dependency waves one after another.

**Rejected.** A configurable order. No order other than the one the graph
implies is correct, so a setting could only be redundant or wrong.

Drawn from: [artifact registry](../../snowflake_semantic_tools/domain/model/registry.py),
[registry faults](../../snowflake_semantic_tools/domain/diagnostics/integrity.py),
[template functions](../../snowflake_semantic_tools/domain/resolve/calls.py),
[plan order](../../snowflake_semantic_tools/domain/plan/order.py).

## Configuration

### A key with no correct alternative is not a setting

**Chosen.** Configuration offers a key only when more than one value is correct.
Keys that could only weaken a check or contradict a fact are recorded in the
schema as removed, and setting one is an error that says what replaced it
(`SST-CFG043`, or a dedicated code): `validation.expression_rules`,
`agents.+create_mode`, `agents.+copy_grants`, `skills.+grant_read_to`, and
`vars.sha_version` (`SST-CFG040`), which SST supplies from the commit being
published. A key the schema does not know at all is a warning (`SST-CFG003`),
unless it is a near miss of a top-level block (`SST-CFG007`).

**Rejected.** Keeping removed keys as escape hatches, or treating them like
unknown keys. A switch on a check that must never be skipped makes it
skippable, and a key that is accepted and ignored misleads more than one that is
refused. An unknown key may be a typo or belong to a newer release, so blocking
on it would break a shared configuration across versions.

### The configuration file evaluates two template forms, not Jinja

**Chosen.** `sst_config.yml` evaluates a whole-value target conditional
(`{{ 'A' if target.name == 'prod' else 'B' }}`, chained or not) and
`{{ var('name') }}`. Every other template, `{{ target.schema }}` among them, is
left as written for the reader of the key.

**Rejected.** A full Jinja pass. Loops, includes, and arbitrary expressions make
the grammar open-ended, so validation could not say what a value is without
running it.

### `profiles.yml` is the only target namespace

**Chosen.** `--target` names a dbt target, and one `profiles.yml` does not
declare is an error (`SST-CFG010`). The same target selects where dbt builds and
where SST publishes.

**Rejected.** SST declaring its own targets. Two namespaces can disagree, and an
artifact would then land somewhere other than the models it reads.

Drawn from: [configuration schema](../../snowflake_semantic_tools/domain/model/config_schema/keys.py),
[configuration templates](../../snowflake_semantic_tools/domain/resolve/config.py),
[profiles](../../snowflake_semantic_tools/adapters/dbt/profiles.py),
[configuration codes](../../snowflake_semantic_tools/domain/diagnostics/specs/cfg.py).

## The dbt seam

### SST runs dbt, and never imports it

**Chosen.** By default SST runs `dbt parse` (or `dbt compile`, as `dbt.command`
says) for the selected target, as an argument list through a subprocess runner
and never through a shell, and reads the manifest it just wrote. `--manifest`
reads a given manifest instead, and on that path a model file that no longer
matches the manifest's checksum is an error (`SST-DBT005`) unless
`--allow-stale-manifest` is given. `dbt.invoke: false` opts out of running dbt
and reads the manifest at dbt's target path, checked the same way, since SST did
not write that one either. `dbt-snowflake` is an optional
extra.

**Rejected.** Reading by default whatever manifest is on disk, which may be
stale or for another target; and dbt's Python API, which ties SST to dbt's
internals and release cadence when the manifest is the interface dbt documents.

### Manifest schema versions are an explicit set

**Chosen.** SST reads the manifest schema versions it is tested against, listed
one by one; `dbt.manifest_schema_versions` can name the set. Any other version
is an error (`SST-DBT017`) unless `--allow-unsupported-manifest-schema` is
given.

**Rejected.** A version range. dbt makes no promise that a later manifest schema
is a superset of an earlier one, so a new schema joins the set once a fixture
proves it.

### Models are read as dbt resolved them

**Chosen.** A model's columns, its `meta.sst` metadata, and its location come
from the manifest, and a view names each table for its dbt model, using the
model's `alias:` only in the relation. `primary_key` and `unique_keys` live in
the model's `meta.sst`, so every view that reads the model renders the same
keys. `meta.sst` `database` and `schema` are read only to be refused
(`SST-DBT030`).

**Rejected.** Re-reading dbt's YAML, which can disagree with dbt's own
resolution; per-view keys, though a key describes the model's data; naming
tables by alias, so renaming a relation renames every member; and honouring a
`meta.sst` location, which points SST at a relation dbt never builds.

### `sst enrich` asks the warehouse which columns exist

**Chosen.** Enrich reads each model's relation for its columns. A column the
relation has and the YAML lacks is added; one the YAML describes and the
relation lacks is reported (`SST-VAL325`) and left in place.

**Rejected.** The manifest's column list, which holds only the columns someone
documented.

Drawn from: [dbt invocation](../../snowflake_semantic_tools/adapters/dbt/invoke.py),
[project source](../../snowflake_semantic_tools/adapters/project_source.py),
[dbt manifest reader](../../snowflake_semantic_tools/adapters/dbt/manifest.py),
[stale models](../../snowflake_semantic_tools/adapters/dbt/project.py),
[view tables](../../snowflake_semantic_tools/adapters/yaml/semantic/build.py),
[template resolver](../../snowflake_semantic_tools/domain/resolve/template.py),
[enrich](../../snowflake_semantic_tools/app/enrich.py).

## References and templating

### Templates are protected once, at the load boundary

**Chosen.** The YAML parser swaps each `{{ ... }}` template for a YAML-safe
placeholder, parses, and restores the source text in every string, recording
each template's position. References resolve afterwards, over parsed scalars,
so a diagnostic points at the template's own line and column. A template that is
not a call of the form `{{ fn('arg') }}` is an error (`SST-REF033`), and a call
naming nothing that exists -- a model, a metric, a member named in instruction
prose -- is an error too.

**Rejected.** Requiring quoted templates, though an unquoted `{{` is not valid
YAML and most authored files use it; resolving by regular expression over raw
text, which loses positions and repeats the work in every consumer; and leaving
unmatched text alone, which publishes it verbatim on a green build.

### One reference dialect: 0.3 syntax is an error

**Chosen.** `{{ table() }}` and `{{ column() }}` are errors wherever they appear
(`SST-REF034`, `SST-REF035`), and so are the other hard-deprecated 0.3 forms: a
0.3 field spelling (`SST-PRS020`), 0.3 key metadata (`SST-DBT032`), and inline
view filters (`SST-VAL403`). Each names its replacement, and
`sst migrate refs --write` performs the reference rename.

**Rejected.** Accepting 0.3 input with a deprecation warning. The resolver would
carry two dialects indefinitely, and a project could publish without ever
migrating.

### Metrics name metrics, in bounded chains

**Chosen.** `{{ metric('x') }}` becomes the referenced metric's name as the view
qualifies it, not its expression. A cycle of `metric()` references is
`SST-REF005`, and a chain nested deeper than the resolver's limit is
`SST-REF900`.

**Rejected.** Inlining the referenced expression, after which the view no longer
records that one metric is built from another; and cycle detection alone, since
an acyclic chain can still be deep enough to exhaust the resolver.

Drawn from: [YAML parser](../../snowflake_semantic_tools/adapters/yaml/parse.py),
[template scanner](../../snowflake_semantic_tools/domain/parse/template.py),
[template resolver](../../snowflake_semantic_tools/domain/resolve/template.py),
[member build](../../snowflake_semantic_tools/adapters/yaml/semantic/build_members.py),
[metric depth](../../snowflake_semantic_tools/domain/resolve/depth.py),
[reference codes](../../snowflake_semantic_tools/domain/diagnostics/specs/ref.py),
[reference migration](../../snowflake_semantic_tools/app/migrate_refs.py).

## Semantic views

### Members attach by table membership

**Chosen.** A metric, relationship, filter, or verified query joins every view
whose tables include all of its own. A member's tables are the models its
templates reference, never a list it writes; a derived metric takes those of the
metrics it names. A member that names no table is an error (`SST-MEM002`), and
one that reaches no view is a warning (`SST-MEM005`). A view may narrow what it exposes with
include or exclude lists of columns, metrics, and relationships.

**Rejected.** Membership by listing, under which every new metric is an edit of
every view that should carry it; and an empty list meaning "every view", which
no author intends.

### Clause order is Snowflake's; member order is sorted

**Chosen.** Clauses render in the order the server accepts, which for
`MAX_STALENESS` is not the published grammar's. Tables render in the order
authored; relationships, facts, dimensions, and metrics render sorted by
qualified name. Variables and verified queries render in the order they are
found.

**Rejected.** Authored order for members. Sorting keeps a re-render
byte-identical when an unrelated column is added, so the fingerprint changes
only when content does.

Drawn from: [membership](../../snowflake_semantic_tools/domain/resolve/membership.py),
[view scope](../../snowflake_semantic_tools/domain/model/semantic_view.py),
[semantic view renderer](../../snowflake_semantic_tools/domain/render/semantic_view.py).

## Rendering and publishing

### Rendering is separate from execution

**Chosen.** Compile renders each artifact into a payload and the statements
that publish it inside a pure phase, from the domain ring's renderers; `apply`
executes them, and a composite type's handler builds its statements from the
rendered artifact. Import contracts keep I/O, the clock, and the environment
out of the domain, and an audit hook refuses a file, socket, or process event
inside a pure phase (`SST-INT002`).

**Rejected.** Rendering and executing in one call. "What would run" is then
unavailable offline, and a golden test of rendered SQL needs an account.

### Every statement is a typed value

**Chosen.** SQL is an `Sql` value composed from constant templates and quoting
constructors, and nothing outside the SQL package can construct one from a
string, so a value from a project file or from Snowflake reaches a statement
only through a constructor that quotes or refuses it.

**Rejected.** String formatting with quoting helpers. One site that forgets to
quote is an injection, and nothing finds it.

### Published versions are added, never rewritten

**Chosen.** An agent is created from a staged specification and left with
exactly one committed version; each later change to its specification adds a
version, and the agent is never replaced. A change to only its comment,
profile, tags, alias, or `secure` adds none: each committed version's comment
carries the fingerprint of its specification, so plan sees the newest version
already holds it. A skill or plugin version's alias carries a digest of its
bundle, so a change adds a version rather than rewriting one.
`apply --temporary` publishes only the agents as session-scoped temporary
agents, records nothing about them in state, skips their evals (`SST-APL105`),
and is refused on a production-like target (`SST-APL013`). A temporary agent is
dropped when the apply's session ends, so it checks that Snowflake accepts a
spec; it is not an agent to iterate on, which a per-developer target is.

**Rejected.** Replacing an agent on each change, which discards its version
history and the grants on it; rewriting an extension version in place, which
changes what an agent pinned to it loads; and iterating on the permanent agent,
which adds a version per experiment to the object consumers use.

### Stages are created when absent, and never changed

**Chosen.** Before its first upload to a stage, a run creates it with
`CREATE STAGE IF NOT EXISTS`: with server-side encryption for agent
specifications, skills, plugins, and profiles, and with the file format eval
runs read for an eval's configuration. A stage of another kind, or an eval
configuration stage with another file format (`SST-APL028`), fails the change;
SST never alters, replaces, or drops a stage.

**Rejected.** Requiring the stage in advance, which lets a plan report success
on a fresh schema and every agent fail at apply.

Drawn from: [rendered artifact](../../snowflake_semantic_tools/domain/model/lifecycle/rendered.py),
[purity guard](../../snowflake_semantic_tools/app/purity.py),
[typed SQL](../../snowflake_semantic_tools/domain/sql/__init__.py),
[agent statements](../../snowflake_semantic_tools/app/compile/agents/compiled.py),
[temporary agents](../../snowflake_semantic_tools/app/apply/temporary.py),
[extensions](../../snowflake_semantic_tools/app/lifecycle/extensions.py),
[upload stages](../../snowflake_semantic_tools/app/apply/stages.py),
[eval stage](../../snowflake_semantic_tools/app/lifecycle/evals.py).

## Manifest, state, and plans

### Each artifact is compared with its own state entry

**Chosen.** `compile` writes the manifest to `target/sst/` as build output, and
an artifact's fingerprint is a digest of its canonical rendered payload,
including what it resolves from its dependencies. Plan compares it with that
artifact's own state entry and with the live object, so an artifact whose
fingerprint matches is unchanged whatever else the project changed.

**Rejected.** Trusting state only when the same manifest wrote it. State is
stamped by whichever manifest last applied, so any other compile would
republish everything.

### Plan is read-only, and apply runs exactly the plan

**Chosen.** `plan` never writes to Snowflake and exits 2 when declared and
actual state differ, so CI can gate on it; `plan --use-cached-state` plans from
a recorded observation and does not connect. `apply --plan` re-plans and
refuses a saved plan made for another target or manifest, or whose observations
or changes no longer match.

**Rejected.** An apply that recomputes and runs whatever it finds, which makes
the reviewed plan advisory.

### State and the run lock live in Snowflake

**Chosen.** `apply` records each published artifact in a state table keyed by
target, by default in the target's own database and schema; the local file is a
cache. A run that writes takes the target's row in a lock table beside it,
receives a fencing token it must present to extend, release, or write state, and
once it holds the lock re-reads state, refusing a plan whose state changed
since. Both are standard tables.

**Rejected.** A local state file, which a fresh CI runner lacks and two machines
would each believe; and hybrid tables, which would enforce the lock row's key
but are not available in every account or region and cannot be created in a
transient database or schema.

**Revisit when.** Hybrid tables are available wherever SST runs and their
behaviour under SST's write pattern is measured.

### Ownership is a marker on the object

**Chosen.** SST writes `[sst:<manifest id>:<fingerprint>]` at the end of an
object's comment, after the description a reader sees first, and reads it from
the `SHOW` rows plan already fetches, wherever in the comment it sits. An object under
an artifact's name that state does not record is never taken over
(`SST-PLN024`, which no setting demotes).

**Rejected.** Adopting unmanaged objects, whose grants, versions, and history
SST did not create and cannot vouch for.

### Failures are contained, and nothing rolls back

**Chosen.** When a change fails, apply skips every change that depends on it
(`SST-APL002`), or, with `--fail-fast`, every change not yet run. What finished
stays published and is recorded in state; apply never rolls back. `--partial`
plans around errors up front: it goes ahead with every artifact that has none
and depends on nothing that does, lists each one it leaves out (`SST-PLN032`),
still exits 1, and cannot be combined with `--prune`. An error it cannot trace
to an artifact leaves nothing to exclude, and the run stops (`SST-PLN033`).

**Rejected.** Rolling back a run, which is a second set of writes that can fail
in its turn when the next plan already shows what remains; all or nothing for
every run, which lets one broken artifact block every unrelated change; and
partial prune, under which an artifact left out for errors would look deleted.

### Removal is narrow, and depends on what it would break

**Chosen.** `--prune` drops semantic views and agents and deactivates profiles.
Skills, plugins, and evals are only reported (`SST-PLN034`), because an agent
may pin a version and a run or baseline may reference an evaluation; tools are
never pruned. `sst drop` removes one object SST owns from one target, under the
run lock, without reading the project.

**Rejected.** One prune rule for every type, which either deletes what others
still use or leaves views and agents behind; and prune as the only way to
remove an object, though it needs a compiling project and removes only what is
no longer declared.

### A stop is an interrupt, and prompts appear only where they can be answered

**Chosen.** Ctrl-C, `SIGTERM`, and `SIGHUP` end a run alike (exit 130,
`SST-PRT107`): an interrupted apply starts no further change, records what
finished, and releases its lock. A confirmation prompt appears only when stdin
and stderr are terminals and the output is not JSON; otherwise, and always for
`sst drop` and `apply --prune`, the command needs `--yes` (`SST-PRT109`).

**Rejected.** Treating a supervisor's signal as a crash, which leaves the lock
held and the run unrecorded; and prompting where nobody can answer, which hangs.

Drawn from: [manifest](../../snowflake_semantic_tools/domain/state/manifest.py),
[change classification](../../snowflake_semantic_tools/domain/plan/classify.py),
[saved plan](../../snowflake_semantic_tools/domain/state/saved_plan.py),
[partial runs](../../snowflake_semantic_tools/app/partial.py),
[state](../../snowflake_semantic_tools/app/state.py),
[run lock](../../snowflake_semantic_tools/domain/state/lock.py),
[lock table](../../snowflake_semantic_tools/adapters/snowflake/connector/lock_table.py),
[ownership marker](../../snowflake_semantic_tools/domain/model/lifecycle/marker.py),
[apply run](../../snowflake_semantic_tools/app/apply/run.py),
[prune](../../snowflake_semantic_tools/domain/plan/prune.py),
[drop](../../snowflake_semantic_tools/app/drop.py),
[signals](../../snowflake_semantic_tools/cli/signals.py).

## Grants

### SST never grants new access

**Chosen.** Access to published objects is managed outside SST. The only
`GRANT` SST issues re-issues an explicit grant an object held before SST
replaced it. Each rendered object's `PublishShape` declares how its grants
survive an update, and apply reads that: a semantic view, procedure, or function
is replaced with `COPY GRANTS`; a Cortex Search service, whose statement cannot
carry the clause, has its explicit grants read before the replace and re-issued
after it (`GrantPreservation.REPLAY`); an agent, a stage, and the composite
types are never replaced. The registry declares the same per type, or per
object kind for `tool`, whose kinds differ, and refuses a declaration that
contradicts how the type publishes (`SST-REG021`).

**Rejected.** Managing access from the project, which makes SST a second
access-control system beside the one that governs the account; and one replace
behaviour for every type, though not every object type accepts `COPY GRANTS`
and a bare replace drops every explicit grant and reports success.

### Grants are verified after every replace

**Chosen.** Apply compares the explicit grants after a replace with those before
it, by privilege, grantee, and grant option, and fails the change when one is
missing (`SST-APL009`); grants it cannot read back leave the change applied
with a warning that they are unverified (`SST-APL008`). It refuses to run a
replace that should carry `COPY GRANTS` and does not (`SST-APL004`), and
`plan` warns when a replace targets an object with explicit grants
(`SST-PLN013`) unless `--no-grants` skips the reads.

**Rejected.** Trusting the statement. A replace that loses a grant succeeds, and
consumers find out first.

Drawn from: [artifact registry](../../snowflake_semantic_tools/domain/model/registry.py),
[apply one change](../../snowflake_semantic_tools/app/apply/one.py),
[tool renderer](../../snowflake_semantic_tools/domain/render/tool.py),
[apply codes](../../snowflake_semantic_tools/domain/diagnostics/specs/apl.py).

## Diagnostics

### Every problem has a stable code

**Chosen.** Each problem is a registered diagnostic with a code, a severity, a
message template, and, for most, a suggestion. A retired number is never
reused, and the registry refuses one (`SST-REG017`). `VAL` is one area split by
hundreds, one band per artifact or member family.

**Rejected.** Free-form messages, recycled numbers, and separate areas for
agents, evals, and skills. A code is a contract users write into overrides,
baselines, and CI scripts, and a new area changes its number.

### Severity resolves in one order, and `--strict` has no exemptions

**Chosen.** Declared severity, then the project's per-code override, then
strict promotion. `--strict` promotes every warning about what the run
selects, in every phase that reports one: validation's, the ones plan and
apply raise while planning (promoted before either decides to refuse), and the
baseline's own expiry notice. A code the registry marks non-demotable cannot be overridden below
error (`SST-CFG033`). A diagnostic the baseline holds never blocks, and is
counted rather than shown, until the baseline expires.

**Rejected.** Exempting rules from `--strict`, which is a second, silent
severity policy; and turning a rule off while a backlog is fixed, which also
hides every new instance of it.

### Reports are complete

**Chosen.** One artifact's error does not stop the others being checked, so a
run reports every artifact's errors at once; an artifact with an error is not
built. A diagnostic that only follows from another names its cause and is
downgraded to info and counted (`--show-cascade` shows it), as `SST-MEM107` is
for a member an unresolved reference poisons. A connected rule
that could not run is reported as `SST-VAL020`. `--output json` lists every
diagnostic at every severity, baselined or not.

**Rejected.** Stopping at the first error, which turns fixing a project into one
run per error; reporting every consequence at full severity, which buries the
cause; silence for a skipped check, which reads as a pass; and filtering the
envelope like the terminal, which hides from CI what it may need to act on.

Drawn from: [diagnostics](../../snowflake_semantic_tools/domain/diagnostics/__init__.py),
[code areas](../../snowflake_semantic_tools/domain/diagnostics/specs/__init__.py),
[retired numbers](../../snowflake_semantic_tools/domain/diagnostics/specs/retired.py),
[severity policy](../../snowflake_semantic_tools/domain/diagnostics/policy.py),
[baseline](../../snowflake_semantic_tools/app/baseline.py),
[output](../../snowflake_semantic_tools/cli/output.py).

## Evidence that a change works

### A successful `CREATE` is not the gate

**Chosen.** Validation encodes the Snowflake rules that would otherwise fail
only at query time: a window function is legal only in a table-scoped window
metric (`SST-VAL102`, `SST-VAL126`), and connected validation reads a range
relationship's target for overlapping ranges (`SST-VAL218`). It compiles each
expression against Snowflake when `validation.snowflake_syntax_check` is on, and
that key has no implicit value: leaving it unset is an error (`SST-CFG031`),
because it decides what a green validate proves.

**Rejected.** Treating a successful create as proof, though Snowflake accepts
definitions it cannot answer queries over and enforces none of the
derived-metric restrictions itself; and a silent default for syntax checking,
under which a reader cannot tell what a passing run checked.

### A dangling agent reference is an error, looked up at every connected plan unless --no-validate

**Chosen.** Snowflake accepts an agent whose consumed extension, pinned version,
or called object does not exist, and the agent then reports success without the
skill or tool. `SST-VAL531` is therefore an error by default, still demotable
through `diagnostics.severity_overrides` for a target whose referenced objects
may be absent. `sst plan` looks up each consumed extension and the version it
pins on every connected plan unless `--no-validate` skips validation, even with
`validation.snowflake_syntax_check` off; the rest of the connected checks still
follow that key.

**Rejected.** A warning that only `--strict` promotes, under which plan and apply
go ahead and publish an agent with no skill; and looking the references up only
with the connected checks on, which leaves them unseen in a pipeline that turns
those checks off.

**Revisit when.** Snowflake refuses a dangling reference at `CREATE AGENT`.

### Smoke probes are a separate suite, not part of apply

**Chosen.** `sst test --suite smoke` queries the published views, their metrics,
and their verified queries, after proving SST owns each object. `apply` runs no
probes, and `validation.smoke_query` is a removed key.

**Rejected.** Probing inside apply, which makes every deploy pay for a full
query sweep and couples publishing to warehouse load.

### Evals score agents, and gate on regressions

**Chosen.** An eval belongs to one agent and runs in the agent's schema. A gated
metric fails the run when its results regress against a captured baseline. A
baseline records the agent version, the dataset, and each metric's version and
judge, so a run it was not captured for is not compared with it, and it
expires. A retry starts only after an attempt that did not pass.

**Rejected.** A single absolute score: each run re-invokes a non-deterministic
agent, so a fixed number either flaps or is too loose to fire.

### The required gate is offline

**Chosen.** Every check a pull request or a release needs runs with no account
and no secrets, and the test process refuses network access to any test not
marked `live`.
Real dbt runs in it only as `dbt parse`, which needs no warehouse.
Checks against a real account run only in a manually triggered workflow, with
credentials a maintainer supplies, once before every release tag.

**Rejected.** A Snowflake-backed gate on every pull request, which would need
one organisation's account and credentials in an open-source repository.

Drawn from: [syntax check key](../../snowflake_semantic_tools/domain/model/config_schema/keys.py),
[window checks](../../snowflake_semantic_tools/domain/validate/semantic/windows.py),
[relationship codes](../../snowflake_semantic_tools/domain/diagnostics/specs/val_relationship.py),
[agent reference lookup](../../snowflake_semantic_tools/app/compile/agents/observe.py),
[smoke suite](../../snowflake_semantic_tools/app/smoke.py),
[eval gate](../../snowflake_semantic_tools/app/evals/gate.py),
[eval runs](../../snowflake_semantic_tools/app/evals/run.py),
[network guard](../../tests/helpers/network_guard.py),
[manual Snowflake checks](../../.github/workflows/slow.yml).

## Inputs and interface

### One interface, few dependencies

**Chosen.** The package exports only its version; behaviour is a command or an
option, and exit codes, the JSON envelope, and diagnostic codes are the
contract. At run time SST depends on PyYAML, ruamel.yaml, the Snowflake
connector, and click; dbt is an optional extra.

**Rejected.** A Python API beside the CLI, which doubles the surface every
change must keep compatible; and convenience libraries those four and the
standard library already cover, each a licence, an audit, and an upgrade to
carry.

### YAML is parsed strictly

**Chosen.** SST's own YAML refuses anchors, aliases, merge keys, non-string
keys, and a key written twice, and every parse is depth-bounded.

**Rejected.** YAML's full feature set. A duplicate key silently drops a value,
an alias makes one entry's content depend on another's position, and unbounded
nesting turns a crafted file into a crash.

Drawn from: [package root](../../snowflake_semantic_tools/__init__.py),
[YAML parser](../../snowflake_semantic_tools/adapters/yaml/parse.py),
[bounded YAML](../../snowflake_semantic_tools/adapters/bounded_yaml.py),
[project metadata](../../pyproject.toml).

## Alternatives considered

**Extending the existing single-artifact tool instead of building a new
engine.** A tool built around one artifact type keeps that type's assumptions in
its parser, its object model, its manifest, and its generator. An agent is a
top-level object, not a clause inside a view, so it would need a second
generator beside the first, and every later type a third. Extending in place
pays most of a rewrite's cost while keeping the coupling, and it leaves
rendering fused to execution, so offline golden tests stay impossible.

**Infrastructure as code, such as Terraform or a declarative change-management
tool.** These fit objects that are written down one by one, and they remain the
right home for warehouses, roles, and grants. A semantic layer is compiled
rather than declared: a metric belongs to every view that holds its tables, and
a view's columns come from dbt metadata. Expressing that as a resource graph
means either generating the resources -- which needs this engine anyway, plus a
second tool -- or giving up the derivation.

**dbt MetricFlow as the semantic source.** It would bring an existing dialect
and dbt-native lineage, but it does not target Snowflake semantic views and has
no representation for agents, tools, evals, or skills. Snowflake features such
as range relationships and verified queries would have no spelling, and the
project would translate between two semantic models permanently.

**Keeping agents in a separate tool.** Splitting the tools splits the
dependency graph. The checks that matter most -- an agent published only after
the views it queries, an eval that fails when a change to a view breaks that
agent -- need both sides in one graph, one manifest, and one plan.

**Letting Snowflake own compilation.** A thin wrapper could hand files to
Snowflake and let `CREATE` decide. But a successful create says little about
whether queries work, so local validation is needed regardless, and that
validation is most of an engine. A wrapper also has no answer for agents,
skills, or the order in which dependent objects publish.

Drawn from: [artifact registry](../../snowflake_semantic_tools/domain/model/registry.py),
[membership](../../snowflake_semantic_tools/domain/resolve/membership.py),
[semantic view renderer](../../snowflake_semantic_tools/domain/render/semantic_view.py),
[plan order](../../snowflake_semantic_tools/domain/plan/order.py).
