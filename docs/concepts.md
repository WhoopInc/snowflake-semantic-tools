# Concepts

The ideas every other page assumes: what SST publishes, how authored pieces find
each other, and what each command does with them.

## Artifacts

An **artifact** is one thing SST publishes and tracks. There are seven types:

| Type | Authored as | Publishes |
|---|---|---|
| `semantic_view` | `semantic_views:` YAML | a semantic view |
| `tool` | tool group YAML | a Cortex Search service, procedure, function, or stage |
| `skill` | a `SKILL.md` folder | a skill-type Cortex Extension |
| `plugin` | `plugin.yml` | a plugin-type Cortex Extension |
| `profile` | `profile.yml` | a CoCo Desktop profile |
| `agent` | `agent.yml` | a Cortex Agent |
| `eval` | an agent's `evals/` folder | an agent evaluation |

Every command works on the same set, in the same order: an artifact is always
published after the artifacts it depends on, so a view exists before the agent
that queries it. The [artifact reference](reference/artifacts.md) lists each
type's position, dependencies, and how it is updated.

## Members

A semantic view is assembled from **members** that are authored separately:

- **facts, dimensions, and time dimensions** come from dbt column metadata
  (`config.meta.sst.column_type`);
- **metrics, relationships, filters, and verified queries** are YAML entries
  under `snowflake_metrics:`, `snowflake_relationships:`, `snowflake_filters:`,
  and `snowflake_verified_queries:`;
- **custom instructions** are YAML entries under `snowflake_custom_instructions:`.

A member is not published on its own; it reaches Snowflake inside every view it
attaches to.

## Association: how members find views

Metrics, relationships, filters, and verified queries attach by **table
membership**: a member joins every semantic view whose `tables:` include all of
the member's `tables:`. Nothing lists a view's metrics; adding a metric over
`orders` adds it to every view that has `orders`.

Two consequences:

- A member whose tables no view covers attaches nowhere, and validation warns
  about it (`SST-MEM005`) instead of dropping it silently.
- A member's `tables:` cannot be empty. An empty list would otherwise mean
  "every view", which is never what an author meant.

Custom instructions are the exception: a view names the ones it wants with
`{{ custom_instructions('<name>') }}`.

## References

Authored files never contain a hardcoded database or schema. They name things,
and SST resolves the name for the target being built:

| Reference | Resolves to | Where it is accepted |
|---|---|---|
| `{{ ref('orders') }}` | the dbt model's relation | view and member `tables:` |
| `{{ ref('orders', 'order_id') }}` | a column of that model | expressions, relationship conditions |
| `{{ metric('order_count') }}` | another metric's expression | metric expressions, verified query SQL |
| `{{ var('name') }}` | a value from `vars:` in `sst_config.yml` | expressions, verified query SQL |
| `{{ custom_instructions('name') }}` | a custom instruction block | a view's `custom_instructions:` |
| `{{ tag('name') }}` | a tag from `tags:` in `sst_config.yml` | a view's `tags:` |
| `{{ semantic_view('name') }}`, `{{ tool('name') }}`, `{{ agent('name') }}` | the published object | agent specs |
| `{{ skill('name') }}`, `{{ plugin('name') }}` | a skill or plugin this project publishes, pinned to its current version | agent skill sources |
| `{{ extension('name') }}` | an extension another project publishes | agent skill sources |
| `{{ eval_metric('name') }}` | a custom judge metric | eval configs |
| `{{ file('path') }}` | the contents of a file beside the artifact | agent instructions |

A reference that does not resolve is an error. SST's `ref()` resolves against
the dbt manifest, so it names models and their columns exactly as dbt does.

## Targets

`--target` selects a target from `profiles.yml`, and that target decides where
everything lands. Configuration writes locations relative to it:

```yaml
semantic_views:
  +database: "{{ target.database }}"
  +schema: "{{ target.schema }}"
```

Keys that start with `+` set a default for everything below them. Under
`semantic_views:`, an unprefixed key names a folder of view files, and its `+`
keys apply to the views in that folder:

```yaml
semantic_views:
  +schema: "{{ target.schema }}"
  finance:
    +schema: FINANCE_SEMANTIC
```

## The pipeline

```text
validate  ->  compile  ->  plan  ->  apply
```

- **`validate`** loads every artifact and checks it. It writes nothing.
- **`compile`** renders every artifact and writes the SST manifest to
  `target/sst/manifest.json`. `--emit-ddl <dir>` also writes each view's DDL.
- **`plan`** compiles, reads what is live in Snowflake, and saves the changes
  to `target/sst/plan.json`. It never writes to Snowflake, and it exits `2`
  when there are changes.
- **`apply`** executes one saved plan, and only if the project still compiles to
  the plan's manifest.

`validate`, `plan`, and `apply` all run the same validation, so a project that
fails `validate` cannot be applied.

## State and ownership

`apply` records every artifact it publishes -- its fingerprint, target, and the
commit it came from -- in a state table, `SST_STATE` in the target schema by
default (`state:` in `sst_config.yml` moves it). The next `plan` compares the
project against that record and against what is live.

### Concurrent applies and the run lock

Only one `apply` runs against a target at a time. Before it writes anything,
`apply` takes the target's row in a lock table beside the state table
(`SST_STATE_LOCK` for the default `SST_STATE`), recording its run id, the role
and machine it runs as, and when the lock expires. The lock is claimed with a
compare-and-set `MERGE` and read back, so of two runs that race for it exactly
one proceeds; the other stops with `SST-APL011` naming the holder. A long apply
extends its lock as it runs, and releases it when it ends, however it ends.
Expiry is judged by Snowflake's clock. A lock left behind by a run that was
killed expires after 30 minutes; `--break-stale-lock` takes over an expired lock
(`SST-APL010`), and never a live one. A run also keeps a lock file beside its
local state cache, which guards one machine only.

A run writes state once, after its changes: one `MERGE` per entry it changed and
one `DELETE` per entry it retired, in a single transaction, then it stamps the
manifest it applied on the target's rows. Entries the run did not touch are left
exactly as they were.

**State and lock table type.** Both tables are standard tables, a choice made in
one place in the connector. From Snowflake's documented semantics:

| | Standard table | Hybrid table |
|---|---|---|
| Primary key | declared, not enforced | required and enforced |
| Concurrent `MERGE`/`UPDATE`/`DELETE` | serialised by a table lock | row-level locks |
| Availability | every account | not every account or region |

A standard table cannot enforce the lock row's key, so SST does not rely on it:
after claiming, it reads the target's lock rows back and holds the lock only if
its row is the only one, withdrawing otherwise. A hybrid table would reject the
second insert outright and let state writes to different targets proceed without
waiting on each other, at the cost of availability. Whether the table lock
serialises two claims exactly as documented, and how a hybrid table behaves
under SST's write pattern, has not yet been measured; a spike against a scratch
schema is pending.

SST changes only objects it published. An object that already exists and that
SST did not publish is reported as **unmanaged** (`SST-PLN024`) and left alone;
adopt it or remove it deliberately. SST never grants new access: access to
published objects is managed outside SST. When it has to replace an object
rather than alter it -- a tool whose definition cannot change in place -- it
re-issues exactly the explicit grants that object already held, and verifies
them afterwards.

`--prune` extends a plan to managed artifacts whose source was deleted. What
happens depends on the type: semantic views and agents are dropped, and
profiles are deactivated. Skills, plugins, and evals are **report-only**: the
plan lists them (`SST-PLN034`, an info diagnostic), but they are never removed,
because an agent elsewhere may still pin a version, or a run or baseline may
still reference an evaluation. A report-only prune never makes `--strict` fail.
`apply --prune` records that the source is gone; until it has, `plan` exits 2,
because that apply still changes the state table. The plan keeps listing the
objects until they are removed by hand.

`--partial` lets `compile`, `plan`, and `apply` go ahead with every artifact
that has no errors and depends on nothing that does. Each artifact left out is
listed (`SST-PLN032`); what is live for it stays as it is, and its state record
is untouched. The command still exits 1 while errors remain, and an error that
names no artifact, such as a configuration error, still stops the run. A saved
partial plan applies only with `--partial`, and `--partial` cannot be combined
with `--prune`, because an artifact left out for errors would look deleted.

## Diagnostics

Every problem SST reports is a **diagnostic** with a stable code, such as
`SST-VAL405`:

- an **error** blocks the command;
- a **warning** does not, unless `--strict` or `validation.strict: true`
  promotes it;
- an **info** never blocks.

Some errors stop other checks from running on the same artifact, so fixing one
error can reveal the next. The [error code reference](reference/error-codes.md)
describes every code and its fix.

## Terms used in the code

The engine's docstrings and diagnostics use a few words in one sense each:

- **key**: an artifact's or member's identity, written `<type>:<name>`, such as
  `semantic_view:jaffle_menu` or `metric:order_count`. Keys are stored in the
  state table and the manifest, and `--select` matches them.
- **subject**: the key a diagnostic is about. `--partial` and the error reports
  use it to tell which artifact an error belongs to.
- **origin**: the file, line, and column a diagnostic points at.
- **fingerprint**: a SHA-256 digest of what an artifact renders. `plan` compares
  it with the fingerprint recorded in state to decide whether an object changed.
- **poisoned**: a member or view that has an error is poisoned. It is still
  checked, so every error is reported at once, but it is not attached to views
  or built; this is why fixing one error can reveal the next.
- **ownership marker**: the `[sst:<manifest id>:<fingerprint>]` tag SST writes in
  an object's comment, which `plan` reads to prove that SST published it.
