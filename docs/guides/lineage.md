# Lineage

`sst compile` records the project's lineage in its manifest, and `sst lineage`
reads it back: from a dbt source, through the models built on it, to the view
members, tools, agents and evals that read them. Lineage answers two questions
before a change ships: what a node is built from, and what a change to it
reaches. The same graph scopes `--select` and `--exclude`, so a plan can cover
only what a model change reaches.

## What the graph holds

| Node | Key | Reads |
|---|---|---|
| A dbt model, seed or snapshot | `model:<name>`, `seed:<name>`, `snapshot:<name>` | the models, seeds, snapshots and sources dbt says it depends on |
| A dbt source table | `source:<source>.<table>` | nothing; it is where the walk ends |
| A dbt test or exposure | `test:<name>`, `exposure:<name>` | the nodes it tests or names; a test is a leaf |
| A view member | `metric:<name>`, `filter:`, `relationship:`, `verified_query:`, `custom_instruction:`; `dimension:<table>.<name>`, `fact:<table>.<name>` | the models its `{{ ref() }}` and `{{ source() }}` calls name, and the members it uses, such as a metric another metric builds on |
| An artifact | `semantic_view:<name>`, `tool:<name>`, `agent:<name>`, `eval:<name>`, ... | its members, the artifacts it uses (an agent's views, tools and skills; an eval's agent), and a routine body's `ref()` and `source()` calls |
| A tool another team owns | `tool_ref:<name>` | nothing SST can see |

dbt's part of the graph is read as the target resolves it, deferral included,
so it is the graph that target would build. A procedure's or function's body
joins the graph when it names its tables with `{{ ref() }}` or
`{{ source() }}`; see [Routine bodies](references.md#routine-bodies). A body
that names them literally is `SST-VAL626`, and its tool is shown as reading
nothing known.

## Tracing one node

```bash
sst lineage tool:order_status                     # both directions
sst lineage model:orders --downstream             # everything orders feeds
sst lineage metric:total_revenue --upstream       # what the metric is built from
sst lineage model:orders --downstream --depth 2   # at most two steps away
```

NODE is any key in the table above, or a bare name, which matches every node of
that name. The name after a kind may glob: `model:stg_*`.

The trace prints as a tree, each node under the one it was reached from, with
its kind and the Snowflake object it is:

```text
tool:order_status  tool  SST_REF_DEV.JAFFLE.ORDER_STATUS
upstream
  model:orders  model  SST_REF_DEV.JAFFLE.ORDERS
downstream
  agent:jaffle_analytics_agent  agent  SST_REF_DEV.JAFFLE.JAFFLE_ANALYTICS_AGENT
    eval:jaffle_analytics_agent  eval  SST_REF_DEV.JAFFLE.EVAL_JAFFLE_ANALYTICS_AGENT_DCE1F8A
✓ traced 4 nodes from tool:order_status
```

A subtree reached a second time prints once, and after that as
`<key>  (shown above)`.

### Where the walk ends

Where the walk meets work outside the project, the node says so rather than
ending silently:

| Marker | Meaning |
|---|---|
| `(raw source)` | a dbt source: the data is loaded, not built, here |
| `(defined in another project; upstream continues there)` | a model of another dbt package that reads nothing in this one |
| `(owned by another team)` | a `tool_ref:` member an agent calls that SST never publishes |
| `(reads: unknown; body names no ref())` | a procedure or function whose body names its tables literally, so SST cannot see what it reads |

### What changed since another run

`--state <dir>` compares with the `manifest.json` in another run's build
directory, such as the one production last applied, and marks with `~ changed`
each artifact that is new or whose fingerprint differs, and each view member
whose fingerprint differs, so a model edit shows which metrics and views it
moved. Members are compared only when the other manifest records their
fingerprints, which manifests from this release on do:

```bash
sst lineage model:orders --downstream --state prod-run/
```

### Reading the compiled manifest, or the project as it is

By default `sst lineage` reads the manifest `sst compile` last wrote, offline,
and exits 4 when there is none. `--no-manifest` compiles the project in memory
instead and writes nothing; what fails to compile is reported and exits 1. A
NODE the graph does not hold exits 3 (`SST-PRT112`).

### Output for scripts

`-o plain` prints one `<depth> <key>` line per node, upstream depths negative:

```text
0 tool:order_status
-1 model:orders
1 agent:jaffle_analytics_agent
2 eval:jaffle_analytics_agent
```

`-o json` returns the nodes, each with `key`, `kind`, `name`, `relation`,
`depth`, `direction` and `marker` (and `changed` under `--state`), and the
edges between them as `from` / `to` pairs.

## Selecting by lineage

Every `--select` and `--exclude` takes dbt's graph operators over the same
graph:

```bash
sst plan --select model:orders+                  # every artifact orders feeds
sst plan --select +agent:support_agent           # the agent and what it reads
sst plan --select 2+metric:total_revenue+1       # two steps up, one step down
sst list --select tool:order_status+             # what calls the procedure
```

`+X` adds what X reads, `X+` what reads X, and a number bounds the steps. dbt
nodes, members and `tool_ref:` name no artifact on their own, so they are
refused without an operator. `sst enrich --select +model:orders` walks dbt's
graph alone, to enrich a model and those it is built from.

[CI/CD](ci-cd.md#what-a-change-reaches) shows these in a pull-request job.

## How plan uses the per-member record

Each apply records a fingerprint per member in state. When `sst plan` does not
read an update's live definition, because of `--no-diff` or a read the role may
not make, it still names the members that changed since the last apply,
without their values: the update's `diff_status` is `from_state`, and
`changed_members` lists them. See [CI/CD](ci-cd.md).
