# The Python API

SST has two supported interfaces: the `sst` command line, and the
`snowflake_semantic_tools.api` package for a program that loads a project once
and asks it questions, such as an editor or a service. Every other module is
internal and may change in any release; import only from
`snowflake_semantic_tools.api`.

The API reads and checks a project. It does not connect to Snowflake, print,
or exit, and it never writes a project file: unsaved edits are read from a
private copy of the project.

## Open a project

```python
from pathlib import Path

from snowflake_semantic_tools.api import ProjectSession

session = ProjectSession.open(Path("my_dbt_project"), target="dev")
```

`open` reads the project as `sst compile` does: it finds `sst_config.yml`, runs
`dbt parse` unless a `manifest_path` is given (as `--manifest` gives one), and
compiles every artifact. A project that cannot be read at all raises
`ProjectError`, carrying the diagnostic `sst` would report; anything wrong in
what was authored is a diagnostic in `validate`, not an exception.

## What a session answers

| Call | Returns |
|---|---|
| `session.catalog()` | an `ObjectSummary` for each compiled artifact, in the order `apply` publishes them: its key, type, name, target, source files, dependencies, members, the dbt models it reads, its fingerprint, its `route_folder`, and its `settings`, each with the step that set it |
| `session.render(key)` | a `Rendered`: what one artifact publishes, its definition and the statements that create it; `KeyError` for a key the project did not compile |
| `session.validate(strict=None, baseline=True, select=(), exclude=())` | a `Validation`: what `sst validate --no-snowflake-syntax-check --output json` reports, as data; `select` and `exclude` narrow it as `--select` and `--exclude` do |
| `session.compiled` | the compile result itself, for a caller that needs more than the summaries give |
| `session.reload(dbt=True)` | a new session from the project's files as they are now |
| `session.with_overlay(files)` | a new session from them with unsaved edits over them; see below |
| `session.schema_for(path)` | the kind of the JSON Schema that checks a project file, a key of `schemas()`; `None` for a file SST does not read |
| `session.lineage(node, upstream=True, downstream=True, depth=None)` | a `Lineage`: what `sst lineage NODE --no-manifest --output json` traces, as `LineageNode` and `LineageEdge` records; `KeyError` for a node the graph does not hold |
| `session.state_table()` | the state table `sst apply` records the session's target in, as SQL, and the target name its rows are recorded under; read offline |
| `session.locations()` | a `Locations`: for every target the profile declares, where each routed block's root and route folders publish, and which setting says so; read offline, with no credential |
| `session.new_files(kind, name, folder=())` | the files a new artifact or member starts as, by project path; see below |
| `session.new_eval(subject)` | the edits that start an eval of an agent or CoWork profile |
| `session.removal(key)` | the edits that take an artifact out of the project; `None` deletes a file |

`validate` applies the project's severity overrides, strict mode
(`validation.strict`, or `strict=` as `--strict` and `--no-strict` give it)
and `.sst/baseline.json` exactly as the command does. Its `diagnostics` are the
envelope's `diagnostics`, entry for entry, and `summary` is the envelope's
`summary`; the [CI/CD guide](ci-cd.md) describes both. `passed` is whether the
command would exit 0.

With `select` or `exclude`, the selectors are `sst validate`'s, graph operators
included: `select=("+agent:support+",)` validates the agent, everything it
reads, and everything that reads it. As the command does, validation rules run
on the chosen artifacts alone and strict mode promotes only their warnings,
while what compiling the project reported is reported whichever artifacts it
concerns; `Validation.selected` lists the keys chosen. A selector the command
refuses, or a selection that chooses nothing, raises `ProjectError` carrying
the command's diagnostic.

An artifact whose files hold an error is not compiled, so `catalog` does not
list it; `validate` reports why.

`schemas()`, imported from `snowflake_semantic_tools.api` and needing no
session, returns every JSON Schema (Draft 2020-12) by kind: one per kind of
authored file, `semantic_file` for any file in the semantic models directory,
and `sst_config`. They are generated from the tables SST reads files by, so a
schema flags what `validate` reports about a file's keys and accepts what it
accepts; [the authored files reference](../reference/files.md) lists the same
keys. `sst schema` writes them to `sst_target/schemas/` for an editor.

## Where each folder publishes

`session.locations()` reads `sst_config.yml` once for every target in the
profile, as a run for that target reads it ([folder routes](configuration.md#folder-routes)
and [precedence](configuration.md#precedence)). Each `BlockLocations` is one
routed block, `agents:` or `skills:` and the rest, then `state:`; its `folders`
are its root and every folder a route names, shallowest first. A `RouteFolder`
carries, by target name, each `Setting` it resolves -- the value with that
target's conditionals resolved, and its `source`, `root` or `route:<a/b>` --
and the `Placement`, database and schema as Snowflake resolves them, where an
artifact there publishes. `exists` is False for a route to a folder that is not
there (`SST-CFG041`).

An artifact's own folder is `ObjectSummary.route_folder`. Routes nest as
folders do, so the folder that places it is the longest of `folders` its
`route_folder` starts with, unless its own `config:` sets the key, which its
`settings` then name as `config`.

A target's own database and schema come from `profiles.yml` alone, with only
those two fields' `env_var()` calls rendered: a target whose schema names a
variable this process lacks has `schema=None`, and its `unreadable` says why.
`target_lists` holds each variable of `vars:` that lists target names, such as
`production_targets`, for naming groups of targets.

```python
for block in session.locations().blocks:
    for folder in block.folders:
        placed = folder.placement.get("prod")
        print(block.block, "/".join(folder.folder) or "(root)", placed)
```

## What a target has deployed

`deployed(session, connection)` reads the target's state table, never writes,
and returns a `Deployed` for each artifact: each the session compiled, in
publish order, then each `apply` recorded that the project no longer compiles.
Its `status` follows `sst plan`: `deployed` when the fingerprint `apply`
recorded is the one the artifact compiles to now, `changed` when another is
recorded, `not_deployed` when none is, and `removed` for a recorded artifact
the project no longer compiles. A state table that exists but cannot be read
raises `SnowflakePortError` rather than reading as empty.

`connection` is a `ConnectionParams`, which opens a session as one user with an
OAuth access token or a key pair and closes it again, or a connector the
caller already holds:

```python
from snowflake_semantic_tools.api import ConnectionParams, ProjectSession, deployed

session = ProjectSession.open(Path("my_project"), target="prod")
connection = ConnectionParams(account="myorg-myaccount", user="ME", role="ANALYST", token=access_token)
for row in deployed(session, connection):
    print(row.key, row.status)
```

`ConnectionParams` never shows its credential in its `repr`. Reading the state
table needs `USAGE` on its database and schema and `SELECT` on it.

## Starting and removing artifacts

`new_files`, `new_eval` and `removal` return edits and write nothing: hand
them to `with_overlay` to see the project as it would be.

`session.new_files(kind, name, folder)` starts an artifact type other than
`eval`, or a member authored in files such as `metric`, where discovery reads
its kind: a one-item file in the semantic models or tools directory, or a
folder named for the item holding its manifest (`agents/<folder>/<name>/agent.yml`,
`skills/<folder>/<name>/SKILL.md`). `folder` is below the kind's directory, so
its routes decide where the item publishes. Each file holds the item's name
in the project's `format:` layout and nothing else, so what it still needs is
what the overlay's `validate` reports. A name or folder that is not one path
segment raises `ValueError`, and a file that exists `FileExistsError`.
`session.new_eval(subject)` starts a dataset and config in `evals/` beside an
agent's or CoWork profile's manifest, and adds the manifest's `evals:` naming
them.

`session.removal(key)` takes an artifact out: an agent, profile, plugin or
skill with every file in its folder, its evals among them, except a folder
nested in it holding another artifact of its type; a semantic view or tool
member with its file, or with its entry when its file declares others, the
file's comments kept; an eval with its dataset and config, its subject's
`evals:` removed. What still refers to the artifact is what the overlay's
`validate` reports, and `session.lineage(key, upstream=False)` names it
beforehand.

```python
draft = session.with_overlay(session.new_files("agent", "support_agent", ("cx",)))
print([item.target for item in draft.catalog() if item.key == "agent:support_agent"])
```

## A session is a snapshot

A session holds what it read and never reads the project again, so every
answer it gives describes the same files. `session.reload()` reads the project
again into a new session and leaves the old one unchanged; a file whose bytes
did not change is not parsed again, and a semantic view none of whose inputs
changed is neither built nor rendered again, so a reload answers exactly as a
session opened fresh on the same files would. Pass `dbt=False` when no dbt
file changed, to read the models as the old session read them instead of
running `dbt parse`.

```python
edited = session.reload(dbt=False)
for item in edited.validate().diagnostics:
    print(item["code"], item["message"])
```

## Unsaved edits

`session.with_overlay(files)` returns a session for the project with edits
that are not saved: each key is a path relative to the project, its bytes
replace the file or create it, and `None` deletes it. It answers exactly as a
session opened on the project with those edits saved would, but the project is
never written to. The session reads a private copy instead: made the first
time an overlay is asked for, and brought back in step with the project for
each one after, so every overlay reads the project's files as they are then.

```python
draft = session.with_overlay({
    "semantic_models/metrics/total_revenue.yml": edited_text.encode(),
    "semantic_models/metrics/old_metric.yml": None,
})
print(draft.validate().passed)
```

An overlay of a session that already has one keeps its edits and adds these.
dbt is read again only when an edit is to something it reads: a model, macro,
seed, snapshot, analysis or test file, a dbt project or package file,
`profiles.yml`, or the settings of `sst_config.yml` it runs with -- `dbt:`,
`defer:` and `project.target_profile`, as they resolve for the session's target;
otherwise the overlay takes the models the session read. Overlays of one project share what they parse and build, so
each reads again only what its edits changed. `draft.overlay` is the edits a
session reads, and `draft.reload()` reads the project again with them.

## Stability

The names `snowflake_semantic_tools.api` exports are its whole interface.
`validate` reports diagnostics in the JSON envelope's form, whose
`schema_version` changes only when a field is removed or changes meaning, as
the [CI/CD guide](ci-cd.md) says; the values the other calls return grow fields
the same way, so read the fields you need rather than comparing whole values.
