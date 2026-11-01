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
| `session.catalog()` | an `ObjectSummary` for each compiled artifact, in the order `apply` publishes them: its key, type, name, target, source files, dependencies, members, the dbt models it reads, and its fingerprint |
| `session.render(key)` | a `Rendered`: what one artifact publishes, its definition and the statements that create it; `KeyError` for a key the project did not compile |
| `session.validate(strict=None, baseline=True)` | a `Validation`: what `sst validate --no-snowflake-syntax-check --output json` reports, as data |
| `session.compiled` | the compile result itself, for a caller that needs more than the summaries give |
| `session.reload(dbt=True)` | a new session from the project's files as they are now |
| `session.with_overlay(files)` | a new session from them with unsaved edits over them; see below |
| `session.schema_for(path)` | the kind of the JSON Schema that checks a project file, a key of `schemas()`; `None` for a file SST does not read |

`validate` applies the project's severity overrides, strict mode
(`validation.strict`, or `strict=` as `--strict` and `--no-strict` give it)
and `.sst/baseline.json` exactly as the command does. Its `diagnostics` are the
envelope's `diagnostics`, entry for entry, and `summary` is the envelope's
`summary`; the [CI/CD guide](ci-cd.md) describes both. `passed` is whether the
command would exit 0.

An artifact whose files hold an error is not compiled, so `catalog` does not
list it; `validate` reports why.

`schemas()`, imported from `snowflake_semantic_tools.api` and needing no
session, returns every JSON Schema (Draft 2020-12) by kind: one per kind of
authored file, `semantic_file` for any file in the semantic models directory,
and `sst_config`. They are generated from the tables SST reads files by, so a
schema flags what `validate` reports about a file's keys and accepts what it
accepts; [the authored files reference](../reference/files.md) lists the same
keys. `sst schema` writes them to `sst_target/schemas/` for an editor.

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
`profiles.yml`, or `sst_config.yml`; otherwise the overlay takes the models
the session read. Overlays of one project share what they parse and build, so
each reads again only what its edits changed. `draft.overlay` is the edits a
session reads, and `draft.reload()` reads the project again with them.

## Stability

The names `snowflake_semantic_tools.api` exports are its whole interface.
`validate` reports diagnostics in the JSON envelope's form, whose
`schema_version` changes only when a field is removed or changes meaning, as
the [CI/CD guide](ci-cd.md) says; the values the other calls return grow fields
the same way, so read the fields you need rather than comparing whole values.
