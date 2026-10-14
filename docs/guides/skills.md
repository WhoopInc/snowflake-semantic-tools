# Skills

A skill is a folder with a `SKILL.md`: instructions an agent loads when a
question matches the skill's description, plus any files those instructions
point at. SST validates the folder, publishes it as a skill-type Cortex
Extension, and pins every agent that uses it to the published version.

A skill can also ship to CoCo Desktop through a profile; see
[plugins and profiles](plugins-and-profiles.md).

These are skills SST publishes. The skills `sst agent-kit` installs into `.cortex/skills` or
`.claude/skills` are a different thing: they teach the coding agent that edits the project,
are never published, and may not sit inside `skills_dir`; see [the agent kit](agent-kit.md).

## Layout

```text
skills/
  sales-semantics/
    SKILL.md
    reference/
      join-paths.md
      metric-shapes.md
  finance/                   # a grouping folder: not a skill, just a directory
    month-close/
      SKILL.md
      check-supply-month.sql
```

- A skill is any directory under `project.skills_dir` (default `skills/`) that
  holds a `SKILL.md`. Other directories only group skills; no folder name has
  special meaning, and the skill is named after its own folder, not the path to
  it.
- A grouping folder holds only folders. One that also holds files of its own is
  a skill whose `SKILL.md` sits one level too deep, which is an error
  (`SST-PRS120`).
- A skill inside another skill is an error (`SST-PRS119`): the inner files would
  belong to both.
- Hidden files, `__pycache__`, and `.pyc` files are skipped.
- SST never follows a symbolic link. A linked file or folder under the skills or
  plugins directory is an error (`SST-PRT009`) and is left out of every bundle,
  even when it points inside the project.
- Every bundled file must have a name a stage accepts: letters, digits, `.`,
  `_`, `-`, and `$` (`SST-VAL857`).

## `SKILL.md`

```markdown
---
name: sales-semantics
description: |-
  Use when writing or reviewing a question against the sales semantic views:
  which view answers which question, and how the joins behave. Do NOT use for
  menu catalogue questions.
---
# Sales semantics

Read [the join paths](reference/join-paths.md) before combining views.
```

- `name` and `description` are both required (`SST-PRS034`).
- The folder name is lowercase kebab-case of at most 64 characters, and `name`
  matches it (`SST-VAL801`). It becomes the extension name in UPPER_SNAKE form,
  so `sales-semantics` publishes `SALES_SEMANTICS`. To publish to the catalog
  that name must start with a letter (`SST-VAL801`), and it must be unique
  across the project's skills and plugins (`SST-VAL832`).
- An agent decides whether to load the skill from the description alone, so
  say when to use it and when not to. The description is also the extension's
  comment.
- A `SKILL.md` with nothing after its frontmatter is a warning (`SST-RND031`).
- Other frontmatter keys pass through unchanged.

## References to bundled files

Every path a skill's Markdown names is checked: links, images, reference-style
definitions, bare paths in prose, and paths inside fenced code blocks.

- A path resolves relative to the file that contains it, or else relative to
  the skill folder. URLs and absolute or `~/` paths are not checked.
- A target that does not exist is an error (`SST-VAL808`). Links, images, and
  reference-style definitions are always checked; a bare path in prose or in a
  code block is checked when it looks like a bundle path: it starts with `./`
  or `../`, its first folder is one the skill has or one of `scripts`,
  `reference`, `references`, `examples`, `assets`, and `templates`, or its file
  name matches one in the skill.
- A path written from the repository root, such as
  `skills/sales-semantics/reference/join-paths.md`, is an error (`SST-VAL810`):
  the published bundle has no repository around it. Write it relative instead.
- A link to a directory is an error (`SST-VAL810`); link the file.
- A file no Markdown references is a warning (`SST-VAL813`).
- A line that shows an example path on purpose can opt out of `SST-VAL808` by
  carrying the text `sst: ignore SST-VAL808`, for example in a trailing
  comment.

Every file that is not Markdown counts as a script. A script that names an
absolute or home-directory path, or any text file that holds what looks like a
literal credential, is a warning (`SST-VAL815`).

## Flattening

Cortex Agents read a skill's supporting files only when they sit beside
`SKILL.md`. SST flattens each bundle in three steps:

1. **Detect collisions.** `reference/notes.md` and a root-level
   `reference__notes.md` would land on one path, so that is an error
   (`SST-VAL809`) before anything is renamed.
2. **Join path components with `__`.** `reference/join-paths.md` becomes
   `reference__join-paths.md`.
3. **Rewrite references.** Every Markdown file is rewritten so its references
   point at the flattened names, including `../` links from one bundled file to
   another. The rewritten text is checked again, and a reference that no longer
   resolves is an error (`SST-VAL810`).

A script that names a sibling path flattening moves is a warning
(`SST-VAL833`): SST rewrites Markdown, not code.

In the published bundle each skill's files sit under `skills/<name>/`, so
`reference/join-paths.md` publishes as
`skills/sales-semantics/reference__join-paths.md`.

CoCo Desktop reads the authored, nested layout, so profiles publish skills
unflattened. Neither is configurable: an extension bundle is always flattened,
and a profile tree never is.

## Limits

The extension scanner accepts at most 50 files, 2 MiB per file, and 10 MiB per
version, and one file over a limit fails the whole version. SST checks the
limits offline (`SST-VAL834`), before anything is uploaded. It also warns when
`SKILL.md` exceeds 25 KiB as authored (`SST-VAL812`) or once flattening has
rewritten it (`SST-RND032`), and when the flattened bundle exceeds 1 MiB
(`SST-VAL811`).

## Versions

A version is named by its content: the skill's `version_prefix` setting
(`skills.+version_prefix`, default `SST_`)
followed by the first 12 hex characters, in upper case, of the flattened
bundle's digest, for example `SST_16D8F6686433`. The digest is a SHA-256 over
every file's path, size, and content hash, so it changes exactly when the
bundle does. The prefix must start with a letter or underscore and hold only
letters, digits, and underscores (`SST-CFG008`).

- Unchanged content keeps its alias, so an unchanged skill publishes nothing.
- Reverting a change returns to an alias that already exists, so the revert
  publishes nothing either: the plan shows an update that re-verifies the
  version and records it in state. The plan warns if the catalog will serve a
  different version (`SST-VAL841`).
- The commit that published each version is recorded in SST's state, not in
  the version name.

## Configuration

```yaml
skills:
  +database: "{{ target.database }}"
  +schema: "{{ target.schema }}"
  +bundle_stage: SKILL_BUNDLE_SRC
  +version_prefix: "SST_"
  +certified: false
  finance:                 # skills/finance/<skill>/SKILL.md
    +schema: FINANCE
```

```markdown
---
name: month-close
description: Close the month.
config:
  certified: true          # this skill's versions alone
---
```

Each skill resolves these settings through the one
[precedence chain](configuration.md#precedence): its frontmatter's `config:`
(the keys without their `+`), then the closest folder route over its folder,
then the `skills:` root, then the default. The frontmatter is published with
the rest of `SKILL.md`, so a `config:` there is part of the bundle's content.

- A skill publishes as a Cortex Extension only where `+bundle_stage` resolves
  for it: the internal stage that versions are built from, in the skill's
  database and schema unless the name is qualified. SST creates it when it is
  absent, with server-side encryption; an existing stage that is not an
  internal server-side encrypted stage blocks the plan (`SST-PLN026`). SST
  never alters a stage.
- `+database` and `+schema` default to the target's. A route or a skill's own
  `config:` that places it outside the root's database and schema is a warning
  (`SST-VAL806`): agents then need USAGE on that schema too.
- Every version is built from the complete, flattened bundle, so a deleted
  file is gone from the next version; neither is configurable.
- `+certified: true` tags each new version with
  `SNOWFLAKE.CORE.CERTIFICATION_STATUS = 'CERTIFIED'` and reads the tag back.
  There is no un-certify step.
- Certification decides what the catalog serves. Once any version of an
  extension is certified, catalog users receive the latest certified version
  instead of the default, so a version published without `+certified: true`
  stays behind a certified one; the plan says so (`SST-VAL841`). Agents pin their
  version and are unaffected.
- `+certified: true` on an extension that state records in another schema
  blocks the plan (`SST-VAL807`): moving it would change the name every pinned
  reference uses.
- `+threads` caps concurrent catalog publishes (default 4, 1 to 16). It is
  root-only: in a folder route or a skill's `config:` it is an error
  (`SST-CFG055`). Profiles always publish one at a time.
- An unprefixed key of `skills:` is a folder route; one that names no folder
  under `skills_dir` is an error (`SST-CFG041`). `skills.catalog`,
  `skills.stage`, and `skills.extensions` moved to `skills:` itself,
  `profiles:`, and `extensions:` (`SST-CFG042`). `+grant_read_to` and
  `+enabled` were removed (`SST-CFG043`): SST issues no grants, and a skill
  with no `+bundle_stage` does not publish.
- The top-level `extensions:` block names extensions this project consumes but
  does not publish; see the [agents guide](agents.md#skills).
- `sst list skill --long` prints each skill's settings and where each came
  from.

## Publishing

For each skill, `sst apply`:

1. creates the bundle stage if it is absent;
2. uploads the flattened bundle to `@<bundle_stage>/<name>/<ALIAS>/`, where
   `<name>` is the skill's folder name, so `SKILL.md` lands at
   `<name>/<ALIAS>/skills/<name>/SKILL.md`; then checks that the prefix holds
   exactly the bundle's files, byte for byte;
3. creates the extension if it is absent, with `TYPE = 'SKILL'` and the skill's
   description as its comment, from that prefix: its first version,
   `VERSION$1`, is the bundle, named by the alias. An extension created
   without a source would begin with an empty, unnamed `VERSION$1` that
   Snowflake can neither drop nor name, and which the catalog would serve;
4. adds a version named by the alias from that prefix, unless it already exists;
5. checks that the version holds exactly the bundle's files, sets the comment
   again if the description changed, and certifies the version when
   `+certified: true` asks for it.

A prefix is never reused for different content, so an upload cannot change a
version that exists. The plan blocks a skill instead of publishing it when:

- an extension of that name exists that SST's state does not record
  (`SST-PLN024`), or one of another type (`SST-VAL802`, `SST-PLN002`);
- the alias already names a version whose files differ from the bundle
  (`SST-PLN027`);
- the prefix holds a file the bundle no longer has, which the new version would
  publish (`SST-VAL824`).

SST never grants, drops, un-certifies, or changes the discoverability of an
extension; access to extensions is managed outside SST. With `--prune`, a skill
whose folder was deleted is only reported (`SST-VAL805`): remove the extension
by hand once nothing uses it.

## Using a skill from an agent

```yaml
  skills:
    - source:
        type: CORTEX_EXTENSION
        path: "{{ skill('sales-semantics') }}"
```

Leave `version:` and `name:` out: SST pins the alias it publishes, and an
authored version on a project skill is an error (`SST-VAL838`); the entry is
named after the skill, and an authored `name:` is an error (`SST-VAL540`). A skill that cannot
publish -- it has errors, or no `+bundle_stage` resolves for it -- leaves the
agent nothing to pin, which is an error on the agent (`SST-VAL856`).

Because the pin names a version, a plan that creates or updates the agent must
include the skill too: with `--select agent:analyst` alone the agent is blocked
(`SST-PLN030`), and `--select agent:analyst --select skill:sales-semantics`
plans the skill as NOOP when its version is already published.

In a project that has agents, a skill that no agent references, no plugin
contains, and no profile includes is a warning (`SST-VAL804`), and a skill that
ships scripts when none of the agents that pin it has a `code_execution` tool is
a warning (`SST-VAL814`). See the [agents guide](agents.md#skills) for
extensions other projects publish.
