# Skills

A skill is a folder with a `SKILL.md`: instructions an agent loads when a
question matches the skill's description, plus any files those instructions
point at. SST validates the folder, publishes it as a skill-type Cortex
Extension, and pins every agent that uses it to the published version.

A skill can also ship to CoCo Desktop through a profile; see
[plugins and profiles](plugins-and-profiles.md).

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

- A skill is any directory under `project.skills_dir` that holds a `SKILL.md`.
  Other directories only group skills; no folder name has special meaning.
- A skill inside another skill is an error (`SST-PRS119`): the inner files would
  belong to both.
- Hidden files and `__pycache__` are skipped.

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

- `name` is lowercase kebab-case and matches the folder name (`SST-VAL801`).
  It becomes the extension name in UPPER_SNAKE form, so `sales-semantics`
  publishes `SALES_SEMANTICS`; that name must be unique across the project's
  skills and plugins (`SST-VAL832`).
- `description` is required (`SST-PRS034`). An agent decides whether to load the
  skill from the description alone, so say when to use it and when not to.
- Other frontmatter keys pass through unchanged.

## References to bundled files

Every path a skill's Markdown names is checked: links, images, reference-style
definitions, bare paths in prose, and paths inside fenced code blocks.

- A path resolves relative to the file that contains it. A target that does not
  exist is an error (`SST-VAL808`).
- A path written from the repository root, such as
  `skills/sales-semantics/reference/join-paths.md`, is an error (`SST-VAL810`):
  the published bundle has no repository around it. Write it relative instead.
- A link to a directory is an error (`SST-VAL810`); link the file.
- A file no Markdown references is a warning (`SST-VAL813`).
- A line that shows an example path on purpose can opt out with a trailing
  `sst: ignore SST-VAL808` comment.

Scripts are checked too: an absolute or home-directory path, or what looks like
a literal credential, is a warning (`SST-VAL815`).

## Flattening

Cortex Agents read a skill's supporting files only when they sit beside
`SKILL.md`. SST flattens each bundle in three steps:

1. **Detect collisions.** `reference/notes.md` and a root-level
   `reference__notes.md` would land on one path, so that is an error
   (`SST-VAL809`) before anything is renamed.
2. **Join path components with `__`.** `reference/join-paths.md` becomes
   `reference__join-paths.md`.
3. **Rewrite references.** Every Markdown file is rewritten so its links point
   at the flattened names, including `../` links from one bundled file to
   another.

A script that names a sibling path flattening moves is a warning
(`SST-VAL833`): SST rewrites Markdown, not code.

CoCo Desktop reads the authored, nested layout, so profiles publish skills
unflattened. `skills.catalog.+flatten` must be `true` and
`skills.stage.+flatten` must be `false`.

## Limits

The extension scanner accepts at most 50 files, 2 MiB per file, and 10 MiB per
version, and one file over a limit fails the whole version. SST checks the
limits offline (`SST-VAL834`), before anything is uploaded. It also warns when
`SKILL.md` exceeds 25 KiB (`SST-VAL812`) or the flattened bundle exceeds 1 MiB
(`SST-VAL811`).

## Versions

A version is named by its content: `skills.+version_prefix` (default `SST_`)
followed by the first 12 hex characters of the flattened bundle's SHA-256 digest,
for example `SST_16D8F6686433`.

- Unchanged content keeps its alias, so an unchanged skill publishes nothing.
- Reverting a change returns to an alias that already exists, so the revert
  publishes nothing either; the plan warns if the catalog will serve a different
  version (`SST-VAL841`).
- The commit that published each version is recorded in SST's state, not in
  the version name.

## Configuration

```yaml
skills:
  +version_prefix: "SST_"
  +certified: false
  catalog:
    +database: "{{ target.database }}"
    +schema: "{{ target.schema }}"
    +bundle_stage: SKILL_BUNDLE_SRC
    +flatten: true
```

- `catalog:` turns on publishing to Cortex Extensions. `+bundle_stage` is the
  internal stage that versions are built from; SST creates it when it is absent.
- `+certified: true` tags each new version with
  `SNOWFLAKE.CORE.CERTIFICATION_STATUS = 'CERTIFIED'` and reads the tag back.
  There is no un-certify step.
- Certification decides what the catalog serves. Once any version of an
  extension is certified, catalog users receive the latest certified version
  instead of the default, so a version published without `+certified: true`
  stays behind a certified one; the plan says so (`SST-VAL841`). Agents pin their
  version and are unaffected.
- `+threads` caps concurrent catalog publishes (1 to 16).

## Publishing

For each skill, `sst apply`:

1. uploads the flattened bundle to `@<bundle_stage>/<name>/<ALIAS>/`, then checks
   that the stage holds exactly the bundle's files, byte for byte;
2. creates the extension if it is absent, with the skill's description as its
   comment;
3. adds a version named by the alias from that prefix, unless it already exists;
4. checks that the new version holds exactly the bundle's files.

A prefix is never reused for different content, so an upload cannot change a
version that exists. SST never grants, drops, or changes the discoverability of
an extension. An extension that exists but that SST did not create is left alone
and reported (`SST-PLN024`).

## Using a skill from an agent

```yaml
  skills:
    - name: sales-semantics
      source:
        type: CORTEX_EXTENSION
        path: "{{ skill('sales-semantics') }}"
```

Leave `version:` out; SST pins the alias it publishes, and an authored version
on a project skill is an error (`SST-VAL838`). Because the pin names a version,
a plan that creates or updates the agent must include the skill too: with
`--select agent:analyst` alone the agent is blocked (`SST-PLN030`), and
`--select agent:analyst --select skill:sales-semantics` plans the skill as NOOP when
its version is already published. In a project that has agents, a
skill that no agent references, no plugin contains, and no profile includes is
a warning (`SST-VAL804`), and a skill that ships scripts to an agent without a
code-execution tool is a warning (`SST-VAL814`). See the
[agents guide](agents.md#skills) for extensions other projects publish.
