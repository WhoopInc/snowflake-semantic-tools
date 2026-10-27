# Plugins and Cortex Code profiles

Skills reach people through two more artifact types:

- a **plugin** publishes several project skills as one plugin-type Cortex
  Extension;
- a **Cortex Code profile** (a Snowflake private preview) publishes an agent
  profile: an AGENT in `CORTEX_CODE.CONFIG` that the Cortex Code CLI, Desktop,
  and Snowsight apply for the roles that can use it. It holds an orchestration
  model, response instructions, and skills.

Both pin the extensions the project's skills and plugins publish, so a skill is
written once and reaches every agent and profile that lists it.

## Plugins

```yaml
# plugins/sales-toolkit/plugin.yml
name: sales-toolkit
description: |-
  The sales analytics toolkit: which semantic view answers which question, and
  the month-close procedure for supply cost.
owner_team: Sales Analytics
skills:
  - "{{ skill('sales-semantics') }}"
  - "{{ skill('month-close') }}"
config:
  certified: true          # this plugin's versions alone
```

- A plugin is a folder under `project.plugins_dir` (default `plugins/`) holding
  `plugin.yml` or `plugin.yaml`; no manifest, or both, is an error
  (`SST-VAL801`). A folder with neither a manifest nor a subfolder is a plugin
  missing its manifest; any other folder only groups plugins, as a grouping
  folder groups skills. The folder name names the plugin, follows the skill
  naming rule, and must match `name:` when it is given (`SST-VAL801`).
- `description` is required, and so is at least one member under `skills`
  (`SST-PRS002`). Each member is one `{{ skill('<name>') }}` call; a bare name
  is an error naming the function to write (`SST-REF050`), and another
  function's call `SST-REF041`. A member listed twice
  is a warning (`SST-VAL837`), and any other key is a warning (`SST-PRS004`).
- Each member must be a project skill (`SST-VAL835`), and a member with errors
  stops the plugin from bundling (`SST-VAL836`); a plugin never publishes with a
  member missing.
- The bundle holds `.cortex-plugin/plugin.json` -- name, description,
  `owner_team` as the author, and `"skills": "./skills/"` -- and each member's
  folder, as authored, under `skills/<member>/`.
- A plugin version is named by its content, like a skill's, and publishes as a
  `TYPE = 'PLUGIN'` extension wherever its `+bundle_stage` resolves. Its
  settings are the `plugins:` block's -- `+database`, `+schema`,
  `+bundle_stage`, `+certified`, and `+version_prefix` -- folded like a
  skill's: the manifest's own `config:` (the keys without their `+`), then the
  closest folder route under `plugins:`, then the block root. A member skill is
  bundled into the plugin whether or not it publishes alone.
- As for a skill, every apply makes the version holding the plugin's declared
  alias the extension's default unless a certified version outranks it
  (`SST-VAL841`), so a revert, or a default moved by hand, is an update that
  moves the default back, and a plugin version published through a live
  version is the default too. See the
  [skills guide](skills.md#versions).
- An agent references it with `{{ plugin('sales-toolkit') }}` and no
  `version:` (`SST-VAL838`). An entry without `name:` loads every member: SST
  renders it as one entry per member, since Snowflake refuses a plugin entry
  without a name. To load one member, name it with a `skill()` call; a bare
  member name is `SST-REF050`, and a skill that is not a member `SST-VAL840`:

  ```yaml
  skills:
    - name: "{{ skill('sales-semantics') }}"
      source:
        type: CORTEX_EXTENSION
        path: "{{ plugin('sales-toolkit') }}"
  ```

## Cortex Code profiles

Cortex Code agent profiles are a Snowflake private preview. A profile is an
AGENT object whose specification accepts only three keys:
`models.orchestration`, `instructions.response`, and `skills`. Cortex Code lists
the AGENT objects in `CORTEX_CODE.CONFIG` that the current role has USAGE on; a
role that sees one profile is given it, and a role that sees several is given
the one named `DEFAULT`.

```text
coco_profiles/
  shared/                 # carried by every profile
    AGENTS.md
    rules/
      sql-style.md
    coco_profile.yml      # skills: only
  default/
    coco_profile.yml
    AGENTS.md             # optional
  sales_analyst/
    coco_profile.yml
```

```yaml
# coco_profiles/sales_analyst/coco_profile.yml
name: sales_analyst
description: "Sales analysts: order, revenue, and month-close questions."
model: auto
skills:
  - "{{ skill('sales-semantics') }}"
plugins:
  - "{{ plugin('sales-toolkit') }}"
```

- Each folder directly under `project.coco_profiles_dir` (default
  `coco_profiles/`) is a profile, named by its folder: lowercase letters,
  digits, and `_`, starting with a letter (`SST-VAL801`), since the name
  publishes as an unquoted AGENT name. `default/` publishes as `DEFAULT`, the
  profile Cortex Code gives a role that sees several. `name:`, when given, must
  match the folder (`SST-VAL801`); `coco_profile.yaml` works too, and having
  neither or both is an error (`SST-VAL801`).
- `description` becomes the AGENT's comment. `model` becomes
  `models.orchestration` and must be in `snowflake.orchestration_models`, which
  defaults to `[auto]` (`SST-VAL894`); leave it out to let Cortex Code choose.
- `skills` and `plugins` list one call each, of `skill()` and `plugin()`; a bare
  name is `SST-REF050`, and another function's call `SST-REF041`. Each is pinned
  exactly as an agent's skill is: to the version alias its extension published
  ([skills guide](skills.md#versions)). A skill the project does not have is
  `SST-REF032`, a plugin `SST-REF036`, and one that has no version to pin --
  no `+bundle_stage`, or errors of its own -- `SST-VAL856`. A plugin pins one
  entry per member. Two entries contributing one skill name are `SST-VAL541`.
- `hooks`, `mcp_servers`, `commands`, `owner_team`, and `allowed_roles` are
  refused (`SST-VAL851`): an agent profile cannot carry them, and access is a
  grant. Any other unknown key is a warning (`SST-PRS004`). `config:` sets the
  profile's own `coco_profiles:` settings without their `+`.
- As with skills, hidden files and caches are skipped, and SST never follows a
  symbolic link (`SST-PRT009`).

### The shared folder

`coco_profiles/shared/` is reserved. Its skills are pinned first in every
profile's `skills`, and its `AGENTS.md` and `rules/*.md` lead every profile's
instructions. Its `coco_profile.yml` is optional and accepts only `skills:`. A
profile that lists a shared skill again gets a warning (`SST-VAL845`), and the
skill is pinned once.

A profile's `instructions.response` is assembled in a fixed order: the shared
`AGENTS.md`, the shared rules sorted by file name, then the profile's own
`AGENTS.md`, each stripped and separated by a blank line. A profile with no text
renders no instructions.

## How a profile publishes

```yaml
coco_profiles:
  +database: CORTEX_CODE
  +schema: CONFIG
  # Targets that share CORTEX_CODE.CONFIG publish apart: SALES_ANALYST_DEV.
  +name_suffix: "{{ '' if target.name == 'prod' else '_DEV' }}"
  default:                 # coco_profiles/default/
    +enabled: true
```

- Each profile resolves its settings through the one
  [precedence chain](configuration.md#precedence): its `config:`, then the
  route that names its folder, then the root. `+database` and `+schema`
  default to the target's; Cortex Code lists only `CORTEX_CODE.CONFIG`, so any
  other location is a rehearsal, noted on every compile (`SST-VAL891`).
- `+name_suffix` is appended to the AGENT name: letters, digits, and `_`, up to
  64 characters (`SST-CFG008` otherwise). `+enabled: false` leaves the profile
  out.
- A profile publishes as an agent does: `CREATE AGENT` from its specification,
  staged under `apply.agent_spec_stage` in `coco-profiles/<profile>/<git sha>/`,
  then a committed version and a live version for each change, with the
  ownership marker in its comment. It carries no alias, tags, or display block.
  `apply --prune` drops a profile whose folder is gone, `sst drop` drops one by
  name, `sst state adopt` takes over one created by hand, and `apply
  --temporary` leaves profiles out (`SST-APL106`).
- **One schema of its own.** Agents, CoWork profiles, and Cortex Code profiles
  are all AGENT objects, which SST tells apart by the schema each kind publishes
  to, so a Cortex Code profile may not share a schema with an agent or CoWork
  profile (`SST-VAL892`).
- **A DEFAULT.** When more than one profile publishes to `CORTEX_CODE.CONFIG`
  and none is `DEFAULT`, a role granted several is given none until its user
  chooses; SST notes it (`SST-VAL893`).
- **Who sees a profile.** SST issues no grants. A role sees a profile when it
  has USAGE on `CORTEX_CODE`, `CORTEX_CODE.CONFIG`, and the AGENT, and loads its
  skills only with USAGE on each extension. The role that runs `sst apply`
  needs `CREATE AGENT` on `CORTEX_CODE.CONFIG`.
- **Preview.** Each compiled profile notes the preview it relies on
  (`SST-VAL890`). The profile contract may change before general availability.

The CoCo Desktop profile registry these replaced is gone: a leftover
`coco_desktop_profiles/` folder is an error (`SST-VAL895`); the
`coco_desktop_profiles:` and `profiles:` blocks, `project.coco_desktop_profiles_dir`,
`project.profiles_dir`, `project.hooks_dir`, `project.mcp_servers_dir`, and
`project.commands_dir` are removed keys (`SST-CFG043`); and `skills.stage` is
`SST-CFG042`.

## Choosing where each publishes

A skill or plugin publishes as an extension only where its `+bundle_stage`
resolves, and an agent or Cortex Code profile can pin it only then. A skill with
no `+bundle_stage` that something lists is `SST-VAL856` there.
