# Plugins and profiles

Skills reach people through two more artifact types:

- a **plugin** publishes several project skills as one plugin-type Cortex
  Extension;
- a **profile** publishes a CoCo Desktop profile: a set of skills, plugins, slash
  commands, a system prompt, MCP servers, and hooks, which Desktop installs for
  everyone who selects the profile.

Both build from the same `skills/` folders as the catalog, so a skill is
written once and published everywhere it is listed.

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
  `owner_team` as the author, and `"skills": "./skills/"` -- and each member
  flattened under `skills/<member>/`.
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

## Profiles

```text
coco_desktop_profiles/
  shared/                 # applies to every profile
    AGENTS.md
    rules/
      sql-style.md
    coco_desktop_profile.yml           # skills: and commands: only
  sales-analyst/
    coco_desktop_profile.yml
    AGENTS.md             # optional
commands/
  daily-summary.md        # /daily-summary
  sql/
    check.md              # /sql:check
hooks/
  sql-guard/
    hook.yml
    sql-guard.sh
mcp-servers/
  sales-docs/
    mcp.json
```

```yaml
# coco_desktop_profiles/sales-analyst/coco_desktop_profile.yml
name: sales-analyst
description: "Sales analysts: order, revenue, and month-close questions."
owner_team: Sales Analytics
skills:
  - "{{ skill('sales-semantics') }}"
  - "{{ skill('month-close') }}"
plugins:
  - "{{ plugin('sales-toolkit') }}"
commands:
  - "{{ command('sql/check') }}"
mcp_servers:
  - "{{ mcp_server('sales-docs') }}"
hooks:
  - "{{ hook('sql-guard') }}"
```

- Each folder directly under `project.coco_desktop_profiles_dir` (default `coco_desktop_profiles/`) is a
  profile, named by its folder: lowercase letters, digits, `-`, or `_`, and not
  `shared` (`SST-VAL801`). Its `config:` sets the profile's own `coco_desktop_profiles:`
  settings, such as `stage`, without their `+`. The name is the registry row's `CONFIG_NAME`, and
  `name:`, when given, must match it (`SST-VAL801`). `description` and
  `owner_team` are optional and fill the row's columns of those names.
- `skills`, `plugins`, `commands`, `mcp_servers`, and `hooks` list one call
  each, of `skill()`, `plugin()`, `command()`, `mcp_server()`, and `hook()`
  respectively; a bare name is an error naming the function to write
  (`SST-REF050`), and another function's call `SST-REF041`. The calls name what
  is in
  `project.skills_dir`, `project.plugins_dir`, `project.commands_dir`,
  `project.mcp_servers_dir` (default `mcp-servers/`), and `project.hooks_dir`.
  An unknown name is an error (`SST-VAL844`, `SST-VAL860`, `SST-VAL858`,
  `SST-VAL847`, `SST-VAL846`), and a skill or plugin with errors stops the
  profile from publishing (`SST-VAL855`), as does a command, hook, or MCP config
  with errors of its own.
- `coco_desktop_profile.yaml` works too; having neither or both is an error (`SST-VAL801`).
  Any other unknown key is a warning (`SST-PRS004`).
- `allowed_roles`, `active`, `env_vars`, and `settings_overrides` are refused
  (`SST-VAL851`). Desktop does not read `allowed_roles`, so access control does
  not belong in the file; to retire a profile, delete its folder and plan and
  apply with `--prune`. Environment variables and settings overrides would change every
  user's local setup, so SST does not publish them.
- Every file a profile publishes must have a name a stage accepts: letters,
  digits, `.`, `_`, `-`, and `$` (`SST-VAL857`). This is checked before anything
  is uploaded.
- As with skills, hidden files and caches are skipped, and SST never follows a
  symbolic link: a linked profile, hook, MCP config, or command file or folder
  is an error (`SST-PRT009`) and is left out.

### The shared folder

`coco_desktop_profiles/shared/` is reserved. Its `AGENTS.md` and `rules/*.md` form the start
of every profile's system prompt, and the skills and commands in its
`coco_desktop_profile.yml` are part of every profile. That `coco_desktop_profile.yml` is optional and
accepts only `skills:` and `commands:`. A profile that lists a shared skill or
command again gets a warning (`SST-VAL845`).

Each profile's prompt is assembled in a fixed order: the shared `AGENTS.md`, the
shared rules sorted by file name, then the profile's own `AGENTS.md`. Blank
parts are dropped, and a profile with no prompt text has no prompt tree.

### Commands

A command is a Markdown file anywhere under `project.commands_dir` (default
`commands/`). A profile names it by its path without `.md`, as
`{{ command('sql/check') }}`; Desktop joins nested folders with `:`, so `sql/check` is `/sql:check` for the user.

```markdown
---
description: Check a query against the SQL style rules.
allowed-tools: [snowflake_sql_execute]
skill: "{{ skill('sql-style') }}"
---
Review the SQL in the last message against the rules in AGENTS.md.
```

Frontmatter is optional. When it is present, it is a YAML mapping of
`description`, `allowed-tools` (a string or a list), `skill`, and `hidden`
(`SST-VAL859`); any other key is a warning (`SST-PRS004`). `skill` is one
`{{ skill('<name>') }}` call naming a skill of this project: a bare name is
`SST-REF050`, and a skill the project does not have `SST-REF032`. The file is
published as written, except that the call is rendered to the skill's name,
`skill: sql-style`, which is what Desktop reads.

### Plugins in a profile

A profile that lists a plugin carries that plugin's bundle, byte for byte the
bundle its catalog extension publishes, and Desktop loads it as a plugin.
Listing a plugin does not require it to publish as an extension: one with no
`+bundle_stage` is built for the profile alone, and its errors block the
profile (`SST-VAL855`).

### Hooks

```yaml
# hooks/sql-guard/hook.yml
event: PreToolUse
type: command
command: bash
matcher: snowflake_sql_execute
timeout: 30
description: Refuses destructive SQL unless the user confirms it.
script: "{{ file('sql-guard.sh') }}"
```

A hook folder holds `hook.yml` (or `hook.yaml`) and its script. `script:` names
the script with one `{{ file('<path>') }}` call, relative to the folder of
`hook.yml`, read by the same rules as every `file()` (see
[References](references.md#files)): a bare path is `SST-REF050`, an absolute
one or one that leaves the project `SST-REF027`, a symbolic link `SST-PRT009`, a
missing file `SST-REF014`, and an empty one `SST-LOD019`. Without `script:`, the
folder's one other file is the script, and a folder with none or several is
`SST-VAL852`. The script joins the profile's source files. `event` and
`command` are required, and `type` may be left out: only command hooks are
supported (`SST-VAL852`). `matcher`, `timeout`, `interactive`, and
`description` are optional; any other key is a warning (`SST-PRS004`). The
script is published with the profile, and the row's `HOOKS` column groups each
profile's hooks by event, then by matcher, each pointing at its script on the
stage.

### MCP configs

```json
{
  "mcpServers": {
    "sales-docs": {
      "type": "stdio",
      "command": "sales-docs-mcp",
      "args": ["--read-only"],
      "env": { "SALES_DOCS_TOKEN": "${SALES_DOCS_TOKEN}" }
    }
  }
}
```

Each folder under `project.mcp_servers_dir` holds one `mcp.json`. A profile's
MCP configs are merged into one `mcp.json`. Each file must hold exactly one
`mcpServers` object and nothing else (`SST-VAL853`), each server must be an
object (`SST-VAL848`), and two configs in one profile cannot define the same
server (`SST-VAL849`). Use `${VAR}` placeholders for secrets: the merged file is
readable by everyone who uses the profile, so a literal value under a key whose
name contains `token`, `secret`, `password`, `passwd`, `api_key`, or
`private_key` is an error (`SST-VAL850`).

## How a profile publishes

```yaml
coco_desktop_profiles:
  +database: "{{ target.database }}"
  +schema: "{{ target.schema }}"
  +stage: PROFILE_BUNDLES
  +registry_table: CORTEX_CODE.CONFIG.PROFILE_REGISTRY
  sales-analyst:           # coco_desktop_profiles/sales-analyst/
    +schema: SALES
```

- Each profile resolves these settings through the one
  [precedence chain](configuration.md#precedence): its `coco_desktop_profile.yml`'s
  `config:`, then the route that names its folder, then the root. A profile
  publishes only where `+stage` resolves for it. Like `+registry_table`, an
  unqualified name is in the profile's database and schema, which default to
  the target's; a qualified name is used as written.
- `+registry_table` defaults to `PROFILE_REGISTRY` in that database and schema,
  which is a rehearsal registry (see below).
- Trees are always uploaded uncompressed and nested as authored, under the
  prefixes below; none of that is configurable.

A profile is content-addressed files on a stage plus one row in the profile
registry table that points at them:

| Stage tree | Holds |
|---|---|
| `skills/shared/<hash>/` | the shared skills, each under `<skill>/`, nested as authored |
| `skills/<profile>/<hash>/` | the profile's own skills, laid out the same way |
| `prompts/<profile>/<hash>/AGENTS.md` | the assembled system prompt |
| `mcp/<profile>/<hash>/mcp.json` | the merged MCP servers |
| `hooks/<profile>/<hash>/<hook>/<script>` | hook scripts |
| `commands/shared/<hash>/` | the shared commands |
| `commands/<profile>/<hash>/` | the profile's own commands |
| `plugins/<profile>/<hash>/<plugin>/` | each listed plugin's bundle |

Each `<hash>` is 12 hex characters of that tree's own digest, so a new version of
a tree lands at a new path and never overwrites one a published row names. A
tree with no files is not published, and its column holds an empty value. The
row's `VERSION` is the `skills:` root's `+version_prefix` followed by 12 hex characters of a
digest of every other column, so it changes whenever anything the profile
carries does.

`sst apply` creates the registry table and the stage when they are absent,
uploads and byte-checks every tree, then writes the registry row in one `MERGE`
guarded by the `VERSION` the plan saw: that is the moment the profile changes
for its users. It then reads the row back with the query Desktop runs (`WHERE
active = TRUE`), and checks that the row carries this `VERSION` and that every
pointer resolves. Old trees stay on the stage, so pointing the row back at them
is a rollback.

- **The registry table.** CoCo Desktop reads profiles only from
  `CORTEX_CODE.CONFIG.PROFILE_REGISTRY`. Any other `+registry_table`, including
  the default, is useful for rehearsal and gets an info diagnostic saying
  Desktop will not see it (`SST-VAL854`, once per registry the profiles name).
  SST creates the table only when it is
  absent. One that exists must have every column SST writes, with the right
  type, or the plan stops (`SST-PLN029`); SST never alters it.
- **What the row holds.** SST writes `CONFIG_NAME`, `DESCRIPTION`,
  `OWNER_TEAM`, `VERSION`, `ACTIVE`, `UPDATED_AT`, and the pointer columns
  `SKILL_REPOS`, `SYSTEM_PROMPT_REPO`, `MCP_SERVERS`, `HOOKS`, `PLUGINS`, and
  `COMMAND_REPOS`. `ENV_VARS` and `SETTINGS_OVERRIDES` are always written empty.
  SST never writes `ALLOWED_ROLES` or `PERMISSIONS` and issues no grants:
  access to the stage and the registry is managed outside SST.
- **Stage.** The stage SST creates is internal with server-side encryption; an
  existing stage of any other kind stops the plan (`SST-PLN026`).
- **Ownership.** A registry row SST did not write blocks the plan for that
  profile, and SST leaves it alone (`SST-PLN024`). A row whose version changed
  since SST last wrote it means another writer is active, and the plan stops
  for that profile (`SST-PLN028`).
- **One at a time.** Profiles publish sequentially.
- **Retiring a profile.** Delete its folder and run `plan` and `apply` with
  `--prune`: SST sets the row's `ACTIVE` to false, but only if the row still has
  the version SST wrote; otherwise the prune fails (`SST-APL012`). A row that is
  already gone counts as retired. Stage trees and rows are never deleted, and
  restoring the folder reactivates the row on the next `apply`.

## Choosing where each publishes

Each item publishes where its own settings say, so the choice is per item, not
per channel:

- a skill or plugin with a `+bundle_stage` publishes as an extension, for
  agents;
- a profile with a `+stage` publishes for Desktop;
- a skill can do both, from the same folder, or only reach Desktop through a
  profile.

With none of `skills:`, `plugins:`, and `coco_desktop_profiles:` written and no item placing
itself, nothing compiles. A skill that publishes as no extension and that no
profile reaches -- directly, through `shared/`, or through a plugin a profile
lists -- is published nowhere, which is a warning (`SST-VAL830`).
