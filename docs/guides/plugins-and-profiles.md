# Plugins and profiles

Skills reach people through two more artifact types:

- a **plugin** publishes several project skills as one plugin-type Cortex
  Extension;
- a **profile** publishes a CoCo Desktop profile: a set of skills, a system
  prompt, MCP servers, and hooks, which Desktop installs for everyone who
  selects the profile.

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
  - sales-semantics
  - month-close
```

- Each member must be a project skill (`SST-VAL835`), and a member with errors
  stops the plugin from bundling (`SST-VAL836`); a plugin never publishes with a
  member missing.
- The bundle holds `.cortex-plugin/plugin.json` -- name, description, and
  `owner_team` as the author -- and each member flattened under
  `skills/<member>/`.
- A plugin version is named by its content, like a skill's, and publishes
  through the same catalog channel and bundle stage.
- An agent references it with `{{ plugin('sales-toolkit') }}`; the skill entry's
  `name:` is optional or must be one of the members.

## Profiles

```text
profiles/
  shared/                 # applies to every profile
    AGENTS.md
    rules/
      sql-style.md
    profile.yml           # skills: only
  sales-analyst/
    profile.yml
    AGENTS.md             # optional
hooks/
  sql-guard/
    hook.yml
    sql-guard.sh
mcp-servers/
  sales-docs/
    mcp.json
```

```yaml
# profiles/sales-analyst/profile.yml
name: sales-analyst
description: "Sales analysts: order, revenue, and month-close questions."
owner_team: Sales Analytics
skills:
  - sales-semantics
  - month-close
mcp_servers:
  - sales-docs
hooks:
  - sql-guard
```

- `skills`, `mcp_servers`, and `hooks` name folders in `project.skills_dir`,
  `project.mcp_servers_dir`, and `project.hooks_dir`. An unknown name is an error
  (`SST-VAL844`, `SST-VAL846`, `SST-VAL847`), and a skill with errors stops the
  profile from publishing (`SST-VAL855`).
- `profile.yaml` works too; having both is an error.
- `allowed_roles`, `active`, `plugins`, `commands`, `env_vars`, and
  `settings_overrides` are refused (`SST-VAL851`). Desktop does not read
  `allowed_roles`, so access control does not belong in the file; to retire a
  profile, delete its folder and plan with `--prune`.

### The shared folder

`profiles/shared/` is reserved. Its `AGENTS.md` and `rules/*.md` form the start
of every profile's system prompt, and the skills in its `profile.yml` are part
of every profile. A profile that lists a shared skill again gets a warning
(`SST-VAL845`).

Each profile's prompt is assembled in a fixed order: the shared `AGENTS.md`, the
shared rules sorted by file name, then the profile's own `AGENTS.md`.

### Hooks

```yaml
# hooks/sql-guard/hook.yml
event: PreToolUse
type: command
command: bash
matcher: snowflake_sql_execute
timeout: 30
description: Refuses destructive SQL unless the user confirms it.
```

A hook folder holds `hook.yml` and exactly one script; with more than one file,
name the script with `script:` (`SST-VAL852`). Only command hooks are supported.
The script is published with the profile and the hook runs it from the stage.

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

A profile's MCP configs are merged into one `mcp.json`. Each file must hold one
`mcpServers` object (`SST-VAL853`), each server must be an object
(`SST-VAL848`), and two configs in one profile cannot define the same server
(`SST-VAL849`). Use `${VAR}` placeholders for secrets: the merged file is
readable by everyone who uses the profile, so a literal credential is an error
(`SST-VAL850`).

## How a profile publishes

```yaml
skills:
  stage:
    +database: "{{ target.database }}"
    +schema: "{{ target.schema }}"
    +stage: PROFILE_BUNDLES
    +registry_table: PROFILE_REGISTRY
```

A profile is content-addressed files on a stage plus one row in the profile
registry table that points at them:

| Stage tree | Holds |
|---|---|
| `skills/shared/<hash>/` | the shared skills, nested as authored |
| `skills/<profile>/<hash>/` | the profile's own skills |
| `prompts/<profile>/<hash>/AGENTS.md` | the assembled system prompt |
| `mcp/<profile>/<hash>/mcp.json` | the merged MCP servers |
| `hooks/<profile>/<hash>/<hook>/<script>` | hook scripts |

Each `<hash>` is 12 hex characters of that tree's own digest, so a new version of
a tree lands at a new path and never overwrites one a published row names.
`sst apply` uploads and byte-checks every tree, then writes the registry row in
one `MERGE`: that is the moment the profile changes for its users. It then reads
the row back the way Desktop does and checks that every pointer resolves. Old
trees stay on the stage, so pointing the row back at them is a rollback.

- **The registry table.** CoCo Desktop reads profiles from
  `CORTEX_CODE.CONFIG.PROFILE_REGISTRY`. Any other `+registry_table` is useful
  for rehearsal and gets an info diagnostic saying Desktop will not see it
  (`SST-VAL854`). SST creates the table only when it is absent and checks the
  columns of one that exists; it never alters it.
- **Ownership.** A registry row SST did not write is left alone (`SST-PLN024`).
  A row whose version changed since SST last wrote it means another writer is
  active, and the plan stops for that profile (`SST-PLN028`).
- **One at a time.** Profiles publish sequentially.
- **Retiring a profile.** Delete its folder and run `plan` and `apply` with
  `--prune`: SST sets the row's `ACTIVE` to false, but only if the row still has
  the version SST wrote. Stage trees are never deleted.

## Choosing channels

Each channel is a block under `skills:`, and omitting a block turns that channel
off:

- `catalog:` alone publishes skills and plugins as extensions, for agents;
- `stage:` alone publishes profiles for Desktop;
- both publish both, from the same folders.

A skill that no channel publishes is a warning (`SST-VAL830`).
