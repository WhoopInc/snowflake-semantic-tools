# The agent kit

The agent kit is a set of skills that teach a coding agent -- Cortex Code or Claude Code --
to work in an SST project: which commands are safe to run, how to author and fix the
semantic YAML, how to publish only when asked, and how to report a bug in SST itself. It
ships inside the SST package, so each release installs the skills written for it, with links
to its own documentation.

`sst agent-kit` copies the kit into the project and keeps it current. The files land on disk
as ordinary changes in the working tree: SST never runs git, and nothing connects to
Snowflake. The kit adds no setting to `sst_config.yml`.

## The skills

| Skill | Use |
|---|---|
| `sst-core` | Start here: every command by risk (offline, reads Snowflake, writes Snowflake, costs credits), the edit, validate, plan, apply loop, exit codes, JSON output, and where things live. |
| `sst-getting-started` | For someone new to SST or dbt: what each piece is (semantic views, agents, tools, skills, profiles, evals), the dbt ideas SST builds on, a first project step by step, and which guide to read. |
| `sst-semantic-authoring` | Column metadata, views, metrics, relationships, filters, verified queries, and custom instructions. |
| `sst-fix-diagnostics` | Reading what `sst validate` reports, `sst explain`, strict mode, cascades, and why the baseline file is not a fix. |
| `sst-agents-and-evals` | Agents, tools, published skills, profiles, and evals. |
| `sst-troubleshoot` | dbt parse failures, profiles and configuration discovery, sign-in, privileges, wrong targets, refused applies, run locks, and how to get more detail (JSON output, log levels, the run log). |
| `sst-migrate-from-0-3` | Moving a 0.3 project to this release. |
| `sst-report-issue` | Confirming a problem is SST's, reproducing it small, removing anything private, and drafting an issue that is filed only with your approval. |

Every skill tells the agent to stay offline unless you ask otherwise, and never to apply,
drop, or run evals on its own.

## Installing

```bash
sst agent-kit install
```

installs every skill for the agent whose folder the project has: `.cortex/skills/` for
Cortex Code, `.claude/skills/` for Claude Code. With both folders, or neither, name the
agent; `--agent` repeats:

```bash
sst agent-kit install --agent cortex --agent claude
```

- `--only <skill>` installs just that skill, and repeats; a later `install` adds more.
- `--dry-run` lists what would be written, and writes nothing.
- The skills sit beside any the project already has. A skill folder of the same name that
  SST did not write refuses the whole run (`SST-PRT013`), and nothing is written; a folder
  that already holds the kit's files exactly, as after a lost lock, is adopted.
- The agent's skills folder may not be, contain, or lie inside a directory SST publishes
  from -- any `project.*_dir`, such as `skills_dir` -- or `sst apply` would publish the kit to
  Snowflake. Such a layout is refused (`SST-CFG052`, exit 4).

Then commit the new files with `.sst/agent-kit.json`, so everyone working on the project gets
the same kit.

## The lock file

`.sst/agent-kit.json` records, for each agent, which skills were asked for and the SHA-256
digest of every file SST wrote. It is what lets `upgrade` and `remove` tell a file SST wrote
and nobody changed from one you edited:

```json
{
  "version": 1,
  "generated_by_sst": "1.0.0",
  "agents": {
    "cortex": {
      "root": ".cortex/skills",
      "selection": [],
      "skills": {
        "sst-core": {"SKILL.md": "sha256:..."}
      }
    }
  }
}
```

An empty `selection` means the whole kit was installed, so `upgrade` also installs a skill a
later release adds. Do not edit the lock by hand; an unreadable one stops every
`sst agent-kit` command (`SST-PRT009`, exit 4).

## Upgrading

After upgrading SST, bring the kit to the new release:

```bash
sst agent-kit upgrade
```

| The file on disk | The new kit | `upgrade` |
|---|---|---|
| as SST wrote it | unchanged | leaves it |
| as SST wrote it | changed | replaces it |
| changed by you | unchanged | keeps your change |
| changed by you | changed | keeps your change and warns (`SST-PRT014`) |
| absent, the kit adds it | | creates it |
| as SST wrote it, the kit drops it | | removes it |

`--force` makes every installed file match the kit, overwriting your changes. A file the kit
now ships that SST did not write is still refused (`SST-PRT013`). `--dry-run` lists what would
change. Each file carries the release's docs links, so after an SST upgrade every skill is
reported outdated until you run `upgrade`.

## Checking

```bash
sst agent-kit status
sst agent-kit status --check
```

list each skill as `current`, `outdated` (an upgrade would change it), `modified` (you
changed a file SST wrote), `missing` (its folder is gone), or `not installed`. With
`--check` the command exits 2 when a skill is outdated or missing, so a job can tell the kit
fell behind the installed SST. Either way, a kit that is behind ends with how many skills
are out of date and the next step, `sst agent-kit upgrade`; agent output lists each skill
that is not current as `<state> <agent> <skill>`. `sst debug` also reports which SST
installed the kit, and whether that is the one running.

## Removing

```bash
sst agent-kit remove
sst agent-kit remove --only sst-migrate-from-0-3
```

removes the files SST wrote and nobody changed, with any folder that leaves empty, and the
lock with the last skill. A file you changed is kept, and stays recorded, unless `--force`
(`SST-PRT014`). After removing some skills, `upgrade` maintains only those still installed.

## Not to be confused with published skills

The project's `skills/` folder (`project.skills_dir`) holds skills SST publishes to Snowflake
for Cortex Agents and Cortex Code profiles; see [Skills](skills.md). The agent kit is for the
coding agent that edits the project, and is never published.

Drawn from: [agent-kit commands](../../snowflake_semantic_tools/cli/commands/agent_kit.py), [planner and lock](../../snowflake_semantic_tools/domain/agent_kit.py), [kit files](../../snowflake_semantic_tools/adapters/agent_kit/__init__.py).
