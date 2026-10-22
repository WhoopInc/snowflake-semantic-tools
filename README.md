# Snowflake Semantic Tools (SST)

Build, validate, and publish Snowflake semantic views, Cortex Agents, and Cortex
skills from your dbt project.

[![Python](https://img.shields.io/badge/python-3.11--3.13-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

---

## What is SST?

SST keeps a Snowflake semantic layer as code, next to the dbt models it
describes, and publishes it the way infrastructure is published: validate
offline, review a plan, apply exactly that plan.

It publishes eight artifact types from one project:

- **Semantic views**, assembled from dbt models plus metrics, relationships,
  filters, verified queries, and custom instructions written in YAML;
- **tools** that agents call: Cortex Search services, procedures, functions, and
  stages, a routine's body naming its tables with `{{ ref() }}` as a view does;
- **Cortex Agents**, with every tool, view, and skill they use resolved and pinned;
- **CoWork profiles** (a Snowflake private preview): an agent that routes each
  question to one of the agents it lists;
- **evals** that score an agent or CoWork profile against a fixed question set and gate a change
  on the result;
- **skills** and **plugins**, published as Cortex Extensions and versioned by
  their content;
- **CoCo Desktop profiles** that bundle skills, a system prompt, MCP servers, and
  hooks.

It records the lineage between them as it compiles, from each dbt source,
through the models built on it, to the view members, tools, agents and evals
that read them. `sst lineage` shows what any of them is built from and what a
change to it reaches, and `--select model:orders+` plans only that.

## Quick start

```bash
pip install snowflake-semantic-tools
cd your-dbt-project        # profiles.yml lives here
sst init
sst enrich --target prod models/marts   # column and data types, read from a production target
sst validate
sst compile                # writes the manifest plan and apply read
sst plan                   # exit 2 means there are changes to apply
sst apply --plan sst_target/plan.json --yes
```

The [getting started guide](docs/getting-started.md) walks through a first
semantic view end to end.

## Commands

| Command | Purpose |
|---------|---------|
| `sst init` | Write `sst_config.yml` and the semantic-model directory into a dbt project, never overwriting a file |
| `sst debug` | Show the resolved configuration, profile, target, and state table, and test the connection (`--no-connect` skips it) |
| `sst enrich` | Fill missing column types, data types, sample values, enums, and synonyms from the warehouse, editing the YAML in place |
| `sst format` | Rewrite YAML into canonical form; `--check` exits 2 when a file would change |
| `sst validate` | Check every artifact, offline or with Snowflake syntax checks |
| `sst compile` | Render every artifact and write the SST manifest; `--emit-ddl` writes each rendered payload |
| `sst plan` | Compare the project with Snowflake and save a reviewable plan |
| `sst apply` | Execute a saved plan |
| `sst diff` | Compare two states -- local, a dbt target, or a saved plan |
| `sst test` | Run smoke probes against published semantic views, and agent evals |
| `sst ask` | Talk to an agent as it compiles now, on scratch copies of your edits, publishing nothing |
| `sst list` | List compiled artifacts and their status |
| `sst lineage` | Show what a dbt node, member, or artifact reads and what reads it, from dbt sources to agents and evals |
| `sst baseline` | Record known, not yet fixed warnings so they stop blocking under `--strict`, and maintain that file |
| `sst explain` | Explain one diagnostic code, offline |
| `sst migrate refs` | Write every bare reference as its `{{ }}` call, 0.3 `table()` / `column()` as `ref()`, and old item settings under `config:`; a dry run unless `--write` |
| `sst migrate split` | Write each listed member to its own single-item file, and tool groups into folders; a dry run unless `--write` |
| `sst docs` | Regenerate the reference pages from the engine's registries |
| `sst clean` | Remove local build output |
| `sst drop` | Break-glass: drop one object SST owns from one target |
| `sst state recover` / `convert` / `unlock` | Repair state: record lost entries from ownership markers and content digests, rebuild the state tables as hybrid or standard, or release a named run's lock |
| `sst agent-kit` | Install, upgrade, show the status of, or remove skills that teach Cortex Code or Claude Code to work in an SST project |

Every command that reports results takes `--output json` for one machine-readable
result on stdout.

## Documentation

- [Documentation index](docs/index.md)
- [Getting started](docs/getting-started.md) and [concepts](docs/concepts.md)
- Guides: [semantic views](docs/guides/semantic-views.md),
  [enriching models](docs/guides/enrich.md),
  [agents](docs/guides/agents.md), [CoWork profiles](docs/guides/cowork-profiles.md),
  [evals](docs/guides/evals.md), [asking an agent](docs/guides/ask.md),
  [skills](docs/guides/skills.md),
  [plugins and profiles](docs/guides/plugins-and-coco-desktop-profiles.md),
  [configuration](docs/guides/configuration.md),
  [references](docs/guides/references.md), [lineage](docs/guides/lineage.md),
  [CI/CD](docs/guides/ci-cd.md),
  [security](docs/guides/security.md),
  [troubleshooting](docs/guides/troubleshooting.md),
  [agent kit](docs/guides/agent-kit.md),
  [testing SST](docs/guides/testing-live.md)
- Reference: [CLI](docs/reference/cli.md), [configuration keys](docs/reference/config.md),
  [error codes](docs/reference/error-codes.md), [artifact types](docs/reference/artifacts.md),
  [coverage matrix](docs/reference/coverage.md)
- Upgrading: [migrating from 0.3](docs/guides/migrating-from-0.3.md)

## Requirements

- Python 3.11–3.13
- A Snowflake account
- A dbt project on the Snowflake adapter, for semantic views, tools, agents, and
  evals. A project that publishes only skills, plugins, and profiles does not
  need dbt (see [projects without dbt](docs/guides/configuration.md#projects-without-dbt)).

---

## Development setup

```bash
git clone https://github.com/WhoopInc/snowflake-semantic-tools.git
cd snowflake-semantic-tools

poetry install
poetry run pre-commit install   # optional: a fast subset of the gates on every commit

poetry run sst --version
poetry run pytest               # offline: no account and no dbt install needed
poetry run sst docs --check     # the generated reference pages are current
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed development guidelines.

---

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for:
- How to report issues
- Development setup instructions
- Code style guidelines
- Pull request process

---

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
