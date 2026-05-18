# Snowflake Semantic Tools (SST)

Build, validate, and deploy Snowflake Semantic Views from your dbt project.

[![Python](https://img.shields.io/badge/python-3.10--3.11-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

---

## What is SST?

SST helps you build **Snowflake Semantic Views**—a standardized semantic layer that lives in Snowflake and powers AI and BI tools—all from within your dbt projects.

**Why Semantic Views matter:**

- **Cortex Analyst & Agents** — Semantic views give Snowflake's AI the context it needs to accurately answer natural language questions about your data
- **BI Tools** — Sigma, Tableau, and other tools can consume semantic views for consistent metrics and definitions across your organization
- **Single source of truth** — Define metrics, relationships, and business logic once in Snowflake, use everywhere

**What SST does:**

- **Define semantics as code** — Metrics, relationships, filters, verified queries as YAML in your dbt project
- **Deploy to Snowflake** — Generate native SEMANTIC VIEW objects from your definitions
- **Enrich automatically** — Pull column types, samples, and metadata from Snowflake schemas (including dbt sources)
- **Validate before deploy** — 50+ validation rules catch errors before they reach Snowflake
- **Compile locally** — Build a manifest for offline tooling, diffing, and CI/CD
- **Diff before deploy** — Preview exactly what will change before touching Snowflake
- **Incremental deploys** — Only regenerate views affected by your changes (`--only-modified`)
- **Version control everything** — Your semantic layer lives in git alongside your dbt models

---

## Quick Start

### Installation

```bash
pip install snowflake-semantic-tools
```

### Setup

```bash
cd your-dbt-project
sst init  # Interactive setup wizard
```

The wizard will:
- Detect your dbt project and profile
- Create `sst_config.yml` with defaults
- Set up the semantic models directory
- Generate example files

### Basic Usage

```bash
# Enrich models with metadata from Snowflake
sst enrich --models customers,orders

# Validate your semantic models (offline, no Snowflake needed)
sst validate

# Preview what will change
sst diff

# Deploy to Snowflake (validates first, then generates)
sst generate --all

# Only deploy views affected by recent changes
sst generate --all --only-modified
```

---

## Commands

| Command | Purpose |
|---------|---------|
| `sst init` | Interactive setup wizard |
| `sst enrich` | Add metadata to YAML from Snowflake (models + sources) |
| `sst format` | YAML linter for consistency |
| `sst compile` | Build local manifest (offline, for tooling/CI) |
| `sst validate` | Check for errors (no Snowflake needed) |
| `sst diff` | Preview semantic view changes before deployment |
| `sst extract` | Load metadata to Snowflake tables |
| `sst generate` | Create semantic views (with `--dry-run` and SQL file output) |
| `sst deploy` | ~~One-step: validate → extract → generate~~ [DEPRECATED] |
| `sst drop` | Remove semantic views (specific or prune orphans) |
| `sst list` | Explore components from compiled manifest |
| `sst clean` | Remove generated artifacts |
| `sst debug` | Show config and test connection |
| `sst migrate-meta` | Migrate legacy `meta.sst` to `config.meta.sst` |

---

## Key Features

### Semantic Modeling
- Define metrics, relationships, filters, verified queries, and custom instructions as YAML
- Compose semantic views from multiple tables with join relationships
- Template metrics with `{{ metric('name') }}` references

### Enrichment
- Auto-populate YAML with column types, sample values, synonyms, and enums
- Enrich dbt sources (`--include-sources`, `--sources-only`, `--source raw.orders`)
- AI-powered descriptions and synonym generation via Cortex

### Validation
- 50+ rules covering references, types, expressions, and structure
- SQL syntax validation against Snowflake
- Schema verification with fuzzy column matching
- Cross-table metric column validation

### Deployment
- Incremental deploys with `--only-modified` (detects YAML + SQL changes)
- Dry-run mode with SQL file output (`--dry-run --output-dir`)
- Diff before deploy to preview additions, removals, and modifications
- Compile to local manifest for CI/CD pipelines

---

## Documentation

See the `docs/` directory for comprehensive documentation:

- [**Documentation Index**](docs/index.md) — Complete documentation navigation
- [**Getting Started**](docs/getting-started.md) — Installation and first steps
- [**CLI Reference**](docs/cli/index.md) — All commands and options
- [**Semantic Models Guide**](docs/concepts/semantic-models.md) — Writing metrics and relationships
- [**Validation Rules**](docs/concepts/validation-rules.md) — Complete list of all validation checks
- [**Authentication**](docs/guides/authentication.md) — Snowflake connection setup
- [**Configuration Reference**](docs/reference/config.md) — sst_config.yml options

---

## Requirements

- Python 3.10–3.11
- Snowflake account
- dbt project (dbt Core or dbt Cloud CLI)

---

## Development Setup

```bash
git clone https://github.com/WhoopInc/snowflake-semantic-tools.git
cd snowflake-semantic-tools

poetry install --with dev
pre-commit install

sst --version
pytest tests/unit/
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
