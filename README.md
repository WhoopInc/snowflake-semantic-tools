# Snowflake Semantic Tools

Build and deploy Snowflake Semantic Views from your dbt project

[![Python](https://img.shields.io/badge/python-3.10--3.11-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

---

## What is SST?

SST helps you build **Snowflake Semantic Views**—a standardized semantic layer that lives in Snowflake and powers AI and BI tools—all from within your dbt projects.

**Why Semantic Views matter:**

- **Cortex Analyst & Agents** - Semantic views give Snowflake's AI the context it needs to accurately answer natural language questions about your data
- **BI Tools** - Sigma, Tableau, and other tools can consume semantic views for consistent metrics and definitions across your organization
- **Single source of truth** - Define metrics, relationships, and business logic once in Snowflake, use everywhere

**What SST does:**

- **Define semantics as code** - Metrics, relationships, filters, verified queries as YAML in your dbt project
- **Deploy to Snowflake** - Generate native SEMANTIC VIEW objects from your definitions
- **Enrich automatically** - Pull column types, samples, and metadata from Snowflake schemas
- **Validate before deploy** - 100+ validation rules catch errors before they reach Snowflake
- **Version control everything** - Your semantic layer lives in git alongside your dbt models

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
# Validate your semantic models
sst validate

# Enrich specific models with metadata from Snowflake
sst enrich --models customers,orders

# Deploy to Snowflake (validate → extract → generate)
sst deploy
```

---

## Key Features

- **Interactive Setup** - `sst init` wizard configures your project in seconds
- **Metadata Enrichment** - Auto-populate YAML with column & data types, sample values, synonyms, and enums
- **Validation** - Catch errors before deployment with comprehensive validation rules
- **SQL Syntax Validation** - Validate metrics and filters against Snowflake
- **Schema Verification** - Verify YAML columns exist in Snowflake with fuzzy matching
- **Semantic Views** - Generate native Snowflake SEMANTIC VIEWs

---

## Documentation

See the `docs/` directory for comprehensive documentation:

- [**Documentation Index**](docs/index.md) - Complete documentation navigation
- [**Getting Started**](docs/getting-started.md) - Installation and first steps
- [**CLI Reference**](docs/cli/index.md) - All commands and options
- [**Semantic Models Guide**](docs/concepts/semantic-models.md) - Writing metrics and relationships
- [**Validation Rules**](docs/concepts/validation-rules.md) - Complete list of all validation checks
- [**Authentication**](docs/guides/authentication.md) - Snowflake connection setup
- [**Configuration Reference**](docs/reference/config.md) - sst_config.yml options

---

## Requirements

- Python 3.10-3.11
- Snowflake account
- dbt project (dbt Core or dbt Cloud CLI)

---

## Commands

| Command | Purpose |
|---------|---------|
| `sst init` | Interactive setup wizard |
| `sst validate` | Check for errors (no Snowflake needed) |
| `sst enrich` | Add metadata to YAML from Snowflake |
| `sst format` | YAML linter for consistency |
| `sst extract` | Load metadata to Snowflake tables |
| `sst generate` | Create semantic views |
| `sst deploy` | One-step: validate → extract → generate |
| `sst debug` | Show config and test connection |
| `sst migrate-meta` | Migrate meta.sst to config.meta.sst (dbt Fusion) |

---

## Development Setup

**Only needed if you're contributing code or developing locally.**

```bash
# Clone the repository
git clone https://github.com/WhoopInc/snowflake-semantic-tools.git
cd snowflake-semantic-tools

# Install with Poetry (includes dev dependencies)
poetry install --with dev

# Verify installation
sst --version

# Run tests
pytest tests/unit/

# Format code
black snowflake_semantic_tools/
isort snowflake_semantic_tools/

# Run linting
flake8 snowflake_semantic_tools/
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed development guidelines.

---

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for:
- How to report issues
- Development setup instructions
- Code style guidelines
- Pull request process

**All contributions must be reviewed by the maintainer before merging.**

---

## Support

- **Documentation**: See the [docs/](docs/) directory
- **Issues**: [GitHub Issues](https://github.com/WhoopInc/snowflake-semantic-tools/issues)

---

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
