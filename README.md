# Snowflake Semantic Tools

dbt extension for managing Snowflake Semantic Views and Cortex Analyst semantic models

[![Python](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

---

## What is SST?

SST integrates Snowflake's semantic layer into your dbt workflow. Define semantic views, metrics, and relationships in YAML alongside your dbt models, then deploy them as native Snowflake SEMANTIC VIEW objects.

**What you can do:**

- **Define semantics in dbt** - Metrics, relationships, filters as YAML in your dbt project
- **Deploy to Snowflake** - Create SEMANTIC VIEW objects for Cortex Analyst and BI tools
- **Enrich metadata** - Auto-populate YAML from Snowflake table schemas
- **Validate before deploy** - Catch errors without Snowflake connection
- **Maintain as code** - Version control your semantic layer with dbt

---

## Quick Start

### Installation

```bash
pip install snowflake-semantic-tools
```

### Basic Usage

```bash
# Validate your semantic models
sst validate

# Enrich dbt models with metadata from Snowflake
sst enrich models/ --database PROD_DB --schema my_schema

# Deploy to Snowflake (validate → extract → generate)
sst deploy --db PROD_DB --schema SEMANTIC_VIEWS
```

---

## Key Features

- **Metadata Enrichment** - Auto-populate YAML with column types, samples, and enums
- **Validation** - Catch errors before deployment (no Snowflake connection needed)
- **Semantic Views** - Generate native Snowflake SEMANTIC VIEWs
- **Defer Database** - Generate dev views that reference prod tables (like dbt defer)
- **YAML Linter** - Consistent formatting across your project
- **Python API** - Full programmatic access to all features

---

## Documentation

See the `docs/` directory for comprehensive documentation:

- [**Getting Started**](docs/getting-started.md) - Installation and first steps
- [**Validation Checklist**](docs/validation-checklist.md) - Complete list of all 98 validation checks
- [**CLI Reference**](docs/cli-reference.md) - All commands and options
- [**User Guide**](docs/user-guide.md) - Enrichment and validation deep dives
- [**Semantic Models Guide**](docs/semantic-models-guide.md) - Writing metrics and relationships
- [**Authentication**](docs/authentication.md) - Snowflake connection setup

---

## Requirements

- Python 3.9+
- Snowflake account
- dbt project with YAML definitions

---

## Commands

| Command | Purpose |
|---------|---------|
| `sst validate` | Check for errors (no Snowflake needed) |
| `sst enrich` | Add metadata to YAML from Snowflake |
| `sst format` | YAML linter for consistency |
| `sst extract` | Load metadata to Snowflake tables |
| `sst generate` | Create semantic views |
| `sst deploy` | One-step: validate → extract → generate |

---

## Installation

**For users (recommended):**

```bash
pip install snowflake-semantic-tools
```

**For developers/contributors:**

If you want to contribute or modify the code, see the [Development Setup](#development-setup) section below or [CONTRIBUTING.md](CONTRIBUTING.md) for detailed instructions.

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
