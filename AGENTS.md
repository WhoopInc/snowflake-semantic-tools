# Snowflake Semantic Tools (SST)

## Project Overview

SST is a CLI tool for managing semantic models for Snowflake's Cortex Analyst. It transforms dbt models into a semantic layer for BI tools and AI-powered analytics.

**Key commands**: `sst validate`, `sst extract`, `sst deploy`, `sst enrich`, `sst generate`

## Architecture

```
snowflake_semantic_tools/
  core/
    validation/          # Semantic model validation
      rules/             # Individual validation rules
        references.py    # Relationship validation (PK, unique_keys, cycles, self-ref)
        duplicates.py    # Duplicate detection
        dbt_models.py    # dbt model validation
      validator.py       # Orchestrates validation rules
    generation/          # SQL generation for semantic views
    parsing/             # dbt manifest + YAML parsing
      parsers/
        dbt_parser.py    # Parses dbt model YAML
        data_extractors.py  # Extracts table/column info
    models/              # Data model schemas
  services/              # High-level service layer
  interfaces/cli/        # Click CLI commands
tests/
  unit/                  # Unit tests (pytest)
```

## Development Setup

- **Package manager**: Poetry (NOT uv)
- **Python**: 3.11 via conda
- **Conda env**: `sst`
- **Editable install**: `pip install -e .` (installs into conda env, NOT `poetry install`)
- **Tests**: `python -m pytest tests/unit/ -v`
- **Test project**: `sst-jaffle-shop` repo (13 dbt models, 5 relationships)

## Conventions

- **Formatting**: Black, isort
- **Column/table names**: Case-insensitive comparisons (`.lower()`)
- **Primary keys**: Stored as uppercase lists in `sm_tables[].primary_key`
- **Unique keys**: Stored as uppercase lists in `sm_tables[].unique_keys`
- **Relationship validation**: Right-side join columns must match `primary_key` OR `unique_keys`
- **Error messages**: Must be actionable -- tell user what's wrong AND how to fix it
- **Imports**: At module level; avoid redundant imports inside functions

## Validation Rules (references.py)

The relationship validator checks:
1. Both tables exist in the catalog
2. No self-references (left_table == right_table)
3. Join columns match the right table's `primary_key` or `unique_keys`
4. No circular relationships (detected via Tarjan's SCC algorithm)
5. Tables with only `unique_keys` (no PK) are valid if join columns match

## Git Workflow

- Feature branches off `main`
- PR naming: `fix/<description>` or `feature/<description>`
- Commit messages: `Fix: <description>` or `Feature: <description>`
- Tests must pass before merge
- Test both unit tests AND end-to-end validation in sst-jaffle-shop
