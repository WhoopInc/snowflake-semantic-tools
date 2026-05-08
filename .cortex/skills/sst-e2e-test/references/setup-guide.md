# Environment Setup Guide

## Prerequisites

- Python 3.11+
- Git access to both repos
- A valid Snowflake connection configured in `~/.dbt/profiles.yml`
- pip (for editable install)

## Locating the SST Repo

The skill assumes you are running from within the `snowflake-semantic-tools` repository. The repo root is identified by the presence of `pyproject.toml` containing `snowflake-semantic-tools`.

## Installing SST (Development Mode)

Always use `pip install -e .` from the SST repo root. Do NOT use `poetry install` — that installs into Poetry's own virtualenv, which may not be the active environment.

```bash
cd <SST_REPO>
pip install -e .
```

Verify:
```bash
sst --version
```

If the user has a conda environment, virtualenv, or other Python environment manager, ask them to activate it first. Do not assume any specific environment name or conda installation path.

## sst-jaffle-shop Test Project

| Property | Value |
|----------|-------|
| GitHub   | https://github.com/WhoopInc/sst-jaffle-shop |
| Branch   | `complete-project` |
| Location | Sibling directory to SST repo, or cloned during setup |
| Models   | 13 dbt models (6 staging + 7 marts) |

The skill checks for the test project as a sibling directory (`../sst-jaffle-shop`). If not found, it clones from GitHub.

## dbt Target Configuration

The dbt target controls which Snowflake database/schema SST writes to. Targets are defined in `~/.dbt/profiles.yml` under the `sst_jaffle_shop` profile.

Common targets:
- `dev` — personal development schema
- `ci` — dedicated CI/CD schema (if configured)
- `prod` — production (use with caution)

The E2E skill will ask which target to use at runtime. Default: `dev`.

## Known Gotchas

1. **sst_config.yaml** — The jaffle-shop project uses `sst_config.yaml` (not `.yml`). It should contain:
   ```yaml
   project:
     semantic_models_dir: "snowflake_semantic_models"
   ```

2. **Template syntax** — The `complete-project` branch uses `{{ ref('model_name') }}` and `{{ ref('model', 'column') }}` syntax (unified ref), not the legacy `{{ table() }}` / `{{ column() }}` syntax.

3. **Never read profiles.yml** — `~/.dbt/profiles.yml` contains secrets. Never cat, read, or log it. SST reads it internally via dbt's profile parser.

4. **Package manager** — SST uses Poetry for dependency management. Never use `uv` or create `uv.lock`.

5. **Editable install** — If `sst --version` shows an unexpected version, re-run `pip install -e .` from the SST repo root to ensure the development branch is active.
