---
name: sst-validate
description: "Run SST validation against a dbt project. Use when: validating semantic models, checking relationships, running sst validate. Triggers: validate, sst validate, check models, relationship errors."
tools: ["bash", "read", "edit"]
---

# SST Validate

Run `sst validate` against a dbt project and interpret the results.

## Prerequisites

- Conda environment with SST installed
- For dev branch testing: `pip install -e .` in the SST repo (NOT `poetry install` -- that targets Poetry's own venv)
- The target dbt project must have `sst_config.yaml` with `project.dbt_models_dir` set
- If Snowflake account is unavailable, set `snowflake_syntax_check: false` in sst_config.yaml

## Workflow

1. **Activate the environment**
   First, list the user's conda environments to find the one with SST installed:
   ```bash
   source "$(conda info --base)/etc/profile.d/conda.sh" && conda env list
   ```
   Present the results to the user and ask which environment has SST installed. Highlight any environments whose names contain "sst" as likely matches. Then activate the chosen environment:
   ```bash
   source "$(conda info --base)/etc/profile.d/conda.sh" && conda activate <env-name>
   ```

2. **Verify SST version**
   ```bash
   sst --version
   ```
   - Production: v1.4.x
   - Dev branch: v0.2.4 (editable install from repo)

3. **Run validation**
   ```bash
   cd <dbt-project-dir>
   sst validate
   ```

4. **Interpret results**
   - **0 errors, 0 warnings**: Clean pass. Ready for `sst extract`.
   - **Warnings only**: Non-blocking. Usually missing metadata (primary_key, synonyms).
   - **Errors**: Must fix before deployment. Common categories:
     - **Relationship errors**: Wrong join columns, self-references, circular references, missing PK/unique_keys
     - **SQL syntax errors**: If `snowflake_syntax_check: true` and Snowflake account unavailable, disable it
     - **Missing metadata**: Tables without `primary_key` in `config.meta.sst`

5. **For verbose output**
   ```bash
   sst validate --verbose
   ```

## Common Fixes

- **"Missing required field 'project.dbt_models_dir'"**: Add `dbt_models_dir: "models"` under `project:` in sst_config.yaml
- **"Table X skipped due to missing critical metadata (primary_key)"**: Add `primary_key: <column>` to the model's `config.meta.sst` section
- **Relationship join column mismatch**: Ensure the right_table's join column matches its declared `primary_key` or `unique_keys`
- **Self-reference error**: A relationship cannot have the same left_table and right_table
- **Circular relationship**: Two or more relationships forming a cycle (A -> B -> A). Remove one direction.

## Key Files

- SST repo: Resolve dynamically with `SST_REPO=$(git rev-parse --show-toplevel)`
- Target dbt project: **Ask the user** for the path to their dbt project before running validation
- Validation rules: `snowflake_semantic_tools/core/validation/rules/`
- Relationship validation: `snowflake_semantic_tools/core/validation/rules/references.py`
