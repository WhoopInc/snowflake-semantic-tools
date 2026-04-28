---
name: sst-test
description: "Run SST unit tests with pytest. Use when: running tests, checking test coverage, verifying fixes. Triggers: test, pytest, unit test, run tests."
---

# SST Test

Run the SST test suite using pytest.

## Prerequisites

- Conda environment with SST installed via `pip install -e .`
- Project uses **Poetry** for dependency management (NOT uv)

## Workflow

### Step 1: Activate environment and navigate to repo

First, list the user's conda environments to find the one with SST installed:
```bash
source "$(conda info --base)/etc/profile.d/conda.sh" && conda env list
```

**⚠️ MANDATORY STOPPING POINT**: Present the results to the user and ask which environment has SST installed. Highlight any environments whose names contain "sst" as likely matches. Do NOT proceed until user responds.

Then activate the chosen environment:
```bash
source "$(conda info --base)/etc/profile.d/conda.sh" && conda activate <env-name>
cd "$(git rev-parse --show-toplevel)"
```

### Step 2: Run all unit tests

```bash
python -m pytest tests/unit/ -v
```

### Step 3: Run specific test file (if targeted testing needed)

```bash
python -m pytest tests/unit/core/validation/test_relationship_validation.py -v
```

### Step 4: Run specific test by name pattern

```bash
python -m pytest tests/unit/ -v -k "test_unique_key"
```

### Step 5: Run with coverage (optional)

```bash
python -m pytest tests/unit/ --cov=snowflake_semantic_tools --cov-report=term-missing
```

### On Failure

**⚠️ MANDATORY STOPPING POINT**: If tests fail, report failures to the user with error messages before taking any corrective action. Do NOT auto-fix without approval.

## Test Structure

```
tests/
  unit/
    core/
      validation/
        test_relationship_validation.py  # Relationship rule tests
        test_validator.py                # Validator integration tests
      generation/
        test_semantic_view_builder.py    # SQL generation tests
      parsing/
        test_dbt_parser.py              # dbt manifest parsing tests
```

## Key Conventions

- Test files: `test_*.py`
- Test functions: `test_*`
- Fixtures and helpers at the top of each test file
- Tests use synthetic data (no Snowflake connection needed)
- Relationship tests build mock `dbt_data` dicts with `sm_tables` and relationship YAML

## Stopping Points

- ✋ Step 1: After listing conda environments (ask user which to activate)
- ✋ On Failure: After test failures (report before taking action)

## Output

Test results summary: total collected, passed count, failed count, and error messages for any failures.
