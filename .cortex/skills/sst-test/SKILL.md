---
name: sst-test
description: "Run SST unit tests with pytest. Use when: running tests, checking test coverage, verifying fixes. Triggers: test, pytest, unit test, run tests."
tools: ["bash", "read"]
---

# SST Test

Run the SST test suite using pytest.

## Prerequisites

- Conda environment with SST installed via `pip install -e .`
- Project uses **Poetry** for dependency management (NOT uv)

## Workflow

1. **Activate environment and navigate to repo**
   First, list the user's conda environments to find the one with SST installed:
   ```bash
   source "$(conda info --base)/etc/profile.d/conda.sh" && conda env list
   ```
   Present the results to the user and ask which environment has SST installed. Highlight any environments whose names contain "sst" as likely matches. Then activate the chosen environment:
   ```bash
   source "$(conda info --base)/etc/profile.d/conda.sh" && conda activate <env-name>
   cd "$(git rev-parse --show-toplevel)"
   ```

2. **Run all unit tests**
   ```bash
   python -m pytest tests/unit/ -v
   ```

3. **Run specific test file**
   ```bash
   python -m pytest tests/unit/core/validation/test_relationship_validation.py -v
   ```

4. **Run specific test by name pattern**
   ```bash
   python -m pytest tests/unit/ -v -k "test_unique_key"
   ```

5. **Run with coverage**
   ```bash
   python -m pytest tests/unit/ --cov=snowflake_semantic_tools --cov-report=term-missing
   ```

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

- Before pushing: Ensure all tests pass
- After modifying validation rules: Run the full relationship test suite
- After any code change: Run at least the relevant test module
