# Contributing to Snowflake Semantic Tools

Thank you for your interest in contributing to SST! This document outlines the process and guidelines.

## Maintainer

This project is maintained by **Matt Luizzi** ([@mluizzi-whoop](https://github.com/mluizzi-whoop)) at WHOOP Inc.

**All external contributions must be reviewed and approved by a maintainer before merging.**

## How to Contribute

### Reporting Issues

We use GitHub issue templates to help structure your report. When you [create a new issue](https://github.com/WhoopInc/snowflake-semantic-tools/issues/new/choose), you'll be able to choose from:

- **Bug Report** - Report unexpected behavior or errors
- **Feature Request** - Suggest new features or enhancements
- **Documentation Issue** - Report missing or unclear documentation

#### What Makes a Great Issue?

**Great issues help us help you faster.** Here's what makes an issue effective:

##### 1. Clear Problem Statement

Describe what's wrong or what you need. Be specific.

**Good**: "SST deployment fails with 'Table not found' error when creating semantic views for newly added dbt models in CI/CD"

**Poor**: "SST doesn't work"

##### 2. Expected vs Actual Behavior

Show what you expected versus what actually happened.

**Good**:
```
Expected: SST should create semantic views after dbt models are materialized
Actual: SST fails with error "Table 'NEW_TABLE' not found in database 'ANALYTICS'"
```

##### 3. Reproduction Steps

Provide minimal, step-by-step instructions to reproduce the issue.

**Good**:
```
1. Create new dbt model: models/sales/new_table.sql
2. Add semantic view YAML with meta.sst block
3. Run `sst deploy --db ANALYTICS --schema SEMANTIC_VIEWS`
4. Observe error: "Table 'NEW_TABLE' not found"
```

##### 4. Environment Details

Always include:
- SST version (`sst --version`)
- Python version (`python --version`)
- dbt version (`dbt --version`)
- Operating system
- Snowflake/warehouse type

##### 5. Relevant Context

Include logs, error messages, code snippets, or configuration files. Use verbose mode (`--verbose`) when available.

```bash
# Get detailed output
sst deploy --db ANALYTICS --schema SEMANTIC_VIEWS --verbose
```

#### Issue Quality Examples

**Bug Report Example:**

> **Title**: [Bug] SST fails when semantic views reference tables not yet materialized in CI/CD
> 
> **Problem**: When dbt models and semantic views are deployed together in CI/CD, SST fails if it runs before dbt completes
> 
> **Expected**: SST should either wait or gracefully handle missing tables
> 
> **Actual**: Deployment fails with "Table 'TABLE_NAME' not found"
> 
> **Steps to Reproduce**:
> 1. Create PR with new dbt model and semantic view
> 2. Merge PR (triggers CI/CD)
> 3. SST and dbt run in parallel
> 4. SST fails with table not found error
> 
> **Environment**: SST 0.1.0, Python 3.11, dbt 1.7, GitHub Actions
> 
> **Root Cause**: No job dependency between dbt and SST workflows in GitHub Actions config

**Feature Request Example:**

> **Title**: [Feature] Add graceful handling for missing tables during deployment
> 
> **Problem**: SST fails when tables don't exist yet, blocking CI/CD deployments
> 
> **Proposed Solution**: Add `--allow-missing-tables` flag to skip semantic views with missing tables instead of failing
> 
> **Alternatives**: Configure CI/CD orchestration (requires external changes)
> 
> **Impact**: Benefits all users running SST in CI/CD pipelines
> 
> **Example Usage**:
> ```bash
> sst deploy --db ANALYTICS --schema SEMANTIC_VIEWS --allow-missing-tables
> ```

**Documentation Issue Example:**

> **Title**: [Docs] Missing CI/CD orchestration guidance
> 
> **Location**: docs/deployment-guide.md
> 
> **Issue**: No documentation explaining SST must run after dbt in CI/CD
> 
> **Suggested Improvement**: Add section on CI/CD pipeline orchestration with GitHub Actions examples
> 
> **Target Audience**: All users deploying via CI/CD

#### Before Submitting

- [ ] Search existing issues to avoid duplicates
- [ ] Use the appropriate issue template
- [ ] Include all required information
- [ ] Provide clear, concise descriptions
- [ ] Add relevant labels if you can

### Proposing Changes

**Before starting work:**
1. Open an issue describing the problem/feature
2. Wait for maintainer feedback before implementing
3. Discuss approach and get alignment

**This prevents wasted effort on changes that won't be accepted.**

### Pull Request Process

1. **Fork the repository** and create a feature branch
2. **Make your changes** following the code style below
3. **Add tests** for new functionality
4. **Update documentation** if needed
5. **Run the test suite** and ensure all tests pass
6. **Submit a pull request** with:
   - Clear description of changes
   - Link to related issue
   - Test results showing everything passes

**Pull requests will only be merged after maintainer review and approval.**

## Development Setup

### Prerequisites

- Python 3.9-3.11
- Poetry for dependency management
- Snowflake account (for integration tests)
- dbt project (for testing)

### Installation

```bash
# Clone the repository
git clone https://github.com/WhoopInc/snowflake-semantic-tools.git
cd snowflake-semantic-tools

# Install dependencies
poetry install

# Activate virtual environment
poetry shell

# Verify installation
sst --version
```

### Running Tests

```bash
# Run all unit tests
pytest tests/unit/

# Run with coverage
pytest --cov=snowflake_semantic_tools tests/unit/

# Run specific test file
pytest tests/unit/core/validation/test_relationship_validation.py

# Run in verbose mode
pytest tests/unit/ -vv
```

### Code Style

- **Formatting**: Black (line length 120)
- **Imports**: isort with black profile
- **Type hints**: Required for new code
- **Docstrings**: Required for public functions/classes

```bash
# Format code
black snowflake_semantic_tools/

# Sort imports
isort snowflake_semantic_tools/

# Type checking
mypy snowflake_semantic_tools/
```

## Code Review Criteria

Pull requests will be evaluated on:

- **Functionality**: Does it solve the stated problem?
- **Tests**: Are there tests covering the changes?
- **Documentation**: Is documentation updated?
- **Code quality**: Follows style guidelines, no linting errors
- **Backward compatibility**: Doesn't break existing functionality
- **Performance**: No significant performance regressions

## What We're Looking For

**Priority areas for contribution:**
- Bug fixes with reproduction steps
- Improved error messages
- Additional validation rules
- Documentation improvements
- Performance optimizations
- Test coverage improvements

**Not currently accepting:**
- Major architectural changes (discuss first)
- Breaking changes (discuss first)
- Features without clear use cases

## Questions?

- Open an issue for questions
- Tag the maintainer (@mluizzi-whoop) for urgent matters
- Check existing documentation in `docs/` folder
- Review existing issues and pull requests

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.

---

Thank you for helping improve Snowflake Semantic Tools!

