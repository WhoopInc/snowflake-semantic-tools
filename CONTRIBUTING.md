# Contributing to Snowflake Semantic Tools

Thank you for your interest in contributing to SST! This document outlines the process and guidelines.

## Maintainer

This project is maintained by **Matt Luizzi** ([@mluizzi-whoop](https://github.com/mluizzi-whoop)) at WHOOP Inc.

**All external contributions must be reviewed and approved by a maintainer before merging.**

## How to Contribute

### Reporting Issues

- Search existing issues before creating a new one
- Include SST version, Python version, and Snowflake details
- Provide minimal reproduction steps
- Share relevant error messages and logs

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
git clone https://github.com/YOUR_ORG/snowflake-semantic-tools.git
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

