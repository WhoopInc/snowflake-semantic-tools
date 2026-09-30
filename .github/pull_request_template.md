## Description

<!-- Provide a clear and concise description of what this PR does -->

## Related Issue

<!-- Link to related issue(s). Use "Closes #123" or "Fixes #123" if this PR resolves an issue -->
<!-- Example: Closes #123 -->

## Type of Change

<!-- Mark the relevant option with an 'x' -->

- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (changes an exit code, the JSON envelope, a diagnostic code's meaning, or other existing behavior)
- [ ] 0.3.x maintenance fix (targets the branch cut from `v0.3.1`; never merged into 1.0)
- [ ] Documentation update
- [ ] Performance improvement
- [ ] Code refactoring
- [ ] Test improvements
- [ ] Other (please describe):

## Changes Made

<!-- Describe the specific changes in this PR -->

## Testing

<!-- Describe how you tested your changes. The gates are listed in CONTRIBUTING.md -->

- [ ] All tests pass (`poetry run pytest tests/`)
- [ ] Coverage floors hold (domain 100%, app 95%, cli 90%, branch coverage)
- [ ] New tests added for new functionality
- [ ] Goldens changed only where rendered output was meant to change, with the reason under Additional Notes
- [ ] Manual testing completed (if applicable; against a non-production schema)

### Test Results

```
<!-- Paste the summary lines, or describe test results -->
<!-- Example: the last line of `poetry run pytest tests/` -->
```

## Checklist

<!-- Mark completed items with an 'x' -->

### Code Quality
- [ ] Formatted with Black, line length 120 (`poetry run black --check snowflake_semantic_tools/`)
- [ ] Imports sorted with isort, black profile (`poetry run isort --check snowflake_semantic_tools/`)
- [ ] Every function is annotated (`poetry run mypy snowflake_semantic_tools` passes)
- [ ] Ring boundaries hold (`poetry run lint-imports` passes)
- [ ] Pre-commit hooks pass (if using pre-commit)

### Diagnostics & Documentation
- [ ] Each new problem has a new `SST-` diagnostic code with an actionable suggestion; no code is reused
- [ ] Reference pages regenerated with `poetry run sst docs` if diagnostics, config keys, CLI options, or artifact types changed (`poetry run sst docs --check` passes)
- [ ] Guides and README updated (if needed)
- [ ] Breaking changes discussed with a maintainer first (see CONTRIBUTING.md)

### Performance
- [ ] Performance impact considered
- [ ] No significant performance regressions

## Screenshots / Examples

<!-- If applicable, add examples such as `sst plan` output or rendered DDL -->

## Additional Notes

<!-- Any additional information that reviewers should know -->

