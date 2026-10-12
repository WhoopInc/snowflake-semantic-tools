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

<!-- Describe how you tested your changes. Every gate, in the order CI runs it, is under "Running the Gates" in CONTRIBUTING.md -->

- [ ] All tests pass (`poetry run pytest tests/ -n auto`)
- [ ] Coverage holds: the per-layer ratchet (`python -m tests.helpers.coverage_ratchet check`) and the floors (domain 100%, app 95%, cli 90%, adapters 90%; branch coverage)
- [ ] New tests added for new functionality
- [ ] Goldens changed only where rendered output was meant to change, rewritten with `--update-golden`, with the reason under Additional Notes
- [ ] Manual testing completed (if applicable; against a non-production schema)

### Test Results

```
<!-- Paste the summary lines, or describe test results -->
<!-- Example: the last line of `poetry run pytest tests/ -n auto` -->
```

## Checklist

<!-- Mark completed items with an 'x' -->

### Code Quality
- [ ] Formatted with ruff, line length 120 (`poetry run ruff format --check snowflake_semantic_tools/ tests/`)
- [ ] Lint-clean, imports sorted (`poetry run ruff check snowflake_semantic_tools/ tests/`)
- [ ] Strictly typed (`poetry run mypy snowflake_semantic_tools tests` passes)
- [ ] Ring boundaries hold (`poetry run lint-imports` passes all eight contracts)
- [ ] SQL is composed only through `domain/sql` (`sql()` with a constant template and `Sql` parts)
- [ ] Security scans pass (`poetry run bandit -r snowflake_semantic_tools -ll`; semgrep with `.semgrep/`)
- [ ] `poetry.lock` regenerated and `DEPENDENCIES.md` updated (if a dependency changed; `poetry check --lock` passes)
- [ ] Pre-commit hooks pass (if using pre-commit)

### Diagnostics & Documentation
- [ ] Each new problem has a new `SST-` diagnostic code with an actionable suggestion; no code is reused
- [ ] Each new code has a `test_<code>_fires` / `test_<code>_silent` pair under `tests/codes/<area>/`; no allowlist entry added; a retired code has its `TOMBSTONES` row
- [ ] Each new option's help text is in `cli/help_text.py`, not `help=`
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

