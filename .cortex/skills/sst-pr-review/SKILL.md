---
name: sst-pr-review
description: "Review a pull request on the SST repo. Use when: reviewing PRs, code review, checking PR changes. Triggers: review PR, pull request, code review, PR review."
tools: ["bash", "read", "glob", "grep"]
---

# SST PR Review

Review a pull request on WhoopInc/snowflake-semantic-tools.

## Workflow

1. **Fetch PR details**
   ```bash
   gh pr view <number> --repo WhoopInc/snowflake-semantic-tools
   gh pr diff <number> --repo WhoopInc/snowflake-semantic-tools
   ```

2. **Review checklist**
   - [ ] **Correctness**: Does the logic handle all edge cases?
   - [ ] **Tests**: Are new/modified behaviors covered by tests?
   - [ ] **No regressions**: Do existing tests still pass?
   - [ ] **Code style**: Consistent with existing patterns (Black formatting, isort imports)?
   - [ ] **Error messages**: Clear, actionable error messages for users?
   - [ ] **Performance**: No unnecessary loops or redundant computation?

3. **Key areas to scrutinize**
   - **Validation rules** (`core/validation/rules/`): Check for short-circuit bugs (missing `continue`/`return`), correct set operations, case sensitivity
   - **Relationship validation** (`references.py`): Verify PK/unique_key matching logic, self-reference detection, cycle detection (Tarjan's SCC)
   - **dbt parsing** (`core/parsing/`): Manifest field access, null handling, backward compatibility
   - **SQL generation** (`core/generation/`): Correct SQL syntax, proper escaping

4. **Run the tests**
   ```bash
   source /opt/anaconda3/etc/profile.d/conda.sh && conda activate sst
   cd /Users/matthew.luizzi/Documents/WHOOP/GitHub/snowflake-semantic-tools
   python -m pytest tests/unit/ -v
   ```

5. **End-to-end validation** (optional but recommended)
   ```bash
   cd /Users/matthew.luizzi/Documents/WHOOP/GitHub/sst-jaffle-shop
   sst validate
   ```

## Bug Categories to Watch For

1. **Logic bugs**: Wrong operator, missing negation, off-by-one in set comparisons
2. **Control flow bugs**: Missing `continue`/`break` after error handling (causes cascading errors)
3. **Scope bugs**: Variables computed inside a branch but needed outside it
4. **Import bugs**: Redundant imports, missing imports, imports inside functions when already at module level
5. **Case sensitivity**: Column/table names should be compared case-insensitively

## Report Format

Structure findings as:
- **Bugs** (numbered, with severity): Must fix before merge
- **Nits** (numbered): Nice to fix but non-blocking
- Include file path, line numbers, and suggested fix for each finding
