---
name: sst-pr-review
description: "Review a pull request on the SST repo. Use when: reviewing PRs, code review, checking PR changes. Triggers: review PR, pull request, code review, PR review."
---

# SST PR Review

Review a pull request on WhoopInc/snowflake-semantic-tools.

## Workflow

### Step 1: Fetch PR metadata and diff

```bash
gh pr view <number> --repo WhoopInc/snowflake-semantic-tools --json title,body,state,author,baseRefName,headRefName,additions,deletions,changedFiles
gh pr diff <number> --repo WhoopInc/snowflake-semantic-tools
```

### Step 2: Process enforcement checks

These are non-negotiable. Fail the review if any are violated.

**Commit message regex** — every commit on the PR must match:
```
^(\[[A-Z][A-Z0-9]*-\d+\]|[a-z]+\([A-Z][A-Z0-9]*-\d+\):|\([A-Z][A-Z0-9]*-\d+\)|Ver: (\d+\.\d+\.\d+(\.\d+)?|\d+\.\d+\.\d+-\d+)|(?i:initial commit))
```
Check with:
```bash
gh pr view <number> --repo WhoopInc/snowflake-semantic-tools --json commits --jq '.commits[].messageHeadline'
```

**Single issue** — the PR body must contain exactly one `Closes #<number>` or `Fixes #<number>`. Multiple linked issues means the PR should be split.

**PR title** — must also match the commit regex (it becomes the merge commit message).

### Step 3: Code review

Read the diff carefully. Focus on:

**Correctness**
- Edge cases: null/empty inputs, case sensitivity (column/table names must use `.lower()`)
- Set operations: set equality vs subset vs superset — wrong choice causes silent bugs
- Control flow: every error-handling branch needs `continue`/`return` or the next check runs on bad state
- Off-by-one: composite key checks (all columns vs any column)

**Consistency with project conventions**
- Read `AGENTS.md` and `CONTRIBUTING.md` for current conventions
- Black formatting (line length 120), isort (black profile), type hints on new code
- Error messages must be actionable — tell the user what's wrong AND how to fix it
- Imports at module level, not inside functions

**Architecture**
- New validation rules belong in `core/validation/rules/` as methods on existing validator classes
- New CLI options follow Click patterns in `interfaces/cli/commands/`
- Template references (`{{ ref() }}`) are resolved in `core/parsing/`
- SM_* table schemas are defined in `core/models/`

### Step 4: Run tests

Load the `sst-test` skill and follow its workflow to run unit tests against the PR branch.

### Step 5: Run E2E tests (for code changes)

If the PR modifies Python code under `snowflake_semantic_tools/` or `tests/`, load the `sst-e2e-test` skill and follow its workflow. Skip for docs/config-only changes.

### Step 6: Present findings

**⚠️ MANDATORY STOPPING POINT**: Present the full review to the user before submitting anything on GitHub. Get explicit approval on the findings.

## Report Format

Structure the review as:

```
## Process Checks
- [ ] Commit messages match regex
- [ ] Single issue linked with Closes #<number>
- [ ] PR title matches regex

## Findings

### Bugs (must fix)
1. [severity] file:line — description → suggested fix

### Nits (non-blocking)
1. file:line — description → suggestion

## Test Results
- Unit tests: X passed, Y failed
- E2E: PASS/FAIL/SKIPPED

## Verdict
APPROVE / REQUEST CHANGES / COMMENT
```

## Bug Patterns Common in This Codebase

1. **Missing `continue` after error** — validation loops that add an error but don't skip to the next item, causing cascading false errors
2. **Case sensitivity** — comparing column names without `.lower()` causes mismatches between YAML (mixed case) and catalog (lowercase)
3. **Set vs list comparison** — composite key validation must use set equality (order-independent), not list equality
4. **Scope leaks** — variables computed inside an `if` branch referenced outside it (e.g., `right_columns_used` computed inside `if columns:` but used after)
5. **Redundant imports** — imports inside functions that already exist at module level

## Stopping Points

- ✋ Step 4: Before running tests (ask user which conda environment via `sst-test` skill)
- ✋ Step 5: Before E2E tests (confirm with user, involves Snowflake compute)
- ✋ Step 6: Before submitting review on GitHub (present findings for approval)

## Output

Structured review report with process checks, categorized findings (bugs/nits with file:line references), test results, and a clear verdict.
