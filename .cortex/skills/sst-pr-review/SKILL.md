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

**Base branch** — 1.0 work targets the 1.0 development branch. A 0.3.x fix targets the maintenance branch cut from `v0.3.1`, and 0.3.x changes are never merged into 1.0.

### Step 3: Code review

Read the diff carefully. Focus on:

**Correctness**
- Edge cases: empty or missing inputs, and identifier case and quoting
- Determinism: rendering and the manifest must be byte-stable. No clock, environment, or randomness in `domain/`, and no dependence on unordered iteration
- Selection: `--select`, `--exclude`, and `--partial` must never hide an artifact's dependencies
- Writes: `plan` never writes to Snowflake; `apply` executes only what the saved plan lists and changes only objects SST published

**Consistency with project conventions**
- Read `AGENTS.md` and `CONTRIBUTING.md` for current conventions
- ruff-formatted and lint-clean (line length 120), and strictly typed for mypy
- Each new problem is a new diagnostic in its code family's module under `domain/model/diagnostic/specs/`: a new `SST-` code (never a reused one) with an actionable suggestion
- A new `sst_config.yml` key is declared in `domain/model/config_schema/keys.py`
- A change to diagnostics, config keys, CLI options, or artifact types ships the regenerated `docs/reference/*.md` (`sst docs`)
- `--output json` prints exactly one envelope on stdout; exit codes match `docs/reference/cli.md`

**Architecture**
- Rings: `cli` → (`app` | `adapters`) → `domain`, with `app` and `adapters` independent. `lint-imports` catches crossing imports; also look for I/O or SDK use hidden behind a helper in `domain/` or `app/`
- The package root gains nothing beside `__init__.py`, `_version.py`, and the four rings, and `__init__.py` exports nothing but `__version__`: there is no Python API
- Where code belongs: commands and options in `cli/`; use cases in `app/`; YAML, dbt, Snowflake, and filesystem access in `adapters/`, behind a port in `domain/ports/`; rules, rendering, reference resolution, plan diffing, and state in `domain/`
- Tests use real in-memory ports (`tests/helpers/recorded_snowflake.py`, `tests/helpers/app_ports.py`), not mocks
- A golden change is intended and explained in the PR; otherwise it is a regression

### Step 4: Run tests

Load the `sst-test` skill and follow its workflow to run the suite and the gates against the PR branch.

### Step 5: Run E2E tests (for code changes)

If the PR modifies Python code under `snowflake_semantic_tools/` or `tests/`, load the `sst-e2e-test` skill and run its offline phases. Its connected phase runs only if the user asks. Skip for docs/config-only changes.

### Step 6: Present findings

**⚠️ MANDATORY STOPPING POINT**: Present the full review to the user before submitting anything on GitHub. Get explicit approval on the findings.

## Report Format

Structure the review as:

```
## Process Checks
- [ ] Commit messages match regex
- [ ] Single issue linked with Closes #<number>
- [ ] PR title matches regex
- [ ] Base branch matches the line being changed

## Findings

### Bugs (must fix)
1. [severity] file:line — description → suggested fix

### Nits (non-blocking)
1. file:line — description → suggestion

## Test Results
- Suite: X passed, Y failed
- Gates: coverage floors, mypy, ruff format, ruff check, lint-imports, sst docs --check
- E2E: PASS/FAIL/SKIPPED

## Verdict
APPROVE / REQUEST CHANGES / COMMENT
```

## What to Look For in 1.0 Changes

1. **Vacuous tests** — a check that passes because a path is wrong or a collection is empty. Guard what the test depends on, as `test_fixture_and_goldens_are_present` does
2. **Nondeterministic output** — ordering, timestamps, or environment leaking into rendered payloads changes goldens and manifest ids
3. **Stdout noise under `--output json`** — anything printed besides the envelope breaks machine consumers
4. **Diagnostic drift** — a reused or renumbered code, a new error without a suggestion, or registry changes without regenerated reference pages
5. **Writes beyond the plan** — anything in `apply` that acts on objects the saved plan does not list, or grants access SST did not already find in place

## Stopping Points

- ✋ Step 4: Before running tests (ask the user which environment to use, via the `sst-test` skill)
- ✋ Step 5: Before any connected E2E phase (it runs only on request and uses Snowflake compute)
- ✋ Step 6: Before submitting review on GitHub (present findings for approval)

## Output

Structured review report with process checks, categorized findings (bugs/nits with file:line references), test results, and a clear verdict.
