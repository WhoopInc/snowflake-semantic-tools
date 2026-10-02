---
name: opening-sst-pr
description: "Create a pull request on the SST repo. Use when: opening a PR, creating a pull request, submitting changes. Triggers: open PR, create PR, pull request, submit PR, push and create PR."
---

# Opening an SST Pull Request

Create a pull request on WhoopInc/snowflake-semantic-tools following project conventions.

## Rules

1. **Every PR must address exactly one GitHub issue.** Do not bundle multiple fixes or features.
2. **Commit messages MUST match this regex** (enforced by GitHub):
   ```
   ^(\[[A-Z][A-Z0-9]*-\d+\]|[a-z]+\([A-Z][A-Z0-9]*-\d+\):|\([A-Z][A-Z0-9]*-\d+\)|Ver: (\d+\.\d+\.\d+(\.\d+)?|\d+\.\d+\.\d+-\d+)|(?i:initial commit))
   ```
   Valid formats:
   - `[SST-119] Fix unique key error` (bracket prefix — **preferred**)
   - `fix(SST-119): unique key error` (conventional commit)
   - `(SST-119) Fix unique key error` (paren prefix)
   - `Ver: 1.0.0` (version bumps only)
3. **Target the line you are changing.** 1.0 work targets the 1.0 development branch. A 0.3.x fix targets the maintenance branch cut from tag `v0.3.1` and is never merged into 1.0 (`CONTRIBUTING.md`, Maintaining 0.3.x).
4. **The gates must pass** before the PR is ready for review.

## Workflow

### Step 1: Identify the GitHub issue

Every PR must link to a GitHub issue. If one doesn't exist yet, load the `sst-create-issue` skill and follow its workflow to create one first. Note the issue number (e.g., `119`).

### Step 2: Create a feature branch

Ask the user which line the change is for if it is not clear, then branch from it:
```bash
git checkout -b <branch-name> <base-branch>
```
Branch naming: `fix/<description>` or `feature/<description>`

### Step 3: Make changes, run the gates

Load the `sst-test` skill and run its workflow. It covers the suite, the coverage floors, mypy, ruff, `lint-imports`, and `sst docs --check`. To format before committing:
```bash
poetry run ruff format snowflake_semantic_tools/ tests/
poetry run ruff check --fix snowflake_semantic_tools/ tests/
```

If the change touches diagnostics, `sst_config.yml` keys, CLI options, or artifact types, run `poetry run sst docs` and include the regenerated `docs/reference/*.md`.

### Step 3b: Run E2E tests (for code changes)

If the PR modifies Python code under `snowflake_semantic_tools/` or `tests/`, load the `sst-e2e-test` skill and run its offline phases. Its connected phase runs only if the user asks.

Skip this step for documentation-only or config-only changes.

### Step 4: Commit and push

**⚠️ MANDATORY STOPPING POINT**: Confirm with the user before committing and pushing. Show them what will be committed (`git status` and `git diff` for modified files). Do NOT commit without explicit approval.

The commit message MUST start with a ticket reference. Stage only the files the user approved — do NOT use `git add .` blindly:
```bash
git add <specific files...>
git commit -m "[SST-<issue#>] <descriptive message>"
git push -u origin <branch-name>
```

### Step 5: Create the PR

**⚠️ MANDATORY STOPPING POINT**: Confirm with the user before creating the PR. Show them the title, base branch, and body that will be used. Do NOT create without explicit approval.

Read the PR template at `.github/pull_request_template.md` and use it as the body. Fill in the sections based on the changes made. Then create the PR:

```bash
gh pr create --repo WhoopInc/snowflake-semantic-tools \
  --base <base-branch> \
  --title "[SST-<issue#>] <description>" \
  --body "<filled-in template>"
```

**CRITICAL**: The Related Issue section MUST contain `Closes #<issue-number>`. This auto-closes the linked issue when the PR merges. Without it, issues accumulate as stale open tickets.

### Step 6: Verify the PR title matches the commit regex

After creation, confirm the title starts with a valid ticket reference:
```bash
gh pr view <pr-number> --repo WhoopInc/snowflake-semantic-tools --json title --jq '.title'
```
If it doesn't match, update it:
```bash
gh pr edit <pr-number> --repo WhoopInc/snowflake-semantic-tools --title "[SST-<issue#>] <description>"
```

## Common Mistakes

- **Missing ticket number**: Commit/merge will be rejected by GitHub. Always include a ticket prefix.
- **Lowercase project key**: `[sst-119]` fails — must be uppercase `[SST-119]`.
- **Multiple issues in one PR**: Split into separate PRs, one per issue.
- **Forgetting to link the issue**: Use `Closes #<number>` in the PR body so it auto-closes on merge.
- **Wrong base branch**: a 0.3.x fix opened against 1.0, or the reverse.
- **Stale reference pages**: a registry change without `sst docs` fails CI's `sst docs --check`.

## Stopping Points

- ✋ Step 4: Before committing and pushing (get user approval)
- ✋ Step 5: Before creating the PR (confirm title, base branch, and body with user)

## Output

A GitHub pull request with proper ticket reference, linked issue, correct base branch, and passing gates.
