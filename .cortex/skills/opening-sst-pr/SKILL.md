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
   - `Ver: 0.2.5` (version bumps only)
3. **Tests must pass** before the PR is ready for review.

## Workflow

### Step 1: Identify the GitHub issue

Every PR must link to a GitHub issue. If one doesn't exist yet, load the `sst-create-issue` skill and follow its workflow to create one first. Note the issue number (e.g., `119`).

### Step 2: Create a feature branch

```bash
git checkout -b <branch-name> main
```
Branch naming: `fix/<description>` or `feature/<description>`

### Step 3: Make changes, run tests

```bash
python -m pytest tests/unit/ -v
black snowflake_semantic_tools/
isort snowflake_semantic_tools/
mypy snowflake_semantic_tools/
```

### Step 3b: Run E2E tests (for code changes)

If the PR modifies Python code under `snowflake_semantic_tools/` or `tests/`, run end-to-end validation to verify the full pipeline still works. Load the `sst-e2e-test` skill and follow its workflow.

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

**⚠️ MANDATORY STOPPING POINT**: Confirm with the user before creating the PR. Show them the title and body that will be used. Do NOT create without explicit approval.

Read the PR template at `.github/pull_request_template.md` and use it as the body. Fill in the sections based on the changes made. Then create the PR:

```bash
gh pr create --repo WhoopInc/snowflake-semantic-tools \
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

## Stopping Points

- ✋ Step 4: Before committing and pushing (get user approval)
- ✋ Step 5: Before creating the PR (confirm title and body with user)

## Output

A GitHub pull request with proper ticket reference, linked issue, and passing tests.
