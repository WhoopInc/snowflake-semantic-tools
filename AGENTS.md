# Snowflake Semantic Tools (SST)

SST is a CLI tool that transforms dbt models into Snowflake Semantic Views for Cortex Analyst, Cortex Agents, and BI tools.

**Key commands**: `sst validate`, `sst extract`, `sst deploy`, `sst enrich`, `sst generate`

## Critical Rules

- **NEVER commit or push without asking the user first.** Always confirm before running `git commit` or `git push`.
- **Commit messages MUST match this regex** (enforced by GitHub):
  ```
  ^(\[[A-Z][A-Z0-9]*-\d+\]|[a-z]+\([A-Z][A-Z0-9]*-\d+\):|\([A-Z][A-Z0-9]*-\d+\)|Ver: (\d+\.\d+\.\d+(\.\d+)?|\d+\.\d+\.\d+-\d+)|(?i:initial commit))
  ```
  Preferred format: `[SST-<issue#>] <description>`
- **Each PR addresses exactly one GitHub issue.** The PR body MUST contain `Closes #<number>`.
- **Feature branches off `main`**: `fix/<description>` or `feature/<description>`
- **When addressing PR review comments**: reply to the comment explaining the fix, then resolve the thread:
  ```bash
  # Reply to the comment
  gh api repos/WhoopInc/snowflake-semantic-tools/pulls/<PR>/comments/<COMMENT_ID>/replies -f body="Fixed in <commit>"
  # Resolve the thread (GraphQL — need the thread node_id from the comment's node_id)
  gh api graphql -f query='mutation { resolveReviewThread(input: {threadId: "<THREAD_NODE_ID>"}) { thread { isResolved } } }'
  ```

## Code Style

- **Formatter**: `black snowflake_semantic_tools/` (line length 120)
- **Import sorting**: `isort snowflake_semantic_tools/` (black profile)
- **Type checking**: `mypy snowflake_semantic_tools/`
- **Tests**: `python -m pytest tests/unit/ -v`
- **Package manager**: Poetry (NOT uv)
- **Error messages**: Must be actionable — tell the user what's wrong AND how to fix it

## Skills

Load the appropriate skill for each task. Do not duplicate skill logic in ad-hoc responses.

| Skill | When to Use |
|-------|-------------|
| `sst-test` | Running unit tests, checking coverage, verifying fixes |
| `sst-validate` | Running `sst validate` against a dbt project, interpreting results |
| `sst-e2e-test` | End-to-end pipeline testing against sst-jaffle-shop (QA before release) |
| `sst-pr-review` | Reviewing pull requests — process checks, code review, test verification |
| `opening-sst-pr` | Creating a new PR — enforces commit regex, single-issue rule, PR template |
| `sst-create-issue` | Filing GitHub issues — uses repo issue templates for bugs, features, docs |
