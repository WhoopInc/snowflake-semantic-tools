# Snowflake Semantic Tools (SST)

SST is a command-line compiler and publisher for a Snowflake semantic layer kept as code in a dbt project: semantic views, agent tools, Cortex Agents, evals, skills, plugins, and CoCo Desktop profiles. It validates offline, saves a reviewable plan, and applies exactly that plan.

**Commands**: `sst init`, `debug`, `validate`, `compile`, `plan`, `apply`, `test --suite golden|smoke|evals`, `list`, `clean`, `docs [--check]`, `migrate refs`. The CLI is the only interface; there is no Python API. Options and exit codes are in `docs/reference/cli.md` (generated).

## Critical Rules

- **NEVER commit or push without asking the user first.** Always confirm before running `git commit` or `git push`.
- **Commit messages MUST match this regex** (enforced by GitHub):
  ```
  ^(\[[A-Z][A-Z0-9]*-\d+\]|[a-z]+\([A-Z][A-Z0-9]*-\d+\):|\([A-Z][A-Z0-9]*-\d+\)|Ver: (\d+\.\d+\.\d+(\.\d+)?|\d+\.\d+\.\d+-\d+)|(?i:initial commit))
  ```
  Preferred format: `[SST-<issue#>] <description>`
- **Each PR addresses exactly one GitHub issue.** The PR body MUST contain `Closes #<number>`.
- **Branch from the line you are changing**: 1.0 work from the 1.0 development branch; a 0.3.x fix from the maintenance branch cut from tag `v0.3.1`, never merged into 1.0 (see `CONTRIBUTING.md`). Name branches `fix/<description>` or `feature/<description>`.
- **Connect to Snowflake only when the user asks**, and then only to a non-production scratch schema. `plan`, `apply`, `test --suite smoke|evals`, `debug --test-connection`, and `validate` with syntax checks connect; everything else, including the whole test suite, is offline.
- **When addressing PR review comments**: reply to the comment explaining the fix, then resolve the thread:
  ```bash
  # Reply to the comment
  gh api repos/WhoopInc/snowflake-semantic-tools/pulls/<PR>/comments/<COMMENT_ID>/replies -f body="Fixed in <commit>"
  # Resolve the thread (GraphQL — need the thread node_id from the comment's node_id)
  gh api graphql -f query='mutation { resolveReviewThread(input: {threadId: "<THREAD_NODE_ID>"}) { thread { isResolved } } }'
  ```

## Layout

`snowflake_semantic_tools/` holds `__init__.py`, `_version.py`, and four rings, nothing else:

| Ring | Holds | Import rules |
|------|-------|--------------|
| `cli/` | click commands; the composition root | may import every ring |
| `app/` | use cases (compile, validate, plan, apply, test suites, migrate refs) | `domain` only, never `adapters`; no `yaml`, `click`, or `snowflake` |
| `adapters/` | YAML, dbt manifest, Snowflake connector, filesystem, config, profile | `domain` models and ports, never `domain.render` or `app` |
| `domain/` | pure: model, render, resolve, plan, state, ports | no I/O, clock, environment, randomness, logging, or SDKs |

`poetry run lint-imports` enforces these as the four contracts in `pyproject.toml`; `tests/unit/test_ring_boundaries.py` proves each one rejects a crossing.

## Gates

What `.github/workflows/test-and-lint.yml` runs; the exact coverage commands are in `CONTRIBUTING.md`.

```bash
poetry run pytest tests/          # plus branch-coverage floors: domain 100%, app 95%, cli 90%
poetry run mypy snowflake_semantic_tools
poetry run black --check snowflake_semantic_tools/
poetry run isort --check snowflake_semantic_tools/
poetry run lint-imports
poetry run sst docs --check
```

## Conventions

- **Package manager**: Poetry (NOT uv). Python 3.11–3.13.
- **Style**: black and isort (black profile), line length 120; mypy requires every function to be annotated.
- **Docstrings and size**: follow "Docstrings and comments" in `CONTRIBUTING.md`; `tests/unit/test_docstrings.py` and `tests/unit/test_structure.py` enforce it against ratchet lists in `tests/unit/ratchets/` that may only shrink.
- **Diagnostics**: every problem is registered in `snowflake_semantic_tools/domain/model/diagnostic.py` with a stable `SST-XXXnnn` code and an actionable suggestion (what is wrong AND how to fix it). Codes are never reused.
- **Config keys** are declared in `snowflake_semantic_tools/domain/model/config_schema.py`.
- **Generated pages**: after changing diagnostics, the config schema, CLI options, or the artifact registry, run `poetry run sst docs` and commit `docs/reference/*.md`.
- **Version**: `snowflake_semantic_tools/_version.py` must equal the version in `pyproject.toml` (a test asserts it).
- **Tests** never connect to Snowflake; fixtures and goldens are described in `tests/README.md`.
- **Hygiene**: no planning identifiers (one capital letter followed by three digits), environment-specific names, or home-directory paths in committed files.

## Skills

Load the appropriate skill for each task. Do not duplicate skill logic in ad-hoc responses.

| Skill | When to Use |
|-------|-------------|
| `sst-test` | Running the test suite and the CI gates, checking coverage floors, verifying fixes |
| `sst-validate` | Running `sst validate` / `sst compile --emit-ddl` against a project, interpreting diagnostics |
| `sst-e2e-test` | QA before a release or PR: the reference project offline (compile, golden suite, codemod); plan/apply against a scratch schema only on request |
| `sst-pr-review` | Reviewing pull requests — process checks, code review, test verification |
| `opening-sst-pr` | Creating a new PR — enforces commit regex, single-issue rule, PR template |
| `sst-create-issue` | Filing GitHub issues — uses repo issue templates for bugs, features, docs |
