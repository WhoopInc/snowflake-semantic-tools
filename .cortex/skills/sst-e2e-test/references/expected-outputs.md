# Expected Outputs — reference project

Baselines for `tests/fixtures/reference_project` under its default `dev` target, compiled from `tests/fixtures/reference_project_manifest.json`. The E2E skill uses these to detect regressions. Update this file when the fixture or the goldens change.

## Artifacts (14)

| Type | Count | Names |
|------|-------|-------|
| semantic_view | 3 | jaffle_menu, jaffle_minimal, jaffle_sales |
| tool | 1 | menu_docs_search |
| agent | 3 | jaffle_analytics_agent, jaffle_delivery_agent, jaffle_minimal_agent |
| eval | 1 | jaffle_analytics_agent |
| skill | 3 | jaffle-catalogue, jaffle-operations, jaffle-semantics |
| plugin | 1 | jaffle-toolkit |
| profile | 2 | jaffle-analyst, jaffle-operator |

After a compile, `sst list --project-dir tests/fixtures/reference_project` lists all 14 as `pending`.

`jaffle_menu` publishes to `SST_REF_DEV.CORE` through a folder route; the other artifacts publish to `SST_REF_DEV.JAFFLE`.

## Validate (offline)

| Check | Expected |
|-------|----------|
| Exit code with `--no-strict` | 0 |
| Last line | `validated 14 artifact(s): 0 errors, 1 warnings` |
| Warning | `SST-VAL528` on `jaffle_delivery_agent`: its agent tool is deliberate |
| Infos | `SST-VAL854`, `SST-VAL711`, `SST-VAL712`, `SST-VAL725`, `SST-VAL020` |
| Exit code without `--no-strict` | 1: the fixture's `validation.strict: true` promotes `SST-VAL528` |

## Compile (`--emit-ddl`)

`wrote 14 artifact payload file(s) to <dir>`, exit 0:

| Files | Artifacts |
|-------|-----------|
| `jaffle_menu.sql`, `jaffle_minimal.sql`, `jaffle_sales.sql` | semantic views |
| `menu_docs_search.sql` | tool |
| `jaffle_analytics_agent.json`, `jaffle_delivery_agent.json`, `jaffle_minimal_agent.json` | agents |
| `jaffle_analytics_agent.yaml` | eval |
| `jaffle-catalogue.json`, `jaffle-operations.json`, `jaffle-semantics.json` | skills |
| `jaffle-toolkit.json` | plugin |
| `jaffle-analyst.json`, `jaffle-operator.json` | profiles |

## Golden Suite

`golden suite passed for 14 artifact(s)`, exit 0. The goldens compared, under `tests/golden/expected/`:

| Directory | Files |
|-----------|-------|
| `ddl/` | `jaffle_menu.sql`, `jaffle_minimal.sql`, `jaffle_sales.sql` |
| `tool/` | `menu_docs_search.sql` |
| `agent/` | one `.json` per agent |
| `eval/` | `jaffle_analytics_agent_repeat.yaml`, `jaffle_analytics_source.sql` |
| `skill/` | one `.bundle.json` per skill |
| `plugin/` | `jaffle-toolkit.bundle.json`, `jaffle-toolkit.plugin.json` |
| `profile/` | one `.profile.json` per profile, and the `jaffle-analyst/` tree |

## `sst migrate refs` (`tests/fixtures/v1_dialect`)

| Run | Expected |
|-----|----------|
| Dry run | exit 2; `would rewrite` for `filters.yml`, `metrics.yml`, `relationships.yml`, `views.yml` |
| `--write`, on a copy | exit 0; the copy's `semantic_models/` equals `expected/converted/semantic_models/` |
| Re-run on the copy | `no legacy references found`, exit 0 |

## Test Suite

Every gate passes. There is no test-count baseline; the suite grows with each change.

## Connected Phase

No fixed baseline: it depends on the project the user names. Pass criteria:

- `sst plan` exits 2 before the first apply, or 0 when the scratch schema already matches
- `sst apply` exits 0
- `sst test --suite smoke` ends with `smoke suite passed: N probe(s)`
- A second `sst plan` exits 0

## Known Diagnostics (Expected, Non-Blocking)

| Phase | Code | Reason |
|-------|------|--------|
| Validate | `SST-VAL528` (warning) | Deliberate: `jaffle_delivery_agent` declares an agent tool |
| Validate | `SST-VAL854` (info) | The fixture publishes profiles to its own registry table, not the one CoCo Desktop reads |
| Validate | `SST-VAL020` (info) | Connected validation skipped by `--no-snowflake-syntax-check` |
