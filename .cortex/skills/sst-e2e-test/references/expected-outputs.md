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

`jaffle_menu` publishes to `SST_REF_DEV.CORE` through a `semantic_views:` folder route, and `jaffle_minimal_agent` to `SST_REF_DEV.OPS` through an `agents:` folder route; the other artifacts publish to `SST_REF_DEV.JAFFLE`, the two profiles as rows of its `PROFILE_REGISTRY` table.

## Validate (offline)

`sst validate --project-dir $P --manifest $M --no-strict --no-snowflake-syntax-check`:

| Check | Expected |
|-------|----------|
| Exit code | 0 |
| stdout | `validated 14 artifact(s): 0 errors, 6 warnings` |
| Last stderr line | `0 errors; 6 warnings; 51 info; 51 info not shown (--show-info)` |
| Warnings | `SST-VAL528`, `SST-RND010`, `SST-RND013`, and `SST-CFG018` twice (see Known Diagnostics), plus `SST-CFG034` on `sst_config.yml`: `--no-strict` disagrees with `validation.strict: true`. Both count lines include it |
| Infos (count) | `SST-MEM011` (15), `SST-VAL125` (9), `SST-VAL020` (4), `SST-VAL831` (4), `SST-MEM103` (3), `SST-VAL319` (3), `SST-VAL816` (3), `SST-DBT016` (2), `SST-VAL217` (2), `SST-DBT025`, `SST-VAL216`, `SST-VAL711`, `SST-VAL712`, `SST-VAL725`, `SST-VAL854` (1 each) |
| Without `--no-strict` | exit 1, `5 errors; 0 warnings; 51 info`: the fixture's `validation.strict: true` promotes the five artifact warnings, and `SST-CFG034` is not raised |

`--output json` gives the same counts in `summary` (`"warning": 6, "info": 51`). With `--show-info`, the infos are printed and seven `SST-DIS200` infos (which type owns each semantic-model file) are added, for 58.

## Compile (`--emit-ddl`)

`compiled 14 artifact(s); manifest tests/fixtures/reference_project/sst_target/manifest.json`, then `wrote 14 file(s) to <dir>`, exit 0:

| Files | Artifacts |
|-------|-----------|
| `jaffle_menu.sql`, `jaffle_minimal.sql`, `jaffle_sales.sql` | semantic views |
| `menu_docs_search.sql` | tool |
| `jaffle_analytics_agent.json`, `jaffle_delivery_agent.json`, `jaffle_minimal_agent.json` | agents |
| `jaffle_analytics_agent.yaml` | eval |
| `jaffle-catalogue.json`, `jaffle-operations.json`, `jaffle-semantics.json` | skills |
| `jaffle-toolkit.json` | plugin |
| `jaffle-analyst.json`, `jaffle-operator.json` | profiles |

## Goldens

`poetry run pytest tests/unit/test_goldens.py`: `9 passed`, exit 0. With nothing drifted,
`poetry run python -m tests.helpers.update_goldens` prints `0 golden file(s) written for 16 artifact(s)` (the 14, and the two scoped views) and exits 0. The goldens compared, under `tests/golden/expected/`:

| Directory | Files |
|-----------|-------|
| `ddl/` | `jaffle_menu.sql`, `jaffle_minimal.sql`, `jaffle_sales.sql` |
| `tool/` | `menu_docs_search.sql` |
| `agent/` | one `.json` per agent |
| `eval/` | `jaffle_analytics_agent_repeat.yaml`, `jaffle_analytics_source.sql` |
| `skill/` | one `.bundle.json` per skill |
| `plugin/` | `jaffle-toolkit.bundle.json`, `jaffle-toolkit.plugin.json` |
| `profile/` | one `.profile.json` per profile, and the `jaffle-analyst/` tree |

`ddl/` also holds `jaffle_customer_orders.sql` and `jaffle_product_costs.sql`, and `enrich/` holds whole YAML files; `unit/test_golden_ddl.py` and `unit/test_golden_enrich.py` compare those (`tests/fixtures/scoped_views/`, `sst enrich`); `update_goldens` rewrites the scoped-view two and not the `enrich` files.

## `sst migrate refs` (`tests/fixtures/v1_dialect`)

| Run | Expected |
|-----|----------|
| Dry run | exit 2; `would rewrite semantic_models/...` for `filters/filters.yml`, `metrics/metrics.yml`, `relationships/relationships.yml`, `semantic_views/views.yml` |
| `--write`, on a copy | exit 0; `rewrote` for the same four; the copy's `semantic_models/` equals `expected/converted/semantic_models/` |
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
| Validate | `SST-VAL528` (warning) | Deliberate: `jaffle_delivery_agent` declares an agent toolset |
| Validate | `SST-RND010` (warning) | Deliberate: `jaffle_minimal_agent` renders with no tools |
| Validate | `SST-RND013` (warning) | `jaffle_analytics_agent`'s generic tool `order_tier_lookup` has resources Snowflake does not validate |
| Validate | `SST-CFG018` (warning, twice) | The `jaffle_partner` tool members `payment_settlement_view` and `partner_invoice_stage` are referenced by nothing |
| Validate | `SST-CFG034` (warning) | Only with `--no-strict`: the flag disagrees with `validation.strict: true`, and the flag wins |
| Validate | `SST-VAL854` (info) | The fixture publishes profiles to its own registry table, not the one CoCo Desktop reads |
| Validate | `SST-VAL020` (info) | Connected rules skipped by `--no-snowflake-syntax-check` |
