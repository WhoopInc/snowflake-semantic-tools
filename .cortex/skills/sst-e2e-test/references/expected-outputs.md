# Expected Outputs — reference project

Baselines for `tests/fixtures/reference_project` under its default `dev` target, compiled from `tests/fixtures/reference_project_manifest.json`. The E2E skill uses these to detect regressions. Update this file when the fixture or the goldens change.

## Artifacts (16)

| Type | Count | Names |
|------|-------|-------|
| semantic_view | 3 | jaffle_menu, jaffle_minimal, jaffle_sales |
| tool | 2 | menu_docs_search, order_status |
| agent | 3 | jaffle_analytics_agent, jaffle_delivery_agent, jaffle_minimal_agent |
| cowork_profile | 1 | jaffle_concierge |
| eval | 1 | jaffle_analytics_agent |
| skill | 3 | jaffle-catalogue, jaffle-operations, jaffle-semantics |
| plugin | 1 | jaffle-toolkit |
| coco_desktop_profile | 2 | jaffle-analyst, jaffle-operator |

After a compile, `sst list --project-dir tests/fixtures/reference_project` lists all 16 as `pending`.

`jaffle_menu` publishes to `SST_REF_DEV.CORE` through a `semantic_views:` folder route, `jaffle_minimal_agent` to `SST_REF_DEV.OPS` through an `agents:` folder route, and `jaffle_concierge` to `SST_REF_DEV.PROFILES`; the other artifacts publish to `SST_REF_DEV.JAFFLE`, the two CoCo Desktop profiles as rows of its `PROFILE_REGISTRY` table.

## Validate (offline)

`sst -o human validate --project-dir $P --manifest $M --no-strict --no-snowflake-syntax-check`:

| Check | Expected |
|-------|----------|
| Exit code | 0 |
| stdout | empty; everything is on stderr |
| Last two stderr lines | `✓ validated 16 artifacts · 0 errors, 7 warnings · <time>`, then `hidden: 55 info (--show-info)` |
| Warnings | `SST-VAL528`, `SST-RND010`, `SST-RND013` twice, and `SST-CFG018` twice (see Known Diagnostics), plus `SST-CFG034` on `sst_config.yml`: `--no-strict` disagrees with `validation.strict: true` |
| Infos (count) | `SST-MEM011` (15), `SST-VAL125` (9), `SST-VAL020` (6), `SST-VAL831` (4), `SST-MEM103` (3), `SST-VAL319` (3), `SST-VAL816` (3), `SST-DBT016` (2), `SST-VAL217` (2), `SST-DBT025`, `SST-VAL216`, `SST-VAL711`, `SST-VAL712`, `SST-VAL725`, `SST-VAL854`, `SST-VAL870`, `SST-VAL878` (1 each) |
| Without `--no-strict` | exit 1, 6 errors, 1 warning, 55 info: the fixture's `validation.strict: true` promotes the six artifact warnings, `SST-CFG034` is not raised, and `SST-CFG037` (strict enforced with no baseline) stays a warning |

`--output json` gives the same counts in `summary` (`"warning": 7, "info": 55`). With `--show-info`, the infos are reported and 33 `SST-DIS200` infos (which type owns each semantic-model file) are added, for 88.

## Compile (`--emit-ddl`)

`sst -o human compile --project-dir $P --manifest $M --emit-ddl <dir>` exits 0 and prints `wrote sst_target/manifest.json` and `wrote 16 files to <dir>` on stdout, then `✓ compiled 16 artifacts · 0 errors, 6 warnings · <time>` on stderr. Each payload goes in a folder named for its type:

| Files | Artifacts |
|-------|-----------|
| `semantic_view/jaffle_menu.sql`, `jaffle_minimal.sql`, `jaffle_sales.sql` | semantic views |
| `tool/menu_docs_search.sql`, `order_status.sql` | tools |
| `agent/jaffle_analytics_agent.json`, `jaffle_delivery_agent.json`, `jaffle_minimal_agent.json` | agents |
| `cowork_profile/jaffle_concierge.json` | CoWork profile |
| `eval/jaffle_analytics_agent.yaml` | eval |
| `skill/jaffle-catalogue.json`, `jaffle-operations.json`, `jaffle-semantics.json` | skills |
| `plugin/jaffle-toolkit.json` | plugin |
| `coco_desktop_profile/jaffle-analyst.json`, `jaffle-operator.json` | CoCo Desktop profiles |

## Goldens

`poetry run pytest tests/unit/test_goldens.py`: `9 passed`, exit 0. With nothing drifted, `poetry run python -m tests.helpers.update_goldens` prints `0 golden file(s) written for 19 artifact(s)` (the 16, the two scoped views, and the CoWork profile's eval) and exits 0. The goldens compared, under `tests/golden/expected/`:

| Directory | Files |
|-----------|-------|
| `ddl/` | `jaffle_menu.sql`, `jaffle_minimal.sql`, `jaffle_sales.sql` |
| `tool/` | `menu_docs_search.sql`, `order_status.sql` |
| `agent/` | one `.json` per agent |
| `cowork_profile/` | `jaffle_concierge.json` |
| `eval/` | `jaffle_analytics_agent_repeat.yaml`, `jaffle_analytics_source.sql`, `jaffle_concierge_repeat.yaml`, `jaffle_concierge_source.sql` |
| `skill/` | one `.bundle.json` per skill |
| `plugin/` | `jaffle-toolkit.bundle.json`, `jaffle-toolkit.plugin.json` |
| `coco_desktop_profile/` | one `.coco_desktop_profile.json` per profile, and the `jaffle-analyst/` tree |

`ddl/` also holds `jaffle_customer_orders.sql` and `jaffle_product_costs.sql`, `enrich/` holds whole YAML files, and `cli/` holds command-output goldens; `unit/test_golden_ddl.py`, `unit/test_golden_enrich.py` and `unit/cli/test_cli_goldens.py` compare those; `update_goldens` rewrites the scoped-view two and not the `enrich` files.

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
- `sst test` ends with `N smoke probe(s) passed`
- A second `sst plan` exits 0

## Known Diagnostics (Expected, Non-Blocking)

| Phase | Code | Reason |
|-------|------|--------|
| Validate | `SST-VAL528` (warning) | Deliberate: `jaffle_delivery_agent` declares an agent toolset |
| Validate | `SST-RND010` (warning) | Deliberate: `jaffle_minimal_agent` renders with no tools |
| Validate | `SST-RND013` (warning, twice) | `jaffle_analytics_agent`'s generic tools `order_tier_lookup` and `order_status` have resources Snowflake does not validate |
| Validate | `SST-CFG018` (warning, twice) | The `jaffle_partner` tool members `payment_settlement_view` and `partner_invoice_stage` are referenced by nothing |
| Validate | `SST-CFG034` (warning) | Only with `--no-strict`: the flag disagrees with `validation.strict: true`, and the flag wins |
| Validate | `SST-CFG037` (warning) | Only without `--no-strict`: `validation.strict: true` is enforced and the fixture has no baseline |
| Validate | `SST-VAL854` (info) | The fixture publishes profiles to its own registry table, not the one CoCo Desktop reads |
| Validate | `SST-VAL020` (info) | Connected rules skipped by `--no-snowflake-syntax-check` |
