# PR Title

```
feat: Simplify CLI commands with profile defaults and defer mechanism
```

---

## Description

This PR simplifies the CLI experience by making `--db` and `--schema` flags optional (defaulting to values from the dbt profile) and adds a comprehensive defer mechanism that allows semantic views to reference tables from a different target (e.g., prod) while deploying to dev.

**Key improvements:**
- **Profile-based defaults**: No more required `--db` and `--schema` flags - values come from your dbt profile
- **Defer mechanism**: Reference production tables while working in dev (`--defer-target prod`)
- **Selective generation**: Only regenerate views affected by changed models (`--only-modified`)
- **Config-based defaults**: Set defer preferences in `sst_config.yml`
- **dbt Cloud CLI support**: Explicit error messages and guidance for dbt Cloud users

## Related Issue

Builds on PR #66 (dbt profiles.yml authentication)

## Type of Change

- [x] New feature (non-breaking change which adds functionality)
- [x] Documentation update

## Changes Made

### New CLI Options

| Command | New Options |
|---------|-------------|
| `extract` | `--target`, `--db` (optional), `--schema` (optional) |
| `generate` | `--defer-target`, `--state`, `--only-modified`, `--no-defer` |
| `deploy` | `--defer-target`, `--state`, `--only-modified`, `--no-defer` |

### New Files
- `snowflake_semantic_tools/interfaces/cli/options.py` - Shared CLI option decorators
- `snowflake_semantic_tools/interfaces/cli/defer.py` - Centralized defer logic
- `tests/unit/interfaces/test_cli_options.py` - Option decorator tests
- `tests/unit/interfaces/test_cli_defer.py` - Defer logic tests
- `tests/unit/core/test_manifest_diff.py` - Manifest comparison tests

### Modified Files
- `snowflake_semantic_tools/interfaces/cli/commands/extract.py` - Profile-based defaults
- `snowflake_semantic_tools/interfaces/cli/commands/generate.py` - Defer support
- `snowflake_semantic_tools/interfaces/cli/commands/deploy.py` - Defer support
- `snowflake_semantic_tools/core/parsing/parsers/manifest_parser.py` - Manifest comparison
- `snowflake_semantic_tools/services/deploy.py` - Defer summary in output
- `snowflake_semantic_tools/shared/config.py` - Defer config schema
- `docs/cli-reference.md` - Updated documentation
- `sst_config.yml.example` - Defer configuration examples

## Testing

- [x] Unit tests pass (`pytest tests/unit/`)
- [x] All existing tests pass
- [x] New tests added for new functionality
- [x] Manual testing completed (if applicable)

### Test Results

```
============== 814 passed, 8 skipped, 25 subtests passed in 1.57s ==============
All done! ✨ 🍰 ✨
157 files would be left unchanged.
```

### Integration Testing (sst-jaffle-shop)

| Test | Result |
|------|--------|
| `sst debug` | ✅ Shows DEV.SST_JAFFLE_SHOP |
| `sst debug --target prod` | ✅ Shows PROD.SST_JAFFLE_SHOP |
| `sst extract` (no flags) | ✅ Uses profile defaults |
| `sst generate --all --no-defer` | ✅ References DEV tables |
| `sst generate --all --defer-target prod` | ✅ References PROD tables |
| `sst generate --all --defer-target prod --only-modified` | ✅ Filters to 2/4 views |
| `sst deploy --defer-target prod` | ✅ Full workflow with defer |
| `sst deploy --defer-target prod --only-modified` | ✅ Selective deployment |

## Checklist

### Code Quality
- [x] Code follows the project's style guidelines (Black, line length 120)
- [x] Imports sorted with isort (black profile)
- [x] Docstrings added for public functions/classes
- [x] No linting errors

### Testing & Validation
- [x] All tests pass (`pytest tests/unit/`)
- [x] New functionality has test coverage
- [x] Test results included above

### Documentation & Compatibility
- [x] Documentation updated (if needed)
- [x] Backward compatibility maintained (if applicable)

### Performance
- [x] Performance impact considered
- [x] No significant performance regressions

## Screenshots / Examples

### Example: Defer to Production

```bash
# Deploy to dev but reference prod tables
$ sst deploy --defer-target prod

DEPLOYING SEMANTIC VIEWS TO SNOWFLAKE
=====================================
Target: DEV.SST_JAFFLE_SHOP
Profile: sst_jaffle_shop.dev
Defer mode enabled (source: cli)
    Defer target: prod
    Using manifest: /path/to/target/manifest.json

...

================================================================================
DEPLOYMENT SUMMARY
================================================================================
Status: SUCCESS

Defer Configuration:
  Target: prod
  Manifest: /path/to/target/manifest.json

Workflow Steps:
  Validation: PASSED
  Extraction: COMPLETED
  Generation: COMPLETED
    Semantic views: 4 created
================================================================================
```

### Example: Selective Generation

```bash
# Only regenerate views affected by changed models
$ sst generate --all --defer-target prod --only-modified

Detected 1 changed model(s): customers
Will regenerate 2 view(s) referencing changed models
Filtering to 2 view(s): customer_analytics, sales_overview
Generating 2 semantic view(s)
...
Generated 2 of 2 view(s)
```

## Additional Notes

This PR is stacked on #66 (dbt profiles.yml authentication). Once #66 is merged, this branch should be rebased from main.

### Config Example

```yaml
# sst_config.yml
defer:
  target: prod                    # Default defer target
  state_path: null                # For dbt Cloud CLI: path to downloaded artifacts
  auto_compile: false             # For dbt Core: auto-compile if manifest not found
```

