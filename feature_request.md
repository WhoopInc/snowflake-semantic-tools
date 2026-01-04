# Feature Request: Simplified `sst enrich` Model Selection

**Title:** `[Feature]: Simplify sst enrich with --models and --directory options`

---

## Problem Statement

Currently, `sst enrich` requires a positional `TARGET_PATH` argument that must be a file path or directory path:

```bash
sst enrich models/marts/customers.sql
sst enrich models/analytics/memberships/
```

This has several friction points:

1. **Path typing is tedious** - Users must type full paths to models, which can be long for nested directories
2. **No model-name selection** - Can't simply specify model names like `customers` or `orders`
3. **Inconsistent with dbt patterns** - dbt allows `dbt run --select customers` without paths
4. **Multi-model selection is awkward** - To enrich 3 specific models, you need 3 separate commands or a common parent directory

Since dbt requires model names to be unique within a project (enforced at compile time), we can leverage this to allow model-name-based selection.

## Proposed Solution

Add two new options to `sst enrich`:

### Option 1: `--models` / `-m` (comma-separated list of model names)
```bash
# Enrich specific models by name
sst enrich --models customers,orders,products

# Short form
sst enrich -m customers,orders
```

### Option 2: `--directory` / `-d` (directory path)
```bash
# Enrich all models in a directory
sst enrich --directory models/marts/

# Short form
sst enrich -d models/staging/
```

### Backward Compatibility

Keep the existing positional `TARGET_PATH` for backward compatibility:
```bash
# Still works (existing behavior)
sst enrich models/marts/customers.sql
sst enrich models/analytics/
```

### Validation Rules

1. Only ONE selection method allowed per invocation:
   - `--models` OR `--directory` OR positional `TARGET_PATH`
   - Error if multiple specified

2. Model name resolution uses `manifest.json`:
   - Model names are looked up via `ManifestParser.model_locations`
   - If model not found, emit helpful error with suggestions

3. Require manifest for `--models`:
   - Since model names are resolved from manifest, it must exist
   - Error with helpful message if missing

## Alternatives Considered

1. **Keep current behavior** - Users continue specifying paths manually
   - Downside: More friction, inconsistent with dbt CLI patterns

2. **Glob patterns** - Allow `sst enrich models/**/customers.*`
   - Downside: Complex, platform-dependent, less intuitive than model names

3. **Interactive mode** - Show model picker
   - Downside: Not scriptable, adds UI complexity

4. **Config file model lists** - Define model groups in `sst_config.yml`
   - Could be a future enhancement, but `--models` solves the immediate need

## Priority

**High - Would significantly improve workflow**

The current path-based selection adds friction to every enrichment operation. Model-name selection would make the CLI feel more natural and align with dbt conventions.

## Impact

- **All SST users** - Everyone running enrichment benefits from simpler model selection
- **CI/CD pipelines** - Easier to script enrichment for specific models
- **Large projects** - Users with deeply nested model directories benefit most
- **dbt users** - Familiar pattern from `dbt run --select`

## Technical Considerations

### Implementation Approach

1. **Add new CLI options** in `enrich.py`:
```python
@click.option("--models", "-m", help="Comma-separated list of model names to enrich")
@click.option("--directory", "-d", type=click.Path(exists=True), help="Directory containing models to enrich")
```

2. **Model resolution** leverages existing `ManifestParser`:
```python
# ManifestParser already has this capability
parser = ManifestParser()
parser.load()

# Get location for model name
location = parser.get_location("customers")
# Returns: {'database': 'PROD', 'schema': 'MARTS', 'original_file_path': 'models/marts/customers.sql', ...}
```

3. **Validate mutual exclusivity**:
```python
if sum([bool(models), bool(directory), bool(target_path)]) > 1:
    raise click.UsageError("Specify only one of: --models, --directory, or TARGET_PATH")
```

4. **Resolve model names to paths**:
```python
def resolve_model_names(model_names: List[str], manifest_parser: ManifestParser) -> List[Path]:
    paths = []
    for name in model_names:
        location = manifest_parser.get_location(name)
        if not location:
            raise click.UsageError(f"Model '{name}' not found in manifest. Run 'dbt compile' first.")
        paths.append(Path(location["original_file_path"]))
    return paths
```

### Existing Infrastructure

The codebase already has the building blocks:
- `ManifestParser.model_locations` - Maps model names to file paths
- `ManifestParser.get_location()` - Retrieves model metadata by name
- `ManifestParser.get_all_models_in_directory()` - Gets models under a directory

## Example Usage

```bash
# Enrich single model by name
sst enrich --models customers

# Enrich multiple models by name
sst enrich --models customers,orders,products --target prod

# Enrich with synonyms for specific models
sst enrich -m customers,orders --synonyms

# Enrich entire directory
sst enrich --directory models/marts/

# Dry run with model names
sst enrich -m customers --dry-run --verbose

# Error cases
sst enrich --models nonexistent
# Error: Model 'nonexistent' not found in manifest. Available models: customers, orders, ...

sst enrich --models customers --directory models/
# Error: Specify only one of: --models, --directory, or TARGET_PATH
```

## Additional Context

This enhancement aligns with the CLI simplification work in PR #67 (profile defaults and defer mechanism) - making SST commands more intuitive and reducing boilerplate.

The manifest-based approach is also consistent with how `sst extract` and `sst validate` already work - they auto-detect from manifest without requiring explicit paths.

---

## Pre-submission Checklist

- [x] I have searched existing issues to avoid duplicates
- [x] I have described a clear problem and solution
- [x] I have considered alternatives and workarounds

