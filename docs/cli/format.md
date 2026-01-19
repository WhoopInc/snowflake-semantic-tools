# sst format

YAML linter for project-wide formatting consistency.

---

## Overview

The `format` command standardizes YAML file structure and formatting across your dbt project. It acts as a linter to ensure all YAML files follow consistent formatting standards, making files easier to read, maintain, and review in pull requests.

**Snowflake Connection:** Not required

---

## Quick Start

```bash
# Format all files in a directory
sst format models/

# Preview changes without modifying
sst format models/ --dry-run

# Check if formatting is needed (for CI)
sst format models/ --check
```

---

## Syntax

```bash
sst format PATH [OPTIONS]
```

**PATH** supports wildcard patterns (`*` and `?`) to match multiple files:
- `"models/analytics/shared_prefix_*"` - matches all files starting with `shared_prefix_`
- `"models/analytics/_intermediate/*"` - matches all files in `_intermediate/` subdirectory
- **Important:** Use quotes around wildcard patterns to prevent shell expansion

---

## Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `PATH` | PATH | Required | File, directory, or wildcard pattern to format |
| `--dry-run` | FLAG | False | Preview changes without modifying files |
| `--check` | FLAG | False | Check if files need formatting (exit code 1 if changes needed) |
| `--force` | FLAG | False | Always write files, even if content appears unchanged |
| `--sanitize` | FLAG | False | Sanitize problematic characters in synonyms, sample values, descriptions |

---

## What It Does

### Field Ordering

- **Models:** `name` → `description` → `meta` → `config` → `columns`
- **Columns:** `name` → `description` → `data_tests` → `meta`
- **SST metadata:** `column_type` → `data_type` → `synonyms` → `sample_values` → `is_enum` → `privacy_category`

### Blank Line Management

- Adds blank line before each new column definition
- Removes excessive consecutive blank lines
- Ensures file ends with exactly one newline

### Indentation

- 2-space indentation for maps/objects
- 4-space indentation for sequences/lists
- Consistent dash offset for list items

### Multi-line Descriptions

- Converts `>` (folded) to `|-` (literal with strip)
- Wraps at 80 characters without breaking words
- Preserves line breaks for readability

### List Formatting

- Empty lists → `[]` (inline format)
- Lists with items → Multi-line format (one item per line)
- Applies to: `synonyms`, `sample_values`, `primary_key`, `unique_keys`

### Character Sanitization (with `--sanitize`)

- **Synonyms:** Removes apostrophes, quotes (SQL-safe for `WITH SYNONYMS` clause)
- **Sample values:** Removes apostrophes, quotes (prevents downstream issues)
- **Descriptions:** Escapes Jinja characters (`{{` → `{ {`), removes control chars
- **Preserves:** Semicolons (useful data delimiters), meaningful content

---

## Examples

### Basic Usage

```bash
# Format a single file
sst format models/users/users.yml

# Format all files in a directory
sst format models/

# Format multiple files matching a pattern (wildcard support)
sst format "models/users/shared_prefix_*"
sst format "models/users/_intermediate/*"

# Format semantic models
sst format snowflake_semantic_models/

# Format entire project
sst format .
```

### Preview and Check

```bash
# Preview changes without modifying files
sst format models/ --dry-run

# Check if formatting is needed (useful for CI)
sst format models/ --check
# Exit code 0: All files properly formatted
# Exit code 1: Some files need formatting
```

### Sanitization

```bash
# Sanitize problematic characters
sst format models/ --sanitize
# Removes apostrophes from synonyms/sample values
# Escapes Jinja characters in descriptions
# Fixes validation warnings about "problematic characters"

# Preview sanitization changes
sst format models/ --sanitize --dry-run
```

---

## Output

```
18:35:00  Formatting 10 file(s)
18:35:01  Formatted 3 of 10 file(s) in 1.2s
```

---

## Best Practices

### Run After Enrichment

Format files after running `sst enrich` to ensure consistent structure:

```bash
sst enrich --models customers
sst format models/customers/
```

### Team Consistency

Run format before committing to maintain consistent code style:

```bash
# Pre-commit hook
sst format models/ --check || (sst format models/ && git add .)
```

### CI Integration

```yaml
# Check formatting in CI
- run:
    name: Check YAML formatting
    command: sst format models/ --check
```

---

## Troubleshooting

### "No files found"

Ensure the path exists and contains YAML files:

```bash
ls models/*.yml
```

### Files not changing

Use `--force` to rewrite files even if content appears unchanged:

```bash
sst format models/ --force
```

### Validation warnings about characters

Use `--sanitize` to fix problematic characters:

```bash
sst format models/ --sanitize
```

---

## Related

- [sst enrich](enrich.md) - Enrich metadata (run format after)
- [sst validate](validate.md) - Validate after formatting
- [sst migrate-meta](migrate-meta.md) - Migrate meta format
