# JSON Output Schema

SST supports machine-readable JSON output via `--output json` for all commands.

---

## Usage

```bash
sst --output json validate
sst --output json generate --all
sst --output json extract
sst --output json deploy
```

---

## Schema (version 1)

```json
{
  "tool": "sst",
  "version": "0.3.0",
  "schema_version": 1,
  "command": "validate",
  "status": "success | error",
  "exit_code": 0,
  "duration_s": 2.34,
  "diagnostics": [
    {
      "severity": "error | warning",
      "message": "Human-readable error description",
      "code": "SST-V002",
      "file": "relative/path/to/file.yml",
      "line": 42,
      "column": 15,
      "suggestion": "Actionable fix text",
      "entity": "metric_name",
      "context": {}
    }
  ],
  "summary": {
    "errors": 1,
    "warnings": 3
  }
}
```

---

## Field Descriptions

### Envelope

| Field | Type | Description |
|-------|------|-------------|
| `tool` | string | Always `"sst"` |
| `version` | string | SST version (e.g., `"0.3.0"`) |
| `schema_version` | integer | Schema version for breaking change detection. Currently `1`. |
| `command` | string | The command that was executed (`validate`, `generate`, `extract`, `deploy`) |
| `status` | string | `"success"` or `"error"` |
| `exit_code` | integer | Process exit code: 0=success, 1=errors, 2=config error |
| `duration_s` | float | Execution time in seconds |
| `diagnostics` | array | List of diagnostic objects (errors + warnings only) |
| `summary` | object | Counts of errors and warnings |

### Diagnostic Object

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `severity` | string | yes | `"error"` or `"warning"` |
| `message` | string | yes | Human-readable description of the issue |
| `code` | string | yes | Stable error code (e.g., `"SST-V002"`) |
| `file` | string | no | Relative path to the source file |
| `line` | integer | no | 1-based line number in the file |
| `column` | integer | no | 1-based column number |
| `suggestion` | string | yes | Actionable fix text |
| `entity` | string | no | Name of the entity being validated (metric, table, etc.) |
| `context` | object | no | Additional structured data (varies by error type) |

---

## Stability Contract

- `schema_version` will be incremented for any breaking changes to the envelope structure
- Error codes (`SST-V*`, `SST-G*`, etc.) are permanent identifiers and will not be reused
- New fields may be added to diagnostic objects without incrementing schema_version
- Fields will not be removed or renamed without incrementing schema_version

---

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success (no errors) |
| 1 | Validation/generation errors found |
| 2 | Configuration or internal error |

---

## Error Code Prefixes

| Prefix | Category | Source |
|--------|----------|--------|
| `SST-V` | Validation | `sst validate` rules |
| `SST-P` | Parsing | YAML/template parsing |
| `SST-E` | Extract | `sst extract` operations |
| `SST-G` | Generate | `sst generate` DDL execution |
| `SST-C` | Config | Configuration issues |

See [error-codes.md](error-codes.md) for the complete registry.
