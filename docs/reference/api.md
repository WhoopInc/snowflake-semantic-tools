# Python API Reference

Programmatic access to all SST features.

---

## Quick Start

```python
from snowflake_semantic_tools import (
    # Services
    MetadataEnrichmentService,
    SemanticMetadataCollectionValidationService,
    SemanticMetadataExtractionService,
    SemanticViewGenerationService,
    DeployService,
    # Configs
    SnowflakeConfig,
    EnrichmentConfig,
    ValidateConfig,
    ExtractConfig,
    GenerateConfig,
    DeployConfig
)
```

---

## Services

All services are importable from `snowflake_semantic_tools`.

### MetadataEnrichmentService

Enrich dbt YAML files with metadata from Snowflake.

```python
service = MetadataEnrichmentService(enrich_config, snowflake_config)
service.connect()
result = service.enrich()
```

**Methods:**
- `connect()` - Connect to Snowflake
- `enrich()` - Run enrichment, returns EnrichmentResult

### SemanticMetadataCollectionValidationService

Validate semantic models against dbt definitions.

```python
service = SemanticMetadataCollectionValidationService.create_from_config()
result = service.execute(validate_config)
```

**Methods:**
- `execute(config, verbose=False)` - Run validation, returns ValidationResult

### SemanticMetadataExtractionService

Extract metadata to Snowflake tables.

```python
service = SemanticMetadataExtractionService.create_from_config(sf_config)
result = service.execute(extract_config)
```

**Methods:**
- `execute(config)` - Run extraction, returns ExtractResult

### SemanticViewGenerationService

Generate semantic views from metadata.

```python
service = SemanticViewGenerationService(snowflake_config)
result = service.execute(generate_config)
```

**Methods:**
- `execute(config)` - Generate views, returns GenerateResult

### DeployService

One-step deployment (validate → extract → generate).

```python
service = DeployService(snowflake_config)
result = service.execute(deploy_config)
```

**Methods:**
- `execute(config)` - Run full deployment, returns DeployResult

---

## Configuration Classes

### SnowflakeConfig

```python
SnowflakeConfig(
    account: str,
    user: str,
    warehouse: str,
    role: str,
    database: str,
    schema: str,
    password: Optional[str] = None,
    private_key_path: Optional[str] = None
)
```

### EnrichmentConfig

```python
EnrichmentConfig(
    target_path: str,
    database: str,
    schema: str,
    excluded_dirs: Optional[List[str]] = None,
    dry_run: bool = False,
    fail_fast: bool = False
)
```

### ValidateConfig

```python
ValidateConfig(
    branch: str,
    repo_url: Optional[str] = None,
    local_path: Optional[Path] = None,
    strict_mode: bool = False,
    exclude_dirs: Optional[List[str]] = None
)
```

### ExtractConfig

```python
ExtractConfig(
    branch: str,
    database: str,
    schema: str,
    repo_url: Optional[str] = None,
    local_path: Optional[Path] = None
)
```

### GenerateConfig

```python
GenerateConfig(
    metadata_database: str,
    metadata_schema: str,
    target_database: str,
    target_schema: str,
    views_to_generate: Optional[List] = None,
    execute: bool = True
)
```

### DeployConfig

```python
DeployConfig(
    local_path: Path,
    database: str,
    schema: str,
    output_type: str = 'all',
    skip_validation: bool = False,
    verbose: bool = False
)
```

---

## Complete Example

```python
from snowflake_semantic_tools import (
    MetadataEnrichmentService,
    EnrichmentConfig,
    SnowflakeConfig
)
from pathlib import Path

# Configure Snowflake
sf_config = SnowflakeConfig(
    account='abc12345',
    user='admin',
    warehouse='MY_WAREHOUSE',
    role='MY_ROLE',
    database='PROD_DB',
    schema='my_schema'
)

# Configure enrichment
enrich_config = EnrichmentConfig(
    target_path='models/',
    database='PROD_DB',
    schema='my_schema'
)

# Execute
service = MetadataEnrichmentService(enrich_config, sf_config)
service.connect()
result = service.enrich()

print(f"Enriched {result.processed}/{result.total} models")
```

---

For detailed examples and workflows, see [User Guide](../user-guide.md).


