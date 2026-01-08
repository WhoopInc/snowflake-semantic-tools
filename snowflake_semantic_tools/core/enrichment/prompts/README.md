# LLM Prompts for Synonym Generation

This directory contains the prompt templates used by the Cortex synonym generator.

## Files

### `table_synonyms.md`
Prompt for generating table-level synonyms.

**Required Variables:**
- `{table_name}` - The technical table name (e.g., `INT_ORDERS`)
- `{readable_name}` - Human-readable table name (e.g., `orders`)
- `{description}` - Table description (truncated to 800 chars)
- `{full_context}` - Complete YAML definition or column context (truncated to 1500 chars)
- `{max_synonyms}` - Maximum number of synonyms to generate
- `{avoid_synonyms_section}` - Optional section listing synonyms to avoid (for deduplication)

### `column_synonyms.md`
Prompt for batch column synonym generation.

**Required Variables:**
- `{table_name}` - The table name
- `{table_description}` - Table description (truncated to 300 chars)
- `{columns_text}` - Formatted list of columns with descriptions, types, and samples
- `{yaml_context}` - Full YAML context (truncated to 800 chars)
- `{max_synonyms}` - Maximum number of synonyms per column

## Usage

```python
from snowflake_semantic_tools.core.enrichment.prompt_loader import render_prompt

prompt = render_prompt(
    "table_synonyms",
    table_name="INT_ORDERS",
    readable_name="orders",
    description="Order transaction data",
    full_context="...",
    max_synonyms=4,
    avoid_synonyms_section=""
)
```

## Modifying Prompts

When modifying prompts:
1. Keep variable placeholders in `{variable_name}` format
2. Use double braces `{{` and `}}` for literal braces in JSON examples
3. Test changes with unit tests in `tests/unit/core/test_prompt_loader.py`
4. Consider token limits when adding content (Cortex has model-specific limits)

