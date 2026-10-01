"""`sst enrich`'s rules: what it fills, how it derives each value, and what it writes per model.

`components` resolves the flags into what a run fills, `infer` holds the derivation rules,
`prompts` the Cortex prompts and responses, `columns` what one model's columns gain, and
`tables` what each semantic view gains as table synonyms. Everything here is pure: the use case
reads the warehouse and the files, and passes what it read in.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.enrich.columns import (
    COMPONENT_KEYS,
    WRITTEN_KEYS,
    ColumnUpdate,
    ModelEnrichment,
    WarehouseColumn,
    column_synonyms,
    enrich_model,
    prompt_columns,
    role,
    sample_columns,
    synonym_columns,
    yaml_column_name,
)
from snowflake_semantic_tools.domain.model.enrich.components import (
    COMPONENT_GROUPS,
    COMPONENT_NAMES,
    DATA_COMPONENTS,
    DEFAULT_COMPONENTS,
    SYNONYM_COMPONENTS,
    Component,
    EnrichOptions,
    collection_refusal,
    parse_components,
    resolve_options,
)
from snowflake_semantic_tools.domain.model.enrich.infer import (
    SampleDecision,
    clean_synonyms,
    decide_samples,
    derive_column_type,
    is_sampled_type,
    same_data_type,
    semantic_data_type,
    taken_names,
    usable_sample,
)
from snowflake_semantic_tools.domain.model.enrich.prompts import (
    COLUMN_SYNONYMS_SCHEMA,
    COLUMNS_PER_PROMPT,
    TABLE_SYNONYMS_SCHEMA,
    PromptColumn,
    column_synonyms_prompt,
    parse_column_synonyms,
    parse_table_synonyms,
    table_synonyms_prompt,
)
from snowflake_semantic_tools.domain.model.enrich.tables import (
    TableSynonymEdit,
    ViewTable,
    avoided_names,
    needs_table_synonyms,
    table_synonym_edits,
)

__all__ = [
    "COLUMNS_PER_PROMPT",
    "COLUMN_SYNONYMS_SCHEMA",
    "COMPONENT_GROUPS",
    "COMPONENT_KEYS",
    "COMPONENT_NAMES",
    "DATA_COMPONENTS",
    "DEFAULT_COMPONENTS",
    "SYNONYM_COMPONENTS",
    "TABLE_SYNONYMS_SCHEMA",
    "WRITTEN_KEYS",
    "ColumnUpdate",
    "Component",
    "EnrichOptions",
    "ModelEnrichment",
    "PromptColumn",
    "SampleDecision",
    "TableSynonymEdit",
    "ViewTable",
    "WarehouseColumn",
    "avoided_names",
    "clean_synonyms",
    "collection_refusal",
    "column_synonyms",
    "column_synonyms_prompt",
    "decide_samples",
    "derive_column_type",
    "enrich_model",
    "is_sampled_type",
    "needs_table_synonyms",
    "parse_column_synonyms",
    "parse_components",
    "parse_table_synonyms",
    "prompt_columns",
    "resolve_options",
    "role",
    "same_data_type",
    "sample_columns",
    "semantic_data_type",
    "synonym_columns",
    "table_synonym_edits",
    "table_synonyms_prompt",
    "taken_names",
    "usable_sample",
    "yaml_column_name",
]
