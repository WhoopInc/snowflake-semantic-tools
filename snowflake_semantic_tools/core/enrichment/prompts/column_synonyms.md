You are analyzing database columns to generate natural language synonyms for ALL columns at once.

TABLE: {table_name}
Description: {table_description}

COLUMNS TO PROCESS:
{columns_text}

FULL CONTEXT (first 800 chars):
{yaml_context}

TASK:
Generate up to {max_synonyms} natural language synonyms for EACH column listed above.

IMPORTANT - Return as a single JSON object:
{{
  "column_name_1": ["synonym 1", "synonym 2", ...],
  "column_name_2": ["synonym 1", "synonym 2", ...],
  ...
}}

GOOD SYNONYM EXAMPLES:
- "transaction timestamp"
- "customer unique identifier"
- "total purchase amount"
- "primary contact email"

BAD EXAMPLES:
- "table_name.column_name" (NO table prefix)
- "column_name" (just repeating name)
- "varchar" (just data type)

REQUIREMENTS:
- Natural, conversational language
- 2-4 words typically
- NO table name prefix
- NO snake_case
- Focus on what data the column contains

Return ONLY the JSON object with all column synonyms.

