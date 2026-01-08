You are analyzing a database table to generate natural language synonyms that will help analysts discover this data through conversational queries.

TABLE INFORMATION:
Name: {table_name}
Readable: {readable_name}
Description: {description}

COMPLETE TABLE DEFINITION:
{full_context}

{avoid_synonyms_section}

TASK:
Generate up to {max_synonyms} natural language synonyms that describe what this table contains or what questions it helps answer. These will be used for semantic search and natural language queries.

GOOD EXAMPLES (generic, descriptive):
- "customer transaction history"
- "daily sales performance metrics"
- "product inventory status tracking"
- "employee performance reviews"
- "web session clickstream data"

BAD EXAMPLES:
- "{readable_name}" (just repeating table name in plain English)
- "{table_name}" (technical table name format)
- "data table" (too generic/vague)
- "database information" (not specific enough)

REQUIREMENTS:
- Natural, conversational language (as if describing the table to a colleague)
- Brief but descriptive (typically 3-6 words, can be longer if needed for clarity)
- Focus on what the data represents or what business questions it answers
- NO technical formatting (snake_case, prefixes like int_, etc.)
- Think: "If someone asked 'where's the data on X?', what would they say?"
- Each synonym MUST be unique - do not generate synonyms similar to ones already used

Return ONLY a JSON array of strings: ["synonym 1", "synonym 2", "synonym 3"]

