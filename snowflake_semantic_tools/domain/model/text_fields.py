"""Which YAML keys hold prose and which hold code or markup, for the formatter and SST-VAL008.

A prose value is read by a person in Snowflake (a comment) or by an eval judge; its soft wraps
can be joined and a long line folded. A structural value is SQL, an expression, or an agent's
instructions, where a line break means something: it is never joined or folded, and a folded
block there is SST-VAL008.
"""

from __future__ import annotations

PROSE_KEYS = frozenset(
    ("description", "comment", "question", "ground_truth_output", "tool_input", "tool_output", "immutable_reason")
)
STRUCTURAL_KEYS = frozenset(
    (
        "sql",
        "expr",
        "expression",
        "prompt",
        "instructions",
        "orchestration",
        "response",
        "system",
        "sql_generation",
        "question_categorization",
    )
)
