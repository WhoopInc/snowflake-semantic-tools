"""Structured completions from Cortex, which write enrich's synonyms.

One AI_COMPLETE call per prompt, at temperature 0, asking for JSON that follows a schema. The
model and the prompt are bound as parameters. The schema is written into the statement as an
OBJECT constant, which AI_COMPLETE requires, built from SST's own schema values with every
string quoted, so nothing a project writes reaches the statement unquoted.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

from snowflake_semantic_tools.adapters.snowflake.connector.session import Session, _variant_value
from snowflake_semantic_tools.domain.model.sql import string_literal
from snowflake_semantic_tools.domain.ports.enrich import CortexPort
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePortError

# A model name as Cortex spells one, such as `mistral-large2` or `claude-sonnet-4-6`.
_MODEL_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


def object_constant(value: object) -> str:
    """Return a JSON-shaped value as the Snowflake constant that equals it.

    A mapping is an OBJECT constant, a list an ARRAY constant, and every string a quoted
    literal; a percent sign is doubled, as the driver's parameter binding requires.

    Raises:
        SnowflakePortError: the value holds something JSON cannot, such as a set.
    """
    if isinstance(value, Mapping):
        entries = ", ".join(f"{object_constant(str(key))}: {object_constant(item)}" for key, item in value.items())
        return "{" + entries + "}"
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(object_constant(item) for item in value) + "]"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, str):
        return string_literal(value).replace("%", "%%")
    if value is None:
        return "NULL"
    raise SnowflakePortError(f"cannot write {type(value).__name__} into a Cortex response schema")


def completion_sql(schema: Mapping[str, object]) -> str:
    """Return the statement asking one model for JSON following `schema`; it binds model and prompt."""
    response_format = object_constant({"type": "json", "schema": schema})
    return f"SELECT AI_COMPLETE(%s, %s, {{'temperature': 0}}, {response_format}) AS RESPONSE"


class CortexMethods(Session, CortexPort):
    """Ask Cortex models for structured answers."""

    def complete_json(self, model: str, prompt: str, schema: Mapping[str, object]) -> object:
        if not _MODEL_NAME.fullmatch(model):
            raise SnowflakePortError(f"{model!r} is not a Cortex model name")
        result = self.query(completion_sql(schema), (model, prompt))
        return _variant_value(result.rows[0][0] if result.rows else None, None)
