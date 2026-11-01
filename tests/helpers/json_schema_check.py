"""A JSON Schema checker for exactly the keywords the SST schemas emit, so the tests need no new dependency.

`check(schema, value)` returns each failure as `(path, keyword)`: the path to the value that
fails, a tuple of keys and indexes, and the keyword it fails. A keyword the schemas do not use
raises, so a renderer that starts emitting one is checked here before it is relied on.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from typing import Any

from snowflake_semantic_tools.domain.render.json_schema import SCHEMA_KEYWORDS

Failure = tuple[tuple[str | int, ...], str]
_TYPES: dict[str, Callable[[object], bool]] = {
    "object": lambda value: isinstance(value, Mapping),
    "array": lambda value: isinstance(value, list),
    "string": lambda value: isinstance(value, str),
    "boolean": lambda value: isinstance(value, bool),
    "integer": lambda value: isinstance(value, int) and not isinstance(value, bool),
    "number": lambda value: isinstance(value, (int, float)) and not isinstance(value, bool),
    "null": lambda value: value is None,
}


def check(schema: Mapping[str, Any], value: object) -> list[Failure]:
    """Return every failure of `value` against `schema`, a document whose `$ref`s point into its `$defs`."""
    return _check(schema, value, (), schema)


def _check(schema: object, value: object, path: tuple[str | int, ...], root: Mapping[str, Any]) -> list[Failure]:
    if schema is True or schema == {}:
        return []
    if schema is False:
        return [(path, "false")]
    assert isinstance(schema, Mapping), schema
    unknown = set(schema) - SCHEMA_KEYWORDS
    if unknown:
        raise ValueError(f"keywords the checker does not implement: {sorted(unknown)}")
    failures: list[Failure] = []
    if "$ref" in schema:
        name = str(schema["$ref"]).removeprefix("#/$defs/")
        failures += _check(root["$defs"][name], value, path, root)
    if "type" in schema:
        types = schema["type"] if isinstance(schema["type"], list) else [schema["type"]]
        if not any(_TYPES[name](value) for name in types):
            return [*failures, (path, "type")]
    if "enum" in schema and value not in schema["enum"]:
        failures.append((path, "enum"))
    if "pattern" in schema and isinstance(value, str) and re.search(schema["pattern"], value) is None:
        failures.append((path, "pattern"))
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if "minimum" in schema and value < schema["minimum"]:
            failures.append((path, "minimum"))
        if "maximum" in schema and value > schema["maximum"]:
            failures.append((path, "maximum"))
    if "items" in schema and isinstance(value, list):
        for index, item in enumerate(value):
            failures += _check(schema["items"], item, (*path, index), root)
    if isinstance(value, Mapping):
        failures += _object(schema, value, path, root)
    if "anyOf" in schema:
        failed = [_check(option, value, path, root) for option in schema["anyOf"]]
        if all(failed):
            # As a best match would: the failures of each option of the value's own type, so a
            # failure is named where it is, not only at the value that holds it.
            failures.append((path, "anyOf"))
            for found in failed:
                if (path, "type") not in found:
                    failures += found
    for rule in schema.get("allOf", ()):
        failures += _check(rule, value, path, root)
    if "if" in schema and not _check(schema["if"], value, path, root):
        failures += _check(schema.get("then", {}), value, path, root)
    return failures


def _object(
    schema: Mapping[str, Any], value: Mapping[Any, Any], path: tuple[str | int, ...], root: Mapping[str, Any]
) -> list[Failure]:
    failures: list[Failure] = [(path, "required") for key in schema.get("required", ()) if key not in value]
    properties = schema.get("properties", {})
    patterns = schema.get("patternProperties", {})
    for key, item in value.items():
        where = (*path, str(key))
        matched = [pattern for pattern in patterns if re.search(pattern, str(key))]
        if key in properties:
            failures += _check(properties[key], item, where, root)
        for pattern in matched:
            failures += _check(patterns[pattern], item, where, root)
        if key not in properties and not matched and "additionalProperties" in schema:
            failures += _check(schema["additionalProperties"], item, where, root)
    return failures
