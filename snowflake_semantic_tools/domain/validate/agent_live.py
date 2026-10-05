"""Compare a compiled agent or tool with what Snowflake reports for the object it publishes or calls.

Connected validation reads the live object through a port and hands these the values; each
compares and reports, and none reads anything itself.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence

from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.validate.agent_tool import input_type_matches, schema_type

_ARGUMENTS = re.compile(r"\((?P<types>[^)]*)\)")


def first_difference(rendered: object, live: object, path: str = "") -> str | None:
    """Return the path of the first place two JSON documents differ; None when they are equal.

    Mapping keys are visited in sorted order, so the same pair gives the same path every run.
    A path reads `tools[0].tool_spec.description`; the whole document is `<root>`.

    Example:
        first_difference({"a": [1, 2]}, {"a": [1, 3]}) == "a[1]"
    """
    if isinstance(rendered, Mapping) and isinstance(live, Mapping):
        for key in sorted({*map(str, rendered), *map(str, live)}):
            here = f"{path}.{key}" if path else key
            if key not in rendered or key not in live:
                return here
            found = first_difference(rendered[key], live[key], here)
            if found is not None:
                return found
        return None
    if isinstance(rendered, list) and isinstance(live, list):
        for index, (ours, theirs) in enumerate(zip(rendered, live, strict=False)):
            found = first_difference(ours, theirs, f"{path}[{index}]")
            if found is not None:
                return found
        return f"{path}[{min(len(rendered), len(live))}]" if len(rendered) != len(live) else None
    return None if rendered == live else (path or "<root>")


def live_spec(text: str) -> Mapping[str, object] | None:
    """Read the specification DESCRIBE AGENT reports as a JSON document; None when it is not one."""
    try:
        value = json.loads(text)
    except ValueError:
        return None
    return value if isinstance(value, Mapping) else None


def names_object(spec: object, qualified_name: QualifiedName) -> bool:
    """Report whether any text value of a live agent specification is the object's three-part name.

    An agent names the semantic views, search services, and routines its tools reach by their
    qualified names, under `tool_resources`; every value is compared, so a new place that names
    an object is still seen. Names compare as Snowflake resolves them, quoted parts as written.
    """
    if isinstance(spec, Mapping):
        return any(names_object(value, qualified_name) for value in spec.values())
    if isinstance(spec, list):
        return any(names_object(value, qualified_name) for value in spec)
    if not isinstance(spec, str) or spec.count(".") < 2:
        return False
    try:
        return QualifiedName.parse(spec).folded == qualified_name.folded
    except ValueError:
        return False


def live_signature(arguments: str) -> tuple[str, ...] | None:
    """Read a routine's argument types from the `arguments` column SHOW lists it with.

    Example:
        live_signature("LOOKUP(VARCHAR, NUMBER) RETURN VARCHAR") == ("VARCHAR", "NUMBER")

    Returns:
        The types, upper-cased, in order; None when the text holds no argument list.
    """
    match = _ARGUMENTS.search(arguments)
    if match is None:
        return None
    return tuple(part.strip().upper() for part in match.group("types").split(",") if part.strip())


def signature_disagreement(types: Sequence[str], input_schema: Mapping[str, object]) -> tuple[str, str] | None:
    """Compare a live routine's argument types with a tool's input schema, in declaration order.

    Returns:
        The live signature and the schema, each as `(...)` text, when the arity or a type
        differs; None when they agree or the schema declares no properties mapping.
    """
    properties = input_schema.get("properties")
    if not isinstance(properties, Mapping):
        return None
    declared = list(properties.values())
    agrees = len(declared) == len(types) and all(
        input_type_matches(sql_type, schema) for sql_type, schema in zip(types, declared, strict=True)
    )
    if agrees:
        return None
    expected = ", ".join(f"{key} {schema_type(value)}" for key, value in properties.items())
    return f"({', '.join(types)})", f"({expected})"
