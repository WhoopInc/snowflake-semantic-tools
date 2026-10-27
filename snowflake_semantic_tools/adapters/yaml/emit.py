"""Write a document an import built as YAML, in the canonical form `sst format` keeps.

A text spanning lines is a literal block, and one holding a template, such as
`{{ semantic_view('sales') }}`, is double-quoted, as hand-written project files spell it. The
dump then goes through `canonical_yaml`, so an imported file is one `sst format --check`
accepts as it is.
"""

from __future__ import annotations

import io
from collections.abc import Mapping

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from ruamel.yaml.scalarstring import DoubleQuotedScalarString, LiteralScalarString

from snowflake_semantic_tools.adapters.roundtrip import scalar
from snowflake_semantic_tools.adapters.yaml.format import styled_yaml
from snowflake_semantic_tools.adapters.yaml.styles import YamlStyle

# Wide enough that no value is ever folded across lines.
_WIDTH = 4096


def emit_document(document: Mapping[str, object], path: str, *, style: YamlStyle | None = None) -> str:
    """Return `document` as canonical YAML text, ending in one newline.

    Args:
        path: How a diagnostic names the file, should formatting refuse it.
        style: The project's `format:` settings; None writes the default form.

    Raises:
        ProjectError: the canonical form would change the document's value (SST-INT003).
    """
    yaml = YAML(typ="rt")
    yaml.width = _WIDTH
    yaml.indent(mapping=2, sequence=4, offset=2)
    stream = io.StringIO()
    yaml.dump(_node(document), stream)
    return styled_yaml(stream.getvalue(), path, style or YamlStyle())


def _node(value: object) -> object:
    """Return a value as the node ruamel dumps: mappings and lists as its own, strings styled."""
    if isinstance(value, Mapping):
        mapping = CommentedMap()
        for key, item in value.items():
            mapping[str(key)] = _node(item)
        return mapping
    if isinstance(value, list | tuple):
        return CommentedSeq(_node(item) for item in value)
    if isinstance(value, str):
        if "\n" in value.strip():
            # A literal block cannot keep trailing spaces or a tab-led line exactly; such a text
            # stays a quoted scalar, which keeps every character.
            lines = value.split("\n")
            if all(line == line.rstrip(" \t") for line in lines) and "\t" not in value:
                return LiteralScalarString(value)
            return DoubleQuotedScalarString(value)
        if "{{" in value or "{%" in value:
            return DoubleQuotedScalarString(value)
        # dbt reads YAML 1.1, where a plain `no` is false; such a text is quoted.
        return scalar(value)
    return value
