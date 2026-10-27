"""Locate a YAML document in its own text, templates and all, for the migrations.

The template calls a file holds are not YAML: an unquoted `- {{ ref('x') }}` reads as a flow
mapping. Each call on a line YAML reads as structure is replaced by as many copies of an
invisible separator as it has characters, so every node keeps the line and column it has in the
real text, and each scalar's calls are put back in order once it is read.
"""

from __future__ import annotations

import re

import yaml

from snowflake_semantic_tools.adapters import bounded_yaml
from snowflake_semantic_tools.adapters.yaml.text_checks import executable_lines
from snowflake_semantic_tools.domain.migrate.outline import MAP, SCALAR, SEQ, Entry, Node

# U+2063 INVISIBLE SEPARATOR: printable to YAML, valid in any scalar style, and never authored.
_FILL = "\u2063"
_RUN = re.compile(f"{_FILL}+")
_CALL = re.compile(r"\{\{.*?\}\}")


def outline(text: str) -> Node | None:
    """Return the file's one document located in `text`; None when it is not one parseable document."""
    filled, calls = _fill(text)
    try:
        root = bounded_yaml.compose(filled)
    except yaml.YAMLError:
        return None
    return _node(root, calls) if root is not None else None


def _fill(text: str) -> tuple[str, list[tuple[int, int, str]]]:
    """Replace each template call on a structural line with filler of its length.

    Returns:
        The filled text, and each call replaced as `(line, col, call)`, in text order.
    """
    lines = text.splitlines(keepends=True)
    calls: list[tuple[int, int, str]] = []
    for number, executable in enumerate(executable_lines(text)):
        if not executable or "{{" not in lines[number]:
            continue

        def filled(match: re.Match[str], number: int = number) -> str:
            calls.append((number, match.start(), match.group(0)))
            return _FILL * len(match.group(0))

        lines[number] = _CALL.sub(filled, lines[number])
    return "".join(lines), calls


def _node(node: yaml.Node, calls: list[tuple[int, int, str]]) -> Node:
    """Convert one composed node, and everything under it, putting each scalar's calls back."""
    start, end = node.start_mark, node.end_mark
    place = (start.line, start.column, end.line, end.column)
    if isinstance(node, yaml.MappingNode):
        entries = tuple(
            Entry(_restored(key, calls) or "", key.start_mark.line, key.start_mark.column, _node(value, calls))
            for key, value in node.value
            if isinstance(key, yaml.ScalarNode)
        )
        return Node(MAP, *place, flow=bool(node.flow_style), entries=entries)
    if isinstance(node, yaml.SequenceNode):
        return Node(SEQ, *place, flow=bool(node.flow_style), items=tuple(_node(item, calls) for item in node.value))
    assert isinstance(node, yaml.ScalarNode)
    null = node.tag.endswith(":null")
    return Node(SCALAR, *place, value=None if null else _restored(node, calls), style=node.style or "")


def _restored(node: yaml.ScalarNode, calls: list[tuple[int, int, str]]) -> str:
    """Return a scalar's value with the calls inside its span back in place of their filler."""
    start, end = (node.start_mark.line, node.start_mark.column), (node.end_mark.line, node.end_mark.column)
    queue = [call for line, col, call in calls if start <= (line, col) < end]

    def back(match: re.Match[str]) -> str:
        length, taken = len(match.group(0)), []
        while queue and length > 0:
            taken.append(queue.pop(0))
            length -= len(taken[-1])
        return "".join(taken) if taken else match.group(0)

    return _RUN.sub(back, str(node.value)) if queue else str(node.value)
