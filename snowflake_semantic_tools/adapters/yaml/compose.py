"""Compose a YAML document into nodes, check them, and construct the Python values they hold.

`compose_single` composes one document and refuses YAML features SST does not accept,
`construct` builds the value of a composed node, and `formatting_findings` reports what
loads but reads differently from how it looks. Every refusal raises `ProjectError` with its
diagnostics; nothing here returns YAML's own exception.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any, NoReturn

import yaml

from snowflake_semantic_tools.adapters.bounded_yaml import BoundedSafeLoader
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.yaml.text_checks import unquoted_colon
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin

_MERGE_TAG = "tag:yaml.org,2002:merge"
_BOOL_TAG = "tag:yaml.org,2002:bool"
_NULL_TAG = "tag:yaml.org,2002:null"
# The YAML 1.1 booleans YAML 1.2 reads as strings. As a key each stays the string it is
# written as, so `on:` names a field; as a value each is reported, since it reads as a bool.
_WORD_BOOLEANS = frozenset(("yes", "no", "on", "off", "y", "n"))


def _refuse(*diagnostics: Diagnostic) -> NoReturn:
    raise ProjectError("; ".join(diagnostic.message for diagnostic in diagnostics), diagnostics=diagnostics)


class _Loader(BoundedSafeLoader):
    """A safe loader, within the nesting bound, that records where each anchor and alias is written."""

    def __init__(self, stream: str) -> None:
        super().__init__(stream)
        self.anchor_marks: list[yaml.Mark] = []

    def compose_node(self, parent: yaml.Node | None, index: int) -> yaml.Node | None:
        event = self.peek_event()  # type: ignore[no-untyped-call]
        if isinstance(event, yaml.AliasEvent) or getattr(event, "anchor", None):
            self.anchor_marks.append(event.start_mark)
        return super().compose_node(parent, index)


def compose_single(text: str, path: str) -> yaml.Node | None:
    """Compose the one document `text` holds; None for a document that is only `null`.

    Raises:
        ProjectError: As the diagnostics below say.

    Diagnostics:
        SST-LOD009: YAML stopped at an unquoted `: ` inside a value on a key's line.
        SST-LOD001: any other YAML syntax error, at YAML's mark, collections nested deeper than
            `adapters.bounded_yaml` allows among them.
        SST-LOD003: the text holds no document: only whitespace or comments.
        SST-LOD008: the text holds more than one document.
        SST-LOD013: an anchor or alias is written; one diagnostic per anchor and alias.
    """
    loader = _Loader(text)
    try:
        nodes: list[yaml.Node | None] = []
        while loader.check_node():
            nodes.append(loader.get_node())
    except yaml.YAMLError as exc:
        _refuse_syntax(text, path, exc)
    finally:
        loader.dispose()
    if not nodes:
        _refuse(D("SST-LOD003", file=path))
    if len(nodes) != 1:
        _refuse(D("SST-LOD008", file=path, count=len(nodes)))
    if loader.anchor_marks:
        _refuse(
            *(
                D("SST-LOD013", origin=Origin(path, mark.line + 1, mark.column + 1), file=path, line=mark.line + 1)
                for mark in loader.anchor_marks
            )
        )
    return nodes[0]


def _refuse_syntax(text: str, path: str, exc: yaml.YAMLError) -> NoReturn:
    mark = getattr(exc, "problem_mark", None)
    line = mark.line + 1 if mark is not None else 1
    col = mark.column + 1 if mark is not None else 1
    problem = str(getattr(exc, "problem", exc))
    colon = unquoted_colon(text, path, line, col - 1) if problem == "mapping values are not allowed here" else None
    diagnostic = colon or D("SST-LOD001", file=path, line=line, col=col, detail=problem)
    raise ProjectError(diagnostic.message, diagnostics=(diagnostic,)) from exc


def construct(node: yaml.Node, path: str) -> Any:
    """Build the Python value of one composed node, as `yaml.safe_load` would, but stricter.

    A plain `yes`, `no`, `on`, `off`, `y` or `n` key stays the string it is written as.

    Raises:
        ProjectError: As the diagnostics below say.

    Diagnostics:
        SST-LOD014: a mapping uses a merge key (`<<`); every one in the document at once.
        SST-LOD015: a mapping key is not a string; every one in the document at once.
        SST-LOD005: a mapping declares one key twice.
        SST-LOD001: YAML cannot construct a value, such as an unknown tag or an impossible date.
    """
    _refuse_unsupported_keys(node, path)
    return _value(node, path)


def _refuse_unsupported_keys(node: yaml.Node, path: str) -> None:
    merges: list[Diagnostic] = []
    keys: list[Diagnostic] = []
    for key_node in _key_nodes(node):
        line = key_node.start_mark.line + 1
        if key_node.tag == _MERGE_TAG:
            merges.append(D("SST-LOD014", origin=Origin(path, line), file=path, line=line))
            continue
        key = _key(key_node, path)
        if not isinstance(key, str):
            keys.append(D("SST-LOD015", origin=Origin(path, line), file=path, line=line, found=repr(key)))
    if merges or keys:
        _refuse(*merges, *keys)


def _key_nodes(node: yaml.Node) -> Iterator[yaml.Node]:
    """Yield every mapping key node of a composed tree, depth first, in document order."""
    if isinstance(node, yaml.MappingNode):
        for key_node, value_node in node.value:
            yield key_node
            yield from _key_nodes(value_node)
    elif isinstance(node, yaml.SequenceNode):
        for child in node.value:
            yield from _key_nodes(child)


def _key(node: yaml.Node, path: str) -> Any:
    if isinstance(node, yaml.ScalarNode) and node.style is None and node.value.casefold() in _WORD_BOOLEANS:
        return node.value
    return _value(node, path)


def _value(node: yaml.Node, path: str) -> Any:
    if isinstance(node, yaml.MappingNode):
        mapping: dict[Any, Any] = {}
        for key_node, value_node in node.value:
            key = _key(key_node, path)
            if key in mapping:
                _refuse(D("SST-LOD005", file=path, line=key_node.start_mark.line + 1, key=str(key)))
            mapping[key] = _value(value_node, path)
        return mapping
    if isinstance(node, yaml.SequenceNode):
        return [_value(child, path) for child in node.value]
    loader = yaml.SafeLoader("")
    try:
        return loader.construct_object(node, deep=True)
    except (yaml.constructor.ConstructorError, ValueError) as exc:
        mark = node.start_mark
        detail = str(getattr(exc, "problem", None) or f"cannot read {node.value!r}: {exc}")
        diagnostic = D("SST-LOD001", file=path, line=mark.line + 1, col=mark.column + 1, detail=detail)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,)) from exc
    finally:
        loader.dispose()


def formatting_findings(
    node: yaml.Node, path: str, *, null_means: Callable[[tuple[str | int, ...]], bool] | None = None
) -> tuple[Diagnostic, ...]:
    """Report each value that loads as something other than it reads, in document order.

    Args:
        null_means: Whether an empty value at a key path has a documented meaning, so reads as
            what it is: such a value is not reported. None gives no path that meaning.

    Diagnostics:
        SST-LOD011: a value is a folded scalar (`>` or `>-`), which joins its lines.
        SST-LOD016: a plain `yes`, `no`, `on`, `off`, `y` or `n` loads as a boolean, or a
            `~` or empty value as null where null means nothing documented.
    """
    findings: list[Diagnostic] = []
    for key_path, value_node in _keyed_scalars(node, ()):
        key = next(str(part) for part in reversed(key_path) if isinstance(part, str))
        line = value_node.start_mark.line + 1
        if value_node.style == ">":
            findings.append(D("SST-LOD011", origin=Origin(path, line), file=path, line=line, key=key))
            continue
        coerced = _coercion(value_node)
        if coerced is not None and not (coerced[1] == "null" and null_means is not None and null_means(key_path)):
            found, expected = coerced
            context: dict[str, Any] = {"file": path, "line": line, "key": key, "found": found, "expected": expected}
            findings.append(D("SST-LOD016", origin=Origin(path, line), **context))
    return tuple(findings)


def _coercion(node: yaml.ScalarNode) -> tuple[str, str] | None:
    """The written text and the value a plain scalar is silently read as; None when it reads as written."""
    if node.style is not None:
        return None
    if node.tag == _BOOL_TAG and node.value.casefold() in _WORD_BOOLEANS:
        return repr(node.value), "true" if node.value.casefold() in ("yes", "on", "y") else "false"
    if node.tag == _NULL_TAG and node.value in ("~", ""):
        return repr(node.value), "null"
    return None


def _keyed_scalars(
    node: yaml.Node, path: tuple[str | int, ...]
) -> Iterator[tuple[tuple[str | int, ...], yaml.ScalarNode]]:
    """Yield each scalar value under a mapping key, with its key path; a list item adds its index."""
    if isinstance(node, yaml.MappingNode):
        for key_node, value_node in node.value:
            yield from _keyed_scalars(value_node, (*path, str(key_node.value)))
    elif isinstance(node, yaml.SequenceNode):
        for index, child in enumerate(node.value):
            yield from _keyed_scalars(child, (*path, index))
    elif isinstance(node, yaml.ScalarNode) and any(isinstance(part, str) for part in path):
        yield path, node
