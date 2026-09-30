"""Parse SST's own YAML: the one place a project file's YAML text becomes Python values.

`parse_yaml_bytes` is the parser; `read_yaml_mapping` and `read_yaml_file` read a file
through it, the first raising and the second collecting diagnostics. Every parse:

- keeps each `{{ ... }}` template exactly as written, quoted or not: it is swapped for a
  YAML-safe placeholder before parsing and its source text restored in every string after
  (a whole-line comment or a block scalar is left to YAML as text, templates and all);
- indexes each node's source position by its path, and records each template's position;
- refuses a key written twice in one mapping (SST-LOD005);
- reports a value YAML cannot construct (an unknown tag, an impossible date, a bad merge)
  as SST-LOD001 at its own position, never as YAML's own exception;
- expands YAML 1.1 merge keys (`<<: *anchor`), the mapping's own keys winning.

A file holds exactly one document, and its root is a mapping.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType
from typing import Any, NoReturn

import yaml

from ...domain.model.diagnostic import D, Diagnostic, Origin
from ..errors import ProjectError
from .documents import NodePath, ParsedYaml, SourcePosition, TemplateSource

_PLACEHOLDER = "__SST_TPL_%d__"


def _neutralize_templates(text: str, path: str) -> tuple[str, dict[str, TemplateSource]]:
    """Replace template spans with YAML-safe scalars before parsing."""
    # Comments and block scalars are already legal YAML and may discuss invalid
    # examples verbatim; only neutralize executable scalar text.
    neutralizable_lines: list[str] = []
    block_indent: int | None = None
    for line in text.splitlines(keepends=True):
        stripped = line.lstrip()
        indent = len(line) - len(stripped)
        if block_indent is not None:
            if stripped.strip() and indent <= block_indent:
                block_indent = None
            else:
                neutralizable_lines.append(" " * len(line.rstrip("\n")) + ("\n" if line.endswith("\n") else ""))
                continue
        if stripped.startswith("#"):
            neutralizable_lines.append(" " * len(line.rstrip("\n")) + ("\n" if line.endswith("\n") else ""))
            continue
        opened = _block_scalar_indent(line.rstrip("\n"))
        if opened is not None:
            block_indent = opened
        neutralizable_lines.append(line)
    neutralizable = "".join(neutralizable_lines)
    spans: list[tuple[int, int]] = []
    diagnostics: list[Diagnostic] = []
    absolute_offset = 0
    for line_number, line in enumerate(neutralizable.splitlines(keepends=True), start=1):
        cursor = 0
        while True:
            start = line.find("{{", cursor)
            if start < 0:
                break
            end = line.find("}}", start + 2)
            nested = line.find("{{", start + 2, end if end >= 0 else len(line))
            if end < 0 or nested >= 0:
                col = (nested if nested >= 0 else start) + 1
                reason = "nested template expression" if nested >= 0 else "unterminated template expression"
                diagnostics.append(
                    D(
                        "SST-LOD004",
                        file=str(path),
                        line=line_number,
                        col=col,
                        reason=reason,
                    )
                )
                break
            spans.append((absolute_offset + start, absolute_offset + end + 2))
            cursor = end + 2
        absolute_offset += len(line)
    if diagnostics:
        raise ProjectError(
            "; ".join(diagnostic.message for diagnostic in diagnostics),
            diagnostics=tuple(diagnostics),
        )
    rewritten = text
    templates: dict[str, TemplateSource] = {}
    for index, (start, end) in reversed(tuple(enumerate(spans))):
        placeholder = _PLACEHOLDER % index
        template_line = text.count("\n", 0, start) + 1
        previous_newline = text.rfind("\n", 0, start)
        templates[placeholder] = TemplateSource(text[start:end], template_line, start - previous_newline)
        rewritten = rewritten[:start] + placeholder + rewritten[end:]
    return rewritten, templates


# Leading spaces and `- ` sequence entries: whatever precedes a line's first key or scalar.
_ENTRY_PREFIX = re.compile(r" *(?:- +)*")


def _block_scalar_indent(line: str) -> int | None:
    """Return the column a block scalar opened on `line` is indented past, or None if it opens none."""
    entries = _ENTRY_PREFIX.match(line)
    prefix = entries.end() if entries else 0
    # Measured from the owning key or entry, not the line's first `-`, so the rest of a
    # list item after its `- key: |` block is still executable text.
    if re.search(r":\s*[>|][+-]?\s*(?:#.*)?$", line):
        return prefix
    if line[:prefix].strip() and re.fullmatch(r"[>|][+-]?\s*(?:#.*)?", line[prefix:]):
        return line.rindex("-", 0, prefix)
    return None


def _restore_templates(value: Any, templates: Mapping[str, TemplateSource]) -> Any:
    if isinstance(value, str):
        restored = value
        for placeholder, source in templates.items():
            restored = restored.replace(placeholder, source.raw)
        return restored
    if isinstance(value, list):
        return [_restore_templates(item, templates) for item in value]
    if isinstance(value, dict):
        return {_restore_templates(key, templates): _restore_templates(item, templates) for key, item in value.items()}
    return value


def _node_path_index(node: yaml.Node) -> Mapping[NodePath, SourcePosition]:
    positions: dict[NodePath, SourcePosition] = {}

    def walk(current: yaml.Node, path: NodePath) -> None:
        positions[path] = SourcePosition(current.start_mark.line + 1, current.start_mark.column + 1)
        if isinstance(current, yaml.MappingNode):
            for key_node, value_node in current.value:
                key = str(key_node.value)
                positions[path + (key,)] = SourcePosition(key_node.start_mark.line + 1, key_node.start_mark.column + 1)
                walk(value_node, path + (key,))
        elif isinstance(current, yaml.SequenceNode):
            for index, child in enumerate(current.value):
                walk(child, path + (index,))

    walk(node, ())
    return MappingProxyType(positions)


def _construct_yaml_node(node: yaml.Node, path: str) -> Any:
    """Build the Python value of one composed node, as `yaml.safe_load` would.

    Differences from safe_load: a key written twice in one mapping is SST-LOD005, and a
    value YAML cannot construct (an unknown tag, an impossible date, a bad merge) is
    SST-LOD001 at its own position rather than an exception. Merge keys (`<<: *anchor`)
    expand as in YAML 1.1, with the mapping's own keys winning over merged ones.
    """
    if isinstance(node, yaml.MappingNode):
        mapping: dict[Any, Any] = {}
        for key_node, value_node in _merged_pairs(node, path):
            mapping[_construct_yaml_node(key_node, path)] = _construct_yaml_node(value_node, path)
        return mapping
    if isinstance(node, yaml.SequenceNode):
        return [_construct_yaml_node(child, path) for child in node.value]
    if isinstance(node, yaml.ScalarNode):
        return _construct_scalar(node, path)
    mark = node.start_mark
    diagnostic = D(
        "SST-LOD001",
        file=str(path),
        line=mark.line + 1,
        col=mark.column + 1,
        detail=f"unsupported YAML node {type(node).__name__}",
    )
    raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))


_MERGE_TAG = "tag:yaml.org,2002:merge"


def _merged_pairs(node: yaml.MappingNode, path: str) -> list[tuple[yaml.Node, yaml.Node]]:
    """A mapping's key/value pairs with merge keys expanded, lowest precedence first.

    Building a dict from the result in order gives YAML 1.1 merge semantics: the
    mapping's own keys win over merged ones, and an earlier mapping in `<<: [*a, *b]`
    wins over a later one. Composed nodes are never modified -- an anchored node is
    shared by every alias that reaches it.
    """
    merged: list[tuple[yaml.Node, yaml.Node]] = []
    own: list[tuple[yaml.Node, yaml.Node]] = []
    for key_node, value_node in node.value:
        if key_node.tag != _MERGE_TAG:
            own.append((key_node, value_node))
            continue
        if isinstance(value_node, yaml.MappingNode):
            sources: list[yaml.Node] = [value_node]
        elif isinstance(value_node, yaml.SequenceNode):
            sources = list(value_node.value)
        else:
            _raise_at(
                value_node, path, f"expected a mapping or list of mappings for merging, found {_node_kind(value_node)}"
            )
        for source in reversed(sources):
            if not isinstance(source, yaml.MappingNode):
                _raise_at(source, path, f"expected a mapping for merging, found {_node_kind(source)}")
            merged.extend(_merged_pairs(source, path))
    _refuse_duplicate_keys(own, path)
    return merged + own


def _node_kind(node: yaml.Node) -> str:
    """`scalar`, `sequence` or `mapping`, as YAML's own messages name a node."""
    return type(node).__name__.removesuffix("Node").lower()


def _refuse_duplicate_keys(pairs: list[tuple[yaml.Node, yaml.Node]], path: str) -> None:
    seen: set[Any] = set()
    for key_node, _ in pairs:
        key = _construct_yaml_node(key_node, path)
        if key in seen:
            diagnostic = D("SST-LOD005", file=path, line=key_node.start_mark.line + 1, key=str(key))
            raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
        seen.add(key)


def _construct_scalar(node: yaml.ScalarNode, path: str) -> Any:
    loader = yaml.SafeLoader("")
    try:
        return loader.construct_object(node, deep=True)
    except (yaml.constructor.ConstructorError, ValueError) as exc:
        _raise_at(node, path, str(getattr(exc, "problem", None) or f"cannot read {node.value!r}: {exc}"), exc)
    finally:
        loader.dispose()


def _raise_at(node: yaml.Node, path: str, detail: str, cause: Exception | None = None) -> NoReturn:
    mark = node.start_mark
    diagnostic = D("SST-LOD001", file=str(path), line=mark.line + 1, col=mark.column + 1, detail=detail)
    raise ProjectError(diagnostic.message, diagnostics=(diagnostic,)) from cause


def parse_yaml_bytes(raw: bytes, path: str) -> ParsedYaml:
    """Parse one YAML file's bytes into its tree, node positions, and template sources.

    A document that is only `null` (`---`, `~`) parses as an empty tree. The tree's
    top-level keys are strings; nested values are as YAML typed them.

    Args:
        path: How every diagnostic names the file, usually its project-relative path.

    Raises:
        ProjectError: The file does not parse; its `diagnostics` say why, as listed below.

    Diagnostics:
        SST-PRS122: the bytes are not UTF-8.
        SST-LOD004: a template is unterminated or nested; every one is reported at once.
        SST-LOD001: a YAML syntax error, or a value YAML cannot construct, at its position.
        SST-LOD005: a key written twice in one mapping.
        SST-LOD003: the file holds only whitespace or comments.
        SST-LOD008: the file holds more than one document.
        SST-LOD002: the document root is not a mapping.
    """
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        diagnostic = D("SST-PRS122", origin=Origin(path), file=path, offset=exc.start)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,)) from exc
    neutralized, templates = _neutralize_templates(text, path)
    try:
        nodes = list(yaml.compose_all(neutralized, Loader=yaml.SafeLoader))
    except yaml.YAMLError as exc:
        mark = getattr(exc, "problem_mark", None)
        line = mark.line + 1 if mark is not None else 1
        col = mark.column + 1 if mark is not None else 1
        detail = str(getattr(exc, "problem", exc))
        diagnostic = D("SST-LOD001", file=str(path), line=line, col=col, detail=detail)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,)) from exc
    if not nodes:
        # Only whitespace or comments: nothing to load, and nothing wrong enough to stop a build.
        diagnostic = D("SST-LOD003", file=str(path))
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    if len(nodes) != 1:
        diagnostic = D("SST-LOD008", file=str(path), count=len(nodes))
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    composed = nodes[0]
    loaded = _restore_templates(None if composed is None else _construct_yaml_node(composed, path), templates)
    if loaded is None:
        return ParsedYaml(MappingProxyType({}), MappingProxyType({}), MappingProxyType(templates))
    if not isinstance(loaded, dict):
        diagnostic = D("SST-LOD002", file=str(path), found=type(loaded).__name__)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    line_index = MappingProxyType({}) if composed is None else _node_path_index(composed)
    return ParsedYaml(
        MappingProxyType({str(key): value for key, value in loaded.items()}),
        line_index,
        MappingProxyType(templates),
    )


def read_yaml_mapping(path: Path) -> dict[str, Any]:
    """Read and parse the YAML file at `path` into a plain dict, raising on the first problem.

    Diagnostics name the file by `str(path)`.

    Raises:
        ProjectError: The file cannot be read (`cannot read {path}: {error}`, no diagnostics)
            or does not parse (as `parse_yaml_bytes` raises).
    """
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ProjectError(f"cannot read {path}: {exc}") from exc
    return dict(parse_yaml_bytes(raw, str(path)).tree)


def read_yaml_file(
    path: Path,
    relative: str,
    sink: list[Diagnostic],
    *,
    pointer_origin: Origin | None = None,
) -> ParsedYaml | None:
    """Read and parse the YAML file at `path`, collecting any problem into `sink`.

    Never raises for a missing, unreadable, or invalid file.

    Args:
        relative: How diagnostics name the file, usually its project-relative path.
        pointer_origin: Where another file names this one; an unreadable file is then
            reported there, as a missing file that one references.

    Returns:
        The parsed file, or None once its problem is in `sink`.

    Diagnostics:
        SST-LOD018: the file cannot be read.
        SST-PRS122: the bytes are not UTF-8.
        SST-LOD004: a template is unterminated or nested.
        SST-LOD001: a YAML syntax error, or a value YAML cannot construct.
        SST-LOD005: a key written twice in one mapping.
        SST-LOD003: the file holds only whitespace or comments.
        SST-LOD008: the file holds more than one document.
        SST-LOD002: the document root is not a mapping.
    """
    try:
        return parse_yaml_bytes(path.read_bytes(), relative)
    except OSError:
        sink.append(
            D(
                "SST-LOD018",
                file=pointer_origin.file if pointer_origin else relative,
                path=relative,
                origin=pointer_origin or Origin(relative),
            )
        )
    except ProjectError as exc:
        sink.extend(exc.diagnostics)
    return None
