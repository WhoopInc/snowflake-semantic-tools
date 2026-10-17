"""Parse SST's own YAML: the one place a project file's YAML text becomes Python values.

`parse_yaml_bytes` is the parser; `read_yaml_mapping` and `read_yaml_file` read a file
through it, the first raising and the second collecting diagnostics. Every parse:

- refuses a file that is too large, starts with a byte-order mark, is not UTF-8, or indents
  with tabs, and reads CRLF line endings as LF (`text_checks`);
- reads each `{{ ... }}` template as the scalar holding it reads: it is swapped for a
  YAML-safe placeholder before parsing, and its source text restored in every string after,
  unescaped as that scalar's quoting says (`''` is one quote in a single-quoted scalar, and a
  backslash escape is decoded in a double-quoted one), so either quoting gives the same
  template; a whole-line comment or a block scalar is left to YAML as text, templates and all;
- refuses anchors, aliases, merge keys, non-string keys, and a key written twice in one
  mapping, and reports a value YAML cannot construct at its own position (`compose`);
- refuses a string, key or value, holding a NUL or a lone surrogate, which a YAML escape can
  write and Snowflake cannot store, at the line it is written on;
- indexes each node's source position by its path, and records each template's position;
- returns what loads but is badly formatted -- CRLF, trailing whitespace, folded scalars,
  implicit booleans and nulls -- as `ParsedYaml.diagnostics`, never raising for them.

A file holds exactly one document, and its root is a mapping.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from pathlib import Path
from types import MappingProxyType
from typing import Any

import yaml

from snowflake_semantic_tools.adapters.bounded_yaml import BoundedSafeLoader
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.yaml.compose import compose_single, construct, formatting_findings
from snowflake_semantic_tools.adapters.yaml.documents import ParsedYaml, TemplateSource
from snowflake_semantic_tools.adapters.yaml.text_checks import (
    decoded_text,
    executable_lines,
    normalised_text,
    refuse_tab_indentation,
    unstorable_diagnostics,
)
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.authored import NodePath, SourcePosition

_PLACEHOLDER = "__SST_TPL_%d__"
_PLACEHOLDERS = re.compile(r"__SST_TPL_\d+__")


def _neutralize_templates(
    text: str, path: str, executable: list[bool] | None = None
) -> tuple[str, dict[str, TemplateSource]]:
    """Replace template spans with YAML-safe scalars before parsing.

    Args:
        executable: `text_checks.executable_lines(text)`, when the caller already has it.
    """
    # Comments and block scalars are already legal YAML and may discuss invalid
    # examples verbatim; only neutralize executable scalar text.
    executable = executable_lines(text) if executable is None else executable
    spans: list[tuple[int, int]] = []
    diagnostics: list[Diagnostic] = []
    absolute_offset = 0
    for line_number, line in enumerate(text.splitlines(keepends=True), start=1):
        cursor = 0
        structural = executable[line_number - 1] if line_number <= len(executable) else False
        while structural:
            start = line.find("{{", cursor)
            if start < 0:
                break
            end = line.find("}}", start + 2)
            nested = line.find("{{", start + 2, end if end >= 0 else len(line))
            if end < 0 or nested >= 0:
                col = (nested if nested >= 0 else start) + 1
                reason = "nested template expression" if nested >= 0 else "unterminated template expression"
                diagnostics.append(D("SST-LOD004", file=str(path), line=line_number, col=col, reason=reason))
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


def _scalar_styles(node: yaml.Node) -> dict[str, str | None]:
    """Return the quoting style of the scalar each placeholder is written in, by placeholder."""
    styles: dict[str, str | None] = {}
    pending = [node]
    while pending:
        current = pending.pop()
        if isinstance(current, yaml.ScalarNode):
            styles.update(dict.fromkeys(_PLACEHOLDERS.findall(current.value), current.style))
        elif isinstance(current, yaml.MappingNode):
            pending.extend(part for pair in current.value for part in pair)
        elif isinstance(current, yaml.SequenceNode):
            pending.extend(current.value)
    return styles


def _unescaped(raw: str, style: str | None) -> str:
    """Return a template's source text as the scalar it is written in reads it.

    A single-quoted scalar doubles a quote; a double-quoted one escapes with a backslash, and
    holding no backslash reads as written. One that does not decode, such as one holding a
    bare `"` (which the placeholder kept from ending the scalar), reads as written too.
    """
    if style == "'":
        return raw.replace("''", "'")
    if style != '"' or "\\" not in raw:
        return raw
    loader = BoundedSafeLoader(f'"{raw}"')
    try:
        decoded = loader.get_single_data()
    except yaml.YAMLError:
        return raw
    finally:
        loader.dispose()
    return decoded if isinstance(decoded, str) else raw


def _restored(node: yaml.Node, templates: Mapping[str, TemplateSource]) -> dict[str, str]:
    """Return each template's text as its scalar reads it, by placeholder, for `_restore_templates`."""
    styles = _scalar_styles(node)
    return {placeholder: _unescaped(source.raw, styles.get(placeholder)) for placeholder, source in templates.items()}


def _restore_templates(value: Any, restored: Mapping[str, str]) -> Any:
    if isinstance(value, str):
        text = value
        for placeholder, source in restored.items():
            text = text.replace(placeholder, source)
        return text
    if isinstance(value, list):
        return [_restore_templates(item, restored) for item in value]
    if isinstance(value, dict):
        return {_restore_templates(key, restored): _restore_templates(item, restored) for key, item in value.items()}
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


def _line_of(positions: Mapping[NodePath, SourcePosition], at: NodePath) -> int | None:
    position = positions.get(at)
    return position.line if position is not None else None


def parse_yaml_bytes(
    raw: bytes, path: str, *, null_means: Callable[[tuple[str | int, ...]], bool] | None = None
) -> ParsedYaml:
    """Parse one YAML file's bytes into its tree, node positions, template sources, and findings.

    A document that is only `null` (`---`, `~`) parses as an empty tree. The tree's
    top-level keys are strings; nested values are as YAML typed them.

    Args:
        path: How every diagnostic names the file, usually its project-relative path.
        null_means: As `compose.formatting_findings` takes it: the key paths where an empty value
            has a documented meaning, so is not reported as a coercion.

    Raises:
        ProjectError: The file does not parse; its `diagnostics` say why, as listed below.

    Diagnostics:
        SST-LOD007, SST-LOD017, SST-LOD006: the file is too large, starts with a byte-order
            mark, or is not UTF-8; raised.
        SST-LOD010: a structural line is indented with a tab; every one at once, raised.
        SST-LOD004: a template is unterminated or nested; every one at once, raised.
        SST-LOD009, SST-LOD001: an unquoted `: ` in a value, or another YAML syntax error, or
            a value YAML cannot construct, at its position; raised.
        SST-LOD003, SST-LOD008: the file holds no document, or more than one; raised.
        SST-LOD013, SST-LOD014, SST-LOD015: an anchor or alias, a merge key, or a key that is
            not a string; every one at once, raised.
        SST-LOD005: a key written twice in one mapping; raised.
        SST-LOD002: the document root is not a mapping; raised.
        SST-LOD022: a string holds a NUL or a lone surrogate; every one at once, raised.
        SST-LOD012, SST-LOD202: CRLF line endings, read as LF, or trailing whitespace; returned.
        SST-LOD011, SST-LOD016: a folded scalar, or an implicit boolean or null; returned.
    """
    text, findings = normalised_text(decoded_text(raw, path), path)
    executable = executable_lines(text)
    refuse_tab_indentation(text, path, executable)
    neutralized, templates = _neutralize_templates(text, path, executable)
    composed = compose_single(neutralized, path)
    if composed is None:
        return ParsedYaml(MappingProxyType({}), MappingProxyType({}), MappingProxyType(templates), findings)
    loaded = _restore_templates(construct(composed, path), _restored(composed, templates))
    if loaded is None:
        return ParsedYaml(MappingProxyType({}), MappingProxyType({}), MappingProxyType(templates), findings)
    if not isinstance(loaded, dict):
        diagnostic = D("SST-LOD002", file=str(path), found=type(loaded).__name__)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    positions = _node_path_index(composed)
    unstorable = unstorable_diagnostics(loaded, path, lambda at: _line_of(positions, at))
    if unstorable:
        raise ProjectError("; ".join(item.message for item in unstorable), diagnostics=unstorable)
    return ParsedYaml(
        MappingProxyType({str(key): value for key, value in loaded.items()}),
        positions,
        MappingProxyType(templates),
        (*findings, *formatting_findings(composed, path, null_means=null_means)),
    )


def read_yaml_mapping(path: Path) -> dict[str, Any]:
    """Read and parse the YAML file at `path` into a plain dict, raising on the first problem.

    Diagnostics name the file by `str(path)`.

    Raises:
        ProjectError: The file cannot be read (SST-PRT009) or does not parse (as
            `parse_yaml_bytes` raises).

    Diagnostics:
        SST-PRT009: the file cannot be read; raised.
    """
    try:
        raw = path.read_bytes()
    except OSError as exc:
        diagnostic = D("SST-PRT009", path=str(path), detail=str(exc))
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,)) from exc
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
        The parsed file, or None once its problem is in `sink`. A file that parses adds its
        formatting findings, `ParsedYaml.diagnostics`, to `sink`.

    Diagnostics:
        SST-LOD018: the file cannot be read.
        Each code `parse_yaml_bytes` lists.
    """
    try:
        parsed = parse_yaml_bytes(path.read_bytes(), relative)
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
    else:
        sink.extend(parsed.diagnostics)
        return parsed
    return None
