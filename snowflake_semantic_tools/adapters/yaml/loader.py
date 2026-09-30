"""Load a dbt + SST project into `domain` models.

Everything it produces is fully resolved: `{{ ref('products') }}` has become
`SST_REF_DEV.JAFFLE.PRODUCTS` and `{{ target.database }}` has been replaced from the
dbt profile, so `domain` never needs to know either exists.

WHERE THE INFORMATION LIVES. A view declares its name, tables, and a few view-level
keys. Everything else in the rendered DDL arrives by ATTACHMENT from elsewhere in the
project:

    semantic_models/semantic_views/**/*.yml   the view
    target/manifest.json                      physical relation, model grain and
                                              resolved column metadata
    semantic_models/<member>/*.yml            metrics, filters, relationships,
                                              verified queries and custom
                                              instructions, attached by the tables
                                              they reference
    profiles.yml + dbt_project.yml            the target database and schema

Every authored key the loader does not read is reported (`AUTHORED_KEYS`), so no
setting is dropped silently.
"""

from __future__ import annotations

import dataclasses
import re
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from types import MappingProxyType
from typing import Any, NoReturn

import yaml

from ...domain.model.compiler import FILTER_EXPR, METRIC_EXPR, VQR_SQL, ResolveContext, resolve_scalar
from ...domain.model.dbt import DbtCatalog, DbtModel
from ...domain.model.diagnostic import D, Diagnostic, DiagnosticBag, Origin, Severity
from ...domain.model.expression import call_arguments, is_aggregate_expression
from ...domain.model.expression import is_boolean_expression as _is_boolean_expression
from ...domain.model.expression import root_function as _root_function
from ...domain.model.project import ParsedMember, ParsedProject, ParsedView, ResolvedProject, SemanticViewProject
from ...domain.model.reference import TemplateCall, TemplateSyntaxError, scan_template_calls, single_template_call
from ...domain.model.registry import SEMANTIC_REGISTRY
from ...domain.model.semantic_view import (
    Column,
    ColumnKind,
    Metric,
    Relationship,
    SemanticView,
    SortKey,
    Table,
    Tag,
    Variable,
    VerifiedQuery,
    Window,
)
from ...domain.model.sql import string_literal
from ...domain.resolve.members import attach_view_members
from ..dbt.manifest import load_manifest_catalog
from ..project import ProjectError
from .documents import (
    NodePath,
    ParsedYaml,
    RawDocument,
    RawDocuments,
    SourcePosition,
    TemplateSource,
    discover_yaml,
    load_documents,
)

PLACEHOLDER = "__SST_TPL_%d__"
# The column types a NON ADDITIVE BY entry or a window may sort or partition by.
DIMENSION_TYPES = frozenset((ColumnKind.DIMENSION.value, ColumnKind.TIME_DIMENSION.value))
NUMERIC_TYPES = frozenset(
    (
        "BIGINT",
        "BYTEINT",
        "DECIMAL",
        "DOUBLE",
        "DOUBLE PRECISION",
        "FLOAT",
        "INT",
        "INTEGER",
        "NUMBER",
        "NUMERIC",
        "REAL",
        "SMALLINT",
        "TINYINT",
    )
)
TEMPORAL_TYPES = frozenset(
    (
        "DATE",
        "DATETIME",
        "TIME",
        "TIMESTAMP",
        "TIMESTAMP_LTZ",
        "TIMESTAMP_NTZ",
        "TIMESTAMP_TZ",
    )
)


@dataclass(frozen=True, slots=True)
class Target:
    """The resolved dbt target -- what `{{ target.database }}` stands for."""

    database: str
    schema: str

    def fqn(self, name: str) -> str:
        return f"{self.database}.{self.schema}.{name.upper()}"


def _render_target_value(value: object, target: Target) -> str:
    return str(value).replace("{{ target.database }}", target.database).replace("{{ target.schema }}", target.schema)


def _semantic_view_defaults(config: dict[str, Any], path: Path, views_dir: Path) -> dict[str, object]:
    """Merge `semantic_views` `+` keys along the folder routes above one file."""
    block = config.get("semantic_views") or {}
    if not isinstance(block, dict):
        return {}
    resolved: dict[str, object] = {key[1:]: value for key, value in block.items() if str(key).startswith("+")}
    relative_parent = path.resolve().relative_to(views_dir.resolve()).parent
    cursor: object = block
    for part in relative_parent.parts:
        if not isinstance(cursor, dict):
            break
        child = cursor.get(part)
        if not isinstance(child, dict):
            break
        resolved.update({key[1:]: value for key, value in child.items() if str(key).startswith("+")})
        cursor = child
    return resolved


def _semantic_view_target(config: dict[str, Any], path: Path, views_dir: Path, target: Target) -> Target:
    resolved = _semantic_view_defaults(config, path, views_dir)
    database = _render_target_value(resolved.get("database", target.database), target)
    schema = _render_target_value(resolved.get("schema", target.schema), target)
    return Target(database=database, schema=schema)


def _folder_route_diagnostics(config: dict[str, Any], views_dir: Path) -> tuple[Diagnostic, ...]:
    block = config.get("semantic_views") or {}
    if not isinstance(block, dict):
        return ()
    diagnostics: list[Diagnostic] = []

    def walk(node: dict[str, Any], directory: Path, route: str) -> None:
        for key, value in node.items():
            key_text = str(key)
            if key_text.startswith("+") or not isinstance(value, dict):
                continue
            child = directory / key_text
            child_route = f"{route}.{key_text}" if route else key_text
            if not child.is_dir():
                diagnostics.append(
                    D(
                        "SST-CFG041",
                        key=key_text,
                        block="semantic_views",
                        root=str(views_dir),
                        subject=f"config_route:{child_route}",
                    )
                )
                continue
            walk(value, child, child_route)

    walk(block, views_dir, "")
    return tuple(diagnostics)


def _stray_view_diagnostics(documents: RawDocuments, views_dir: Path) -> tuple[Diagnostic, ...]:
    """Report each view list outside `views_dir` as an unread key (SST-PRS004).

    Views are validated from every file but built only from files under `views_dir`, so
    such a list would otherwise be dropped without a word.
    """
    root_key = _node_root("semantic_view")
    built = {document.path for document in documents.under(views_dir, root_key)}
    diagnostics: list[Diagnostic] = []
    for document in documents.documents:
        if document.path in built or not isinstance(document.tree.get(root_key), list):
            continue
        position = document.position((root_key,))
        origin = Origin(document.path, position.line if position else None, position.col if position else None)
        diagnostics.append(D("SST-PRS004", origin=origin, artifact=document.path, field=root_key))
    return tuple(diagnostics)


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
        placeholder = PLACEHOLDER % index
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


def _parse_yaml_bytes(raw: bytes, path: str) -> ParsedYaml:
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


def _read_yaml(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ProjectError(f"cannot read {path}: {exc}") from exc
    return dict(_parse_yaml_bytes(raw, str(path)).tree)


def _as_str_tuple(value: Any) -> tuple[str, ...]:
    """Normalise a YAML scalar-or-list into a tuple of strings.

    Numbers are stringified because `sample_values: [1100, 700]` is authored as
    integers but renders as `'1100', '700'` -- YAML typing is not SQL typing.
    """
    if value is None:
        return ()
    if isinstance(value, bool):
        return ("true" if value else "false",)
    if isinstance(value, (str, int, float)):
        return (str(value),)
    if isinstance(value, list):
        return tuple("true" if v is True else "false" if v is False else str(v) for v in value)
    raise ProjectError(f"expected a scalar or list, found {type(value).__name__}")


def _synonyms_diagnostics(
    value: object,
    *,
    artifact: str,
    subject: str,
) -> tuple[Diagnostic, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        return (
            D(
                "SST-PRS029",
                artifact=artifact,
                found=type(value).__name__,
                subject=subject,
            ),
        )
    diagnostics = []
    for synonym in value:
        if any(character in synonym for character in ("'", '"')):
            diagnostics.append(
                D(
                    "SST-PRS030",
                    artifact=artifact,
                    value=synonym,
                    detail="quotes",
                    subject=subject,
                )
            )
    return tuple(diagnostics)


def resolve_target(project_dir: Path, target_name: str | None = None) -> Target:
    """Resolve a declared dbt target's database and schema from `profiles.yml`.

    The profile adapter reads the file: it is plain YAML, not a semantic model, so
    the template-preserving parser would keep a single-quoted scalar's `''`
    escapes inside `{{ env_var(...) }}` and the value would never resolve.
    """
    from ..profile import profile_output

    try:
        _profile, selected_target, output = profile_output(project_dir, target_name)
    except ValueError as exc:
        raise ProjectError(str(exc)) from exc
    database, schema = output.get("database"), output.get("schema")
    if not database or not schema:
        raise ProjectError(f"target {selected_target!r} must set both `database` and `schema`")
    return Target(database=str(database), schema=str(schema))


def _target_path(project_dir: Path) -> Path:
    target_path = str(_read_yaml(project_dir / "dbt_project.yml").get("target-path") or "target")
    return project_dir / target_path / "manifest.json"


def _run_dbt_parse(project_dir: Path, target_name: str | None) -> None:
    command = [
        "dbt",
        "parse",
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(project_dir),
    ]
    if target_name:
        command.extend(("--target", target_name))
    try:
        completed = subprocess.run(command, capture_output=True, text=True, check=False)
    except OSError as exc:
        raise ProjectError(f"cannot run dbt parse: {exc}") from exc
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise ProjectError(f"dbt parse failed with exit {completed.returncode}: {detail}")


def load_models(
    project_dir: Path,
    *,
    target_name: str | None = None,
    manifest_path: Path | None = None,
    invoke_dbt: bool = True,
) -> dict[str, DbtModel]:
    """Load dbt models only from the manifest dbt resolved for this target."""
    path = manifest_path or _target_path(project_dir)
    if manifest_path is None and invoke_dbt:
        _run_dbt_parse(project_dir, target_name)
    catalog: DbtCatalog = load_manifest_catalog(path)
    return {model.name.casefold(): model for model in catalog.models}


@dataclass(frozen=True, slots=True)
class NonAdditiveDef:
    """One `non_additive_dimensions` entry as authored: a dimension, its table, and its sort."""

    dimension: str
    table: str | None = None
    descending: bool | None = None
    nulls_first: bool | None = None

    @property
    def key(self) -> SortKey:
        name = self.dimension.upper()
        return SortKey(f"{self.table.upper()}.{name}" if self.table else name, self.descending, self.nulls_first)


SORT_DIRECTIONS: Mapping[str, bool] = MappingProxyType({"ascending": False, "descending": True})
NULL_ORDERS: Mapping[str, bool] = MappingProxyType({"first": True, "last": False})


def _non_additive(entry: Mapping[str, Any]) -> NonAdditiveDef:
    table = entry.get("table")
    return NonAdditiveDef(
        dimension=str(entry["dimension"]).strip(),
        table=table.strip() if isinstance(table, str) and table.strip() else None,
        descending=SORT_DIRECTIONS.get(str(entry.get("sort_direction"))),
        nulls_first=NULL_ORDERS.get(str(entry.get("null_order"))),
    )


@dataclass(frozen=True, slots=True)
class WindowOrderDef:
    """One `window.order_by` entry: a `{{ ref() }}` or `{{ metric() }}` call and its sort."""

    ref: str
    descending: bool | None = None
    nulls_first: bool | None = None


@dataclass(frozen=True, slots=True)
class WindowDef:
    """A metric's `window:` block as authored, with its references unresolved."""

    partition_by: tuple[str, ...] = ()
    partition_excluding: tuple[str, ...] = ()
    order_by: tuple[WindowOrderDef, ...] = ()
    # The canonical frame clause; None when absent or not a frame (SST-PRS124).
    frame: str | None = None
    has_frame: bool = False

    def references(self) -> tuple[tuple[str, str], ...]:
        """Every entry, with the field label a diagnostic names it by."""
        return (
            *((f"partition_by[{index}]", text) for index, text in enumerate(self.partition_by)),
            *((f"partition_by_excluding[{index}]", text) for index, text in enumerate(self.partition_excluding)),
            *((f"order_by[{index}]", entry.ref) for index, entry in enumerate(self.order_by)),
        )


# Snowflake's window frame grammar, which is all a frame may be: it is never passed
# through as free SQL.
_FRAME_BOUND = (
    r"(?:UNBOUNDED\s+(?:PRECEDING|FOLLOWING)|CURRENT\s+ROW|(?:\d+|INTERVAL\s+'[^']*')\s+(?:PRECEDING|FOLLOWING))"
)
_FRAME = re.compile(rf"(ROWS|RANGE)\s+BETWEEN\s+({_FRAME_BOUND})\s+AND\s+({_FRAME_BOUND})", re.IGNORECASE)


def _frame(value: object) -> str | None:
    """The frame clause in canonical spelling, or None when `value` is not one."""
    match = _FRAME.fullmatch(value.strip()) if isinstance(value, str) else None
    if match is None:
        return None
    return f"{match.group(1).upper()} BETWEEN {_frame_bound(match.group(2))} AND {_frame_bound(match.group(3))}"


def _frame_bound(bound: str) -> str:
    return " ".join(token if token.startswith("'") else token.upper() for token in re.findall(r"'[^']*'|\S+", bound))


def _window(value: object) -> WindowDef | None:
    if not isinstance(value, dict):
        return None

    def strings(field: str) -> tuple[str, ...]:
        return tuple(item for item in _list_of(value.get(field)) if isinstance(item, str))

    order_by: list[WindowOrderDef] = []
    for entry in _list_of(value.get("order_by")):
        if isinstance(entry, str):
            order_by.append(WindowOrderDef(entry))
        elif isinstance(entry, dict) and isinstance(entry.get("ref"), str):
            order_by.append(
                WindowOrderDef(
                    entry["ref"],
                    SORT_DIRECTIONS.get(str(entry.get("sort_direction"))),
                    NULL_ORDERS.get(str(entry.get("null_order"))),
                )
            )
    return WindowDef(
        partition_by=strings("partition_by"),
        partition_excluding=strings("partition_by_excluding"),
        order_by=tuple(order_by),
        frame=_frame(value.get("frame")),
        has_frame=value.get("frame") is not None,
    )


@dataclass(frozen=True, slots=True)
class MetricDef:
    """A metric as authored, with its `ref()`s still unresolved."""

    name: str
    expr: str
    description: str | None
    synonyms: tuple[str, ...]
    tables: tuple[str, ...] = ()
    derived: bool = False
    using_relationships: tuple[str, ...] = ()
    non_additive: tuple[NonAdditiveDef, ...] = ()
    access_modifier: str = "public_access"
    has_tables_key: bool = False
    origin: Origin | None = None
    template_calls: tuple[TemplateCall, ...] = ()
    poisoned: bool = False
    window: WindowDef | None = None

    @property
    def calls(self) -> tuple[TemplateCall, ...]:
        if self.template_calls:
            return self.template_calls
        try:
            return scan_template_calls(self.expr)
        except TemplateSyntaxError:
            return ()

    @property
    def referenced_models(self) -> tuple[str, ...]:
        """Which models this metric's expr refs, in first-seen order."""
        seen: list[str] = []
        for call in self.calls:
            if call.function == "ref" and call.args and call.args[0].casefold() not in seen:
                seen.append(call.args[0].casefold())
        return tuple(seen)

    @property
    def referenced_metrics(self) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(
                call.args[0].casefold() for call in self.calls if call.function == "metric" and len(call.args) == 1
            )
        )


def load_metrics(documents: RawDocuments, project_dir: Path, semantic_models_dir: str) -> tuple[MetricDef, ...]:
    """Read `<semantic_models_dir>/metrics/*.yml` under the `snowflake_metrics:` key."""
    metrics_dir = project_dir / semantic_models_dir / "metrics"
    out: list[MetricDef] = []
    for document, index, node in _load_nodes(documents, metrics_dir, _member_root("metric")):
        if not node.get("name") or not node.get("expr"):
            continue
        expression = str(node["expr"])
        try:
            template_calls = scan_template_calls(expression)
        except TemplateSyntaxError:
            template_calls = ()
        out.append(
            MetricDef(
                name=str(node["name"]),
                expr=expression,
                description=" ".join(str(node.get("description") or "").splitlines()).strip() or None,
                synonyms=_as_str_tuple(node.get("synonyms")),
                tables=_safe_table_refs(node.get("tables")),
                derived=bool(node.get("derived", False)),
                using_relationships=tuple(str(value).upper() for value in node.get("using_relationships") or []),
                non_additive=tuple(
                    _non_additive(value)
                    for value in _list_of(node.get("non_additive_dimensions"))
                    if isinstance(value, dict)
                    and isinstance(value.get("dimension"), str)
                    and value["dimension"].strip()
                ),
                access_modifier=str(node.get("access_modifier") or "public_access"),
                has_tables_key="tables" in node,
                origin=_node_origin(document, _member_root("metric"), index),
                template_calls=template_calls,
                poisoned=_table_refs_poisoned(node.get("tables")),
                window=_window(node.get("window")),
            )
        )
    return tuple(out)


def _metric_parse_diagnostics(
    documents: RawDocuments,
    project_dir: Path,
    semantic_models_dir: str,
) -> tuple[Diagnostic, ...]:
    metrics_dir = project_dir / semantic_models_dir / "metrics"
    diagnostics: list[Diagnostic] = []
    allowed_access = ("private_access", "public_access")
    for document, index, node in _load_nodes(documents, metrics_dir, _member_root("metric")):
        name = str(node.get("name") or "<unnamed>")
        subject = f"metric:{name}"
        origin = _node_origin(document, _member_root("metric"), index)
        if "expr" not in node:
            diagnostics.append(
                D(
                    "SST-PRS002",
                    artifact=subject,
                    field="expr",
                    subject=subject,
                    origin=origin,
                )
            )
        elif not isinstance(node.get("expr"), str):
            diagnostics.append(
                D(
                    "SST-PRS113",
                    artifact=subject,
                    field="expr",
                    found=type(node.get("expr")).__name__,
                    subject=subject,
                    origin=origin,
                )
            )
        if "tables" in node and not isinstance(node.get("tables"), list):
            diagnostics.append(
                D(
                    "SST-PRS003",
                    artifact=subject,
                    field="tables",
                    expected="a list",
                    found=type(node.get("tables")).__name__,
                    subject=subject,
                    origin=origin,
                )
            )
        using = node.get("using_relationships")
        if using is not None and (not isinstance(using, list) or any(not isinstance(item, str) for item in using)):
            diagnostics.append(
                D(
                    "SST-PRS003",
                    artifact=subject,
                    field="using_relationships",
                    expected="a list of strings",
                    found=type(using).__name__,
                    subject=subject,
                    origin=origin,
                )
            )
        access = node.get("access_modifier")
        if access is not None and str(access) not in allowed_access:
            diagnostics.append(
                D(
                    "SST-PRS013",
                    artifact=subject,
                    field="access_modifier",
                    found=access,
                    expected=", ".join(allowed_access),
                    subject=subject,
                    origin=origin,
                )
            )
        diagnostics.extend(_non_additive_parse_diagnostics(node.get("non_additive_dimensions"), subject, origin))
        diagnostics.extend(_window_parse_diagnostics(node, subject, origin))
        diagnostics.extend(_synonyms_diagnostics(node.get("synonyms"), artifact=subject, subject=subject))
    return tuple(diagnostics)


def _filter_parse_diagnostics(
    documents: RawDocuments,
    project_dir: Path,
    semantic_models_dir: str,
) -> tuple[Diagnostic, ...]:
    """Shape checks on filter keys whose wrong type would otherwise be read silently.

    Diagnostics:
        SST-PRS003: `labels` is not a list of strings.
    """
    filters_dir = project_dir / semantic_models_dir / "filters"
    diagnostics: list[Diagnostic] = []
    for document, index, node in _load_nodes(documents, filters_dir, _member_root("filter")):
        labels = node.get("labels")
        if labels is None or (isinstance(labels, list) and all(isinstance(label, str) for label in labels)):
            continue
        subject = f"filter:{node.get('name') or '<unnamed>'}"
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=subject,
                field="labels",
                expected="a list of strings",
                found=type(labels).__name__,
                subject=subject,
                origin=_node_origin(document, _member_root("filter"), index),
            )
        )
    return tuple(diagnostics)


def _window_parse_diagnostics(node: Mapping[str, Any], subject: str, origin: Origin) -> tuple[Diagnostic, ...]:
    """The shape of a metric's `window:` block; whether each entry resolves is checked with the models."""
    value = node.get("window")
    if value is None:
        return ()

    def wrong_type(field: str, expected: str, found: object) -> Diagnostic:
        return D(
            "SST-PRS003",
            artifact=subject,
            field=field,
            expected=expected,
            found=type(found).__name__,
            subject=subject,
            origin=origin,
        )

    if not isinstance(value, dict):
        return (wrong_type("window", "a mapping", value),)
    diagnostics: list[Diagnostic] = [
        # Snowflake's window metric grammar has neither clause.
        D("SST-PRS014", artifact=subject, field="window", other=other, subject=subject, origin=origin)
        for other in ("using_relationships", "non_additive_dimensions")
        if node.get(other)
    ]
    for field in ("partition_by", "partition_by_excluding"):
        entries = value.get(field)
        if entries is not None and (not isinstance(entries, list) or not all(isinstance(e, str) for e in entries)):
            diagnostics.append(wrong_type(f"window.{field}", "a list of references", entries))
    if value.get("partition_by") and value.get("partition_by_excluding"):
        diagnostics.append(
            D(
                "SST-PRS014",
                artifact=subject,
                field="window.partition_by",
                other="window.partition_by_excluding",
                subject=subject,
                origin=origin,
            )
        )
    order_by = value.get("order_by")
    if order_by is not None and not isinstance(order_by, list):
        diagnostics.append(wrong_type("window.order_by", "a list", order_by))
    for position, entry in enumerate(order_by if isinstance(order_by, list) else ()):
        field = f"window.order_by[{position}]"
        if isinstance(entry, str):
            continue
        if not isinstance(entry, dict):
            diagnostics.append(wrong_type(field, "a reference or a mapping", entry))
            continue
        if not isinstance(entry.get("ref"), str):
            diagnostics.append(D("SST-PRS002", artifact=subject, field=f"{field}.ref", subject=subject, origin=origin))
        for key, allowed in (("sort_direction", SORT_DIRECTIONS), ("null_order", NULL_ORDERS)):
            if key in entry and (not isinstance(entry[key], str) or entry[key] not in allowed):
                diagnostics.append(
                    D(
                        "SST-PRS013",
                        artifact=subject,
                        field=f"{field}.{key}",
                        found=entry[key],
                        expected=", ".join(allowed),
                        subject=subject,
                        origin=origin,
                    )
                )
    frame = value.get("frame")
    if frame is not None and _frame(frame) is None:
        diagnostics.append(D("SST-PRS124", artifact=subject, value=frame, subject=subject, origin=origin))
    return tuple(diagnostics)


def _list_of(value: object) -> list[Any]:
    return value if isinstance(value, list) else []


def _non_additive_parse_diagnostics(value: object, subject: str, origin: Origin) -> tuple[Diagnostic, ...]:
    """The shape of `non_additive_dimensions`; whether each entry resolves is checked with the models."""
    if value is None:
        return ()
    if not isinstance(value, list):
        return (
            D(
                "SST-PRS003",
                artifact=subject,
                field="non_additive_dimensions",
                expected="a list",
                found=type(value).__name__,
                subject=subject,
                origin=origin,
            ),
        )
    diagnostics: list[Diagnostic] = []
    for position, entry in enumerate(value):
        field = f"non_additive_dimensions[{position}]"
        if not isinstance(entry, dict):
            found = type(entry).__name__
            diagnostics.append(
                D(
                    "SST-PRS003",
                    artifact=subject,
                    field=field,
                    expected="a mapping",
                    found=found,
                    subject=subject,
                    origin=origin,
                )
            )
            continue
        dimension = entry.get("dimension")
        if not isinstance(dimension, str) or not dimension.strip():
            diagnostics.append(
                D("SST-PRS002", artifact=subject, field=f"{field}.dimension", subject=subject, origin=origin)
            )
        table = entry.get("table")
        if "table" in entry and (not isinstance(table, str) or not table.strip() or "{{" in table):
            diagnostics.append(
                D(
                    "SST-PRS003",
                    artifact=subject,
                    field=f"{field}.table",
                    expected="a bare model name",
                    found=repr(table),
                    subject=subject,
                    origin=origin,
                )
            )
        for key, allowed in (("sort_direction", SORT_DIRECTIONS), ("null_order", NULL_ORDERS)):
            if key in entry and (not isinstance(entry[key], str) or entry[key] not in allowed):
                diagnostics.append(
                    D(
                        "SST-PRS013",
                        artifact=subject,
                        field=f"{field}.{key}",
                        found=entry[key],
                        expected=", ".join(allowed),
                        subject=subject,
                        origin=origin,
                    )
                )
    return tuple(diagnostics)


def _metric_cycles(metrics: tuple[MetricDef, ...]) -> tuple[tuple[str, ...], ...]:
    graph = {metric.name.casefold(): metric.referenced_metrics for metric in metrics}
    cycles: list[tuple[str, ...]] = []
    visited: set[str] = set()
    active: list[str] = []

    def visit(name: str) -> None:
        if name in active:
            start = active.index(name)
            cycle = tuple(active[start:] + [name])
            variants = [tuple(cycle[index:-1] + cycle[:index] + (cycle[index],)) for index in range(len(cycle) - 1)]
            canonical = min(variants)
            if canonical not in cycles:
                cycles.append(canonical)
            return
        if name in visited or name not in graph:
            return
        active.append(name)
        for dependency in graph[name]:
            visit(dependency)
        active.pop()
        visited.add(name)

    for metric_name in sorted(graph):
        visit(metric_name)
    return tuple(cycles)


def _base_type(data_type: str | None) -> str:
    return re.sub(r"\s*\(.*\)\s*$", "", (data_type or "").strip().upper())


def _metric_diagnostics(
    metrics: tuple[MetricDef, ...],
    models: dict[str, DbtModel],
    variables: Mapping[str, object] | None = None,
) -> tuple[Diagnostic, ...]:
    diagnostics: list[Diagnostic] = []
    counts: dict[str, int] = {}
    for metric in metrics:
        key = metric.name.casefold()
        counts[key] = counts.get(key, 0) + 1
    duplicate_names = {name for name, count in counts.items() if count > 1}
    known_metrics = {metric.name.casefold() for metric in metrics}
    metric_by_name = {metric.name.casefold(): metric for metric in metrics}
    diagnostics.extend(
        D("SST-VAL001", type="metric", name=name, subject=f"metric:{name}") for name in sorted(duplicate_names)
    )
    for metric in metrics:
        if metric.name.casefold() in duplicate_names:
            continue
        if not metric.derived and not metric.has_tables_key:
            diagnostics.append(D("SST-VAL109", metric=metric.name, subject=f"metric:{metric.name}"))
        elif not metric.derived and not metric.tables:
            diagnostics.append(
                D(
                    "SST-PRS102",
                    artifact=f"metric:{metric.name}",
                    subject=f"metric:{metric.name}",
                )
            )
        if metric.derived and metric.has_tables_key:
            diagnostics.append(D("SST-VAL108", metric=metric.name, subject=f"metric:{metric.name}"))
        if metric.derived and metric.using_relationships:
            diagnostics.append(D("SST-VAL113", metric=metric.name, subject=f"metric:{metric.name}"))
        if len(metric.using_relationships) > 1:
            diagnostics.append(
                D(
                    "SST-VAL115",
                    metric=metric.name,
                    count=len(metric.using_relationships),
                    subject=f"metric:{metric.name}",
                )
            )
        if metric.access_modifier not in ("public_access", "private_access"):
            diagnostics.append(
                D(
                    "SST-VAL121",
                    metric=metric.name,
                    found=metric.access_modifier,
                    subject=f"metric:{metric.name}",
                )
            )
        referenced_tables = metric.tables or metric.referenced_models
        owner = referenced_tables[0] if len(referenced_tables) == 1 else None
        for entry in metric.non_additive:
            # Without `table:` the dimension belongs to the metric's own table.
            model = models.get((entry.table or owner or "").casefold())
            column = model.column(entry.dimension) if model is not None else None
            if column is None or column.excluded or column.column_type not in DIMENSION_TYPES:
                diagnostics.append(
                    D(
                        "SST-VAL118",
                        metric=metric.name,
                        value=f"{entry.table}.{entry.dimension}" if entry.table else entry.dimension,
                        subject=f"metric:{metric.name}",
                        origin=metric.origin,
                    )
                )
        if not metric.derived and metric.window is not None:
            diagnostics.extend(_window_diagnostics(metric, metric_by_name, models))
        elif not metric.derived and metric.tables and not is_aggregate_expression(metric.expr):
            diagnostics.append(
                D(
                    "SST-VAL101",
                    metric=metric.name,
                    subject=f"metric:{metric.name}",
                    origin=metric.origin,
                )
            )
        if metric.derived:
            window = re.search(
                r"\b([A-Z_][A-Z0-9_]*)\s*\([\s\S]*?\)\s+OVER\s*\(",
                metric.expr,
                re.IGNORECASE,
            )
            if window or metric.window is not None:
                diagnostics.append(
                    D(
                        "SST-VAL102",
                        function=window.group(1).upper() if window else _root_function(metric.expr) or "window",
                        metric=metric.name,
                        subject=f"metric:{metric.name}",
                    )
                )
            for referenced_metric in metric.referenced_metrics:
                pattern = (
                    rf"\b(?:SUM|AVG|MIN|MAX|COUNT|MEDIAN)\s*\([^)]*"
                    rf"metric\s*\(\s*['\"]{re.escape(referenced_metric)}['\"]\s*\)[^)]*\)"
                )
                if re.search(pattern, metric.expr, re.IGNORECASE):
                    diagnostics.append(
                        D(
                            "SST-VAL103",
                            metric=metric.name,
                            other=referenced_metric,
                            subject=f"metric:{metric.name}",
                        )
                    )
            for call in metric.calls:
                if call.function == "ref" and len(call.args) == 2:
                    diagnostics.append(
                        D(
                            "SST-VAL104",
                            metric=metric.name,
                            column=f"{call.args[0]}.{call.args[1]}",
                            subject=f"metric:{metric.name}",
                        )
                    )
            for member in re.findall(
                r"\b(?:fact|dimension)\s*\(\s*['\"]([^'\"]+)['\"]\s*\)",
                metric.expr,
                re.IGNORECASE,
            ):
                diagnostics.append(
                    D(
                        "SST-VAL105",
                        metric=metric.name,
                        member_type="member",
                        other=member,
                        subject=f"metric:{metric.name}",
                    )
                )
        else:
            for referenced_metric in metric.referenced_metrics:
                referenced = metric_by_name.get(referenced_metric)
                if referenced is None:
                    continue
                if referenced.derived:
                    diagnostics.append(
                        D(
                            "SST-VAL106",
                            metric=metric.name,
                            other=referenced.name,
                            subject=f"metric:{metric.name}",
                        )
                    )
                if referenced.non_additive:
                    diagnostics.append(
                        D(
                            "SST-VAL107",
                            metric=metric.name,
                            other=referenced.name,
                            subject=f"metric:{metric.name}",
                        )
                    )
        for referenced_metric in metric.referenced_metrics:
            if referenced_metric not in known_metrics:
                diagnostics.append(
                    D(
                        "SST-REF006",
                        origin=metric.origin,
                        subject=f"metric:{metric.name}",
                        name=referenced_metric,
                    )
                )
            elif metric_by_name[referenced_metric].window is not None:
                diagnostics.append(
                    D(
                        "SST-VAL128",
                        metric=metric.name,
                        other=metric_by_name[referenced_metric].name,
                        subject=f"metric:{metric.name}",
                        origin=metric.origin,
                    )
                )
        try:
            calls = scan_template_calls(metric.expr)
        except TemplateSyntaxError as exc:
            diagnostics.append(
                D(
                    "SST-LOD004",
                    origin=metric.origin,
                    file=metric.origin.file if metric.origin else "<expression>",
                    line=exc.line,
                    col=exc.col,
                    reason=exc.reason,
                    subject=f"metric:{metric.name}",
                )
            )
            continue
        for call in calls:
            if call.function == "var" and (
                len(call.args) != 1 or variables is not None and call.args[0] not in variables
            ):
                diagnostics.append(_var_diagnostic(call, metric.origin, f"metric:{metric.name}"))
                continue
            if call.function != "ref" or len(call.args) not in (1, 2):
                continue
            model_name = call.args[0]
            model = models.get(model_name.casefold())
            if model is None:
                diagnostics.append(
                    D(
                        "SST-REF001",
                        model=model_name,
                        subject=f"metric:{metric.name}",
                        origin=metric.origin,
                    )
                )
            elif len(call.args) == 2:
                column_name = call.args[1]
                column = model.column(column_name)
                if column is None:
                    diagnostics.append(
                        D(
                            "SST-REF002",
                            model=model_name,
                            column=column_name,
                            subject=f"metric:{metric.name}",
                            origin=metric.origin,
                        )
                    )
                elif column.excluded:
                    diagnostics.append(
                        D(
                            "SST-VAL318",
                            artifact=f"metric:{metric.name}",
                            member=metric.name,
                            column=column_name,
                            subject=f"metric:{metric.name}",
                            origin=metric.origin,
                        )
                    )
            if (
                model is not None
                and metric.tables
                and model_name.casefold() not in metric.tables
                and all(table in models for table in metric.tables)
            ):
                diagnostics.append(
                    D(
                        "SST-VAL112",
                        origin=metric.origin,
                        subject=f"metric:{metric.name}",
                        metric=metric.name,
                        artifact=f"metric:{metric.name}",
                        outside=model_name,
                    )
                )
        for identifier in _bare_column_identifiers(
            metric.expr,
            metric.tables,
            models,
            variables or {},
        ):
            diagnostics.append(
                D(
                    "SST-VAL110",
                    metric=metric.name,
                    column=identifier,
                    subject=f"metric:{metric.name}",
                    origin=metric.origin,
                )
            )
            break
    expressions: dict[tuple[str, tuple[str, ...], bool, tuple[SortKey, ...], WindowDef | None], str] = {}
    for metric in metrics:
        # The non-additive ordering and the window are part of what a metric
        # computes: the same SUM at the latest and at the earliest snapshot, or over
        # two windows, is two metrics, not one.
        canonical = (
            " ".join(metric.expr.split()).casefold(),
            tuple(sorted(metric.tables)),
            metric.derived,
            tuple(entry.key for entry in metric.non_additive),
            metric.window,
        )
        previous = expressions.get(canonical)
        if previous is not None and previous.casefold() != metric.name.casefold() and metric.tables:
            diagnostics.append(
                D(
                    "SST-VAL124",
                    metric=metric.name,
                    other=previous,
                    subject=f"metric:{metric.name}",
                    origin=metric.origin,
                )
            )
        else:
            expressions[canonical] = metric.name
    return tuple(diagnostics)


def _metric_owner(metric: MetricDef) -> str | None:
    """The one table a metric belongs to, casefolded; None for a cross-table metric."""
    tables = metric.tables or metric.referenced_models
    return tables[0] if len(tables) == 1 else None


def _window_diagnostics(
    metric: MetricDef,
    metric_by_name: Mapping[str, MetricDef],
    models: Mapping[str, DbtModel],
) -> list[Diagnostic]:
    """Snowflake's rules for a window function metric that its own compile does not report plainly."""
    window = metric.window
    assert window is not None
    subject = f"metric:{metric.name}"
    diagnostics: list[Diagnostic] = []
    arguments = call_arguments(metric.expr)
    # The window must apply to a metric or an aggregate: over a raw column it is a
    # row-level window, which Snowflake allows only in a fact or dimension.
    if not arguments or not is_aggregate_expression(arguments[0]):
        diagnostics.append(
            D(
                "SST-VAL126",
                metric=metric.name,
                function=_root_function(metric.expr) or "the expression",
                subject=subject,
                origin=metric.origin,
            )
        )
    owner = _metric_owner(metric)
    if owner is None:
        # Without exactly one table the metric renders at view level, as a derived
        # metric does, and cannot carry a window either.
        diagnostics.append(
            D(
                "SST-VAL102",
                function=_root_function(metric.expr) or "window",
                metric=metric.name,
                subject=subject,
                origin=metric.origin,
            )
        )
    for field, text in window.references():
        dimensions_only = field.startswith("partition_by_excluding")
        try:
            column_call = single_template_call(text, "ref")
            metric_call = single_template_call(text, "metric")
        except TemplateSyntaxError:
            column_call = metric_call = None
        resolves = False
        if column_call is not None and len(column_call.args) == 2:
            model = models.get(column_call.args[0].casefold())
            column = model.column(column_call.args[1]) if model is not None else None
            resolves = column is not None and not column.excluded and column.column_type in DIMENSION_TYPES
        elif metric_call is not None and len(metric_call.args) == 1 and not dimensions_only:
            other = metric_by_name.get(metric_call.args[0].casefold())
            resolves = (
                other is not None
                and not other.derived
                and other.window is None
                and owner is not None
                and _metric_owner(other) == owner
            )
        if not resolves:
            diagnostics.append(
                D(
                    "SST-VAL125",
                    metric=metric.name,
                    field=field,
                    value=text,
                    expected="a dimension" if dimensions_only else "a dimension or a metric of the same table",
                    subject=subject,
                    origin=metric.origin,
                )
            )
    if window.frame is not None and not window.order_by:
        diagnostics.append(
            D("SST-VAL127", metric=metric.name, value=window.frame, subject=subject, origin=metric.origin)
        )
    return diagnostics


def _expression_reference_diagnostics(
    members: tuple[FilterDef | VerifiedQueryDef, ...],
    models: dict[str, DbtModel],
    *,
    metric_names: frozenset[str],
    variables: Mapping[str, object],
) -> tuple[Diagnostic, ...]:
    diagnostics: list[Diagnostic] = []
    for member in members:
        text = member.expr if isinstance(member, FilterDef) else member.sql
        subject = f"{'filter' if isinstance(member, FilterDef) else 'verified_query'}:{member.name}"
        declared = {table.casefold() for table in member.tables}
        if isinstance(member, VerifiedQueryDef):
            for table_name in _sql_tables(member.sql):
                if declared and table_name not in declared:
                    diagnostics.append(
                        D("SST-VAL413", origin=member.origin, subject=subject, member=member.name, name=table_name)
                    )
        try:
            calls = scan_template_calls(text)
        except TemplateSyntaxError as exc:
            diagnostics.append(
                D(
                    "SST-LOD004",
                    origin=member.origin,
                    file=member.origin.file if member.origin else "<expression>",
                    line=exc.line,
                    col=exc.col,
                    reason=exc.reason,
                    subject=subject,
                )
            )
            continue
        for call in calls:
            if call.function == "metric" and isinstance(member, FilterDef):
                diagnostics.append(
                    D(
                        "SST-REF041",
                        origin=member.origin,
                        subject=subject,
                        artifact=subject,
                        function="metric",
                        field="a filter expression",
                    )
                )
                continue
            if call.function == "metric" and (len(call.args) != 1 or call.args[0].casefold() not in metric_names):
                if len(call.args) != 1:
                    diagnostics.append(
                        D(
                            "SST-REF042",
                            origin=member.origin,
                            subject=subject,
                            artifact=subject,
                            detail=f"metric() takes one name, found {len(call.args)} in {call.raw}",
                        )
                    )
                else:
                    diagnostics.append(D("SST-REF006", origin=member.origin, subject=subject, name=call.args[0]))
                continue
            if call.function == "var" and (len(call.args) != 1 or call.args[0] not in variables):
                diagnostics.append(_var_diagnostic(call, member.origin, subject))
                continue
            if call.function in ("table", "column"):
                # SST-REF034/SST-REF035 already name the legacy global at its position.
                continue
            if call.function not in ("ref", "metric", "var"):
                diagnostics.append(
                    D(
                        "SST-REF041",
                        origin=member.origin,
                        subject=subject,
                        artifact=subject,
                        function=call.function,
                        field="an expression",
                    )
                )
                continue
            if call.function != "ref" or len(call.args) not in (1, 2):
                continue
            model_name = call.args[0]
            model = models.get(model_name.casefold())
            if model is None:
                diagnostics.append(
                    D(
                        "SST-REF001",
                        origin=member.origin,
                        subject=subject,
                        model=model_name,
                    )
                )
            elif len(call.args) == 2:
                column_name = call.args[1]
                column = model.column(column_name)
                if column is None:
                    diagnostics.append(
                        D(
                            "SST-REF002",
                            origin=member.origin,
                            subject=subject,
                            model=model_name,
                            column=column_name,
                        )
                    )
                elif column.excluded:
                    diagnostics.append(
                        D(
                            "SST-VAL318",
                            origin=member.origin,
                            subject=subject,
                            artifact=subject,
                            member=member.name,
                            column=column_name,
                        )
                    )
            elif declared and model_name.casefold() not in declared:
                diagnostics.append(
                    D("SST-REF043", origin=member.origin, subject=subject, artifact=subject, model=model_name)
                )
    return tuple(diagnostics)


def _filter_diagnostics(filters: tuple[FilterDef, ...]) -> tuple[Diagnostic, ...]:
    diagnostics: list[Diagnostic] = []
    for filter_def in filters:
        boolean = _is_boolean_expression(filter_def.expr)
        if filter_def.entity_level and not boolean:
            code = "SST-VAL401"
        elif not filter_def.entity_level and not filter_def.labeled and boolean:
            # A boolean predicate with no labels: key has no native home; it
            # must carry labels: [filter] to render as a LABELS = (FILTER) dimension.
            code = "SST-VAL405"
        else:
            continue
        diagnostics.append(
            D(code, member=filter_def.name, subject=f"filter:{filter_def.name}", origin=filter_def.origin)
        )
    return tuple(diagnostics)


def _bare_column_identifiers(
    expression: str,
    tables: tuple[str, ...],
    models: Mapping[str, DbtModel],
    variables: Mapping[str, object],
) -> tuple[str, ...]:
    text = re.sub(r"\{\{.*?\}\}", " ", expression)
    text = re.sub(r"'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"", " ", text)
    functions = {match.group(1).casefold() for match in re.finditer(r"\b([A-Za-z_][A-Za-z0-9_$]*)\s*\(", text)}
    known_variables = {name.casefold() for name in variables}
    known_columns = {
        column.name.casefold()
        for table in tables
        for model in (models.get(table.casefold()),)
        if model is not None
        for column in model.columns
    }
    return tuple(
        dict.fromkeys(
            identifier
            for identifier in re.findall(r"\b[A-Za-z_][A-Za-z0-9_$]*\b", text)
            if identifier.casefold() in known_columns
            and identifier.casefold() not in functions
            and identifier.casefold() not in known_variables
        )
    )


def _dbt_column_diagnostics(
    models: dict[str, DbtModel],
    referenced_models: frozenset[str] | None = None,
) -> tuple[Diagnostic, ...]:
    sentinels = frozenset(("nan", "none", "null", "<na>"))
    diagnostics: list[Diagnostic] = []
    for model in sorted(models.values(), key=lambda item: item.name):
        is_referenced = referenced_models is None or model.name.casefold() in referenced_models
        for column in model.columns:
            subject = f"dbt_column:{model.name}.{column.name}"
            artifact = f"dbt_model:{model.name}"
            if is_referenced and column.column_type is None:
                diagnostics.append(
                    D(
                        "SST-VAL308",
                        artifact=artifact,
                        member=column.name,
                        subject=subject,
                    )
                )
            if is_referenced and column.data_type is None:
                diagnostics.append(
                    D(
                        "SST-VAL309",
                        artifact=artifact,
                        member=column.name,
                        subject=subject,
                    )
                )
            elif is_referenced and column.column_type == "fact" and _base_type(column.data_type) not in NUMERIC_TYPES:
                diagnostics.append(
                    D(
                        "SST-VAL305",
                        artifact=artifact,
                        member=column.name,
                        found=column.data_type,
                        subject=subject,
                    )
                )
            elif (
                is_referenced
                and column.column_type == "time_dimension"
                and _base_type(column.data_type) not in TEMPORAL_TYPES
            ):
                diagnostics.append(
                    D(
                        "SST-VAL306",
                        artifact=artifact,
                        member=column.name,
                        found=column.data_type,
                        subject=subject,
                    )
                )
            if is_referenced and column.is_enum and not column.sample_values:
                diagnostics.append(
                    D(
                        "SST-VAL314",
                        artifact=artifact,
                        member=column.name,
                        subject=subject,
                    )
                )
            elif is_referenced and not column.is_enum and len(column.sample_values) >= 5:
                diagnostics.append(
                    D(
                        "SST-VAL315",
                        artifact=artifact,
                        member=column.name,
                        count=len(column.sample_values),
                        subject=subject,
                    )
                )
            if is_referenced and column.description is None:
                diagnostics.append(
                    D(
                        "SST-VAL003",
                        type="column",
                        name=f"{model.name}.{column.name}",
                        subject=subject,
                    )
                )
            if is_referenced:
                diagnostics.extend(
                    D("SST-PRS004", artifact=subject, field=f"meta.sst.{key}", subject=subject)
                    for key in column.unknown_meta_keys
                )
            if is_referenced and column.declared_data_type is not None:
                diagnostics.append(
                    D(
                        "SST-DBT004",
                        model=model.name,
                        column=column.name,
                        found=column.data_type,
                        expected=column.declared_data_type,
                        subject=subject,
                    )
                )
            for value in column.sample_values:
                if value.casefold() in sentinels:
                    diagnostics.append(
                        D(
                            "SST-VAL316",
                            artifact=artifact,
                            member=column.name,
                            field="sample_values",
                            value=value,
                            subject=subject,
                        )
                    )
    return tuple(diagnostics)


def _dbt_model_diagnostics(
    models: dict[str, DbtModel],
    referenced_models: frozenset[str] | None = None,
) -> tuple[Diagnostic, ...]:
    diagnostics: list[Diagnostic] = []
    for model in sorted(models.values(), key=lambda item: item.name):
        subject = f"dbt_model:{model.name}"
        diagnostics.extend(
            D("SST-DBT030", model=model.name, key=key, subject=subject) for key in model.forbidden_location_keys
        )
        if referenced_models is not None and model.name.casefold() not in referenced_models:
            continue
        diagnostics.extend(
            D("SST-DBT005", model=model.name, field=field, subject=subject) for field in model.legacy_key_fields
        )
        diagnostics.extend(
            D("SST-PRS004", artifact=subject, field=f"meta.sst.{key}", subject=subject)
            for key in model.unknown_meta_keys
        )
        if not model.primary_key and not model.unique_keys and not model.legacy_key_fields:
            diagnostics.append(
                D(
                    "SST-VAL312",
                    artifact=subject,
                    name=model.name,
                    subject=subject,
                )
            )
        column_names = {column.name.casefold() for column in model.columns}
        declared_keys = tuple(model.primary_key) + tuple(
            column for unique_key in model.unique_keys for column in unique_key
        )
        for column in declared_keys:
            if column.casefold() not in column_names:
                diagnostics.append(
                    D(
                        "SST-VAL310",
                        artifact=subject,
                        column=column,
                        name=model.name,
                        subject=subject,
                    )
                )
        primary = {column.casefold(): column for column in model.primary_key}
        unique = {column.casefold(): column for unique_key in model.unique_keys for column in unique_key}
        for folded in sorted(primary.keys() & unique.keys()):
            diagnostics.append(
                D(
                    "SST-VAL223",
                    artifact=subject,
                    column=primary[folded],
                    model=model.name,
                    subject=subject,
                )
            )
    return tuple(diagnostics)


def _description_diagnostics(
    views: tuple[ParsedView, ...],
    metrics: tuple[MetricDef, ...],
) -> tuple[Diagnostic, ...]:
    diagnostics: list[Diagnostic] = []
    for view in views:
        if not str(view.source.get("description") or "").strip():
            diagnostics.append(
                D(
                    "SST-VAL003",
                    type="semantic_view",
                    name=view.name,
                    subject=f"semantic_view:{view.name}",
                    origin=view.origin,
                )
            )
    diagnostics.extend(
        D(
            "SST-VAL003",
            type="metric",
            name=metric.name,
            subject=f"metric:{metric.name}",
            origin=metric.origin,
        )
        for metric in metrics
        if metric.description is None
    )
    return tuple(diagnostics)


def _relationship_diagnostics(
    relationships: tuple[Relationship, ...],
    view_table_sets: tuple[tuple[str, frozenset[str]], ...],
    origins: Mapping[str, Origin] | None = None,
    models: Mapping[str, DbtModel] | None = None,
) -> tuple[Diagnostic, ...]:
    diagnostics: list[Diagnostic] = []
    for relationship in relationships:
        origin = (origins or {}).get(relationship.name.casefold())
        endpoints = {
            relationship.from_table.casefold(),
            relationship.to_table.casefold(),
        }
        subject = f"relationship:{relationship.name.casefold()}"
        if not any(endpoints.issubset(tables) for _, tables in view_table_sets):
            closest_name, closest_tables = max(
                view_table_sets,
                key=lambda item: len(endpoints.intersection(item[1])),
                default=("semantic_view:<none>", frozenset()),
            )
            missing = sorted(endpoints - closest_tables)[0]
            diagnostics.append(
                D(
                    "SST-VAL203",
                    relationship=relationship.name.casefold(),
                    name=missing,
                    artifact=closest_name,
                    subject=subject,
                    origin=origin,
                )
            )
            diagnostics.append(
                D(
                    "SST-MEM005",
                    member=subject,
                    type="semantic_view",
                    subject=subject,
                    caused_by="SST-VAL203",
                    origin=origin,
                )
            )
            continue
        target = (models or {}).get(relationship.to_table.casefold())
        if target is None:
            continue
        if relationship.asof_index is not None or relationship.range_bounds is not None:
            continue
        join_columns = {column.casefold() for column in relationship.to_columns}
        keys = tuple(key for key in (target.primary_key, *target.unique_keys) if key)
        if not keys:
            # Snowflake refuses a REFERENCES target that declares no key at all.
            diagnostics.append(
                D(
                    "SST-VAL311",
                    artifact=subject,
                    name=target.name,
                    subject=subject,
                    origin=origin,
                )
            )
        elif not any({column.casefold() for column in key}.issubset(join_columns) for key in keys):
            diagnostics.append(
                D(
                    "SST-VAL210",
                    relationship=relationship.name.casefold(),
                    name=target.name,
                    value=", ".join(relationship.to_columns),
                    subject=subject,
                    origin=origin,
                )
            )
    return tuple(diagnostics)


def _relationship_cycle_diagnostics(
    relationships: tuple[Relationship, ...],
    view_table_sets: tuple[tuple[str, frozenset[str]], ...],
) -> tuple[Diagnostic, ...]:
    """Snowflake refuses a view whose relationships form a cycle, a self-reference included."""
    diagnostics: list[Diagnostic] = []
    for artifact, tables in view_table_sets:
        edges: dict[str, set[str]] = {}
        for relationship in relationships:
            left, right = relationship.from_table.casefold(), relationship.to_table.casefold()
            if left in tables and right in tables:
                edges.setdefault(left, set()).add(right)
        cycle = _first_cycle({node: tuple(sorted(targets)) for node, targets in edges.items()})
        if cycle is not None:
            diagnostics.append(D("SST-VAL215", artifact=artifact, cycle=" -> ".join(cycle), subject=artifact))
    return tuple(diagnostics)


def _first_cycle(edges: Mapping[str, tuple[str, ...]]) -> tuple[str, ...] | None:
    visiting: list[str] = []
    done: set[str] = set()

    def visit(node: str) -> tuple[str, ...] | None:
        visiting.append(node)
        for target in edges.get(node, ()):
            if target in visiting:
                return (*visiting[visiting.index(target) :], target)
            if target not in done:
                found = visit(target)
                if found is not None:
                    return found
        visiting.pop()
        done.add(node)
        return None

    for node in sorted(edges):
        if node not in done:
            found = visit(node)
            if found is not None:
                return found
    return None


def _relationship_parse_diagnostics(
    documents: RawDocuments,
    project_dir: Path,
    semantic_models_dir: str,
) -> tuple[Diagnostic, ...]:
    root = project_dir / semantic_models_dir / "relationships"
    diagnostics: list[Diagnostic] = []
    for document, index, node in _load_nodes(documents, root, _member_root("relationship")):
        if not node.get("name"):
            continue
        conditions = node.get("relationship_conditions")
        # The 0.3 `relationship_columns` spelling is reported as renamed instead.
        if (not isinstance(conditions, list) or not conditions) and "relationship_columns" not in node:
            name = str(node["name"])
            diagnostics.append(
                D(
                    "SST-VAL201",
                    relationship=name,
                    subject=f"relationship:{name}",
                    origin=_node_origin(document, _member_root("relationship"), index),
                )
            )
    return tuple(diagnostics)


def _multipath_diagnostics(
    relationships: tuple[Relationship, ...],
    metrics: tuple[MetricDef, ...],
    view_table_sets: tuple[tuple[str, frozenset[str]], ...],
) -> tuple[Diagnostic, ...]:
    pair_counts: dict[tuple[str, str], int] = {}
    for relationship in relationships:
        first, second = sorted((relationship.from_table.casefold(), relationship.to_table.casefold()))
        pair = (first, second)
        pair_counts[pair] = pair_counts.get(pair, 0) + 1
    diagnostics: list[Diagnostic] = []
    for pair, count in sorted(pair_counts.items()):
        if count < 2:
            continue
        a, b = pair
        diagnostics.extend(
            D("SST-VAL209", artifact=artifact, count=count, a=a, b=b)
            for artifact, tables in view_table_sets
            if {a, b}.issubset(tables)
        )
        for metric in metrics:
            if not metric.tables or metric.using_relationships:
                continue
            owner = metric.tables[0]
            if owner == a:
                diagnostics.append(
                    D(
                        "SST-VAL116",
                        metric=metric.name,
                        count=count,
                        name=b,
                        subject=f"metric:{metric.name}",
                    )
                )
                break
            elif owner == b:
                diagnostics.append(
                    D(
                        "SST-VAL116",
                        metric=metric.name,
                        count=count,
                        name=a,
                        subject=f"metric:{metric.name}",
                    )
                )
                break
    return tuple(diagnostics)


def _resolve_expression(
    text: str,
    *,
    policy: Any,
    origin: Origin,
    catalog: DbtCatalog,
    logical_by_model: Mapping[str, str],
    metric_names: Mapping[str, str],
    instruction_names: frozenset[str],
    variables: Mapping[str, object],
    field: str,
) -> str:
    context = ResolveContext(
        catalog,
        metric_names=frozenset(metric_names),
        metric_values=metric_names,
        instruction_names=instruction_names,
        variables=variables,
    )
    resolved, diagnostics = resolve_scalar(
        text,
        policy,
        origin,
        context,
        field=field,
        ref_value=lambda call: logical_by_model.get(call.args[0].casefold(), call.raw),
    )
    if diagnostics:
        raise ProjectError(
            "; ".join(diagnostic.message for diagnostic in diagnostics),
            diagnostics=tuple(diagnostics),
        )
    return resolved.text


def _table_refs(value: object) -> tuple[str, ...]:
    refs: list[str] = []
    for raw in value if isinstance(value, list) else []:
        try:
            call = single_template_call(str(raw), "ref")
        except TemplateSyntaxError as exc:
            diagnostic = D(
                "SST-LOD004",
                file="<tables>",
                line=exc.line,
                col=exc.col,
                reason=exc.reason,
            )
            raise ProjectError(diagnostic.message, diagnostics=(diagnostic,)) from exc
        if call is not None:
            if len(call.args) != 1:
                raise ProjectError(f"table attachment ref() must have one argument, found {raw!r}")
            refs.append(call.args[0].casefold())
            continue
        legacy = single_template_call(str(raw), "table")
        if legacy is not None and len(legacy.args) == 1:
            refs.append(legacy.args[0].casefold())
            continue
        literal = str(raw).strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", literal):
            raise ProjectError(f"table attachment must be a model name, found {raw!r}")
        refs.append(literal.casefold())
    return tuple(refs)


def _safe_table_refs(value: object) -> tuple[str, ...]:
    try:
        return _table_refs(value)
    except ProjectError:
        return ()


def _table_refs_poisoned(value: object) -> bool:
    try:
        _table_refs(value)
    except ProjectError:
        return True
    return False


@dataclass(frozen=True, slots=True)
class FilterDef:
    name: str
    expr: str
    description: str | None
    tables: tuple[str, ...]
    entity_level: bool
    origin: Origin | None = None
    template_calls: tuple[TemplateCall, ...] = ()
    poisoned: bool = False
    labeled: bool = False


@dataclass(frozen=True, slots=True)
class InstructionDef:
    name: str
    ai_sql_generation: str | None
    ai_question_categorization: str | None
    origin: Origin | None = None
    poisoned: bool = False


@dataclass(frozen=True, slots=True)
class VerifiedQueryDef:
    name: str
    question: str
    sql: str
    tables: tuple[str, ...]
    verified_at: int | None
    verified_by: str | None
    onboarding_question: bool | None
    origin: Origin | None = None
    template_calls: tuple[TemplateCall, ...] = ()
    poisoned: bool = False


# The keys the loader reads, per semantic-model node. Anything else is reported,
# because a key the loader skips changes nothing in the DDL and would otherwise
# pass review looking as though it does.
AUTHORED_KEYS: Mapping[str, frozenset[str]] = MappingProxyType(
    {
        "semantic_view": frozenset(
            (
                "name",
                "description",
                "tables",
                "table_config",
                "custom_instructions",
                "variables",
                "tags",
                "max_staleness",
                "enabled",
            )
        ),
        "metric": frozenset(
            (
                "name",
                "expr",
                "description",
                "synonyms",
                "tables",
                "derived",
                "using_relationships",
                "non_additive_dimensions",
                "access_modifier",
                "window",
            )
        ),
        "filter": frozenset(("name", "expr", "description", "tables", "labels")),
        # Snowflake has no clause for a description on the next three: it documents
        # the YAML for its readers and is never published.
        "custom_instruction": frozenset(("name", "description", "ai_sql_generation", "ai_question_categorization")),
        "verified_query": frozenset(
            (
                "name",
                "description",
                "question",
                "sql",
                "sql_file",
                "tables",
                "verified_at",
                "verified_by",
                "use_as_onboarding_question",
            )
        ),
        "relationship": frozenset(("name", "description", "left_table", "right_table", "relationship_conditions")),
    }
)
# Mappings below a node, by the scope that holds them: a block, each item of a list
# field, or each value of a name map.
NESTED_KEYS: Mapping[tuple[str, str], frozenset[str]] = MappingProxyType(
    {
        ("semantic_view", "table_config"): frozenset(("synonyms", "distinct_range")),
        ("semantic_view", "variables"): frozenset(("name", "data_type", "default_value", "description")),
        ("semantic_view", "tags"): frozenset(("name", "value")),
        ("metric", "non_additive_dimensions"): frozenset(("dimension", "table", "sort_direction", "null_order")),
        ("metric", "window"): frozenset(("partition_by", "partition_by_excluding", "order_by", "frame")),
        ("metric.window", "order_by"): frozenset(("ref", "sort_direction", "null_order")),
    }
)
NAME_MAPS = frozenset((("semantic_view", "table_config"),))
# Fields whose value is one mapping; every other nested field is a list.
BLOCKS = frozenset((("metric", "window"),))
# 0.3 spellings, by the node or entry that carries them. Each is an error naming
# the 1.0 key: reading the old spelling would keep two dialects alive.
RENAMED_KEYS: Mapping[tuple[str, str], str] = MappingProxyType(
    {
        ("custom_instruction", "sql_generation"): "ai_sql_generation",
        ("custom_instruction", "question_categorization"): "ai_question_categorization",
        ("relationship", "relationship_columns"): "relationship_conditions",
        ("metric", "visibility"): "access_modifier",
        ("metric", "non_additive_by"): "non_additive_dimensions",
        ("metric.non_additive_dimensions", "order"): "sort_direction",
        ("metric.non_additive_dimensions", "nulls"): "null_order",
        ("metric.window.order_by", "column"): "ref",
        ("metric.window.order_by", "direction"): "sort_direction",
    }
)


def _authored_key_diagnostics(documents: RawDocuments) -> tuple[Diagnostic, ...]:
    """Every key the loader does not read: SST-PRS020 for a 0.3 spelling, SST-PRS004 otherwise."""
    diagnostics: list[Diagnostic] = []
    for document in documents.documents:
        for node_type, allowed in AUTHORED_KEYS.items():
            root_key = _node_root(node_type)
            nodes = document.tree.get(root_key) if root_key in document.root_keys else None
            for index, node in enumerate(nodes if isinstance(nodes, list) else ()):
                if isinstance(node, dict):
                    subject = f"{node_type}:{node.get('name') or index}"
                    diagnostics.extend(_unread_keys(document, subject, node_type, (root_key, index), "", node, allowed))
    return tuple(diagnostics)


# Types whose duplicates SST-VAL001 already reports.
_NAMED_ELSEWHERE = frozenset(("semantic_view", "metric"))


def _member_name_diagnostics(documents: RawDocuments) -> tuple[Diagnostic, ...]:
    """A node with no name (SST-PRS107) or a name its type already uses (SST-PRS106).

    Either would otherwise be skipped or overwritten without a word.
    """
    diagnostics: list[Diagnostic] = []
    seen: set[tuple[str, str]] = set()
    for document in documents.documents:
        for node_type in AUTHORED_KEYS:
            root_key = _node_root(node_type)
            nodes = document.tree.get(root_key) if root_key in document.root_keys else None
            for index, node in enumerate(nodes if isinstance(nodes, list) else ()):
                origin = _node_origin(document, root_key, index)
                if not isinstance(node, dict) or not node.get("name"):
                    diagnostics.append(
                        D(
                            "SST-PRS107",
                            artifact=document.path,
                            member_type=node_type,
                            index=index,
                            subject=f"{node_type}:{index}",
                            origin=origin,
                        )
                    )
                    continue
                name = str(node["name"])
                if node_type in _NAMED_ELSEWHERE:
                    continue
                if (node_type, name.casefold()) in seen:
                    diagnostics.append(
                        D(
                            "SST-PRS106",
                            artifact=document.path,
                            member_type=node_type,
                            name=name,
                            subject=f"{node_type}:{name}",
                            origin=origin,
                        )
                    )
                seen.add((node_type, name.casefold()))
    return tuple(diagnostics)


def _unread_keys(
    document: RawDocument,
    subject: str,
    scope: str,
    path: NodePath,
    prefix: str,
    mapping: Mapping[Any, Any],
    allowed: frozenset[str],
) -> list[Diagnostic]:
    diagnostics = [
        _unread_key(document, (*path, str(key)), scope, f"{prefix}{key}", subject)
        for key in mapping
        if str(key) not in allowed
    ]
    for (owner, field), keys in NESTED_KEYS.items():
        value = mapping.get(field) if owner == scope else None
        if isinstance(value, list):
            entries: tuple[tuple[str | int | None, object, str], ...] = tuple(
                (key, entry, f"{prefix}{field}[{key}].") for key, entry in enumerate(value)
            )
        elif isinstance(value, dict) and (owner, field) in NAME_MAPS:
            entries = tuple((str(key), entry, f"{prefix}{field}.{key}.") for key, entry in value.items())
        elif isinstance(value, dict) and (owner, field) in BLOCKS:
            entries = ((None, value, f"{prefix}{field}."),)
        else:
            # Absent, or the wrong shape, which the field's own check reports.
            entries = ()
        for key, entry, label in entries:
            if isinstance(entry, dict):
                where: NodePath = (*path, field) if key is None else (*path, field, key)
                diagnostics.extend(_unread_keys(document, subject, f"{owner}.{field}", where, label, entry, keys))
    return diagnostics


def _node_root(node_type: str) -> str:
    if node_type == "semantic_view":
        root_key = SEMANTIC_REGISTRY.artifacts[node_type].root_key
        assert root_key is not None
        return root_key
    return _member_root(node_type)


def _unread_key(document: RawDocument, path: NodePath, scope: str, field: str, subject: str) -> Diagnostic:
    position = document.position(path)
    origin = Origin(
        document.path,
        position.line if position is not None else None,
        position.col if position is not None else None,
    )
    renamed = RENAMED_KEYS.get((scope, str(path[-1])))
    if renamed is not None:
        return D("SST-PRS020", origin=origin, subject=subject, artifact=subject, field=field, expected=renamed)
    return D("SST-PRS004", origin=origin, subject=subject, artifact=subject, field=field)


def _uses_legacy_globals(value: object) -> bool:
    try:
        calls = scan_template_calls(str(value))
    except TemplateSyntaxError:
        return False
    return any(call.function in ("table", "column") for call in calls)


def _legacy_reference_diagnostics(documents: RawDocuments) -> tuple[Diagnostic, ...]:
    diagnostics: list[Diagnostic] = []

    def walk(value: Any, file: str) -> None:
        if isinstance(value, str):
            try:
                calls = scan_template_calls(value)
            except TemplateSyntaxError:
                return
            for call in calls:
                if call.function == "table" and len(call.args) == 1:
                    diagnostics.append(
                        D(
                            "SST-REF034",
                            file=file,
                            line=call.line,
                            col=call.col,
                            model=call.args[0],
                        )
                    )
                elif call.function == "column" and len(call.args) == 2:
                    diagnostics.append(
                        D(
                            "SST-REF035",
                            file=file,
                            line=call.line,
                            col=call.col,
                            model=call.args[0],
                            column=call.args[1],
                        )
                    )
            return
        if isinstance(value, list):
            for item in value:
                walk(item, file)
        elif isinstance(value, Mapping):
            for item in value.values():
                walk(item, file)

    for document in documents.documents:
        walk(document.tree, document.path)
    return tuple(diagnostics)


def _node_origin(document: RawDocument, root_key: str, index: int, field: str | None = None) -> Origin:
    path: NodePath = (root_key, index) if field is None else (root_key, index, field)
    position = document.position(path)
    return Origin(
        file=document.path,
        line=position.line if position is not None else None,
        col=position.col if position is not None else None,
    )


def _load_nodes(
    documents: RawDocuments, directory: Path, root_key: str
) -> tuple[tuple[RawDocument, int, dict[str, Any]], ...]:
    del directory
    loaded: list[tuple[RawDocument, int, dict[str, Any]]] = []
    for document in documents.documents:
        if root_key not in document.root_keys:
            continue
        for index, node in enumerate(document.tree.get(root_key) or []):
            if isinstance(node, dict):
                loaded.append((document, index, _normalise_node_strings(node)))
    return tuple(loaded)


def _member_root(name: str) -> str:
    return SEMANTIC_REGISTRY.members[name].root_key


def _normalise_node_strings(node: dict[str, Any]) -> dict[str, Any]:
    return {
        str(key): (
            " ".join(value.splitlines()).strip() if str(key) == "description" and isinstance(value, str) else value
        )
        for key, value in node.items()
    }


def load_filters(documents: RawDocuments, project_dir: Path, semantic_models_dir: str) -> tuple[FilterDef, ...]:
    out: list[FilterDef] = []
    root = project_dir / semantic_models_dir / "filters"
    for document, index, node in _load_nodes(documents, root, _member_root("filter")):
        if not node.get("name") or not node.get("expr"):
            continue
        labels = node.get("labels")
        labels = {str(label).casefold() for label in labels} if isinstance(labels, list) else set()
        expression = str(node["expr"])
        try:
            template_calls = scan_template_calls(expression)
        except TemplateSyntaxError:
            template_calls = ()
        out.append(
            FilterDef(
                name=str(node["name"]),
                expr=expression,
                description=str(node.get("description") or "").strip() or None,
                tables=_safe_table_refs(node.get("tables")),
                entity_level="filter" in labels,
                origin=_node_origin(document, _member_root("filter"), index),
                template_calls=template_calls,
                poisoned=_table_refs_poisoned(node.get("tables")),
                labeled="labels" in node,
            )
        )
    return tuple(out)


def load_instructions(
    documents: RawDocuments, project_dir: Path, semantic_models_dir: str
) -> dict[str, InstructionDef]:
    root = project_dir / semantic_models_dir / "custom_instructions"
    out: dict[str, InstructionDef] = {}
    for document, index, node in _load_nodes(documents, root, _member_root("custom_instruction")):
        if not node.get("name"):
            continue
        name = str(node["name"])
        out[name.casefold()] = InstructionDef(
            name=name,
            ai_sql_generation=str(node.get("ai_sql_generation") or "").strip() or None,
            ai_question_categorization=str(node.get("ai_question_categorization") or "").strip() or None,
            origin=_node_origin(document, _member_root("custom_instruction"), index),
        )
    return out


def _sql_tables(sql: str) -> tuple[str, ...]:
    """The logical tables a verified query reads: FROM/JOIN names that are not its own CTEs.

    String literals are masked first, so `IN ('Join Flow')` is not read as a join.
    """
    lines = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
    statement = re.sub(r"'(?:[^']|'')*'", "''", lines)
    names = re.findall(r"(?i)\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_$]*)", statement)
    ctes = {
        name.casefold()
        for name in re.findall(r"(?i)(?:\bWITH\s+(?:RECURSIVE\s+)?|,\s*)([A-Za-z_][A-Za-z0-9_$]*)\s+AS\s*\(", statement)
    }
    return tuple(dict.fromkeys(name.casefold() for name in names if name.casefold() not in ctes))


def _resolve_verified_query_sql(
    query: VerifiedQueryDef,
    logical_by_model: Mapping[str, str],
    resolved_metric_names: Mapping[str, str],
    config: dict[str, Any],
    catalog: DbtCatalog,
) -> str:
    raw_variables = config.get("vars")
    variables: dict[str, object] = (
        {str(key): value for key, value in raw_variables.items()} if isinstance(raw_variables, dict) else {}
    )
    return _resolve_expression(
        query.sql,
        policy=VQR_SQL,
        origin=query.origin or Origin("<verified-query>"),
        catalog=catalog,
        logical_by_model=logical_by_model,
        metric_names=resolved_metric_names,
        instruction_names=frozenset(),
        variables=variables,
        field="verified_query.sql",
    )


def _strip_sql_header(sql: str) -> str:
    lines = sql.splitlines()
    while lines and (not lines[0].strip() or lines[0].lstrip().startswith("--")):
        lines.pop(0)
    return "\n".join(lines).rstrip()


def _verified_at(value: object, *, path: Path, name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            from datetime import datetime, timezone

            return int(datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
        except ValueError as exc:
            raise ProjectError(f"{path}: verified query {name} has invalid verified_at {value!r}") from exc
    raise ProjectError(f"{path}: verified query {name} has unsupported verified_at {value!r}")


def load_verified_queries(
    documents: RawDocuments, project_dir: Path, semantic_models_dir: str
) -> tuple[VerifiedQueryDef, ...]:
    root = project_dir / semantic_models_dir / "verified_queries"
    out: list[VerifiedQueryDef] = []
    for document, index, node in _load_nodes(documents, root, _member_root("verified_query")):
        if not node.get("name") or not node.get("question"):
            continue
        has_sql = node.get("sql") is not None
        has_sql_file = node.get("sql_file") is not None
        if has_sql == has_sql_file:
            continue
        sql = str(node.get("sql") or "")
        if not sql and node.get("sql_file"):
            sql_path = document.abs_path.parent / str(node["sql_file"])
            try:
                sql = sql_path.read_text(encoding="utf-8").rstrip("\n")
            except OSError:
                continue
            if not sql.strip():
                continue
        sql = _strip_sql_header(sql)
        name = str(node["name"])
        try:
            template_calls = scan_template_calls(sql)
        except TemplateSyntaxError:
            template_calls = ()
        out.append(
            VerifiedQueryDef(
                name=name,
                question=str(node["question"]),
                sql=sql,
                tables=_safe_table_refs(node.get("tables")),
                verified_at=_verified_at(node.get("verified_at"), path=document.abs_path, name=name),
                verified_by=str(node.get("verified_by") or "").strip() or None,
                onboarding_question=(
                    bool(node["use_as_onboarding_question"]) if "use_as_onboarding_question" in node else None
                ),
                origin=_node_origin(document, _member_root("verified_query"), index),
                template_calls=template_calls,
                poisoned=_table_refs_poisoned(node.get("tables")),
            )
        )
    return tuple(out)


def _verified_query_diagnostics(
    documents: RawDocuments, project_dir: Path, semantic_models_dir: str
) -> tuple[Diagnostic, ...]:
    root = project_dir / semantic_models_dir / "verified_queries"
    diagnostics: list[Diagnostic] = []
    for document, index, node in _load_nodes(documents, root, _member_root("verified_query")):
        name = str(node.get("name") or "<unnamed>")
        subject = f"verified_query:{name}"
        origin = _node_origin(document, _member_root("verified_query"), index)
        has_sql = node.get("sql") is not None
        has_sql_file = node.get("sql_file") is not None
        if has_sql == has_sql_file:
            diagnostics.append(
                D(
                    "SST-VAL412",
                    member=name,
                    detail=("sql and sql_file are both present" if has_sql else "neither sql nor sql_file is present"),
                    subject=subject,
                    origin=origin,
                )
            )
            continue
        if has_sql_file:
            sql_path = document.abs_path.parent / str(node["sql_file"])
            if not sql_path.is_file():
                diagnostics.append(
                    D(
                        "SST-LOD018",
                        file=document.path,
                        path=str(node["sql_file"]),
                        subject=subject,
                        origin=origin,
                    )
                )
            elif not sql_path.read_bytes():
                diagnostics.append(
                    D(
                        "SST-LOD019",
                        path=str(node["sql_file"]),
                        file=document.path,
                        subject=subject,
                        origin=origin,
                    )
                )
    return tuple(diagnostics)


_ENDPOINT_REF = re.compile(r"^\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}$")


def _endpoint_diagnostics(node: Mapping[str, Any], subject: str, origin: Origin) -> tuple[Diagnostic, ...]:
    """A 0.3 `{{ ref('x') }}` endpoint: `left_table` and `right_table` take the bare model name."""
    return tuple(
        D("SST-REF045", origin=origin, subject=subject, artifact=subject, field=field, found=str(node[field]).strip())
        for field in ("left_table", "right_table")
        if _ENDPOINT_REF.fullmatch(str(node.get(field) or "").strip())
    )


def load_relationships(
    documents: RawDocuments,
    project_dir: Path,
    semantic_models_dir: str,
    models: Mapping[str, DbtModel] | None = None,
) -> tuple[tuple[tuple[Relationship, Origin], ...], tuple[Diagnostic, ...]]:
    """Every well-formed relationship, and one diagnostic for each that is not.

    A malformed relationship is reported and left out rather than stopping the
    load, so one bad entry cannot hide every other diagnostic in the project.
    """
    root = project_dir / semantic_models_dir / "relationships"
    out: list[tuple[Relationship, Origin]] = []
    diagnostics: list[Diagnostic] = []
    equality = re.compile(
        r"^\s*\{\{\s*ref\(['\"]([^'\"]+)['\"],\s*['\"]([^'\"]+)['\"]\)\s*\}\}\s*=\s*"
        r"\{\{\s*ref\(['\"]([^'\"]+)['\"],\s*['\"]([^'\"]+)['\"]\)\s*\}\}\s*$"
    )
    asof = re.compile(
        r"^\s*\{\{\s*ref\(['\"]([^'\"]+)['\"],\s*['\"]([^'\"]+)['\"]\)\s*\}\}\s*>=\s*"
        r"\{\{\s*ref\(['\"]([^'\"]+)['\"],\s*['\"]([^'\"]+)['\"]\)\s*\}\}\s*$"
    )
    range_condition = re.compile(
        r"^\s*\{\{\s*ref\(['\"]([^'\"]+)['\"],\s*['\"]([^'\"]+)['\"]\)\s*\}\}\s+BETWEEN\s+"
        r"\{\{\s*ref\(['\"]([^'\"]+)['\"],\s*['\"]([^'\"]+)['\"]\)\s*\}\}\s+AND\s+"
        r"\{\{\s*ref\(['\"]([^'\"]+)['\"],\s*['\"]([^'\"]+)['\"]\)\s*\}\}\s*$",
        re.IGNORECASE,
    )
    for document, index, node in _load_nodes(documents, root, _member_root("relationship")):
        if not node.get("name"):
            continue
        raw_conditions = node.get("relationship_conditions")
        if not isinstance(raw_conditions, list) or not raw_conditions:
            continue
        if any(_uses_legacy_globals(condition) for condition in raw_conditions):
            # SST-REF034/SST-REF035 report the legacy globals; parsing further
            # would only bury that diagnostic under a condition-shape error.
            continue
        pairs: list[tuple[str, str]] = []
        asof_index: int | None = None
        range_bounds: tuple[str, str] | None = None
        origin = _node_origin(document, _member_root("relationship"), index)
        subject = f"relationship:{node['name']}"
        endpoint_problems = _endpoint_diagnostics(node, subject, origin)
        if endpoint_problems:
            diagnostics.extend(endpoint_problems)
            continue
        left_endpoint = str(node.get("left_table")).strip()
        right_endpoint = str(node.get("right_table")).strip()
        problem: Diagnostic | None = None
        for condition in raw_conditions:
            match = equality.fullmatch(str(condition))
            if match is not None:
                left_table, left_column, right_table, right_column = match.groups()
            else:
                match = asof.fullmatch(str(condition))
                if match is not None:
                    left_table, left_column, right_table, right_column = match.groups()
                    asof_index = len(pairs)
                else:
                    range_match = range_condition.fullmatch(str(condition))
                    if range_match is None or range_match.group(3).casefold() != range_match.group(5).casefold():
                        problem = D("SST-PRS110", origin=origin, subject=subject, artifact=subject, value=condition)
                        break
                    (
                        left_table,
                        left_column,
                        right_table,
                        range_start,
                        _range_table,
                        range_end,
                    ) = range_match.groups()
                    right_column = range_start
                    range_bounds = (range_start.upper(), range_end.upper())
            endpoints = (left_endpoint, right_endpoint)
            mismatch = next(
                (
                    (table, column, endpoint)
                    for table, column, endpoint in (
                        (left_table, left_column, endpoints[0]),
                        (right_table, right_column, endpoints[1]),
                    )
                    if table.casefold() != endpoint.casefold()
                ),
                None,
            )
            if mismatch is not None:
                table, column, endpoint = mismatch
                problem = D(
                    "SST-VAL204",
                    origin=origin,
                    subject=subject,
                    relationship=node["name"],
                    column=f"{table}.{column}",
                    name=endpoint,
                )
                break
            pairs.append((left_column.upper(), right_column.upper()))
        if problem is None and models is not None:
            for table_name, column_name in (
                pair
                for left_column, right_column in pairs
                for pair in (
                    (left_endpoint.casefold(), left_column),
                    (right_endpoint.casefold(), right_column),
                )
            ):
                model = models.get(table_name)
                if model is None:
                    problem = D("SST-REF001", origin=origin, subject=subject, model=table_name)
                    break
                if model.column(column_name) is None:
                    problem = D("SST-REF002", origin=origin, subject=subject, model=table_name, column=column_name)
                    break
        if problem is not None:
            diagnostics.append(problem)
            continue
        if pairs and len(pairs) == len(raw_conditions):
            out.append(
                (
                    Relationship(
                        name=str(node["name"]).upper(),
                        from_table=left_endpoint.upper(),
                        from_columns=tuple(left for left, _ in pairs),
                        to_table=right_endpoint.upper(),
                        to_columns=tuple(right for _, right in pairs),
                        asof_index=asof_index,
                        range_bounds=range_bounds,
                    ),
                    _node_origin(document, _member_root("relationship"), index),
                )
            )
    return tuple(out), tuple(diagnostics)


def parse_semantic_project(
    documents: RawDocuments,
    project_dir: Path,
    semantic_models_dir: str,
    models: Mapping[str, DbtModel] | None = None,
) -> ParsedProject:
    """Parse loaded trees into immutable unresolved view and member records."""
    views: list[ParsedView] = []
    members: list[ParsedMember] = []
    view_root = SEMANTIC_REGISTRY.artifacts["semantic_view"].root_key
    assert view_root is not None
    for document in documents.documents:
        for index, node in enumerate(document.tree.get(view_root) or []):
            if not isinstance(node, dict) or not node.get("name"):
                continue
            calls: list[TemplateCall] = []
            malformed = False
            for raw in node.get("tables") or []:
                try:
                    calls.extend(scan_template_calls(str(raw)))
                except TemplateSyntaxError:
                    malformed = True
            views.append(
                ParsedView(
                    name=str(node["name"]),
                    origin=_node_origin(document, view_root, index),
                    source_path=document.path,
                    source=MappingProxyType({str(key): value for key, value in node.items()}),
                    declared_tables=_safe_table_refs(node.get("tables")),
                    template_calls=tuple(calls),
                    poisoned=malformed,
                )
            )

    metrics = load_metrics(documents, project_dir, semantic_models_dir)
    filters = load_filters(documents, project_dir, semantic_models_dir)
    instructions = load_instructions(documents, project_dir, semantic_models_dir)
    verified_queries = load_verified_queries(documents, project_dir, semantic_models_dir)
    relationship_records, relationship_diagnostics = load_relationships(
        documents, project_dir, semantic_models_dir, models
    )
    members.extend(
        ParsedMember(
            "metric",
            metric.name,
            metric.origin or Origin("<unknown>"),
            metric,
            metric.tables if metric.has_tables_key else None,
            metric.template_calls,
            metric.poisoned,
        )
        for metric in metrics
    )
    members.extend(
        ParsedMember(
            "filter",
            filter_def.name,
            filter_def.origin or Origin("<unknown>"),
            filter_def,
            filter_def.tables,
            filter_def.template_calls,
            filter_def.poisoned,
        )
        for filter_def in filters
    )
    members.extend(
        ParsedMember(
            "custom_instruction",
            instruction.name,
            instruction.origin or Origin("<unknown>"),
            instruction,
            None,
            (),
            instruction.poisoned,
        )
        for instruction in instructions.values()
    )
    members.extend(
        ParsedMember(
            "verified_query",
            query.name,
            query.origin or Origin("<unknown>"),
            query,
            query.tables,
            query.template_calls,
            query.poisoned,
        )
        for query in verified_queries
    )
    members.extend(
        ParsedMember(
            "relationship",
            relationship.name,
            origin,
            relationship,
            (relationship.from_table.casefold(), relationship.to_table.casefold()),
        )
        for relationship, origin in relationship_records
    )
    for model in (models or {}).values():
        model_origin = Origin(
            model.patch_path or model.original_file_path or "<dbt-manifest>",
            dbt_node=model.unique_id,
        )
        for column in model.columns:
            if column.column_type not in (
                ColumnKind.FACT.value,
                ColumnKind.DIMENSION.value,
            ):
                continue
            members.append(
                ParsedMember(
                    column.column_type,
                    f"{model.name}.{column.name}",
                    model_origin,
                    (model, column),
                    (model.name.casefold(),),
                )
            )
    return ParsedProject(
        tuple(views), tuple(members), DiagnosticBag((*documents.diagnostics, *relationship_diagnostics))
    )


def load_semantic_views_result(
    project_dir: Path,
    *,
    target_name: str | None = None,
    manifest_path: Path | None = None,
    invoke_dbt: bool = True,
) -> SemanticViewProject:
    """Load healthy views while collecting view-local failures."""
    config = _read_yaml(project_dir / "sst_config.yml")
    semantic_models_dir = str((config.get("project") or {}).get("semantic_models_dir") or "semantic_models")
    documents = load_documents(discover_yaml(project_dir, semantic_models_dir), _parse_yaml_bytes)
    target = resolve_target(project_dir, target_name)
    models = load_models(
        project_dir,
        target_name=target_name,
        manifest_path=manifest_path,
        invoke_dbt=invoke_dbt,
    )
    parsed = parse_semantic_project(documents, project_dir, semantic_models_dir, models)
    metrics = tuple(
        member.source for member in parsed.members_by_type.get("metric", ()) if isinstance(member.source, MetricDef)
    )
    cycles = _metric_cycles(metrics)
    poisoned_metrics = {name for cycle in cycles for name in cycle[:-1]}
    healthy_metrics = tuple(metric for metric in metrics if metric.name.casefold() not in poisoned_metrics)
    filters = tuple(
        member.source for member in parsed.members_by_type.get("filter", ()) if isinstance(member.source, FilterDef)
    )
    instructions = {
        member.name.casefold(): member.source
        for member in parsed.members_by_type.get("custom_instruction", ())
        if isinstance(member.source, InstructionDef)
    }
    verified_queries = tuple(
        member.source
        for member in parsed.members_by_type.get("verified_query", ())
        if isinstance(member.source, VerifiedQueryDef)
    )
    relationship_members = parsed.members_by_type.get("relationship", ())
    relationships = tuple(member.source for member in relationship_members if isinstance(member.source, Relationship))
    relationship_records = tuple(
        (member.source, member.origin) for member in relationship_members if isinstance(member.source, Relationship)
    )

    views_dir = project_dir / semantic_models_dir / "semantic_views"
    if not views_dir.is_dir():
        raise ProjectError(f"no semantic_views/ directory under {project_dir / semantic_models_dir}")

    out: list[SemanticView] = []
    diagnostics: list[Diagnostic] = list(parsed.diagnostics)
    view_counts: dict[str, int] = {}
    for parsed_view in parsed.views:
        name = parsed_view.name.casefold()
        view_counts[name] = view_counts.get(name, 0) + 1
    diagnostics.extend(
        D(
            "SST-VAL001",
            type="semantic_view",
            name=name,
            subject=f"semantic_view:{name}",
        )
        for name, count in sorted(view_counts.items())
        if count > 1
    )
    duplicate_views = {name for name, count in view_counts.items() if count > 1}
    for view in parsed.views:
        if view.poisoned:
            diagnostics.append(
                D(
                    "SST-LOD004",
                    origin=view.origin,
                    file=view.origin.file,
                    line=view.origin.line or 1,
                    col=view.origin.col or 1,
                    reason="malformed table reference",
                    subject=f"semantic_view:{view.name}",
                )
            )
    for member in parsed.members:
        if member.poisoned and member.type_name in (
            "metric",
            "filter",
            "verified_query",
        ):
            diagnostics.append(
                D(
                    "SST-LOD004",
                    origin=member.origin,
                    file=member.origin.file,
                    line=member.origin.line or 1,
                    col=member.origin.col or 1,
                    reason="malformed tables reference",
                    subject=member.key,
                )
            )
    referenced_models = frozenset(
        table for view in parsed.views if not view.poisoned for table in view.declared_tables if table in models
    )
    diagnostics.extend(_folder_route_diagnostics(config, views_dir))
    diagnostics.extend(_stray_view_diagnostics(documents, views_dir))
    diagnostics.extend(_authored_key_diagnostics(documents))
    diagnostics.extend(_member_name_diagnostics(documents))
    diagnostics.extend(_description_diagnostics(parsed.views, metrics))
    diagnostics.extend(_metric_parse_diagnostics(documents, project_dir, semantic_models_dir))
    diagnostics.extend(_filter_parse_diagnostics(documents, project_dir, semantic_models_dir))
    diagnostics.extend(_relationship_parse_diagnostics(documents, project_dir, semantic_models_dir))
    diagnostics.extend(_dbt_model_diagnostics(models, referenced_models))
    diagnostics.extend(_dbt_column_diagnostics(models, referenced_models))
    legacy_diagnostics = _legacy_reference_diagnostics(documents)
    diagnostics.extend(legacy_diagnostics)
    legacy_files = {
        diagnostic.context.get("file")
        for diagnostic in legacy_diagnostics
        if isinstance(diagnostic.context.get("file"), str)
    }
    verified_query_diagnostics = _verified_query_diagnostics(documents, project_dir, semantic_models_dir)
    diagnostics.extend(verified_query_diagnostics)
    raw_variables = config.get("vars")
    variables: dict[str, object] = (
        {str(key): value for key, value in raw_variables.items()} if isinstance(raw_variables, dict) else {}
    )
    expression_diagnostics = _expression_reference_diagnostics(
        filters + verified_queries,
        models,
        metric_names=frozenset(metric.name.casefold() for metric in metrics),
        variables=variables,
    )
    diagnostics.extend(
        diagnostic
        for diagnostic in expression_diagnostics
        if diagnostic.code not in ("SST-REF034", "SST-REF035")
        or diagnostic.origin is None
        or diagnostic.origin.file not in legacy_files
    )
    diagnostics.extend(_filter_diagnostics(filters))
    metric_diagnostics = _metric_diagnostics(metrics, models, variables)
    diagnostics.extend(
        diagnostic
        for diagnostic in metric_diagnostics
        if diagnostic.code not in ("SST-REF034", "SST-REF035")
        or diagnostic.origin is None
        or diagnostic.origin.file not in legacy_files
    )
    poisoned_metric_subjects = {
        diagnostic.subject
        for diagnostic in metric_diagnostics
        if diagnostic.subject is not None and diagnostic.severity is Severity.ERROR
    }
    poisoned_expression_subjects = {
        diagnostic.subject.casefold()
        for diagnostic in expression_diagnostics
        if diagnostic.subject is not None and diagnostic.severity is Severity.ERROR
    }
    known_models = set(models)
    invalid_metrics: set[str] = set()
    invalid_member_subjects: set[str] = set()
    for metric in metrics:
        for table_name in metric.tables:
            if table_name not in known_models:
                invalid_metrics.add(metric.name.casefold())
                diagnostics.append(
                    D(
                        "SST-MEM003",
                        member=f"metric:{metric.name}",
                        name=table_name,
                        subject=f"metric:{metric.name}",
                    )
                )
    attachment_members_to_validate: tuple[FilterDef | VerifiedQueryDef, ...] = filters + verified_queries
    for authored_member in attachment_members_to_validate:
        subject = f"{'filter' if isinstance(authored_member, FilterDef) else 'verified_query'}:{authored_member.name}"
        for table_name in authored_member.tables:
            if table_name not in known_models:
                invalid_member_subjects.add(subject.casefold())
                diagnostics.append(D("SST-MEM003", member=subject, name=table_name, subject=subject))
    metric_by_name = {metric.name.casefold(): metric for metric in metrics}
    for cycle in cycles:
        participants = tuple(metric_by_name[name] for name in cycle[:-1] if name in metric_by_name)
        origins = tuple(metric.origin for metric in participants if metric.origin is not None)
        diagnostics.append(
            D(
                "SST-REF005",
                cycle=" -> ".join(cycle),
                subject=f"metric:{cycle[0]}",
                origin=origins[0] if origins else None,
                related=origins,
            )
        )
    healthy_metrics = tuple(
        metric
        for metric in healthy_metrics
        if metric.name.casefold() not in invalid_metrics and f"metric:{metric.name}" not in poisoned_metric_subjects
    )
    view_table_sets = [(f"semantic_view:{view.name}", frozenset(view.declared_tables)) for view in parsed.views]
    view_root_key = SEMANTIC_REGISTRY.artifacts["semantic_view"].root_key
    assert view_root_key is not None
    relationship_diagnostics = _relationship_diagnostics(
        relationships,
        tuple(view_table_sets),
        {relationship.name.casefold(): origin for relationship, origin in relationship_records},
        models,
    )
    diagnostics.extend(relationship_diagnostics)
    diagnostics.extend(_relationship_cycle_diagnostics(relationships, tuple(view_table_sets)))
    diagnostics.extend(_multipath_diagnostics(relationships, healthy_metrics, tuple(view_table_sets)))
    poisoned_relationships = {
        diagnostic.subject
        for diagnostic in relationship_diagnostics
        if diagnostic.code == "SST-VAL203" and diagnostic.subject is not None
    }
    poisoned_member_keys = {
        subject.casefold() for subject in poisoned_metric_subjects | poisoned_relationships if subject is not None
    }
    poisoned_member_keys.update(f"metric:{name}" for name in poisoned_metrics)
    poisoned_member_keys.update(poisoned_expression_subjects)
    poisoned_member_keys.update(invalid_member_subjects)
    poisoned_member_keys.update(member.key for member in parsed.members if member.origin.file in legacy_files)
    known_relationships = {relationship.name.casefold(): relationship for relationship in relationships}
    for metric in healthy_metrics:
        for relationship_name in metric.using_relationships:
            relationship = known_relationships.get(relationship_name.casefold())
            if relationship is None:
                diagnostics.append(
                    D(
                        "SST-VAL214",
                        metric=metric.name,
                        relationship=relationship_name,
                        subject=f"metric:{metric.name}",
                        origin=metric.origin,
                    )
                )
                poisoned_member_keys.add(f"metric:{metric.name}".casefold())
                continue
            if metric.tables and relationship.from_table.casefold() != metric.tables[0]:
                diagnostics.append(
                    D(
                        "SST-VAL114",
                        metric=metric.name,
                        other=relationship_name,
                        name=metric.tables[0],
                        subject=f"metric:{metric.name}",
                        origin=metric.origin,
                    )
                )
                poisoned_member_keys.add(f"metric:{metric.name}".casefold())
    attachment_members = tuple(
        (dataclasses.replace(member, poisoned=True) if member.key in poisoned_member_keys else member)
        for member in parsed.members
    )
    view_named_members: dict[str, frozenset[str]] = {}
    known_instruction_names = frozenset(instructions)
    for parsed_view in parsed.views:
        raw_instructions = parsed_view.source.get("custom_instructions")
        instruction_values = raw_instructions if isinstance(raw_instructions, list) else []
        names: set[str] = set()
        for raw in instruction_values:
            try:
                calls = scan_template_calls(str(raw))
            except TemplateSyntaxError as exc:
                diagnostics.append(
                    D(
                        "SST-LOD004",
                        origin=parsed_view.origin,
                        file=parsed_view.origin.file,
                        line=exc.line,
                        col=exc.col,
                        reason=exc.reason,
                        subject=f"semantic_view:{parsed_view.name}",
                    )
                )
                continue
            for call in calls:
                view_subject = f"semantic_view:{parsed_view.name}"
                if call.function != "custom_instructions":
                    diagnostics.append(
                        D(
                            "SST-REF041",
                            origin=parsed_view.origin,
                            subject=view_subject,
                            artifact=view_subject,
                            function=call.function,
                            field="custom_instructions",
                        )
                    )
                    continue
                if len(call.args) != 1:
                    diagnostics.append(
                        D(
                            "SST-REF042",
                            origin=parsed_view.origin,
                            subject=view_subject,
                            artifact=view_subject,
                            detail=f"custom_instructions() takes one name, found {len(call.args)} in {call.raw}",
                        )
                    )
                    continue
                instruction_name = call.args[0].casefold()
                if instruction_name not in known_instruction_names:
                    diagnostics.append(
                        D(
                            "SST-REF039",
                            origin=parsed_view.origin,
                            subject=view_subject,
                            artifact=view_subject,
                            name=call.args[0],
                        )
                    )
                    continue
                names.add(instruction_name)
        view_named_members[f"semantic_view:{parsed_view.name}"] = frozenset(names)
    poisoned_views = {
        diagnostic.subject.casefold()
        for diagnostic in diagnostics
        if diagnostic.subject is not None
        and diagnostic.subject.casefold().startswith("semantic_view:")
        and diagnostic.severity is Severity.ERROR
    }
    poisoned_views.update(f"semantic_view:{view.name}".casefold() for view in parsed.views if view.poisoned)
    # A view authored with the legacy globals is rejected by SST-REF034; building
    # it would only report the same call again as an internal error.
    poisoned_views.update(
        f"semantic_view:{view.name}".casefold() for view in parsed.views if view.origin.file in legacy_files
    )
    metric_dependencies = {
        member.key: tuple(f"metric:{name}" for name in member.source.referenced_metrics)
        for member in attachment_members
        if member.type_name == "metric" and isinstance(member.source, MetricDef)
    }
    attachment = attach_view_members(
        {artifact: tables for artifact, tables in view_table_sets},
        attachment_members,
        SEMANTIC_REGISTRY,
        view_named_members=view_named_members,
        metric_dependencies=metric_dependencies,
    )
    for document in documents.under(views_dir, view_root_key):
        path = document.abs_path
        for node in document.tree.get(view_root_key) or []:
            if not isinstance(node, dict) or not node.get("name"):
                continue
            if node.get("enabled") is False:
                continue
            if node.get("enabled") is None and _semantic_view_defaults(config, path, views_dir).get("enabled") is False:
                continue
            if str(node["name"]).casefold() in duplicate_views:
                continue
            if f"semantic_view:{node['name']}".casefold() in poisoned_views:
                continue
            view_target = _semantic_view_target(config, path, views_dir, target)
            try:
                out.append(
                    _build_view(
                        node,
                        path,
                        project_dir,
                        view_target,
                        models,
                        attachment_members,
                        attachment,
                        config,
                    )
                )
            except ProjectError as exc:
                view_origin = Origin(path.resolve().relative_to(project_dir.resolve()).as_posix())
                view_subject = f"semantic_view:{node['name']}"
                if exc.diagnostics:
                    diagnostics.extend(
                        replace(item, origin=item.origin or view_origin, subject=item.subject or view_subject)
                        for item in exc.diagnostics
                    )
                else:
                    diagnostics.append(
                        D(
                            "SST-PRS123",
                            origin=view_origin,
                            subject=view_subject,
                            artifact=view_subject,
                            detail=str(exc),
                        )
                    )
    resolved = ResolvedProject(
        views=tuple(out),
        attachment=attachment,
        custom_instruction_names=MappingProxyType(
            {artifact.casefold(): tuple(sorted(names)) for artifact, names in view_named_members.items()}
        ),
        diagnostics=DiagnosticBag(diagnostics),
    )
    return SemanticViewProject(resolved.views, resolved.diagnostics)


def _build_view(
    node: dict[str, Any],
    path: Path,
    project_dir: Path,
    target: Target,
    models: dict[str, DbtModel],
    members: tuple[ParsedMember, ...],
    attachment: Mapping[str, tuple[str, ...]],
    config: dict[str, Any],
) -> SemanticView:
    name = str(node["name"])
    artifact_key = f"semantic_view:{name}"
    attached_members = tuple(member for member in members if artifact_key in attachment.get(member.key, ()))
    metrics = tuple(
        member.source
        for member in attached_members
        if member.type_name == "metric" and isinstance(member.source, MetricDef)
    )
    filters = tuple(
        member.source
        for member in attached_members
        if member.type_name == "filter" and isinstance(member.source, FilterDef)
    )
    instructions = {
        member.name.casefold(): member.source
        for member in attached_members
        if member.type_name == "custom_instruction" and isinstance(member.source, InstructionDef)
    }
    verified_queries = tuple(
        member.source
        for member in attached_members
        if member.type_name == "verified_query" and isinstance(member.source, VerifiedQueryDef)
    )
    relationships = tuple(
        member.source
        for member in attached_members
        if member.type_name == "relationship" and isinstance(member.source, Relationship)
    )
    catalog = DbtCatalog("v12", None, None, tuple(models.values()))
    raw_variables = config.get("vars")
    project_variables: dict[str, object] = (
        {str(key): value for key, value in raw_variables.items()} if isinstance(raw_variables, dict) else {}
    )

    # Tables keep DECLARATION order -- that is authored information and the golden
    # preserves it. Members are sorted later, by the renderer.
    tables: list[Table] = []
    logical_by_model: dict[str, str] = {}
    table_config = node.get("table_config") or {}
    for raw in node.get("tables") or []:
        call = single_template_call(str(raw), "ref")
        if call is None or len(call.args) != 1:
            diagnostic = D("SST-REF044", artifact=f"semantic_view:{name}", found=repr(raw))
            raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
        model_name = call.args[0]
        model = models.get(model_name.lower())
        if model is None:
            diagnostic = D("SST-REF001", model=model_name, subject=f"semantic_view:{name}")
            raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
        logical = model_name.upper()
        if logical in logical_by_model.values():
            # Role-playing tables are not supported in 1.0, so one physical table
            # cannot appear twice under two logical names.
            diagnostic = D("SST-PRS006", type="table", name=model_name)
            raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
        logical_by_model[model_name.lower()] = logical
        per_table = table_config.get(model_name) if isinstance(table_config, dict) else None
        table_synonyms = _as_str_tuple(per_table.get("synonyms")) if isinstance(per_table, dict) else ()
        distinct_range = _distinct_range(per_table, path=path, view_name=name, table_name=model_name)
        tables.append(
            Table(
                logical_name=logical,
                fqn=model.relation_name,
                primary_key=tuple(c.upper() for c in model.primary_key),
                unique_keys=tuple(tuple(column.upper() for column in key) for key in model.unique_keys),
                synonyms=table_synonyms,
                distinct_range=distinct_range,
            )
        )

    columns: list[Column] = []
    for model_key, logical in logical_by_model.items():
        for col in models[model_key].columns:
            if col.column_type is None or col.excluded:
                continue
            try:
                kind = ColumnKind(col.column_type)
            except ValueError as exc:
                role = D(
                    "SST-DBT003",
                    subject=f"dbt_model:{models[model_key].name}",
                    model=f"{models[model_key].name}.{col.name}",
                    found=col.column_type,
                )
                raise ProjectError(role.message, diagnostics=(role,)) from exc
            if kind is ColumnKind.TIME_DIMENSION:
                kind = ColumnKind.DIMENSION
            columns.append(
                Column(
                    table=logical,
                    name=col.name.upper(),
                    kind=kind,
                    expr=f"{logical}.{col.name.upper()}",
                    comment=col.description,
                    synonyms=col.synonyms,
                    sample_values=col.sample_values,
                    is_enum=col.is_enum,
                )
            )

    attached: list[Metric] = []
    resolved_metric_names: dict[str, str] = {}
    for metric in metrics:
        metric_name = metric.name.casefold()
        referenced = metric.tables or metric.referenced_models
        owner = logical_by_model[referenced[0]] if len(referenced) == 1 else None
        resolved_metric_names[metric_name] = metric.name.upper() if owner is None else f"{owner}.{metric.name.upper()}"

    for metric in metrics:
        referenced = metric.tables or metric.referenced_models
        owner = logical_by_model[referenced[0]] if len(referenced) == 1 else None
        outside = next(
            (entry for entry in metric.non_additive if entry.table and entry.table.casefold() not in logical_by_model),
            None,
        )
        if outside is not None:
            diagnostic = D(
                "SST-VAL118",
                metric=metric.name,
                value=f"{outside.table}.{outside.dimension}",
                subject=f"semantic_view:{name}",
            )
            raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
        expr = _resolve_expression(
            metric.expr,
            policy=METRIC_EXPR,
            origin=metric.origin or Origin(str(path)),
            catalog=catalog,
            logical_by_model=logical_by_model,
            metric_names=resolved_metric_names,
            instruction_names=frozenset(instructions),
            variables=project_variables,
            field="metric.expression",
        )
        window: Window | None = None
        if metric.window is not None and owner is not None:
            _require_reachable(metric, owner, relationships, logical_by_model, f"semantic_view:{name}")

            def resolve(text: str, metric: MetricDef = metric) -> str:
                return _resolve_expression(
                    text,
                    policy=METRIC_EXPR,
                    origin=metric.origin or Origin(str(path)),
                    catalog=catalog,
                    logical_by_model=logical_by_model,
                    metric_names=resolved_metric_names,
                    instruction_names=frozenset(instructions),
                    variables=project_variables,
                    field="metric.window",
                )

            window = Window(
                partition_by=tuple(resolve(text) for text in metric.window.partition_by),
                partition_excluding=tuple(resolve(text) for text in metric.window.partition_excluding),
                order_by=tuple(
                    SortKey(resolve(entry.ref), entry.descending, entry.nulls_first) for entry in metric.window.order_by
                ),
                frame=metric.window.frame,
            )
        attached.append(
            Metric(
                name=metric.name.upper(),
                expr=expr,
                table=owner,
                comment=metric.description,
                synonyms=metric.synonyms,
                using_relationships=metric.using_relationships,
                non_additive_by=tuple(entry.key for entry in metric.non_additive),
                access_modifier=metric.access_modifier,
                window=window,
            )
        )

    entity_filters: list[Column] = []
    standalone_filters: list[FilterDef] = []
    for filter_def in filters:
        if not filter_def.entity_level:
            standalone_filters.append(filter_def)
            continue
        referenced = filter_def.tables or tuple(
            call.args[0] for call in scan_template_calls(filter_def.expr) if call.function == "ref" and call.args
        )
        if len(referenced) != 1:
            raise ProjectError(f"filter {filter_def.name!r} must resolve to exactly one table")
        model_name = referenced[0].casefold()
        expr = _resolve_expression(
            filter_def.expr,
            policy=FILTER_EXPR,
            origin=filter_def.origin or Origin(str(path)),
            catalog=catalog,
            logical_by_model=logical_by_model,
            metric_names=resolved_metric_names,
            instruction_names=frozenset(instructions),
            variables=project_variables,
            field="filter.expression",
        )
        entity_filters.append(
            Column(
                table=logical_by_model[model_name],
                name=filter_def.name.upper(),
                kind=ColumnKind.FILTER,
                expr=expr,
                comment=filter_def.description,
            )
        )

    attached_relationships = relationships

    view_instructions = list(instructions.values())
    sql_instruction_parts = [item.ai_sql_generation for item in view_instructions if item.ai_sql_generation]
    sql_instruction_parts.extend(_standalone_filter_instruction(item, logical_by_model) for item in standalone_filters)
    question_parts = [item.ai_question_categorization for item in view_instructions if item.ai_question_categorization]

    attached_queries = tuple(
        VerifiedQuery(
            name=query.name.upper(),
            question=query.question,
            sql=_resolve_verified_query_sql(query, logical_by_model, resolved_metric_names, config, catalog),
            verified_at=query.verified_at,
            verified_by=query.verified_by,
            onboarding_question=query.onboarding_question,
        )
        for query in verified_queries
    )

    variables = tuple(_variable(value, path=path, view_name=name) for value in node.get("variables") or [])
    for variable in variables:
        attached = [_replace_metric_variable_name(metric, variable.name) for metric in attached]
        entity_filters = [
            _replace_column_variable_name(entity_filter, variable.name) for entity_filter in entity_filters
        ]
    raw_tags = node.get("tags")
    if raw_tags is not None and not isinstance(raw_tags, list):
        diagnostic = D("SST-PRS027", artifact=f"semantic_view:{name}", found=type(raw_tags).__name__)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    tags = tuple(_tag(value, config=config, target=target, path=path, view_name=name) for value in raw_tags or [])
    max_staleness = node.get("max_staleness")

    source_path = path.resolve().relative_to(project_dir.resolve()).as_posix()
    source_files = {source_path}
    source_files.update(member.origin.file for member in attached_members)
    for model_key in logical_by_model:
        model = models[model_key]
        model_path = model.patch_path or model.original_file_path
        if model_path:
            source_files.add(model_path)
    return SemanticView(
        fqn=target.fqn(name),
        tables=tuple(tables),
        relationships=attached_relationships,
        variables=variables,
        columns=tuple(columns + entity_filters),
        metrics=tuple(attached),
        comment=(node.get("description") or "").strip() or None,
        ai_sql_generation="\n\n".join(sql_instruction_parts) or None,
        ai_question_categorization="\n\n".join(question_parts) or None,
        custom_instruction_names=tuple(item.name for item in view_instructions),
        verified_queries=attached_queries,
        max_staleness=f"{max_staleness} seconds" if max_staleness is not None else None,
        tags=tags,
        source_path=source_path,
        source_files=tuple(sorted(source_files)),
        referenced_models=tuple(sorted(logical_by_model)),
    )


def _require_reachable(
    metric: MetricDef,
    owner: str,
    relationships: tuple[Relationship, ...],
    logical_by_model: Mapping[str, str],
    subject: str,
) -> None:
    """A window's dimensions must be ones the metric's table reaches through the view's relationships."""
    reached = {owner}
    frontier = [owner]
    while frontier:
        table = frontier.pop()
        for relationship in relationships:
            if relationship.from_table == table and relationship.to_table not in reached:
                reached.add(relationship.to_table)
                frontier.append(relationship.to_table)
    assert metric.window is not None
    for field, text in metric.window.references():
        call = single_template_call(text, "ref")
        if call is None or len(call.args) != 2:
            continue
        if logical_by_model.get(call.args[0].casefold()) not in reached:
            diagnostic = D(
                "SST-VAL125",
                metric=metric.name,
                field=field,
                value=text,
                expected=f"a dimension {owner} reaches in this view",
                subject=subject,
            )
            raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))


def _var_diagnostic(call: TemplateCall, origin: Origin | None, subject: str) -> Diagnostic:
    if len(call.args) != 1:
        return D(
            "SST-REF042",
            origin=origin,
            subject=subject,
            artifact=subject,
            detail=f"var() takes one name, found {len(call.args)} in {call.raw}",
        )
    return D("SST-REF038", origin=origin, subject=subject, artifact=subject, name=call.args[0])


def _distinct_range(
    per_table: object,
    *,
    path: Path,
    view_name: str,
    table_name: str,
) -> tuple[str, str] | None:
    if not isinstance(per_table, dict) or "distinct_range" not in per_table:
        return None
    value = per_table["distinct_range"]
    if not isinstance(value, dict) or not value.get("start") or not value.get("end"):
        raise ProjectError(f"{path}: view {view_name} table {table_name} has an invalid distinct_range")
    return str(value["start"]).upper(), str(value["end"]).upper()


def _sql_value(value: object) -> str:
    """The SQL literal for a YAML scalar: a boolean or number as written, anything else a string."""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return string_literal(str(value))


def _variable(value: object, *, path: Path, view_name: str) -> Variable:
    if (
        not isinstance(value, dict)
        or not value.get("name")
        or not value.get("data_type")
        or "default_value" not in value
    ):
        raise ProjectError(f"{path}: view {view_name} has an invalid variable {value!r}")
    data_type = str(value["data_type"]).upper()
    raw_default = value["default_value"]
    if data_type == "BOOLEAN" and not isinstance(raw_default, bool):
        raise ProjectError(f"{path}: view {view_name} variable {value['name']} requires a boolean default")
    if data_type.startswith("NUMBER") and (not isinstance(raw_default, (int, float)) or isinstance(raw_default, bool)):
        raise ProjectError(f"{path}: view {view_name} variable {value['name']} requires a numeric default")
    if data_type.startswith(("VARCHAR", "TEXT", "STRING")) and not isinstance(raw_default, str):
        raise ProjectError(f"{path}: view {view_name} variable {value['name']} requires a string default")
    default = _sql_value(raw_default)
    return Variable(
        name=str(value["name"]).upper(),
        data_type=data_type,
        default=default,
        comment=str(value.get("description") or "").strip() or None,
    )


def _tag(value: object, *, config: dict[str, Any], target: Target, path: Path, view_name: str) -> Tag:
    def invalid(detail: str) -> NoReturn:
        diagnostic = D("SST-REF040", artifact=f"semantic_view:{view_name}", detail=detail)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))

    if not isinstance(value, dict) or not value.get("name") or "value" not in value:
        invalid(f"tag {value!r} needs a name and a value")
    call = single_template_call(str(value["name"]), "tag")
    if call is None or len(call.args) != 1:
        invalid(f"tag name must be one tag() call, found {value['name']!r}")
    tags = config.get("tags") or {}
    tag_name = call.args[0]
    if not isinstance(tags, dict) or tag_name not in tags:
        invalid(f"tag('{tag_name}') names no tag under tags: in sst_config.yml")
    prefix = (
        str(tags.get("default_prefix") or "")
        .replace("{{ target.database }}", target.database)
        .replace("{{ target.schema }}", target.schema)
    )
    return Tag(name=f"{prefix}.{tag_name.upper()}", value=str(value["value"]))


def _standalone_filter_instruction(filter_def: FilterDef, logical_by_model: dict[str, str]) -> str:
    if len(filter_def.tables) != 1:
        raise ProjectError(f"standalone filter {filter_def.name!r} must attach to exactly one table")
    table = logical_by_model[filter_def.tables[0]]
    description = filter_def.description or ""
    return _wrap_text(f"For {table}, {filter_def.name} is {filter_def.expr}. {description}".strip())


def _replace_metric_variable_name(metric: Metric, variable_name: str) -> Metric:
    from dataclasses import replace

    expr = re.sub(
        rf"\b{re.escape(variable_name)}\b",
        variable_name.upper(),
        metric.expr,
        flags=re.IGNORECASE,
    )
    return replace(metric, expr=expr)


def _replace_column_variable_name(column: Column, variable_name: str) -> Column:
    from dataclasses import replace

    expr = re.sub(
        rf"\b{re.escape(variable_name)}\b",
        variable_name.upper(),
        column.expr,
        flags=re.IGNORECASE,
    )
    return replace(column, expr=expr)


def _wrap_text(text: str, width: int = 77) -> str:
    import textwrap

    return "\n".join(textwrap.wrap(text, width=width, break_long_words=False, break_on_hyphens=False))
