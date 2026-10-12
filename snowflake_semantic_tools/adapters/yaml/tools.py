"""Load tool-group YAML into immutable domain declarations."""

from __future__ import annotations

import posixpath
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.paths import walk_refusal
from snowflake_semantic_tools.adapters.yaml.fields import optional_string, refused, strings, unknown_keys
from snowflake_semantic_tools.adapters.yaml.parse import parse_yaml_bytes
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.authored import NodePath, SourcePosition
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from snowflake_semantic_tools.domain.model.tool import (
    CREATION_KEYS,
    TOOL_GROUP_KEYS,
    TOOL_MEMBER_KEYS,
    TOOLS_FILE_KEYS,
    ToolCatalog,
    ToolColumn,
    ToolGroup,
    ToolMember,
    ToolOwnership,
    ToolParameter,
)
from snowflake_semantic_tools.domain.parse.fields import unknown_field
from snowflake_semantic_tools.domain.parse.template import TemplateSyntaxError, scan_template_calls
from snowflake_semantic_tools.domain.resolve.calls import syntax_problem
from snowflake_semantic_tools.domain.validate.dbt_seam import literal_relation_diagnostic
from snowflake_semantic_tools.domain.validate.tool import validate_tool_catalog


def load_tool_catalog(
    project_dir: Path,
    dbt: DbtCatalog,
    *,
    target_name: str,
    declared_targets: frozenset[str],
    tools_dir: str = "tools",
) -> ToolCatalog:
    """Read every tool group of each `*.yml` or `*.yaml` directly under `tools_dir`, then validate them.

    Files are read in name order. A missing `tools_dir` gives an empty catalog, unvalidated. A
    file that cannot be read or parsed, or has no `tools:` list, contributes its diagnostics
    and no groups.

    Args:
        dbt: The dbt models each defined search service's `on:` must name.
        target_name: The target every reference's `relations` must cover.
        declared_targets: Every target `profiles.yml` declares.

    Returns:
        The groups in file order, with every loading diagnostic and every finding of
        `validate_tool_catalog`, sorted by origin file (none first), then code.

    Diagnostics:
        SST-PRT009: when a file, or a folder between the project and it, is a symbolic link, or it
            resolves outside the project; the file is not read.
        SST-LOD018: when a file cannot be read.
        SST-LOD004: when a template in a file or in an `on:` is malformed.
        SST-LOD006: when a file is not UTF-8.
        SST-LOD001: when a file is not valid YAML.
        SST-LOD005: when a file writes a key twice in one mapping.
        SST-LOD003: when a file holds only whitespace or comments.
        SST-LOD008: when a file holds more than one document.
        SST-LOD002: when a file's root is not a mapping.
        SST-PRS003: when `tools:` is absent or not a list, or a group, member or field of one has
            the wrong type.
        SST-PRS002: when a group has no `group`, or a member no `name` or no `type`.
        SST-PRS022, SST-PRS004: when a file's root, a group, or a member writes a key it does not
            model; SST-PRS022 when the key is a near miss of one it does, which it names.
        SST-VAL608: when a member's `on:` is not one `{{ ref('<model>') }}` call.
        Every code `validate_tool_catalog` lists, for the groups that load.
        SST-DBT006: a reference relation names a dbt model by the name its alias replaced.
    """
    groups: list[ToolGroup] = []
    diagnostics: list[Diagnostic] = []
    root = project_dir / tools_dir
    if not root.is_dir():
        return ToolCatalog((), target_name, declared_targets)
    for path in sorted(root.glob("*.y*ml")):
        if refused(project_dir, path, diagnostics):
            continue
        relative = path.relative_to(project_dir).as_posix()
        try:
            parsed = parse_yaml_bytes(path.read_bytes(), relative)
            loaded = dict(parsed.tree)
            lines = parsed.line_index
            diagnostics.extend(parsed.diagnostics)
        except ProjectError as exc:
            diagnostics.extend(exc.diagnostics)
            continue
        except OSError:
            diagnostics.append(D("SST-LOD018", file=relative, path=relative, origin=Origin(relative)))
            continue
        if not isinstance(loaded, dict):
            diagnostics.append(
                D("SST-PRS003", artifact=relative, field="root", expected="mapping", found=type(loaded).__name__)
            )
            continue
        diagnostics.extend(
            unknown_field(
                str(key), TOOLS_FILE_KEYS, artifact=relative, origin=_key_origin(relative, lines, (str(key),))
            )
            for key in unknown_keys(loaded, TOOLS_FILE_KEYS)
        )
        raw_groups = loaded.get("tools")
        if not isinstance(raw_groups, list):
            diagnostics.append(
                D("SST-PRS003", artifact=relative, field="tools", expected="list", found=type(raw_groups).__name__)
            )
            continue
        for group_index, raw_group in enumerate(raw_groups):
            group, group_diagnostics = _parse_group(project_dir, relative, lines, group_index, raw_group)
            diagnostics.extend(group_diagnostics)
            if group is not None:
                groups.append(group)
    catalog = ToolCatalog(tuple(groups), target_name, declared_targets, DiagnosticBag(diagnostics))
    return ToolCatalog(
        catalog.groups,
        catalog.target_name,
        catalog.declared_targets,
        DiagnosticBag((*validate_tool_catalog(catalog, dbt), *_aliased_relations(catalog, dbt))),
    )


def _aliased_relations(catalog: ToolCatalog, dbt: DbtCatalog) -> tuple[Diagnostic, ...]:
    """Report each `reference:` relation for the current target that names a dbt model dbt built elsewhere.

    Diagnostics:
        SST-DBT006: as `literal_relation_diagnostic` reports it.
    """
    found: list[Diagnostic] = []
    for group in catalog.groups:
        for member in group.members:
            relation = member.relations.get(catalog.target_name)
            diagnostic = (
                literal_relation_diagnostic(dbt, relation, subject=member.declaration_key) if relation else None
            )
            if diagnostic is not None:
                found.append(replace(diagnostic, origin=member.origin))
    return tuple(found)


def _key_origin(
    source_file: str, lines: Mapping[NodePath, SourcePosition], path: NodePath, fallback: Origin | None = None
) -> Origin:
    """Where the key at `path` is written; else `fallback`, or the file's start."""
    position = lines.get(path)
    if position is not None:
        return Origin(source_file, position.line, position.col)
    return fallback or Origin(source_file, 1, 1)


def _parse_group(
    project_dir: Path,
    source_file: str,
    lines: Mapping[NodePath, SourcePosition],
    index: int,
    value: object,
) -> tuple[ToolGroup | None, tuple[Diagnostic, ...]]:
    """Read one `tools:` entry into a group and its members, `define:` before `reference:`.

    The group and what it reports point at line `index + 1` of the file: its place in the
    list, not where it is written; an unknown key points where `lines` says it is written.
    Its name is kept as written.

    Returns:
        The group, None when the entry is not a mapping or names no group; with what reading the
        group and each of its members reported.

    Diagnostics:
        SST-PRS003: when the entry is not a mapping, or its `define:` or `reference:` not a list.
        SST-PRS002: when the entry has no non-blank `group`.
        SST-PRS022, SST-PRS004: when the entry writes a key a group does not model.
    """
    origin = Origin(source_file, index + 1, 1)
    if not isinstance(value, dict):
        return None, (
            D(
                "SST-PRS003",
                artifact=source_file,
                field=f"tools[{index}]",
                expected="mapping",
                found=type(value).__name__,
                origin=origin,
            ),
        )
    name = value.get("group")
    if not isinstance(name, str) or not name.strip():
        return None, (D("SST-PRS002", artifact=f"tools[{index}]", field="group", origin=origin),)
    members: list[ToolMember] = []
    diagnostics: list[Diagnostic] = [
        unknown_field(
            str(key),
            TOOL_GROUP_KEYS,
            artifact=f"tool_group:{name}",
            origin=_key_origin(source_file, lines, ("tools", index, str(key)), origin),
        )
        for key in unknown_keys(value, TOOL_GROUP_KEYS)
    ]
    for ownership in ToolOwnership:
        raw_members = value.get(ownership.value)
        if raw_members is None:
            continue
        if not isinstance(raw_members, list):
            diagnostics.append(
                D(
                    "SST-PRS003",
                    artifact=f"tool_group:{name}",
                    field=ownership.value,
                    expected="list",
                    found=type(raw_members).__name__,
                    origin=origin,
                )
            )
            continue
        for member_index, raw_member in enumerate(raw_members):
            member, member_diagnostics = _parse_member(
                project_dir,
                source_file,
                lines,
                ("tools", index, ownership.value),
                name,
                ownership,
                member_index,
                raw_member,
            )
            diagnostics.extend(member_diagnostics)
            if member is not None:
                members.append(member)
    return (
        ToolGroup(
            name=name,
            origin=origin,
            source_file=source_file,
            description=optional_string(value.get("description")),
            owner=optional_string(value.get("owner")),
            immutable=bool(value.get("immutable", False)),
            members=tuple(members),
        ),
        tuple(diagnostics),
    )


def _parse_member(
    project_dir: Path,
    source_file: str,
    lines: Mapping[NodePath, SourcePosition],
    parent: NodePath,
    group: str,
    ownership: ToolOwnership,
    index: int,
    value: object,
) -> tuple[ToolMember | None, tuple[Diagnostic, ...]]:
    """Read one member of a group's `define:` or `reference:` list; None when it lacks a name or type.

    It points at line `index + 1` of the file: its place in its list; an unknown key points
    where `lines` says it is written, below `parent`, its list's path. YAML 1.1 reads an unquoted
    `on:` key as `true`, so that key counts as `on:`. `body_file:` is read relative to the
    project root, as a project walk reads a file; one refused is None, as is one unreadable,
    which `validate_tool_catalog` reports for a `define:` member. An explicit `search_column` or
    `attribute_columns` wins over the one `on:` gives.

    Diagnostics:
        SST-PRS003: when the entry is not a mapping, or `on:`, `relations`, `secrets`,
            `signature` or `columns_and_descriptions` has the wrong type.
        SST-PRS002: when the entry has no non-blank `name`, or no non-blank `type`.
        SST-PRS022, SST-PRS004: when the entry writes a key a member does not model.
        SST-LOD004, SST-REF033, SST-REF003: when the template in `on:` does not parse.
        SST-VAL608: when `on:` is not one single-argument `ref()` call.
        SST-PRT009: when `body_file:` is absolute, climbs out of the project, is or is reached
            through a symbolic link, or resolves outside the project; it is not read.
    """
    origin = Origin(source_file, index + 1, 1)
    if not isinstance(value, dict):
        return None, (
            D(
                "SST-PRS003",
                artifact=f"tool_group:{group}",
                field=ownership.value,
                expected="mapping entries",
                found=type(value).__name__,
                origin=origin,
            ),
        )
    name, type_name = value.get("name"), value.get("type")
    diagnostics: list[Diagnostic] = []
    if not isinstance(name, str) or not name.strip():
        diagnostics.append(D("SST-PRS002", artifact=f"tool_group:{group}", field="name", origin=origin))
    if not isinstance(type_name, str) or not type_name.strip():
        diagnostics.append(D("SST-PRS002", artifact=f"tool_group:{group}", field="type", origin=origin))
    if diagnostics:
        return None, tuple(diagnostics)
    diagnostics.extend(
        unknown_field(
            str(key),
            TOOL_MEMBER_KEYS,
            artifact=artifact_key("tool", str(name)),
            origin=_key_origin(source_file, lines, (*parent, index, str(key)), origin),
        )
        for key in unknown_keys((key for key in value if key is not True), TOOL_MEMBER_KEYS)
    )
    on_model, search_column, attributes = _on_fields(value.get("on", value.get(True)), origin, str(name), diagnostics)
    body_file = optional_string(value.get("body_file"))
    body = _body(project_dir, body_file, str(name), origin, diagnostics) if body_file else None
    relations = _string_map(value.get("relations"), "relations", str(name), origin, diagnostics)
    columns = _columns(value.get("columns_and_descriptions"), str(name), origin, diagnostics)
    signature = _signature(value.get("signature"), str(name), origin, diagnostics)
    return (
        ToolMember(
            group=group,
            name=str(name),
            type=str(type_name),
            ownership=ownership,
            origin=origin,
            source_file=source_file,
            description=optional_string(value.get("description")),
            on_model=on_model,
            search_column=optional_string(value.get("search_column")) or search_column,
            attribute_columns=strings(value.get("attribute_columns")) or attributes,
            title_column=optional_string(value.get("title_column")),
            id_column=optional_string(value.get("id_column")),
            columns=columns,
            relative_path_column=optional_string(value.get("relative_path_column")),
            warehouse=optional_string(value.get("warehouse")),
            signature=signature,
            where=optional_string(value.get("where")),
            target_lag=optional_string(value.get("target_lag")),
            embedding_model=optional_string(value.get("embedding_model")),
            refresh_mode=optional_string(value.get("refresh_mode")),
            language=optional_string(value.get("language")),
            runtime_version=optional_string(value.get("runtime_version")),
            handler=optional_string(value.get("handler")),
            body_file=body_file,
            body=body,
            returns=optional_string(value.get("returns")),
            execute_as=optional_string(value.get("execute_as")),
            packages=strings(value.get("packages")),
            imports=strings(value.get("imports")),
            external_access_integrations=strings(value.get("external_access_integrations")),
            secrets=MappingProxyType(_string_map(value.get("secrets"), "secrets", str(name), origin, diagnostics)),
            relations=MappingProxyType(relations),
            creation_keys=tuple(
                sorted(({"on"} if True in value else set()) | CREATION_KEYS.intersection(str(key) for key in value))
            ),
        ),
        tuple(diagnostics),
    )


def _body(project_dir: Path, body_file: str, name: str, origin: Origin, diagnostics: list[Diagnostic]) -> str | None:
    """Read a member's `body_file:` relative to the project; None when it is refused or unreadable.

    Diagnostics:
        SST-PRT009: when the file is refused as `_body_path` refuses it; it is not read.
    """
    sidecar, refusal = _body_path(project_dir, body_file)
    if refusal is not None:
        diagnostics.append(
            D(
                "SST-PRT009",
                path=posixpath.normpath(body_file),
                detail=refusal,
                subject=artifact_key("tool", name),
                origin=origin,
            )
        )
        return None
    try:
        return sidecar.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return None


def _body_path(project_dir: Path, body_file: str) -> tuple[Path, str | None]:
    """Return the file a `body_file:` names, relative to the project, and why SST refuses to read it.

    Its `..` is taken away as written, so the path returned is the one checked. It is refused when
    absolute or climbing out of the project, and else as `walk_refusal` refuses a path.

    Returns:
        The file, and the reason, worded to follow "could not read <path>: " in SST-PRT009;
        None when it may be read.
    """
    relative = posixpath.normpath(body_file)
    path = project_dir / relative
    if posixpath.isabs(relative) or relative == ".." or relative.startswith("../"):
        return path, "it resolves outside the project"
    return path, walk_refusal(project_dir, path)


def _on_fields(
    value: object,
    origin: Origin,
    name: str,
    diagnostics: list[Diagnostic],
) -> tuple[str | None, str | None, tuple[str, ...]]:
    """Read `on:`, either a `ref()` string or a mapping of `table`, `search_column` and `attributes`.

    Returns:
        `(model, search_column, attributes)`: the model as the `ref()` names it, None when
        `on:` is absent or cannot be read; and the mapping's search column, stripped, and its
        attributes, else None and `()`.

    Diagnostics:
        SST-PRS003: when `on:` is neither a string nor a mapping, or its `table` is set and not text.
        SST-LOD004, SST-REF033, SST-REF003: when its template does not parse.
        SST-VAL608: when it is not exactly one single-argument `ref()` call.
    """
    raw: object = value
    search_column: str | None = None
    attributes: tuple[str, ...] = ()
    if isinstance(value, dict):
        raw = value.get("table")
        search_column = optional_string(value.get("search_column"))
        attributes = strings(value.get("attributes"))
    if raw is None:
        return None, search_column, attributes
    if not isinstance(raw, str):
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=artifact_key("tool", name),
                field="on",
                expected="ref() string",
                found=type(raw).__name__,
                origin=origin,
            )
        )
        return None, search_column, attributes
    try:
        calls = scan_template_calls(raw)
    except TemplateSyntaxError as exc:
        diagnostics.append(syntax_problem(exc, origin.file))
        return None, search_column, attributes
    if len(calls) != 1 or calls[0].function != "ref" or len(calls[0].args) != 1 or calls[0].raw.strip() != raw.strip():
        diagnostics.append(D("SST-VAL608", name=name, value=raw, origin=origin))
        return None, search_column, attributes
    return calls[0].args[0], search_column, attributes


def _signature(value: object, name: str, origin: Origin, diagnostics: list[Diagnostic]) -> tuple[ToolParameter, ...]:
    """Read `signature:` as the routine's parameters, in order, skipping each malformed entry.

    A parameter needs a string `name` and `type`; `required` is read as a truth value, False
    when absent.

    Diagnostics:
        SST-PRS003: when `signature:` is not a list, or an entry is not a mapping with a string
            `name` and `type`, once per entry.
    """
    if value is None:
        return ()
    if not isinstance(value, list):
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=artifact_key("tool", name),
                field="signature",
                expected="list",
                found=type(value).__name__,
                origin=origin,
            )
        )
        return ()
    parameters: list[ToolParameter] = []
    for item in value:
        if not isinstance(item, dict) or not isinstance(item.get("name"), str) or not isinstance(item.get("type"), str):
            diagnostics.append(
                D(
                    "SST-PRS003",
                    artifact=artifact_key("tool", name),
                    field="signature",
                    expected="name/type entries",
                    found=type(item).__name__,
                    origin=origin,
                )
            )
            continue
        parameters.append(ToolParameter(str(item["name"]), str(item["type"]), bool(item.get("required", False))))
    return tuple(parameters)


def _columns(value: object, name: str, origin: Origin, diagnostics: list[Diagnostic]) -> tuple[ToolColumn, ...]:
    """Read `columns_and_descriptions:`, each column name to its settings, in authored order.

    An absent `description` or `type` reads as `""`, and `searchable` and `filterable` as truth
    values, False when absent.

    Diagnostics:
        SST-PRS003: when the value is not a mapping, or a column's settings are not, once per column.
    """
    if value is None:
        return ()
    if not isinstance(value, dict):
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=artifact_key("tool", name),
                field="columns_and_descriptions",
                expected="mapping",
                found=type(value).__name__,
                origin=origin,
            )
        )
        return ()
    columns: list[ToolColumn] = []
    for column_name, raw in value.items():
        if not isinstance(raw, dict):
            diagnostics.append(
                D(
                    "SST-PRS003",
                    artifact=artifact_key("tool", name),
                    field=f"columns_and_descriptions.{column_name}",
                    expected="mapping",
                    found=type(raw).__name__,
                    origin=origin,
                )
            )
            continue
        columns.append(
            ToolColumn(
                str(column_name),
                str(raw.get("description") or ""),
                str(raw.get("type") or ""),
                bool(raw.get("searchable", False)),
                bool(raw.get("filterable", False)),
            )
        )
    return tuple(columns)


def _string_map(
    value: object,
    field: str,
    name: str,
    origin: Origin,
    diagnostics: list[Diagnostic],
) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, dict) or any(not isinstance(item, str) for item in value.values()):
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=artifact_key("tool", name),
                field=field,
                expected="string mapping",
                found=type(value).__name__,
                origin=origin,
            )
        )
        return {}
    return {str(key): str(item) for key, item in value.items()}
