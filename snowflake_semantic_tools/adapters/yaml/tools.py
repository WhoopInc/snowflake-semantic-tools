"""Load tool-group YAML into immutable domain declarations."""

from __future__ import annotations

from pathlib import Path
from types import MappingProxyType

from ...domain.model.dbt import DbtCatalog
from ...domain.model.diagnostic import D, Diagnostic, DiagnosticBag, Origin
from ...domain.model.reference import TemplateSyntaxError, scan_template_calls
from ...domain.model.tool import (
    CREATION_KEYS,
    ToolCatalog,
    ToolColumn,
    ToolGroup,
    ToolMember,
    ToolOwnership,
    ToolParameter,
    validate_tool_catalog,
)
from .loader import _parse_yaml_bytes


def load_tool_catalog(
    project_dir: Path,
    dbt: DbtCatalog,
    *,
    target_name: str,
    declared_targets: frozenset[str],
    tools_dir: str = "tools",
) -> ToolCatalog:
    groups: list[ToolGroup] = []
    diagnostics: list[Diagnostic] = []
    root = project_dir / tools_dir
    if not root.is_dir():
        return ToolCatalog((), target_name, declared_targets)
    for path in sorted(root.glob("*.y*ml")):
        relative = path.relative_to(project_dir).as_posix()
        try:
            loaded = dict(_parse_yaml_bytes(path.read_bytes(), relative).tree)
        except (OSError, ValueError) as exc:
            diagnostics.append(
                D("SST-LOD004", file=relative, line=1, col=1, reason=str(exc), origin=Origin(relative, 1, 1))
            )
            continue
        if not isinstance(loaded, dict):
            diagnostics.append(
                D("SST-PRS003", artifact=relative, field="root", expected="mapping", found=type(loaded).__name__)
            )
            continue
        raw_groups = loaded.get("tools")
        if not isinstance(raw_groups, list):
            diagnostics.append(
                D("SST-PRS003", artifact=relative, field="tools", expected="list", found=type(raw_groups).__name__)
            )
            continue
        for group_index, raw_group in enumerate(raw_groups):
            group, group_diagnostics = _parse_group(project_dir, relative, group_index, raw_group)
            diagnostics.extend(group_diagnostics)
            if group is not None:
                groups.append(group)
    catalog = ToolCatalog(tuple(groups), target_name, declared_targets, DiagnosticBag(diagnostics))
    return ToolCatalog(
        catalog.groups,
        catalog.target_name,
        catalog.declared_targets,
        validate_tool_catalog(catalog, dbt),
    )


def _parse_group(
    project_dir: Path,
    source_file: str,
    index: int,
    value: object,
) -> tuple[ToolGroup | None, tuple[Diagnostic, ...]]:
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
    diagnostics: list[Diagnostic] = []
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
            description=_optional_string(value.get("description")),
            owner=_optional_string(value.get("owner")),
            immutable=bool(value.get("immutable", False)),
            members=tuple(members),
        ),
        tuple(diagnostics),
    )


def _parse_member(
    project_dir: Path,
    source_file: str,
    group: str,
    ownership: ToolOwnership,
    index: int,
    value: object,
) -> tuple[ToolMember | None, tuple[Diagnostic, ...]]:
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
    on_model, search_column, attributes = _on_fields(value.get("on", value.get(True)), origin, str(name), diagnostics)
    body_file = _optional_string(value.get("body_file"))
    body: str | None = None
    if body_file:
        sidecar = (project_dir / body_file).resolve()
        try:
            sidecar.relative_to(project_dir.resolve())
            body = sidecar.read_text(encoding="utf-8")
        except (OSError, ValueError):
            body = None
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
            description=_optional_string(value.get("description")),
            on_model=on_model,
            search_column=_optional_string(value.get("search_column")) or search_column,
            attribute_columns=_strings(value.get("attribute_columns")) or attributes,
            title_column=_optional_string(value.get("title_column")),
            id_column=_optional_string(value.get("id_column")),
            columns=columns,
            relative_path_column=_optional_string(value.get("relative_path_column")),
            warehouse=_optional_string(value.get("warehouse")),
            signature=signature,
            where=_optional_string(value.get("where")),
            target_lag=_optional_string(value.get("target_lag")),
            embedding_model=_optional_string(value.get("embedding_model")),
            language=_optional_string(value.get("language")),
            runtime_version=_optional_string(value.get("runtime_version")),
            handler=_optional_string(value.get("handler")),
            body_file=body_file,
            body=body,
            returns=_optional_string(value.get("returns")),
            execute_as=_optional_string(value.get("execute_as")),
            packages=_strings(value.get("packages")),
            imports=_strings(value.get("imports")),
            external_access_integrations=_strings(value.get("external_access_integrations")),
            secrets=MappingProxyType(_string_map(value.get("secrets"), "secrets", str(name), origin, diagnostics)),
            relations=MappingProxyType(relations),
            creation_keys=tuple(
                sorted(({"on"} if True in value else set()) | CREATION_KEYS.intersection(str(key) for key in value))
            ),
        ),
        tuple(diagnostics),
    )


def _on_fields(
    value: object,
    origin: Origin,
    name: str,
    diagnostics: list[Diagnostic],
) -> tuple[str | None, str | None, tuple[str, ...]]:
    raw: object = value
    search_column: str | None = None
    attributes: tuple[str, ...] = ()
    if isinstance(value, dict):
        raw = value.get("table")
        search_column = _optional_string(value.get("search_column"))
        attributes = _strings(value.get("attributes"))
    if raw is None:
        return None, search_column, attributes
    if not isinstance(raw, str):
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=f"tool:{name}",
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
        diagnostics.append(
            D("SST-LOD004", file=origin.file, line=exc.line, col=exc.col, reason=exc.reason, origin=origin)
        )
        return None, search_column, attributes
    if len(calls) != 1 or calls[0].function != "ref" or len(calls[0].args) != 1 or calls[0].raw.strip() != raw.strip():
        diagnostics.append(D("SST-VAL608", name=name, value=raw, origin=origin))
        return None, search_column, attributes
    return calls[0].args[0], search_column, attributes


def _signature(value: object, name: str, origin: Origin, diagnostics: list[Diagnostic]) -> tuple[ToolParameter, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=f"tool:{name}",
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
                    artifact=f"tool:{name}",
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
    if value is None:
        return ()
    if not isinstance(value, dict):
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=f"tool:{name}",
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
                    artifact=f"tool:{name}",
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
                artifact=f"tool:{name}",
                field=field,
                expected="string mapping",
                found=type(value).__name__,
                origin=origin,
            )
        )
        return {}
    return {str(key): str(item) for key, item in value.items()}


def _strings(value: object) -> tuple[str, ...]:
    return tuple(str(item) for item in value) if isinstance(value, list) else ()


def _optional_string(value: object) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None
