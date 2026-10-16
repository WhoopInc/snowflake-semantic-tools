"""Load tool members from YAML into immutable domain declarations."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import MappingProxyType
from typing import Any

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.yaml.fields import (
    checked_choice,
    file_call,
    optional_string,
    refused,
    resolve_file,
    strings,
    unknown_keys,
)
from snowflake_semantic_tools.adapters.yaml.parse import parse_yaml_bytes
from snowflake_semantic_tools.adapters.yaml.routes import routed_diagnostics
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.authored import NodePath, SourcePosition
from snowflake_semantic_tools.domain.model.config_schema import resolve_settings
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from snowflake_semantic_tools.domain.model.registry import ARTIFACT_REGISTRY
from snowflake_semantic_tools.domain.model.tool import (
    CREATION_KEYS,
    MOVED_TOOL_KEYS,
    TOOL_COLUMN_KEYS,
    TOOL_MEMBER_KEYS,
    ToolCatalog,
    ToolColumn,
    ToolMember,
    ToolOwnership,
    ToolParameter,
)
from snowflake_semantic_tools.domain.parse.fields import unknown_field
from snowflake_semantic_tools.domain.parse.template import TemplateCall, TemplateSyntaxError, scan_template_calls
from snowflake_semantic_tools.domain.resolve.calls import literal_problem, syntax_problem
from snowflake_semantic_tools.domain.resolve.ref_fields import ref_field
from snowflake_semantic_tools.domain.resolve.template import ResolveContext, resolve_scalar
from snowflake_semantic_tools.domain.resolve.tool_column import ColumnSite, tool_column
from snowflake_semantic_tools.domain.validate.dbt_seam import literal_relation_diagnostic
from snowflake_semantic_tools.domain.validate.settings import item_config_diagnostics
from snowflake_semantic_tools.domain.validate.tool import validate_tool_catalog

# The tool type as the registry declares it, as every other loader reads its type: the root
# holding one member (`tool:`) and the root holding a list (`tools:`), the `sst_config.yml` block
# its settings and folder routes resolve through, and the kind part of each member's key.
_TOOL = ARTIFACT_REGISTRY.artifacts["tool"]
_ONE, _MANY = str(_TOOL.singular_root), str(_TOOL.root_key)
_FILE_KEYS = frozenset((_ONE, _MANY))
# Keys of a list entry that only a tool group wrote; an entry with one is a group.
_GROUP_KEYS = ("group", "define", "reference")


@dataclass(frozen=True, slots=True)
class _Source:
    """One tools file being read, and what reading its members resolves against.

    Attributes:
        source_file: The file, relative to the project root.
        folder: Its directory below `project.tools_dir`, which the `tools:` folder routes name.
        block: The `tools:` block of the configuration, as parsed; None without one.
        variables: What `{{ var('<name>') }}` in a `where` resolves to.
    """

    project_dir: Path
    source_file: str
    lines: Mapping[NodePath, SourcePosition]
    folder: tuple[str, ...]
    dbt: DbtCatalog
    block: object
    variables: Mapping[str, object]

    def origin(self, path: NodePath) -> Origin:
        """Place the node at `path` where it is written, else at the file's start."""
        position = self.lines.get(path)
        return Origin(self.source_file, position.line, position.col) if position else Origin(self.source_file, 1, 1)


def load_tool_catalog(
    project_dir: Path,
    dbt: DbtCatalog,
    *,
    target_name: str,
    declared_targets: frozenset[str],
    tools_dir: str = "tools",
    config: Mapping[str, Any] | None = None,
) -> ToolCatalog:
    """Read every tool member of each `*.yml` or `*.yaml` below `tools_dir`, then validate them.

    Files are found in every folder below `tools_dir` and read in path order. A file's root
    `tool:` holds one member, and its root `tools:` a list of them. A member's folder below
    `tools_dir` is what the `tools:` folder routes name, and its settings resolve over them. A
    missing `tools_dir` gives an empty catalog, unvalidated. A file that cannot be read or
    parsed, or holds neither root, contributes its diagnostics and no members.

    Args:
        dbt: The dbt models each defined search service's `on:` must name.
        target_name: The target every reference's `relations` must cover.
        declared_targets: Every target `profiles.yml` declares.
        config: The run's resolved configuration, whose `tools:` block each member's settings
            resolve against, whose folder routes are checked against the folders under
            `tools_dir`, and whose `vars:` a `where` may read; None reads no configuration.

    Returns:
        The members in file order, with every loading diagnostic and every finding of
        `validate_tool_catalog`, sorted by origin file (none first), then code.

    Diagnostics:
        SST-CFG041: a `tools:` folder route names no folder under `tools_dir`; reported first.
        SST-PRT009: a file, or a folder between the project and it, is a symbolic link, or it
            resolves outside the project; the file is not read.
        SST-LOD018: a file cannot be read.
        SST-LOD004: a template in a file or in an `on:` is malformed.
        SST-LOD006: a file is not UTF-8.
        SST-LOD001: a file is not valid YAML.
        SST-LOD005: a file writes a key twice in one mapping.
        SST-LOD003: a file holds only whitespace or comments.
        SST-LOD008: a file holds more than one document.
        SST-LOD002: a file's root is not a mapping.
        SST-PRS003: a file holds both `tool:` and `tools:`, or neither; `tools:` is not a list;
            or a member or a field of one has the wrong type.
        SST-PRS022, SST-PRS004: a file's root or a member writes a key it does not model;
            SST-PRS022 when the key is a near miss of one it does, which it names.
        Every code `_parse_member` lists, for each member.
        Every code `validate_tool_catalog` lists, for the members that load.
        SST-DBT006: a reference relation names a dbt model by the name its alias replaced.
    """
    tree = config if config is not None else {}
    diagnostics: list[Diagnostic] = list(routed_diagnostics(tree, project_dir, _TOOL.name)) if config else []
    root = project_dir / tools_dir
    if not root.is_dir():
        return ToolCatalog((), target_name, declared_targets, DiagnosticBag(diagnostics))
    members: list[ToolMember] = []
    declared = tree.get("vars")
    variables = (
        {str(key): value for key, value in declared.items() if value is not None} if isinstance(declared, dict) else {}
    )
    for path in sorted(root.glob("**/*.y*ml")):
        if refused(project_dir, path, diagnostics):
            continue
        relative = path.relative_to(project_dir).as_posix()
        try:
            parsed = parse_yaml_bytes(path.read_bytes(), relative)
            loaded = dict(parsed.tree)
            diagnostics.extend(parsed.diagnostics)
        except ProjectError as exc:
            diagnostics.extend(exc.diagnostics)
            continue
        except OSError:
            diagnostics.append(D("SST-LOD018", file=relative, path=relative, origin=Origin(relative)))
            continue
        source = _Source(
            project_dir,
            relative,
            parsed.line_index,
            path.parent.relative_to(root).parts,
            dbt,
            tree.get(_TOOL.routed_block),
            MappingProxyType(variables),
        )
        diagnostics.extend(_unknown(loaded, _FILE_KEYS, relative, source, ()))
        for node, value in _entries(loaded, source, diagnostics):
            member, member_diagnostics = _parse_member(source, node, value)
            diagnostics.extend(member_diagnostics)
            if member is not None:
                members.append(member)
    catalog = ToolCatalog(tuple(members), target_name, declared_targets, DiagnosticBag(diagnostics))
    return replace(
        catalog,
        diagnostics=DiagnosticBag((*validate_tool_catalog(catalog, dbt), *_aliased_relations(catalog, dbt))),
    )


def _entries(
    loaded: Mapping[str, Any], source: _Source, diagnostics: list[Diagnostic]
) -> tuple[tuple[NodePath, object], ...]:
    """Return each member entry of a tools file with its node path: `tool:`'s one, or `tools:`'s list.

    Diagnostics:
        SST-PRS003: the root holds both `tool:` and `tools:`, or neither, or `tools:` is not a list.
    """
    file = source.source_file
    if _ONE in loaded and _MANY in loaded:
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=file,
                field="root",
                expected=f"one of {_ONE}: and {_MANY}:",
                found="both",
                origin=Origin(file),
            )
        )
        return ()
    if _ONE in loaded:
        return (((_ONE,), loaded[_ONE]),)
    listed = loaded.get(_MANY)
    if not isinstance(listed, list):
        expected = "list" if _MANY in loaded else f"a {_ONE}: mapping or a {_MANY}: list"
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=file,
                field=_MANY,
                expected=expected,
                found=type(listed).__name__,
                origin=Origin(file),
            )
        )
        return ()
    return tuple(((_MANY, index), value) for index, value in enumerate(listed))


def _aliased_relations(catalog: ToolCatalog, dbt: DbtCatalog) -> tuple[Diagnostic, ...]:
    """Report each relation for the current target that names a dbt model dbt built elsewhere.

    Diagnostics:
        SST-DBT006: as `literal_relation_diagnostic` reports it.
    """
    found: list[Diagnostic] = []
    for member in catalog.members:
        relation = member.relations.get(catalog.target_name)
        subject = f"tool_ref:{member.name.casefold()}"
        diagnostic = literal_relation_diagnostic(dbt, relation, subject=subject) if relation else None
        if diagnostic is not None:
            found.append(replace(diagnostic, origin=member.origin))
    return tuple(found)


def _unknown(
    value: Mapping[Any, Any],
    known: frozenset[str],
    artifact: str,
    source: _Source,
    path: NodePath,
    *,
    subject: str | None = None,
) -> list[Diagnostic]:
    """Report each key of the mapping at `path` that `known` does not model, where it is written.

    Diagnostics:
        SST-PRS126: a member's key is one tools wrote before they lost their groups, which the
            message says where to write now.
        SST-PRS022: the key is a near miss of one `known` holds, which it names.
        SST-PRS004: otherwise.
    """
    found: list[Diagnostic] = []
    for key in unknown_keys(value, known):
        origin = source.origin((*path, str(key)))
        moved = MOVED_TOOL_KEYS.get(str(key)) if known is TOOL_MEMBER_KEYS else None
        found.append(
            D("SST-PRS126", origin=origin, subject=subject, artifact=artifact, field=str(key), reason=moved)
            if moved is not None
            else unknown_field(str(key), known, artifact=artifact, origin=origin, subject=subject)
        )
    return found


# A search service's refresh modes, as a refusal of a value that is not text names them; text
# is matched, in any case, by the tool validator.
_REFRESH_MODES = ("AUTO", "FULL", "INCREMENTAL")


def _parse_member(source: _Source, path: NodePath, value: object) -> tuple[ToolMember | None, tuple[Diagnostic, ...]]:
    """Read one member: `tool:`'s mapping, or an entry of `tools:`; None when it lacks a name or type.

    It points where it is written. YAML 1.1 reads an unquoted `on:` key as `true`, so that key
    counts as `on:`. A member that writes `relations:` is a reference; any other is defined.
    An explicit `search_column` or `attribute_columns` wins over the one `on:` gives. Its
    settings resolve over its `config:`, the folder routes over its file, and the block root.

    Diagnostics:
        SST-PRS003: `on:`, `relations`, `secrets`, `signature`, `columns` or `meta` has the wrong
            type.
        SST-PRS022, SST-PRS004, SST-PRS126: the entry writes a key a member does not model.
        SST-CFG004, SST-CFG053, SST-CFG054, SST-CFG055, SST-CFG008: as `item_config_diagnostics`
            reports its `config:` block.
        SST-LOD004, SST-REF033, SST-REF003: the template in `on:` does not parse.
        SST-VAL608: `on:` is not one single-argument `ref()` call.
        SST-REF008: `description`, published as written, holds a template.
        SST-PRS013: `refresh_mode` is present and not text, such as an unquoted `yes` or empty.
        SST-PRS130: a `signature:` entry writes `required`; every parameter is required.
        Every code `_header`, `_Columns.read`, `_columns_of`, `_where` and `_body` list.
    """
    header, problems = _header(source, path, value)
    if header is None:
        return None, problems
    entry, name, type_name = header
    diagnostics: list[Diagnostic] = []
    origin = source.origin(path)
    subject = artifact_key(_TOOL.name, name.casefold())
    keys = {key: item for key, item in entry.items() if key is not True}
    diagnostics.extend(_unknown(keys, TOOL_MEMBER_KEYS, subject, source, path, subject=subject))
    referenced = "relations" in entry
    on_model, on_attributes = _on_fields(entry.get("on", entry.get(True)), origin, name, diagnostics)
    columns = _Columns(source, path, entry, ColumnSite(name, "", subject, subject, origin), referenced, on_model)
    config = entry.get("config")
    diagnostics.extend(
        item_config_diagnostics(
            _TOOL.routed_block,
            config,
            subject=subject,
            origin_of=lambda key: source.origin((*path, "config", key) if key else (*path, "config")),
        )
    )
    body_file, body = _body(source, entry.get("body"), name, (*path, "body"), diagnostics)
    relations = _string_map(entry.get("relations"), "relations", subject, origin, diagnostics)
    member = ToolMember(
        name=name,
        type=type_name,
        ownership=ToolOwnership.REFERENCE if referenced else ToolOwnership.DEFINE,
        origin=origin,
        source_file=source.source_file,
        description=_described(entry.get("description"), subject, source.origin((*path, "description")), diagnostics),
        on_model=on_model,
        search_column=columns.written("search_column") or columns.written("on", "search_column"),
        attribute_columns=columns.attributes(on_attributes),
        title_column=columns.written("title_column"),
        id_column=columns.written("id_column"),
        columns=_columns_of(entry.get("columns"), name, path, source, columns.read, diagnostics),
        relative_path_column=columns.written("relative_path_column"),
        signature=_signature(entry.get("signature"), subject, origin, diagnostics),
        where=_where(source, entry.get("where"), name, on_model, referenced, (*path, "where"), diagnostics),
        refresh_mode=checked_choice(
            entry.get("refresh_mode"),
            _REFRESH_MODES,
            diagnostics,
            field="refresh_mode",
            artifact=subject,
            origin=source.origin((*path, "refresh_mode")),
            subject=subject,
            written="refresh_mode" in entry,
        ),
        language=optional_string(entry.get("language")),
        runtime_version=optional_string(entry.get("runtime_version")),
        handler=optional_string(entry.get("handler")),
        body_file=body_file,
        body=body,
        returns=optional_string(entry.get("returns")),
        packages=strings(entry.get("packages")),
        imports=strings(entry.get("imports")),
        external_access_integrations=strings(entry.get("external_access_integrations")),
        secrets=MappingProxyType(_string_map(entry.get("secrets"), "secrets", subject, origin, diagnostics)),
        relations=MappingProxyType(relations),
        creation_keys=tuple(
            sorted(({"on"} if True in entry else set()) | CREATION_KEYS.intersection(str(key) for key in entry))
        ),
        folder=source.folder,
        meta=MappingProxyType(_mapping(entry.get("meta"), "meta", subject, origin, diagnostics)),
        config=MappingProxyType(dict(config) if isinstance(config, dict) else {}),
        settings=resolve_settings(source.block, source.folder, config),
    )
    return member, (*diagnostics, *columns.diagnostics)


def _header(
    source: _Source, path: NodePath, value: object
) -> tuple[tuple[Mapping[Any, Any], str, str] | None, tuple[Diagnostic, ...]]:
    """Return a member entry with its name and type, or None and why it holds no member.

    Diagnostics:
        SST-PRS003: the entry is not a mapping.
        SST-PRS126: the entry is a tool group, once per group key it writes.
        SST-PRS002: the entry has no non-blank `name`, or no non-blank `type`.
    """
    file = source.source_file
    origin = source.origin(path)
    label = ".".join(str(part) for part in path)
    if not isinstance(value, dict):
        return None, (
            D("SST-PRS003", artifact=file, field=label, expected="mapping", found=type(value).__name__, origin=origin),
        )
    if any(key in value for key in _GROUP_KEYS):
        return None, tuple(
            D("SST-PRS126", origin=source.origin((*path, key)), artifact=file, field=key, reason=MOVED_TOOL_KEYS[key])
            for key in _GROUP_KEYS
            if key in value
        )
    name, type_name = value.get("name"), value.get("type")
    problems = tuple(
        D("SST-PRS002", artifact=file, field=f"{label}.{field}", origin=origin)
        for field, written in (("name", name), ("type", type_name))
        if not isinstance(written, str) or not written.strip()
    )
    if problems or not isinstance(name, str) or not isinstance(type_name, str):
        return None, problems
    return (value, name, type_name), ()


@dataclass(slots=True)
class _Columns:
    """Read the columns one member names, as its ownership and `on:` model say they are written.

    Attributes:
        site: Where the member is written; each column's diagnostics name its own field and node.
        diagnostics: What reading the columns reported, in the order they were read.
    """

    source: _Source
    path: NodePath
    entry: Mapping[Any, Any]
    site: ColumnSite
    referenced: bool
    on_model: str | None
    diagnostics: list[Diagnostic] = field(default_factory=list)

    def read(self, name: str, raw: object, node: NodePath) -> str | None:
        """Return the column `raw` names under the field `name`, written at `node`; None when it names none.

        Diagnostics:
            Every code `tool_column` lists.
        """
        text = optional_string(raw)
        if text is None:
            return None
        site = replace(self.site, field=name, origin=self.source.origin(node))
        resolved, problem = tool_column(text, site, referenced=self.referenced, on_model=self.on_model)
        if problem is not None:
            self.diagnostics.append(problem)
        return resolved

    def written(self, *keys: str) -> str | None:
        """Return the column at `keys` in the member, as `read` reads it."""
        return self.read(".".join(keys), _at(self.entry, keys), (*self.path, *keys))

    def attributes(self, on_attributes: tuple[str, ...]) -> tuple[str, ...]:
        """Return the attribute columns: `attribute_columns:` beside `on:`, else `on.attributes`."""
        key: NodePath = ("attribute_columns",) if "attribute_columns" in self.entry else ("on", "attributes")
        written = strings(self.entry.get("attribute_columns")) or on_attributes
        label = ".".join(str(part) for part in key)
        found = (self.read(f"{label}.{index}", item, (*self.path, *key, index)) for index, item in enumerate(written))
        return tuple(column for column in found if column is not None)


def _at(value: Mapping[Any, Any], keys: tuple[str, ...]) -> object:
    """Return what `keys` reach through nested mappings; None when nothing. `on` is also YAML 1.1's `true`."""
    node: object = value
    for key in keys:
        if not isinstance(node, dict):
            return None
        node = node.get(key, node.get(True)) if key == "on" else node.get(key)
    return node


def _body(
    source: _Source, value: object, name: str, path: NodePath, diagnostics: list[Diagnostic]
) -> tuple[str | None, str | None]:
    """Read a routine's `body: "{{ file('<path>') }}"` through `resolve_file`, relative to the tool file.

    Returns:
        The file relative to the project root, and its text; None and None when `body:` is
        absent or refused. A refused `body:` is still declared, so the member is not also
        reported as having none (SST-VAL604).

    Diagnostics:
        SST-PRS003: `body:` is not a string.
        Every code `file_call` lists, for a `body:` that is not one `file()` call.
        Every code `resolve_file` lists, for the file it names.
    """
    subject = artifact_key(_TOOL.name, name.casefold())
    origin = source.origin(path)
    if value is None:
        return None, None
    if not isinstance(value, str):
        found = type(value).__name__
        diagnostics.append(
            D("SST-PRS003", artifact=subject, field="body", expected="string", found=found, origin=origin)
        )
        return None, None
    call = file_call(value, origin, field="body", artifact=subject, subject=subject)
    if isinstance(call, Diagnostic):
        diagnostics.append(call)
        return None, None
    sidecar = resolve_file(source.project_dir, source.source_file, call, origin=origin, subject=subject)
    if isinstance(sidecar, Diagnostic):
        diagnostics.append(sidecar)
        return None, None
    return sidecar.path, sidecar.text


def _where(
    source: _Source,
    value: object,
    name: str,
    on_model: str | None,
    referenced: bool,
    path: NodePath,
    diagnostics: list[Diagnostic],
) -> str | None:
    """Read a search service's `where:`, its `ref()` and `var()` calls resolved as a filter's are.

    A column ref becomes the bare column, upper-cased, as the service's query selects it; it
    must name the `on:` model. A referenced member's `where:` is a creation key it may not
    write (SST-VAL605), so it is kept as written.

    Diagnostics:
        Every code `resolve_scalar` lists for the filter-expression policy.
        SST-VAL622: a column ref names a model other than the `on:` model.
    """
    text = optional_string(value)
    if text is None or referenced:
        return text
    subject = artifact_key(_TOOL.name, name.casefold())
    origin = source.origin(path)

    def column_value(call: TemplateCall, column: str) -> str | Diagnostic:
        if on_model is not None and call.args[0].casefold() != on_model.casefold():
            return D(
                "SST-VAL622",
                origin=origin,
                subject=subject,
                name=name,
                field="where",
                found=call.args[0],
                expected=on_model,
            )
        return column.upper()

    resolved, problems = resolve_scalar(
        text,
        ref_field("tool.where"),
        origin,
        ResolveContext(source.dbt, variables=source.variables),
        field="tool.where",
        artifact=subject,
        subject=subject,
        column_value=column_value,
    )
    diagnostics.extend(problems)
    return resolved.text


def _on_fields(
    value: object,
    origin: Origin,
    name: str,
    diagnostics: list[Diagnostic],
) -> tuple[str | None, tuple[str, ...]]:
    """Read `on:`, either a `ref()` string or a mapping of `table`, `search_column` and `attributes`.

    Its `search_column` is read with the member's other columns.

    Returns:
        `(model, attributes)`: the model as the `ref()` names it, None when `on:` is absent or
        cannot be read; and the mapping's attributes as written, else `()`.

    Diagnostics:
        SST-PRS003: `on:` is neither a string nor a mapping, or its `table` is set and not text.
        SST-LOD004, SST-REF033, SST-REF003: its template does not parse.
        SST-VAL608: it is not exactly one single-argument `ref()` call.
    """
    raw: object = value
    attributes: tuple[str, ...] = ()
    if isinstance(value, dict):
        raw = value.get("table")
        attributes = strings(value.get("attributes"))
    if raw is None:
        return None, attributes
    if not isinstance(raw, str):
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=artifact_key(_TOOL.name, name),
                field="on",
                expected="ref() string",
                found=type(raw).__name__,
                origin=origin,
            )
        )
        return None, attributes
    try:
        calls = scan_template_calls(raw)
    except TemplateSyntaxError as exc:
        diagnostics.append(syntax_problem(exc, origin.file))
        return None, attributes
    if len(calls) != 1 or calls[0].function != "ref" or len(calls[0].args) != 1 or calls[0].raw.strip() != raw.strip():
        diagnostics.append(D("SST-VAL608", name=name, value=raw, origin=origin))
        return None, attributes
    return calls[0].args[0], attributes


def _signature(value: object, subject: str, origin: Origin, diagnostics: list[Diagnostic]) -> tuple[ToolParameter, ...]:
    """Read `signature:` as the routine's parameters, in order, skipping each malformed entry.

    A parameter needs a string `name` and `type`. Every parameter is required, since the routine
    renders no DEFAULT, so an entry that writes `required` is refused and the parameter kept.

    Diagnostics:
        SST-PRS003: when `signature:` is not a list, or an entry is not a mapping with a string
            `name` and `type`, once per entry.
        SST-PRS130: when an entry writes `required`, once per entry.
    """
    if value is None:
        return ()
    if not isinstance(value, list):
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=subject,
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
                    artifact=subject,
                    field="signature",
                    expected="name/type entries",
                    found=type(item).__name__,
                    origin=origin,
                )
            )
            continue
        if "required" in item:
            field = f"signature.{item['name']}.required"
            diagnostics.append(D("SST-PRS130", artifact=subject, field=field, subject=subject, origin=origin))
        parameters.append(ToolParameter(str(item["name"]), str(item["type"])))
    return tuple(parameters)


def _columns_of(
    value: object,
    name: str,
    path: NodePath,
    source: _Source,
    column: Callable[[str, object, NodePath], str | None],
    diagnostics: list[Diagnostic],
) -> tuple[ToolColumn, ...]:
    """Read `columns:`, a list of entries each naming its column with `column:`, in authored order.

    An absent `description` or `type` reads as `""`, and `searchable` and `filterable` as truth
    values, False when absent. An entry whose column does not resolve is left out.

    Args:
        column: Reads one column as the member names its columns, reporting what it finds.

    Diagnostics:
        SST-PRS003: the value is not a list, or an entry is not a mapping, once per entry.
        SST-PRS022, SST-PRS004: an entry writes a key a column does not model.
        SST-PRS002: an entry names no `column`.
        Every code `tool_column` lists, for each entry's `column:`.
    """
    subject = artifact_key(_TOOL.name, name.casefold())
    if value is None:
        return ()
    if not isinstance(value, list):
        origin = source.origin((*path, "columns"))
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=subject,
                field="columns",
                expected="list",
                found=type(value).__name__,
                origin=origin,
            )
        )
        return ()
    columns: list[ToolColumn] = []
    for index, raw in enumerate(value):
        node = (*path, "columns", index)
        if not isinstance(raw, dict):
            found = type(raw).__name__
            diagnostics.append(
                D(
                    "SST-PRS003",
                    artifact=subject,
                    field=f"columns.{index}",
                    expected="mapping",
                    found=found,
                    origin=source.origin(node),
                )
            )
            continue
        diagnostics.extend(_unknown(raw, TOOL_COLUMN_KEYS, subject, source, node, subject=subject))
        if optional_string(raw.get("column")) is None:
            diagnostics.append(
                D("SST-PRS002", artifact=subject, field=f"columns.{index}.column", origin=source.origin(node))
            )
            continue
        resolved = column(f"columns.{index}.column", raw.get("column"), (*node, "column"))
        if resolved is None:
            continue
        columns.append(
            ToolColumn(
                str(resolved),
                str(raw.get("description") or ""),
                str(raw.get("type") or ""),
                bool(raw.get("searchable", False)),
                bool(raw.get("filterable", False)),
            )
        )
    return tuple(columns)


def _mapping(value: object, field: str, subject: str, origin: Origin, diagnostics: list[Diagnostic]) -> dict[str, Any]:
    """Return a mapping field with text keys; `{}` when absent, and reported when of another type.

    Diagnostics:
        SST-PRS003: the value is present and not a mapping.
    """
    if value is None:
        return {}
    if not isinstance(value, dict):
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=subject,
                field=field,
                expected="mapping",
                found=type(value).__name__,
                origin=origin,
            )
        )
        return {}
    return {str(key): item for key, item in value.items()}


def _described(value: object, subject: str, origin: Origin, diagnostics: list[Diagnostic]) -> str | None:
    """Read a member's `description`, published as written: a template in it is SST-REF008."""
    text = optional_string(value)
    problem = literal_problem(text or "", origin, field="description", artifact=subject, subject=subject)
    if problem is not None:
        diagnostics.append(problem)
    return text


def _string_map(
    value: object,
    field: str,
    subject: str,
    origin: Origin,
    diagnostics: list[Diagnostic],
) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, dict) or any(not isinstance(item, str) for item in value.values()):
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=subject,
                field=field,
                expected="string mapping",
                found=type(value).__name__,
                origin=origin,
            )
        )
        return {}
    return {str(key): str(item) for key, item in value.items()}
