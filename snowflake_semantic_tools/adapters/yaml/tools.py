"""Load tool members from YAML into immutable domain declarations."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType
from typing import Any

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.yaml.fields import (
    refused,
)
from snowflake_semantic_tools.adapters.yaml.parse import parse_yaml_bytes
from snowflake_semantic_tools.adapters.yaml.routes import routed_diagnostics
from snowflake_semantic_tools.adapters.yaml.tool_members import TOOL, ToolSource, parse_member, unknown_tool_keys
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.authored import NodePath
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from snowflake_semantic_tools.domain.model.tool import (
    ToolCatalog,
    ToolMember,
)
from snowflake_semantic_tools.domain.validate.dbt_seam import literal_relation_diagnostic
from snowflake_semantic_tools.domain.validate.tool import validate_tool_catalog

# The tool type as the registry declares it, as every other loader reads its type: the root
# holding one member (`tool:`) and the root holding a list (`tools:`), the `sst_config.yml` block
# its settings and folder routes resolve through, and the kind part of each member's key.
_ONE, _MANY = str(TOOL.singular_root), str(TOOL.root_key)
_FILE_KEYS = frozenset((_ONE, _MANY))


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
        Every code `parse_member` lists, for each member.
        Every code `validate_tool_catalog` lists, for the members that load.
        SST-DBT006: a reference relation names a dbt model by the name its alias replaced.
    """
    tree = config if config is not None else {}
    diagnostics: list[Diagnostic] = list(routed_diagnostics(tree, project_dir, TOOL.name)) if config else []
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
        source = ToolSource(
            project_dir,
            relative,
            parsed.line_index,
            path.parent.relative_to(root).parts,
            dbt,
            tree.get(TOOL.routed_block),
            MappingProxyType(variables),
        )
        diagnostics.extend(unknown_tool_keys(loaded, _FILE_KEYS, relative, source, ()))
        for node, value in _entries(loaded, source, diagnostics):
            member, member_diagnostics = parse_member(source, node, value)
            diagnostics.extend(member_diagnostics)
            if member is not None:
                members.append(member)
    catalog = ToolCatalog(tuple(members), target_name, declared_targets, DiagnosticBag(diagnostics))
    return replace(
        catalog,
        diagnostics=DiagnosticBag((*validate_tool_catalog(catalog, dbt), *_aliased_relations(catalog, dbt))),
    )


def _entries(
    loaded: Mapping[str, Any], source: ToolSource, diagnostics: list[Diagnostic]
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


# A search service's refresh modes, as a refusal of a value that is not text names them; text
# is matched, in any case, by the tool validator.
