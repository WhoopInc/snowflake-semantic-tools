"""Index what an import maps live names to: the project's compiled artifacts, dbt models and members.

`project_index` reads one compile of the project and its dbt manifest for the target an import
reads, so each physical name an imported object holds resolves to what publishes it there.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import PurePosixPath
from types import MappingProxyType

from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.domain.file_names import file_name
from snowflake_semantic_tools.domain.importing.references import Folded, Placed, ProjectIndex, ReferencedTool
from snowflake_semantic_tools.domain.model.config_schema import configured_dir
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.model.semantic_view import ColumnKind
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolOwnership

# The semantic member kinds an import checks the project for before writing one.
_MEMBER_KINDS = ("metric", "relationship", "filter", "verified_query", "custom_instruction")
# The types whose every artifact has a folder of its own, named after it, below its route.
OWN_FOLDER_TYPES = frozenset(("agent", "cowork_profile", "tool", "skill", "plugin"))


def project_index(
    result: CompileResult,
    dbt: DbtCatalog,
    tools: ToolCatalog,
    target_name: str,
    directories: Mapping[str, str],
) -> ProjectIndex:
    """Index the project's compiled names on the target, for an import to map live names to.

    Args:
        directories: Each artifact type's directory, as `sst_config.yml` sets it, which an
            artifact's folder is read below.
    """
    placed: list[Placed] = []
    for item in result.compiled:
        root = directories.get(item.artifact_type, "")
        # A view's sources include its members' files; its own is the one it was declared in.
        own_file = item.view.source_path if isinstance(item, CompiledView) else None
        source = own_file or (item.source_files[0] if item.source_files else "")
        # The key holds the name as the project spells it; a view's `name` is its object's.
        name = item.artifact_key.split(":", 1)[1]
        own = item.artifact_type in OWN_FOLDER_TYPES
        target = item.rendered_artifact.target
        placed.append(Placed(item.artifact_type, name, target, folder_below(source, root, name, own=own), source))
    models: dict[Folded, str] = {}
    for model in dbt.models:
        relation = _qualified(model.relation_name)
        if relation is not None:
            models[relation.folded] = model.name
    sources: dict[Folded, tuple[str, str]] = {}
    for table in dbt.sources:
        relation = _qualified(table.relation_name or "")
        if relation is not None:
            sources[relation.folded] = (table.source_name, table.name)
    referenced: dict[Folded, ReferencedTool] = {}
    for member in tools.members:
        if member.ownership is ToolOwnership.REFERENCE:
            relation = _qualified(member.relations.get(target_name, ""))
            if relation is not None:
                referenced[relation.folded] = ReferencedTool(member.name, member.type, relation)
    skills = frozenset(item.name.casefold() for item in placed if item.artifact_type == "skill")
    return ProjectIndex(
        target_name,
        tuple(placed),
        MappingProxyType(models),
        MappingProxyType(referenced),
        skills,
        MappingProxyType({model.name: model for model in dbt.models}),
        MappingProxyType(sources),
        MappingProxyType(_members(result)),
        MappingProxyType(_view_models(result, models)),
        tuple(
            (
                item.view.ai_sql_generation or "",
                item.view.ai_question_categorization or "",
                item.view.custom_instruction_names,
            )
            for item in result.compiled
            if isinstance(item, CompiledView) and item.view.custom_instruction_names
        ),
    )


def _view_models(result: CompileResult, models: Mapping[Folded, str]) -> dict[str, frozenset[str]]:
    """Return the dbt models each compiled view's tables are, by the view's name."""
    found: dict[str, frozenset[str]] = {}
    for item in result.compiled:
        if isinstance(item, CompiledView):
            relations = (_qualified(table.fqn) for table in item.view.tables)
            names = {models[relation.folded] for relation in relations if relation and relation.folded in models}
            found[item.artifact_key.split(":", 1)[1]] = frozenset(names)
    return found


def _members(result: CompileResult) -> dict[str, frozenset[str]]:
    """Return the names of the semantic members the compiled views hold, casefolded, by kind."""
    found: dict[str, set[str]] = {kind: set() for kind in _MEMBER_KINDS}
    for item in result.compiled:
        if not isinstance(item, CompiledView):
            continue
        view = item.view
        found["metric"].update(metric.name.casefold() for metric in view.metrics)
        found["relationship"].update(relationship.name.casefold() for relationship in view.relationships)
        found["filter"].update(column.name.casefold() for column in view.columns if column.kind is ColumnKind.FILTER)
        found["verified_query"].update(query.name.casefold() for query in view.verified_queries)
        found["custom_instruction"].update(name.casefold() for name in view.custom_instruction_names)
    return {kind: frozenset(names) for kind, names in found.items()}


def artifact_directories(config: Mapping[str, object]) -> dict[str, str]:
    """Return each artifact type's directory, as `sst_config.yml` sets it, else its default."""
    return {
        name: configured_dir(config, artifact.dir_key, artifact.dir_key.removesuffix("_dir"))
        for name, artifact in SEMANTIC_REGISTRY.artifacts.items()
        if artifact.dir_key
    }


def folder_below(source: str, root: str, name: str, *, own: bool) -> tuple[str, ...]:
    """Return a source file's folder below `root`, less the artifact's own folder when its type has one."""
    parts = PurePosixPath(source).parts
    root_parts = PurePosixPath(root).parts if root else ()
    if root_parts and parts[: len(root_parts)] == root_parts:
        parts = parts[len(root_parts) :]
    folder = parts[:-1]
    if own and folder and folder[-1] == file_name(name):
        folder = folder[:-1]
    return tuple(folder)


def _qualified(value: str) -> QualifiedName | None:
    try:
        return QualifiedName.parse(value)
    except ValueError:
        return None
