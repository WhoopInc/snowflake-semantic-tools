"""The semantic load: parse, check, attach and build every view, collecting each diagnostic on the way.

`load_semantic_views_result` runs the load as a fixed sequence of phases. The checks live
in `phases` and `view_instructions`, the poisoning in `poison`; attaching and building
live here. Every phase returns its diagnostics, which the loader reports in phase order.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from types import MappingProxyType
from typing import Any

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.yaml.documents import RawDocuments, discover_yaml, load_documents
from snowflake_semantic_tools.adapters.yaml.parse import parse_yaml_bytes, read_yaml_mapping
from snowflake_semantic_tools.adapters.yaml.semantic.build import _build_view
from snowflake_semantic_tools.adapters.yaml.semantic.checks.fanout import _attachment_diagnostics
from snowflake_semantic_tools.adapters.yaml.semantic.checks.rules import _rule_diagnostics
from snowflake_semantic_tools.adapters.yaml.semantic.checks.scope import _scope_diagnostics
from snowflake_semantic_tools.adapters.yaml.semantic.collect import parse_semantic_project
from snowflake_semantic_tools.adapters.yaml.semantic.defs import MetricDef
from snowflake_semantic_tools.adapters.yaml.semantic.phases import (
    LoadContext,
    _relationship_checks,
    _semantic_checks,
    _structural_checks,
    _typed_members,
    _using_checks,
    _view_tables,
)
from snowflake_semantic_tools.adapters.yaml.semantic.poison import Poison, _member_poison, _view_poison
from snowflake_semantic_tools.adapters.yaml.semantic.target import _semantic_view_defaults, _semantic_view_target
from snowflake_semantic_tools.adapters.yaml.semantic.view_instructions import _view_instructions
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.dbt import DbtModel, DbtTarget
from snowflake_semantic_tools.domain.model.project import ParsedMember, ResolvedProject, SemanticViewProject
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.model.semantic_view import SemanticView
from snowflake_semantic_tools.domain.resolve.members import attach_view_members


@dataclass(frozen=True, slots=True)
class SemanticInputs:
    """What the semantic load reads from SST's own files: the config and every semantic-model document."""

    config: dict[str, Any]
    semantic_models_dir: str
    documents: RawDocuments


def read_semantic_inputs(project_dir: Path) -> SemanticInputs:
    """Read `sst_config.yml`, then discover and parse every semantic-model document once.

    The caller reads these before it loads the dbt target and models, so a broken config or a
    missing semantic-models directory is reported before dbt is consulted.
    """
    config = read_yaml_mapping(project_dir / "sst_config.yml")
    semantic_models_dir = str((config.get("project") or {}).get("semantic_models_dir") or "semantic_models")
    documents = load_documents(discover_yaml(project_dir, semantic_models_dir), parse_yaml_bytes)
    return SemanticInputs(config, semantic_models_dir, documents)


def load_semantic_views_result(
    project_dir: Path,
    inputs: SemanticInputs,
    *,
    target: DbtTarget,
    models: dict[str, DbtModel],
) -> SemanticViewProject:
    """Load healthy views while collecting view-local failures.

    Eleven phases run in this order, and their diagnostics are reported in it:

    1. Inputs: parse the documents into views and members.
    2. Typed members: split the members by type and find the metric cycles. Only then is
       a missing semantic_views/ directory refused, so an unreadable member is reported first.
    3. Structural checks: repeated view names, malformed tables, routes, authored keys,
       descriptions, shapes, the dbt models and columns in use, legacy globals, verified queries.
    4. Semantic checks: filter, verified query and metric references, tables that are not
       dbt models, metric cycles.
    5. Member poison, from what phases 2-4 found; this also fixes the healthy metrics.
    6. Relationships, against the views and the healthy metrics; one whose tables share no
       view is poisoned.
    7. The healthy metrics' `using_relationships`; a metric naming a relationship that is
       missing or starts elsewhere is poisoned.
    8. View instructions, scope and content: each view's `custom_instructions()` entries, the
       columns, metrics and relationships it lists or excludes, then the file, prose and
       per-view rules of `checks.rules`.
    9. Poisoned views: each view an error of phases 1-8 names, whose name repeats, whose
       tables are malformed, or whose file uses the legacy globals.
    10. Attach every unpoisoned member to the views it belongs to, and report where each went.
    11. Build every enabled, unpoisoned view under semantic_views/; one that fails is
        reported and left out.

    Poisoning keeps one fault to one diagnostic: a poisoned member attaches to no view and
    a poisoned view is not built, while everything else still builds. Phase 9 reads every
    diagnostic reported before it, so only a check that runs before phase 9 can keep the
    view it reports on from being built.

    Args:
        inputs: The project config and semantic-model documents, as `read_semantic_inputs` read them.
        target: The dbt target that `{{ target.database }}` and `{{ target.schema }}` name.
        models: The target's dbt models, by casefolded name.

    Raises:
        ProjectError: A member cannot be read at all, or there is no semantic_views/ directory.
    """
    parsed = parse_semantic_project(inputs.documents, project_dir, inputs.semantic_models_dir, models)
    members = _typed_members(parsed)
    context = _load_context(project_dir, inputs, target, models)
    structure = _structural_checks(context, parsed, members.metrics)
    semantic = _semantic_checks(context, members, structure.legacy_files)
    poison = _member_poison(parsed.members, members, structure.legacy_files, semantic)
    healthy = poison.healthy_metrics(members.metrics)
    view_tables = _view_tables(parsed.views)
    relationship_diagnostics, unattached = _relationship_checks(context, members, healthy, view_tables)
    using_diagnostics, misrouted = _using_checks(members, healthy)
    poison = poison.with_members(unattached | misrouted)
    instruction_names, instruction_diagnostics = _view_instructions(parsed.views, members.instruction_names)
    scope_diagnostics = _scope_diagnostics(parsed.views, members.metrics, members.relationships, context.models)
    rule_diagnostics = _rule_diagnostics(context.documents, parsed, context.models, context.config, instruction_names)
    reported = (
        *parsed.diagnostics,
        *structure.diagnostics,
        *semantic.diagnostics,
        *relationship_diagnostics,
        *using_diagnostics,
        *instruction_diagnostics,
        *scope_diagnostics,
        *rule_diagnostics,
    )
    # After every check: the views left unbuilt are read from the errors reported so far.
    poison = _view_poison(poison, parsed.views, reported, structure.duplicate_views, structure.legacy_files)
    attached_members, attachment = _attach(parsed.members, poison, view_tables, instruction_names)
    attachment_diagnostics = _attachment_diagnostics(parsed.views, attached_members, attachment)
    views, build_diagnostics = _build_views(context, poison, attached_members, attachment)
    resolved = ResolvedProject(
        views=views,
        attachment=attachment,
        custom_instruction_names=MappingProxyType(
            {artifact.casefold(): tuple(sorted(names)) for artifact, names in instruction_names.items()}
        ),
        diagnostics=DiagnosticBag((*reported, *attachment_diagnostics, *build_diagnostics)),
    )
    return SemanticViewProject(resolved.views, resolved.diagnostics)


def _load_context(
    project_dir: Path, inputs: SemanticInputs, target: DbtTarget, models: dict[str, DbtModel]
) -> LoadContext:
    """Gather the fixed inputs every later phase reads.

    Raises:
        ProjectError: `<semantic_models_dir>/semantic_views` is not a directory.
    """
    views_dir = project_dir / inputs.semantic_models_dir / "semantic_views"
    if not views_dir.is_dir():
        raise ProjectError(f"no semantic_views/ directory under {project_dir / inputs.semantic_models_dir}")
    return LoadContext(
        project_dir, inputs.config, inputs.semantic_models_dir, inputs.documents, views_dir, target, models
    )


def _attach(
    parsed_members: tuple[ParsedMember, ...],
    poison: Poison,
    view_tables: tuple[tuple[str, frozenset[str]], ...],
    view_instructions: Mapping[str, frozenset[str]],
) -> tuple[tuple[ParsedMember, ...], Mapping[str, tuple[str, ...]]]:
    """Mark the poisoned members, then attach every member to the views it belongs to.

    Returns:
        Every member, the poisoned ones marked so, and the view keys each member key attaches to.
    """
    members = tuple(
        (replace(member, poisoned=True) if member.key in poison.member_keys else member) for member in parsed_members
    )
    metric_dependencies = {
        member.key: tuple(artifact_key("metric", name) for name in member.source.referenced_metrics)
        for member in members
        if member.type_name == "metric" and isinstance(member.source, MetricDef)
    }
    attachment = attach_view_members(
        dict(view_tables),
        members,
        SEMANTIC_REGISTRY,
        view_named_members=view_instructions,
        metric_dependencies=metric_dependencies,
    )
    return members, attachment


def _build_views(
    context: LoadContext,
    poison: Poison,
    members: tuple[ParsedMember, ...],
    attachment: Mapping[str, tuple[str, ...]],
) -> tuple[tuple[SemanticView, ...], tuple[Diagnostic, ...]]:
    """Build each view `_buildable_nodes` yields, reporting a failure to build instead of raising it."""
    views: list[SemanticView] = []
    diagnostics: list[Diagnostic] = []
    for path, node in _buildable_nodes(context, poison):
        view_target = _semantic_view_target(context.config, path, context.views_dir, context.target)
        try:
            views.append(
                _build_view(
                    node, path, context.project_dir, view_target, context.models, members, attachment, context.config
                )
            )
        except ProjectError as exc:
            diagnostics.extend(_build_failure(context.project_dir, path, node, exc))
    return tuple(views), tuple(diagnostics)


def _buildable_nodes(context: LoadContext, poison: Poison) -> Iterator[tuple[Path, dict[str, Any]]]:
    """Yield, in document order, each named view under semantic_views/ that is enabled and not poisoned.

    A view's own `enabled` wins; only an unset one falls back to the folder routes' `+enabled`.
    """
    root_key = SEMANTIC_REGISTRY.artifacts["semantic_view"].root_key
    assert root_key is not None
    for document in context.documents.under(context.views_dir, root_key):
        path = document.abs_path
        for node in document.tree.get(root_key) or []:
            if not isinstance(node, dict) or not node.get("name"):
                continue
            if node.get("enabled") is False:
                continue
            if (
                node.get("enabled") is None
                and _semantic_view_defaults(context.config, path, context.views_dir).get("enabled") is False
            ):
                continue
            if str(node["name"]).casefold() in poison.view_names:
                continue
            if artifact_key("semantic_view", node["name"]).casefold() in poison.view_keys:
                continue
            yield path, node


def _build_failure(project_dir: Path, path: Path, node: dict[str, Any], exc: ProjectError) -> tuple[Diagnostic, ...]:
    """Report a view that failed to build, at its file and key wherever the failure names neither."""
    view_origin = Origin(path.resolve().relative_to(project_dir.resolve()).as_posix())
    view_subject = artifact_key("semantic_view", node["name"])
    if exc.diagnostics:
        return tuple(
            replace(item, origin=item.origin or view_origin, subject=item.subject or view_subject)
            for item in exc.diagnostics
        )
    return (D("SST-PRS123", origin=view_origin, subject=view_subject, artifact=view_subject, detail=str(exc)),)
