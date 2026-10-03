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
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.yaml.discover import discover_yaml
from snowflake_semantic_tools.adapters.yaml.documents import LoadCache, RawDocuments, load_documents
from snowflake_semantic_tools.adapters.yaml.ownership import assign_owners
from snowflake_semantic_tools.adapters.yaml.parse import parse_yaml_bytes
from snowflake_semantic_tools.adapters.yaml.semantic.build import _build_view
from snowflake_semantic_tools.adapters.yaml.semantic.checks.fanout import _attachment_diagnostics
from snowflake_semantic_tools.adapters.yaml.semantic.checks.instructions import _contradiction_diagnostics
from snowflake_semantic_tools.adapters.yaml.semantic.checks.rules import _rule_diagnostics
from snowflake_semantic_tools.adapters.yaml.semantic.checks.scope import _scope_diagnostics, view_scope
from snowflake_semantic_tools.adapters.yaml.semantic.checks.verified_queries import _duplicate_question_diagnostics
from snowflake_semantic_tools.adapters.yaml.semantic.collect import parse_semantic_project
from snowflake_semantic_tools.adapters.yaml.semantic.membership import membership
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
from snowflake_semantic_tools.adapters.yaml.semantic.view_instructions import _instruction_texts, _view_instructions
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtModel, DbtTarget
from snowflake_semantic_tools.domain.model.project import ParsedMember, ParsedView, ResolvedProject, SemanticViewProject
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.model.semantic_view import SemanticView
from snowflake_semantic_tools.domain.resolve.rendered import rendered_diagnostics
from snowflake_semantic_tools.domain.validate.dbt_seam import fan_out_diagnostics, seam_summary


@dataclass(frozen=True, slots=True)
class SemanticInputs:
    """What the semantic load reads from SST's own files: the config and every semantic-model document."""

    config: dict[str, Any]
    semantic_models_dir: str
    documents: RawDocuments


def read_semantic_inputs(
    files: ProjectPaths, config: Mapping[str, Any], cache: LoadCache | None = None
) -> SemanticInputs:
    """Read the configuration, then discover, parse, and assign every semantic-model document once.

    The caller reads these before it loads the dbt target and models, so a broken config or a
    missing semantic-models directory is reported before dbt is consulted. The documents carry
    discovery's diagnostics, then load's, then each file no registered type owns.

    Args:
        config: The run's configuration, with its templates resolved for the run's target.
        cache: The parses this run already holds, which an unchanged file is served from.

    Raises:
        ProjectError: the run has no configuration file, or a document cannot be read.
    """
    if files.config_file is None:
        raise ProjectError(f"no configuration file in {files.project_dir}")
    project_dir = files.project_dir
    config = dict(config)
    semantic_models_dir = str((config.get("project") or {}).get("semantic_models_dir") or "semantic_models")
    documents = load_documents(discover_yaml(project_dir, semantic_models_dir, config=config), parse_yaml_bytes, cache)
    unowned, _ = assign_owners(documents)
    documents = replace(documents, diagnostics=(*documents.diagnostics, *unowned))
    return SemanticInputs(config, semantic_models_dir, documents)


def load_semantic_views_result(
    project_dir: Path,
    inputs: SemanticInputs,
    *,
    target: DbtTarget,
    models: dict[str, DbtModel],
    catalog: DbtCatalog | None = None,
) -> SemanticViewProject:
    """Load healthy views while collecting view-local failures.

    Twelve phases run in this order, and their diagnostics are reported in it:

    1. Inputs: parse the documents into views and members, and resolve the member references
       in each custom instruction's text; a block whose text does not resolve is poisoned.
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
    8. View instructions, scope and content: each view's `custom_instructions()` entries and
       the pairs of them one view attaches that contradict each other, the columns, metrics and
       relationships it lists or excludes, then the file, prose and per-view rules of
       `checks.rules`.
    9. Poisoned views: each view an error of phases 1-8 names, whose name repeats, whose
       tables are malformed, or whose file uses the legacy globals.
    10. Member resolution: attach every unpoisoned member to the views it belongs to, and
        report what the attachment means (`domain.resolve.membership`), each member that
        reaches no view, and the verified queries one view attaches that share a question.
    11. Build every enabled, unpoisoned view under semantic_views/; one that fails is
        reported and left out. Then the dbt seam: each model that feeds several built views,
        and a summary of what was read.
    12. Check each built view against the members attached to it (`domain.resolve.rendered`).

    Poisoning keeps one fault to one diagnostic: a poisoned member attaches to no view and
    a poisoned view is not built, while everything else still builds. Phase 9 reads every
    diagnostic reported before it, so only a check that runs before phase 9 can keep the
    view it reports on from being built.

    Args:
        inputs: The project config and semantic-model documents, as `read_semantic_inputs` read them.
        target: The dbt target that `{{ target.database }}` and `{{ target.schema }}` name.
        models: The target's dbt models, by casefolded name.
        catalog: The manifest the models come from; None stands for one holding just `models`.

    Raises:
        ProjectError: A member cannot be read at all, or there is no semantic_views/ directory.
    """
    parsed = parse_semantic_project(inputs.documents, project_dir, inputs.semantic_models_dir, models)
    resolved_members, instruction_text_diagnostics, unresolved_instructions = _instruction_texts(
        parsed, catalog or DbtCatalog("v12", None, None, tuple(models.values()))
    )
    parsed = replace(parsed, members=resolved_members)
    members = _typed_members(parsed)
    context = _load_context(project_dir, inputs, target, models, catalog)
    structure = _structural_checks(context, parsed, members.metrics)
    semantic = _semantic_checks(context, members, structure.legacy_files)
    poison = _member_poison(parsed.members, members, structure.legacy_files, semantic)
    healthy = poison.healthy_metrics(members.metrics)
    view_tables = _view_tables(parsed.views)
    relationship_diagnostics, unattached = _relationship_checks(context, members, healthy, view_tables)
    using_diagnostics, misrouted = _using_checks(members, healthy)
    poison = poison.with_members(unattached | misrouted | unresolved_instructions)
    instruction_names, instruction_diagnostics = _view_instructions(parsed.views, members.instruction_names)
    scope_diagnostics = _scope_diagnostics(parsed.views, members.metrics, members.relationships, context.models)
    rule_diagnostics = _rule_diagnostics(
        context.documents, parsed, context.models, context.config, instruction_names, context.catalog.unreadable_models
    )
    reported = (
        *parsed.diagnostics,
        *structure.diagnostics,
        *semantic.diagnostics,
        *relationship_diagnostics,
        *using_diagnostics,
        *instruction_diagnostics,
        *instruction_text_diagnostics,
        *_contradiction_diagnostics({item.name.casefold(): item for item in members.instructions}, instruction_names),
        *scope_diagnostics,
        *rule_diagnostics,
    )
    # After every check: the views left unbuilt are read from the errors reported so far.
    poison = _view_poison(poison, parsed.views, reported, structure.duplicate_views, structure.legacy_files)
    built, attachment, diagnostics = _resolve_and_build(
        context, parsed.views, parsed.members, poison, dict(view_tables), instruction_names, reported
    )
    resolved = ResolvedProject(
        views=tuple(built.values()),
        attachment=attachment,
        custom_instruction_names=MappingProxyType(
            {artifact.casefold(): tuple(sorted(names)) for artifact, names in instruction_names.items()}
        ),
        diagnostics=DiagnosticBag((*reported, *diagnostics)),
    )
    return SemanticViewProject(resolved.views, resolved.diagnostics, _disabled_views(context))


def _resolve_and_build(
    context: LoadContext,
    views: tuple[ParsedView, ...],
    members: tuple[ParsedMember, ...],
    poison: Poison,
    view_tables: Mapping[str, frozenset[str]],
    instruction_names: Mapping[str, frozenset[str]],
    reported: tuple[Diagnostic, ...],
) -> tuple[dict[str, SemanticView], Mapping[str, tuple[str, ...]], tuple[Diagnostic, ...]]:
    """Phases 10 to 12: resolve membership, build the buildable views, and check what built.

    Returns:
        The views that built, by artifact key; the attachment; and the diagnostics of these
        phases in report order: member resolution's, each unattached member's, the build's,
        the dbt seam's, then the built views'.
    """
    built_nodes = tuple(_buildable_nodes(context, poison))
    scopes = {artifact_key("semantic_view", str(node["name"]).casefold()): view_scope(node) for _, node in built_nodes}
    attached_members, decided = membership(
        members,
        poison.member_keys,
        view_tables=view_tables,
        reported_views=frozenset(scopes),
        view_instructions=instruction_names,
        known_models=frozenset(context.models),
        reported=reported,
        view_scopes=scopes,
    )
    attachment = decided.attachment
    orphans = _unreported_orphans(_attachment_diagnostics(views, attached_members, attachment), decided.diagnostics)
    built, build_diagnostics = _build_views(context, built_nodes, attached_members, attachment)
    return (
        built,
        attachment,
        (
            *decided.diagnostics,
            *orphans,
            *_duplicate_question_diagnostics(attached_members, attachment),
            *build_diagnostics,
            *_seam_diagnostics(built, context.catalog),
            *rendered_diagnostics(built, attached_members, attachment),
        ),
    )


def _unreported_orphans(orphans: tuple[Diagnostic, ...], decided: tuple[Diagnostic, ...]) -> tuple[Diagnostic, ...]:
    """Drop each SST-VAL007 for a member that member resolution already reported unattached.

    SST-MEM005 names a member with tables that attaches to no view; SST-VAL007 says the same
    of any authored member, so it is kept only for the members SST-MEM005 does not cover,
    such as a custom instruction no view names.
    """
    unattached = {item.subject for item in decided if item.code == "SST-MEM005"}
    return tuple(item for item in orphans if item.subject not in unattached)


def _seam_diagnostics(built: Mapping[str, SemanticView], catalog: DbtCatalog) -> tuple[Diagnostic, ...]:
    """Report the dbt seam of the built views: each model several feed, then what was read.

    Diagnostics:
        SST-DBT016: as `fan_out_diagnostics` reports it.
        SST-DBT025: as `seam_summary` reports it.
    """
    feeds = {key: view.referenced_models for key, view in built.items()}
    return (
        *fan_out_diagnostics(feeds, catalog),
        seam_summary(
            catalog, {name for view in built.values() for name in view.referenced_models}, sum(map(len, feeds.values()))
        ),
    )


def _load_context(
    project_dir: Path,
    inputs: SemanticInputs,
    target: DbtTarget,
    models: dict[str, DbtModel],
    catalog: DbtCatalog | None = None,
) -> LoadContext:
    """Gather the fixed inputs every later phase reads.

    Raises:
        ProjectError: `<semantic_models_dir>/semantic_views` is not a directory.
    """
    views_dir = project_dir / inputs.semantic_models_dir / "semantic_views"
    if not views_dir.is_dir():
        raise ProjectError(f"no semantic_views/ directory under {project_dir / inputs.semantic_models_dir}")
    catalog = catalog or DbtCatalog(
        schema_version="", dbt_version=None, project_name=None, models=tuple(models.values())
    )
    return LoadContext(
        project_dir, inputs.config, inputs.semantic_models_dir, inputs.documents, views_dir, target, models, catalog
    )


def _build_views(
    context: LoadContext,
    nodes: tuple[tuple[Path, dict[str, Any]], ...],
    members: tuple[ParsedMember, ...],
    attachment: Mapping[str, tuple[str, ...]],
) -> tuple[dict[str, SemanticView], tuple[Diagnostic, ...]]:
    """Build each view of `nodes`, reporting a failure to build instead of raising it.

    Returns:
        The views that built, in node order, by their casefolded artifact keys; and the failures.
    """
    views: dict[str, SemanticView] = {}
    diagnostics: list[Diagnostic] = []
    for path, node in nodes:
        view_target = _semantic_view_target(context.config, path, context.views_dir, context.target)
        default_staleness = _semantic_view_defaults(context.config, path, context.views_dir).get("max_staleness")
        if "max_staleness" not in node and default_staleness is not None:
            # A view's own max_staleness wins; only an unset one takes the folder routes' default.
            node = {**node, "max_staleness": default_staleness}
        try:
            views[artifact_key("semantic_view", str(node["name"]).casefold())] = _build_view(
                node,
                path,
                context.project_dir,
                view_target,
                context.models,
                members,
                attachment,
                context.config,
                context.catalog.sources,
            )
        except ProjectError as exc:
            diagnostics.extend(_build_failure(context.project_dir, path, node, exc))
    return views, tuple(diagnostics)


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


def _disabled_views(context: LoadContext) -> tuple[str, ...]:
    """The casefolded names of the views under semantic_views/ that are disabled, in document order."""
    root_key = SEMANTIC_REGISTRY.artifacts["semantic_view"].root_key
    assert root_key is not None
    names: list[str] = []
    for document in context.documents.under(context.views_dir, root_key):
        for node in document.tree.get(root_key) or []:
            if not isinstance(node, dict) or not node.get("name"):
                continue
            enabled = node.get("enabled")
            if enabled is None:
                enabled = _semantic_view_defaults(context.config, document.abs_path, context.views_dir).get("enabled")
            if enabled is False:
                names.append(str(node["name"]).casefold())
    return tuple(names)


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
