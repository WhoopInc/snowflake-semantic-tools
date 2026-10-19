"""Build the deterministic compiled manifest from one compile result, and check one read back.

`manifest_for` builds it from what `ProjectInputs.manifest_sources` read about the project's
files; `stale_manifest` reports why a run must not start when `sst compile` wrote another one,
and `target_mismatch` when it compiled for another target. `read_notes` reports how a stored
manifest was read, and `dbt_manifest_moved` a dbt manifest rewritten while a run read it.
`build_manifest` does the building: each member's `attached_to` and each dbt model's
`referenced_by` list artifact keys sorted, and the id
hashes the canonical document, so the order of the compiled artifacts never changes the id,
while the SST version and each recorded checksum do. Artifact names are recorded casefolded,
and each artifact lists, sorted, the codes of the diagnostics whose subject is its key.
"""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.app.compile import CompiledArtifact, CompiledView, CompileResult
from snowflake_semantic_tools.app.plan_diff import compiled_fingerprints
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Severity
from snowflake_semantic_tools.domain.lineage.members import member_edges, member_expressions
from snowflake_semantic_tools.domain.model.dbt import DbtNode
from snowflake_semantic_tools.domain.ports.project import ManifestSources
from snowflake_semantic_tools.domain.state import MANIFEST_SCHEMA_VERSION, SST_VERSION, ArtifactEntry, Manifest


def manifest_for(result: CompileResult, sources: ManifestSources) -> Manifest:
    """Build the manifest of `result`, recording what `sources` read about the project's files."""
    return build_manifest(
        result,
        project_root=".",
        semantic_path=sources.semantic_path,
        dbt_project_name=sources.dbt_project_name,
        config_checksum=sources.config_checksum,
        dbt_manifest_path=sources.dbt_manifest_path,
        dbt_schema_version=sources.dbt_schema_version,
        dbt_digest=sources.dbt_digest,
        model_count=sources.model_count,
        file_checksums=dict(sources.file_checksums),
        target_name=sources.target_name,
        dbt_graph=sources.dbt_graph,
    )


def stale_manifest(compiled: Manifest, current: Manifest, *, before: str) -> Diagnostic | None:
    """Report a compile that no longer matches the project, so a run must not start; None if it does.

    Args:
        compiled: The manifest `sst compile` wrote.
        current: The manifest of the project as it compiles now.
        before: The run that must wait for a fresh compile, as the message names it.

    Diagnostics:
        SST-MAN009: the compiled manifest is not the one the project compiles to now.
    """
    if compiled.manifest_id != current.manifest_id:
        return D("SST-MAN009", command=before)
    return None


def target_mismatch(compiled: Manifest, current: Manifest) -> Diagnostic | None:
    """Report a compiled manifest made for another target than the run's; None when they agree.

    A manifest that records no target, as one written before targets were recorded, agrees
    with every target.

    Diagnostics:
        SST-MAN006: the compiled manifest records another target than the current one.
    """
    found = str(compiled.project.get("target") or "")
    expected = str(current.project.get("target") or "")
    if found and expected and found != expected:
        return D("SST-MAN006", found=found, expected=expected)
    return None


def read_notes(manifest: Manifest, path: str) -> tuple[Diagnostic, ...]:
    """Report how a stored manifest was read: migrated in memory from an older schema, or as written.

    Diagnostics:
        SST-MAN201: the manifest declared an older schema and was migrated in memory; the file
            is left as it is.
    """
    if manifest.migrated_from is None:
        return ()
    return (D("SST-MAN201", path=path, found=manifest.migrated_from, expected=manifest.schema_version),)


def dbt_manifest_moved(before: ManifestSources, after: ManifestSources) -> Diagnostic | None:
    """Report a dbt manifest whose models changed between two reads of one run; None when they agree.

    Diagnostics:
        SST-MAN031: the dbt manifest's model digest differs between the two reads, so the run
            compiled from one manifest and would record another.
    """
    if before.dbt_digest != after.dbt_digest:
        return D("SST-MAN031")
    return None


def build_manifest(
    result: CompileResult,
    *,
    project_root: str = ".",
    semantic_path: str = "semantic_models",
    dbt_project_name: str = "",
    config_checksum: str = "",
    dbt_manifest_path: str = "target/manifest.json",
    dbt_schema_version: str = "v12",
    dbt_digest: str = "",
    model_count: int = 0,
    file_checksums: dict[str, str] | None = None,
    target_name: str = "",
    dbt_graph: tuple[DbtNode, ...] = (),
) -> Manifest:
    """Build the manifest of `result`, with what the keyword arguments say about the project.

    Args:
        target_name: The target the artifacts were compiled for; recorded under `project.target`
            unless empty.
    """
    artifacts: dict[str, ArtifactEntry] = {}
    by_member: dict[str, tuple[str, ...]] = {}
    dbt_models: dict[str, object] = {}
    for compiled in result.compiled:
        members = compiled.member_keys
        artifacts[compiled.artifact_key] = _artifact_entry(compiled, result)
        relation_by_model = dict(compiled.dbt_relations)
        for model_name in compiled.referenced_models:
            existing = dbt_models.get(model_name)
            referenced_by = [compiled.artifact_key]
            if isinstance(existing, dict):
                referenced_by.extend(str(item) for item in existing.get("referenced_by", []))
            dbt_models[model_name] = {
                "relation": relation_by_model.get(model_name, ""),
                "referenced_by": sorted(set(referenced_by)),
            }
        for member in members:
            by_member[member] = tuple(sorted({*by_member.get(member, ()), compiled.artifact_key}))

    files = {path: {"checksum": checksum} for path, checksum in sorted((file_checksums or {}).items())}
    manifest = Manifest(
        schema_version=MANIFEST_SCHEMA_VERSION,
        manifest_id="",
        generator=MappingProxyType({"name": "sst", "version": SST_VERSION}),
        project=MappingProxyType(
            {
                "root": project_root,
                "semantic_path": semantic_path,
                "dbt_project_name": dbt_project_name,
                "config_checksum": config_checksum,
                **({"target": target_name} if target_name else {}),
            }
        ),
        sources=MappingProxyType(
            {
                "dbt_manifest": {
                    "path": dbt_manifest_path,
                    "dbt_schema_version": dbt_schema_version,
                    "digest": dbt_digest,
                    "model_count": model_count,
                },
                "semantic_file_count": len(files),
            }
        ),
        artifacts=MappingProxyType(artifacts),
        members=MappingProxyType(_members(by_member, result)),
        dbt_models=MappingProxyType(dbt_models),
        dbt_nodes=MappingProxyType({node.unique_id: _dbt_node(node) for node in dbt_graph}),
        files=MappingProxyType(files),
        diagnostics_summary=MappingProxyType(
            {
                "error": result.diagnostics.count(Severity.ERROR),
                "warning": result.diagnostics.count(Severity.WARNING),
                "info": result.diagnostics.count(Severity.INFO),
            }
        ),
    )
    return manifest.with_computed_id()


# What a member's manifest entry records it reads, in `MemberEdges` order.
_MEMBER_READS = ("tables", "uses", "columns", "unresolved_columns")


def _members(by_member: dict[str, tuple[str, ...]], result: CompileResult) -> dict[str, object]:
    """Return each member's entry: the views it attaches to, then what it reads in any of them.

    A member several views attach is one node of lineage, so what it reads is the union of what
    each view's copy reads; its expression is the first view's, in compile order.
    """
    read: dict[str, dict[str, set[str]]] = {}
    expressions: dict[str, str] = {}
    for compiled in result.compiled:
        if not isinstance(compiled, CompiledView):
            continue
        for key, text in member_expressions(compiled.view).items():
            expressions.setdefault(key, text)
        for key, edges in member_edges(compiled.view).items():
            found = read.setdefault(key, {field: set() for field in _MEMBER_READS})
            for field, values in zip(
                _MEMBER_READS, (edges.tables, edges.uses, edges.columns, edges.unresolved), strict=True
            ):
                found[field].update(values)
    return {
        key: {
            "attached_to": list(views),
            **{field: sorted(values) for field, values in read.get(key, {}).items() if values},
            **({"expr": expressions[key]} if key in expressions else {}),
        }
        for key, views in sorted(by_member.items())
    }


def _dbt_node(node: DbtNode) -> dict[str, object]:
    """Return one node of dbt's graph as the manifest records it under `dbt_nodes`."""
    return {
        "kind": node.kind,
        "name": node.name,
        "package": node.package,
        "relation": node.relation,
        "depends_on": list(node.depends_on),
    }


def _artifact_entry(compiled: CompiledArtifact, result: CompileResult) -> ArtifactEntry:
    """The manifest entry of one compiled artifact, listing the codes of the diagnostics about it."""
    rendered = compiled.rendered_artifact
    return ArtifactEntry(
        type=compiled.artifact_type,
        name=compiled.name.casefold(),
        fingerprint=rendered.fingerprint,
        source_files=compiled.source_files,
        member_keys=compiled.member_keys,
        depends_on=rendered.depends_on,
        references=rendered.references,
        publish_target=rendered.target.sql,
        byte_length=len(rendered.content.encode("utf-8")),
        object_type=rendered.object_type,
        render_dialect=rendered.render_dialect,
        diagnostic_codes=tuple(
            sorted(diagnostic.code for diagnostic in result.diagnostics if diagnostic.subject == compiled.artifact_key)
        ),
        component_fingerprints=rendered.component_fingerprints,
        physical_resources=tuple((object_type, name.sql) for object_type, name in rendered.physical_resources),
        settings=compiled.settings,
        member_fingerprints=compiled_fingerprints(compiled),
    )
