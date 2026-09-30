"""Build the deterministic compiled manifest from one compile result."""

from __future__ import annotations

from types import MappingProxyType

from ..domain.model.diagnostic import Severity
from ..domain.state import SST_VERSION, ArtifactEntry, ImpactIndex, Manifest
from .compile import CompileResult


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
) -> Manifest:
    artifacts: dict[str, ArtifactEntry] = {}
    by_file: dict[str, tuple[str, ...]] = {}
    by_dbt_model: dict[str, tuple[str, ...]] = {}
    by_member: dict[str, tuple[str, ...]] = {}
    dbt_models: dict[str, object] = {}
    for compiled in result.compiled:
        rendered = compiled.rendered_artifact
        source_files = compiled.source_files
        members = compiled.member_keys
        artifacts[compiled.artifact_key] = ArtifactEntry(
            type=compiled.artifact_type,
            name=compiled.name.casefold(),
            fingerprint=rendered.fingerprint,
            source_files=source_files,
            member_keys=members,
            depends_on=rendered.depends_on,
            publish_target=rendered.target.sql,
            byte_length=len(rendered.content.encode("utf-8")),
            object_type=rendered.object_type,
            render_dialect=rendered.render_dialect,
            diagnostic_codes=tuple(
                sorted(
                    diagnostic.code for diagnostic in result.diagnostics if diagnostic.subject == compiled.artifact_key
                )
            ),
            component_fingerprints=rendered.component_fingerprints,
            physical_resources=tuple((object_type, name.sql) for object_type, name in rendered.physical_resources),
        )
        for source_file in source_files:
            by_file[source_file] = tuple(sorted({*by_file.get(source_file, ()), compiled.artifact_key}))
        relation_by_model = dict(compiled.dbt_relations)
        for model_name in compiled.referenced_models:
            by_dbt_model[model_name] = tuple(sorted({*by_dbt_model.get(model_name, ()), compiled.artifact_key}))
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
        schema_version=2,
        manifest_id="",
        generator=MappingProxyType({"name": "sst", "version": SST_VERSION}),
        project=MappingProxyType(
            {
                "root": project_root,
                "semantic_path": semantic_path,
                "dbt_project_name": dbt_project_name,
                "config_checksum": config_checksum,
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
        members=MappingProxyType({key: {"attached_to": list(value)} for key, value in sorted(by_member.items())}),
        dbt_models=MappingProxyType(dbt_models),
        files=MappingProxyType(files),
        impact=ImpactIndex(
            MappingProxyType(by_file),
            MappingProxyType(by_dbt_model),
            MappingProxyType(by_member),
        ),
        diagnostics_summary=MappingProxyType(
            {
                "error": result.diagnostics.count(Severity.ERROR),
                "warning": result.diagnostics.count(Severity.WARNING),
                "info": result.diagnostics.count(Severity.INFO),
            }
        ),
    )
    return manifest.with_computed_id()
