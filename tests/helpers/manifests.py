"""Build small valid manifests for tests, straight from rendered artifacts."""

from __future__ import annotations

from types import MappingProxyType
from typing import Mapping

from snowflake_semantic_tools.domain.model.lifecycle import RenderedArtifact
from snowflake_semantic_tools.domain.state.model import (
    MANIFEST_SCHEMA_VERSION,
    SST_VERSION,
    ArtifactEntry,
    ImpactIndex,
    Manifest,
)


def build_minimal_manifest(
    artifacts: Mapping[str, RenderedArtifact],
    *,
    project: Mapping[str, object],
    sources: Mapping[str, object],
    members: Mapping[str, object],
    files: Mapping[str, object],
    impact: ImpactIndex,
    diagnostics_summary: Mapping[str, int],
) -> Manifest:
    entries = {
        key: ArtifactEntry(
            type=artifact.artifact_type,
            name=artifact.target.artifact_name,
            fingerprint=artifact.fingerprint,
            source_files=(),
            member_keys=(),
            depends_on=artifact.depends_on,
            publish_target=artifact.target.sql,
            byte_length=len(artifact.ddl.encode("utf-8")),
            object_type=artifact.object_type,
            render_dialect=artifact.render_dialect,
            component_fingerprints=artifact.component_fingerprints,
            physical_resources=tuple((object_type, name.sql) for object_type, name in artifact.physical_resources),
        )
        for key, artifact in artifacts.items()
    }
    manifest = Manifest(
        schema_version=MANIFEST_SCHEMA_VERSION,
        manifest_id="",
        generator=MappingProxyType({"name": "sst", "version": SST_VERSION}),
        project=MappingProxyType(dict(project)),
        sources=MappingProxyType(dict(sources)),
        artifacts=MappingProxyType(entries),
        members=MappingProxyType(dict(members)),
        dbt_models=MappingProxyType({}),
        files=MappingProxyType(dict(files)),
        impact=impact,
        diagnostics_summary=MappingProxyType(dict(diagnostics_summary)),
    )
    return manifest.with_computed_id()
