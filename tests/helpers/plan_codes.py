"""Plans the per-code PLN tests build: small views, observations, and states, and a result to split."""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle import (
    ChangeSet,
    GrantRow,
    ObservedArtifact,
    OwnershipMarker,
    RenderedArtifact,
    SnowflakeObservation,
)
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.plan import build_changeset
from snowflake_semantic_tools.domain.plan.preflight import Preflight
from snowflake_semantic_tools.domain.state import STATE_SCHEMA_VERSION, AppliedEntry, ImpactIndex, Manifest, State
from tests.helpers.manifests import build_minimal_manifest
from tests.helpers.sql_values import statement


def target() -> TargetIdentity:
    return TargetIdentity("dev", "acct", Identifier.parse("db"), Identifier.parse("sch"), "DEPLOYER", "WH")


def view(
    name: str,
    *,
    depends_on: tuple[str, ...] = (),
    relations: tuple[str, ...] = (),
    database: str = "db",
    schema: str = "sch",
) -> RenderedArtifact:
    """A semantic view `database.schema.name` that depends on `depends_on` and reads `relations`."""
    return RenderedArtifact.create(
        key=f"semantic_view:{name.casefold()}",
        artifact_type="semantic_view",
        target=QualifiedName.from_parts(database, schema, name),
        ddl=statement(f"create semantic view {name}"),
        depends_on=depends_on,
        required_relations=tuple(QualifiedName.parse(relation) for relation in relations),
    )


def manifest_of(*artifacts: RenderedArtifact) -> Manifest:
    return build_minimal_manifest(
        {item.key: item for item in artifacts},
        project={},
        sources={},
        members={},
        files={},
        impact=ImpactIndex(),
        diagnostics_summary={},
    )


def entry(artifact: RenderedArtifact, manifest_id: str, *, qualified_name: str | None = None) -> AppliedEntry:
    """The state entry apply records for `artifact` under `manifest_id`."""
    return AppliedEntry(
        artifact.fingerprint,
        qualified_name or artifact.target.sql,
        "now",
        "run",
        "applied",
        artifact.fingerprint,
        manifest_id,
    )


def live(
    artifact: RenderedArtifact,
    *,
    marker: OwnershipMarker | None = None,
    object_type: str = "SEMANTIC VIEW",
    raw_name: str | None = None,
    grants: tuple[GrantRow, ...] = (),
) -> ObservedArtifact:
    """What observation finds under `artifact`'s key."""
    return ObservedArtifact(
        artifact.key,
        raw_name or artifact.target.name.folded,
        artifact.target,
        object_type,
        "OWNER",
        "now",
        marker.text if marker else None,
        marker,
        grants,
    )


def state_of(manifest: Manifest, applied: dict[str, AppliedEntry] | None = None) -> State:
    return State(STATE_SCHEMA_VERSION, target(), manifest.manifest_id, "cfg", None, MappingProxyType(applied or {}))


def plan(
    rendered: tuple[RenderedArtifact, ...],
    *,
    observed: tuple[ObservedArtifact, ...] = (),
    applied: dict[str, AppliedEntry] | None = None,
    preflight: Preflight | None = None,
    include_prune: bool = False,
    manifest: Manifest | None = None,
) -> ChangeSet:
    """Plan `rendered` against `observed` and the state `applied` records, in the domain."""
    planned = manifest or manifest_of(*rendered)
    return build_changeset(
        {item.key: item for item in rendered},
        SnowflakeObservation(MappingProxyType({item.key: item for item in observed}), "now"),
        planned,
        state_of(planned, applied),
        SEMANTIC_REGISTRY,
        target(),
        include_prune=include_prune,
        preflight=preflight,
    )


def codes(changeset: ChangeSet) -> list[str]:
    return [item.code for item in changeset.diagnostics]


def only(changeset: ChangeSet, code: str) -> list[Diagnostic]:
    """The diagnostics of `changeset` carrying `code`."""
    return [item for item in changeset.diagnostics if item.code == code]


@dataclass(frozen=True)
class _Rendered:
    depends_on: tuple[str, ...]


@dataclass(frozen=True)
class _Compiled:
    artifact_key: str
    depends_on: tuple[str, ...] = ()

    @property
    def rendered_artifact(self) -> _Rendered:
        return _Rendered(self.depends_on)


def partial_result(*diagnostics: Diagnostic) -> CompileResult:
    """A compile result `--partial` splits: two views, an agent on both, and an eval of the agent."""
    compiled = (
        _Compiled("semantic_view:sales"),
        _Compiled("semantic_view:menu"),
        _Compiled("agent:analyst", ("semantic_view:menu", "semantic_view:sales")),
        _Compiled("eval:analyst", ("agent:analyst",)),
    )
    return CompileResult(compiled, DiagnosticBag(diagnostics))  # type: ignore[arg-type]
