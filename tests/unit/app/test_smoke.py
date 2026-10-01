"""Smoke ownership over in-memory ports: probes run only against objects SST is proven to own."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.manifest import manifest_for
from snowflake_semantic_tools.app.smoke import SmokePublished
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker
from snowflake_semantic_tools.domain.state import AppliedEntry, Manifest, State
from tests.helpers.project_inputs import EMPTY_SOURCES, dev_target

from .conftest import InMemorySnowflake, InMemoryStateStore
from .test_prepare_plan import compiled

STATE_TABLE = QualifiedName.parse("DB.SCH.SST_STATE")


class MarkerCountingSnowflake(InMemorySnowflake):
    def __init__(self) -> None:
        super().__init__()
        self.described: list[str] = []

    def describe_marker(
        self, qualified_name: QualifiedName, object_type: str = "SEMANTIC VIEW"
    ) -> OwnershipMarker | None:
        self.described.append(qualified_name.sql)
        return super().describe_marker(qualified_name, object_type)


def published(result: CompileResult) -> tuple[Manifest, MarkerCountingSnowflake]:
    """A target where apply published every artifact of `result` from its manifest."""
    manifest = manifest_for(result, EMPTY_SOURCES)
    port = MarkerCountingSnowflake()
    port.remote_state = MappingProxyType(
        {
            artifact.key: AppliedEntry(
                artifact.fingerprint,
                artifact.target.sql,
                "now",
                "run",
                "applied",
                artifact.fingerprint,
                manifest.manifest_id,
            )
            for artifact in result.rendered
        }
    )
    port.markers = {
        artifact.target.sql: OwnershipMarker(manifest.manifest_id, artifact.fingerprint) for artifact in result.rendered
    }
    return manifest, port


def smoke(result: CompileResult, manifest: Manifest, port: InMemorySnowflake, store: InMemoryStateStore | None = None):  # type: ignore[no-untyped-def]
    """Run the suite against `port`; without `store`, no local state is cached, so state reads clean."""
    cache = store or InMemoryStateStore()
    return SmokePublished(port, cache).run(
        result, manifest, target=dev_target(), state_table=STATE_TABLE, fail_fast=True
    )


def test_owned_objects_are_probed_as_apply_published_them() -> None:
    result = compiled("MENU", "SALES")
    manifest, port = published(result)

    outcome = smoke(result, manifest, port)

    assert outcome.success and outcome.diagnostics == ()
    assert [probe.key for probe in outcome.attempted] == ["semantic_view:menu:view", "semantic_view:sales:view"]
    assert [sql for sql, _ in port.queries] == [probe.sql for probe in outcome.attempted]
    assert port.described == ["DB.SCH.MENU", "DB.SCH.SALES"]


def test_state_from_another_manifest_and_a_changed_marker_keep_every_probe_from_running() -> None:
    result = compiled("MENU", "SALES")
    manifest, port = published(result)
    port.markers["DB.SCH.SALES"] = OwnershipMarker(manifest.manifest_id, "f" * 64)
    other = manifest_for(compiled("OTHER"), EMPTY_SOURCES)

    changed = smoke(result, manifest, port)
    stale = smoke(result, other, published(result)[1])

    assert [(item.code, item.context["artifact"]) for item in changed.diagnostics] == [
        ("SST-APL012", "semantic_view:sales")
    ]
    assert changed.attempted == () and port.queries == []
    assert stale.diagnostics[0].context["artifact"] == "manifest"
    assert not stale.success


def test_an_entry_that_does_not_match_is_reported_without_reading_the_live_marker() -> None:
    result = compiled("ALPHA", "BRAVO", "CHARLIE")
    manifest, port = published(result)
    state = dict(port.remote_state or {})
    alpha = state["semantic_view:alpha"]
    state.pop("semantic_view:bravo")
    state["semantic_view:alpha"] = AppliedEntry(
        alpha.fingerprint, "DB.SCH.ELSEWHERE", "now", "run", "applied", alpha.fingerprint, manifest.manifest_id
    )
    port.remote_state = MappingProxyType(state)

    outcome = smoke(result, manifest, port)

    assert [item.context["artifact"] for item in outcome.diagnostics] == ["semantic_view:alpha", "semantic_view:bravo"]
    assert port.described == ["DB.SCH.CHARLIE"]


def test_anything_reading_state_reports_keeps_the_probes_from_running_even_a_warning() -> None:
    result = compiled()
    manifest, port = published(result)
    cached = InMemoryStateStore(State.empty(dev_target()))

    outcome = smoke(result, manifest, port, cached)

    assert [item.code for item in outcome.diagnostics] == ["SST-MAN027"]
    assert outcome.success and outcome.attempted == ()


def test_unreadable_state_is_reported_with_every_object_it_cannot_vouch_for() -> None:
    result = compiled()
    manifest, port = published(result)
    port.remote_state = None

    outcome = smoke(result, manifest, port)

    assert [item.code for item in outcome.diagnostics] == ["SST-PLN001", "SST-APL012", "SST-APL012"]
    assert port.queries == []
