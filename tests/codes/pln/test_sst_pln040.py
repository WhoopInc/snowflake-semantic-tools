"""SST-PLN040: the tags on a semantic view a planned update replaces could not be read."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.app.observe import observe_replaced_tags
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag, Severity
from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker, SnowflakeObservation
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.plan_codes import entry, live, manifest_of, view
from tests.helpers.snowflake_fake import FakeSnowflake


def _read(port: FakeSnowflake) -> tuple[SnowflakeObservation, DiagnosticBag]:
    """Read the tags of `sales`, which the plan updates: state records an earlier definition."""
    current, before = view("sales"), view("before")
    planned_manifest = manifest_of(current)
    marker = OwnershipMarker(planned_manifest.manifest_id, before.fingerprint)
    observation = SnowflakeObservation(MappingProxyType({current.key: live(current, marker=marker)}), "now")
    recorded = entry(before, planned_manifest.manifest_id, qualified_name=current.target.sql)
    return observe_replaced_tags(port, observation, {current.key: current}, {current.key: recorded}, SEMANTIC_REGISTRY)


def test_sst_pln040_fires() -> None:
    port = FakeSnowflake()
    port.fail("tag_references", SnowflakePortError("denied"))
    observation, [diagnostic] = _read(port)
    assert (diagnostic.code, diagnostic.severity) == ("SST-PLN040", Severity.WARNING)
    assert diagnostic.message == (
        "the tags on DB.SCH.SALES were not read, so the tags its replace removes are not checked: denied"
    )
    # The view is observed all the same; only its tags are unread.
    assert [item.tags for item in observation.artifacts.values()] == [None]


def test_sst_pln040_silent() -> None:
    port = FakeSnowflake()
    port.tags["DB.SCH.SALES"] = ("GOV.T.PII",)
    observation, diagnostics = _read(port)
    assert diagnostics == ()
    assert [item.tags for item in observation.artifacts.values()] == [("GOV.T.PII",)]
