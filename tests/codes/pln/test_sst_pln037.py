"""SST-PLN037: `plan --capture-prior` could not read a planned object's prior definition."""

from __future__ import annotations

from snowflake_semantic_tools.app.observe import ObserveOptions, observe
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import ShowRow
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from tests.helpers.artifact_builders import rendered
from tests.helpers.snowflake_fake import FakeSnowflake

_CAPTURE = ObserveOptions(grants=False, capture_prior=True)


def test_sst_pln037_fires() -> None:
    artifact = rendered()
    port = FakeSnowflake()
    port.show(ShowRow("V", "DB", "SCHEMA", "OWNER", "now"))
    observation, [diagnostic] = observe(
        port,
        SEMANTIC_REGISTRY,
        (artifact.target,),
        fetched_at="now",
        desired_artifacts={artifact.key: artifact},
        options=_CAPTURE,
    )
    assert (diagnostic.code, diagnostic.severity) == ("SST-PLN037", Severity.WARNING)
    assert diagnostic.message == (
        "the prior definition of DB.SCHEMA.V was not captured: GET_DDL refused for SEMANTIC VIEW DB.SCHEMA.V"
    )
    # The object is observed all the same; only its definition is missing.
    assert observation.artifacts[artifact.key].definition is None


def test_sst_pln037_silent() -> None:
    artifact = rendered()
    port = FakeSnowflake(definitions={artifact.target.sql: "create or replace semantic view V"})
    port.show(ShowRow("V", "DB", "SCHEMA", "OWNER", "now"))
    _, diagnostics = observe(
        port,
        SEMANTIC_REGISTRY,
        (artifact.target,),
        fetched_at="now",
        desired_artifacts={artifact.key: artifact},
        options=_CAPTURE,
    )
    assert diagnostics == ()
