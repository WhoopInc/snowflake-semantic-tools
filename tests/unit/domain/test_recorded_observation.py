"""A recorded observation: what a plan read of one target, written and read back, and whose target it is."""

from __future__ import annotations

import json
from dataclasses import replace
from types import MappingProxyType

import pytest

from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle import (
    GrantRow,
    ObservedArtifact,
    OwnershipMarker,
    SnowflakeObservation,
)
from snowflake_semantic_tools.domain.plan.preflight import Preflight
from snowflake_semantic_tools.domain.plan.recorded import RECORDED_OBSERVATION_SCHEMA_VERSION, RecordedObservation
from snowflake_semantic_tools.domain.state import State, StoredDocumentError

TARGET = TargetIdentity("dev", "LIVE123", Identifier.parse("DB"), Identifier.parse("S"), "ROLE_A", "WH")


def _artifact(key: str, *, full: bool) -> ObservedArtifact:
    return ObservedArtifact(
        key=key,
        raw_name=key.split(":", 1)[1].upper(),
        qualified_name=QualifiedName.parse(f"DB.S.{key.split(':', 1)[1].upper()}"),
        object_type="SEMANTIC VIEW",
        owner="OWNER",
        created_on="now",
        comment="[sst:m:f]" if full else None,
        marker=OwnershipMarker("a" * 64, "b" * 64) if full else None,
        grants=(GrantRow("SELECT", "ROLE", "ANALYST", "OWNER", True), GrantRow("USAGE", "ROLE", "R")) if full else None,
        definition="create semantic view ..." if full else None,
        has_live_version=full,
        routine_signature=("NUMBER",) if full else (),
        aliases=("ALIAS",) if full else (),
        tags=("DB.S.TAG",) if full else (),
    )


def _preflight() -> Preflight:
    return Preflight(
        target_name="dev",
        role="ROLE_A",
        missing_databases=frozenset({"OTHER"}),
        missing_schemas=frozenset({("DB", "MISSING")}),
        missing_relations=MappingProxyType({"semantic_view:a": (QualifiedName.parse("DB.S.T"),)}),
        missing_privileges=MappingProxyType({("DB", "S"): ("CREATE SEMANTIC VIEW",)}),
        occupied=frozenset({"semantic_view:b"}),
        locked=frozenset({("DB", "S", "LOCKED")}),
        referenced=MappingProxyType({"semantic_view:a": (QualifiedName.parse("DB.S.USER_VIEW"),)}),
        warehouse="WH",
        warehouse_usable=False,
    )


def _recorded(*, preflight: Preflight | None) -> RecordedObservation:
    artifacts = {
        key: _artifact(key, full=full) for key, full in (("semantic_view:a", True), ("semantic_view:b", False))
    }
    observation = SnowflakeObservation(MappingProxyType(artifacts), "2026-01-01T00:00:00+00:00")
    return RecordedObservation(TARGET, observation, State.empty(TARGET), preflight, declared_account="acct")


def _round_trip(recorded: RecordedObservation) -> RecordedObservation:
    return RecordedObservation.from_dict(json.loads(json.dumps(recorded.as_dict())))


@pytest.mark.parametrize("preflight", [_preflight(), None])
def test_a_record_reads_back_as_written(preflight: Preflight | None) -> None:
    recorded = _recorded(preflight=preflight)
    assert _round_trip(recorded) == recorded
    assert recorded.fetched_at == "2026-01-01T00:00:00+00:00"


def test_a_record_written_before_the_declared_account_reads_as_unset() -> None:
    document = _recorded(preflight=None).as_dict()
    del document["declared_account"]
    assert RecordedObservation.from_dict(document).declared_account == ""


def test_a_record_is_foreign_to_another_account_name_database_or_schema() -> None:
    recorded = _recorded(preflight=None)
    declared = replace(TARGET, account_locator="ACCT", role=None, warehouse=None)
    assert recorded.foreign_to(declared) is None
    assert recorded.foreign_to(replace(declared, account_locator="other")) == "target dev on account acct"
    unset = replace(recorded, declared_account="")
    assert unset.foreign_to(declared) == "target dev on account (unset)"
    assert recorded.foreign_to(replace(declared, name="prod")) == "target dev for DB.S"
    assert recorded.foreign_to(replace(declared, schema=Identifier.parse("ELSEWHERE"))) == "target dev for DB.S"


@pytest.mark.parametrize(
    ("document", "detail"),
    [
        ([], "not an object"),
        (
            {"schema_version": RECORDED_OBSERVATION_SCHEMA_VERSION + 1},
            f"schema {RECORDED_OBSERVATION_SCHEMA_VERSION + 1}",
        ),
    ],
)
def test_a_document_that_is_not_a_record_of_this_schema_is_refused(document: object, detail: str) -> None:
    with pytest.raises(StoredDocumentError) as raised:
        RecordedObservation.from_dict(document)
    assert (raised.value.code, raised.value.context) == ("SST-PRT009", {"detail": detail})


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("artifacts", {}, "observation artifacts must be a list"),
        ("artifacts", ["not an artifact"], "observation artifact must be an object"),
        ("preflight", [], "observation preflight must be an object"),
    ],
)
def test_a_malformed_record_is_refused(field: str, value: object, message: str) -> None:
    document = _recorded(preflight=_preflight()).as_dict()
    document[field] = value
    with pytest.raises(ValueError, match=message):
        RecordedObservation.from_dict(document)
