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
from snowflake_semantic_tools.domain.plan.attach import AttachmentReading
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
        newest_version="VERSION$2" if full else None,
        newest_spec="c" * 64 if full else None,
        default_version="VERSION$1" if full else None,
        alias_versions=(("PROMOTED", "VERSION$2"),) if full else (),
        last_is_eval_model=full,
        default_pinned=True if full else None,
    )


def _preflight() -> Preflight:
    return Preflight(
        target_name="dev",
        role="ROLE_A",
        missing_databases=frozenset({"OTHER"}),
        missing_schemas=frozenset({("DB", "MISSING")}),
        missing_relations=MappingProxyType({"semantic_view:a": (QualifiedName.parse("DB.S.T"),)}),
        missing_privileges=MappingProxyType({("DB", "S"): ("CREATE SEMANTIC VIEW",)}),
        secondary_privileges=MappingProxyType({("DB", "S"): ("CREATE SEMANTIC VIEW",)}),
        unreached_privileges=MappingProxyType({("DB", "OTHER"): ("CREATE AGENT",)}),
        occupied=frozenset({"semantic_view:b"}),
        locked=frozenset({("DB", "S", "LOCKED")}),
        referenced=MappingProxyType({"semantic_view:a": (QualifiedName.parse("DB.S.USER_VIEW"),)}),
        warehouse="WH",
        warehouse_usable=False,
        stage_types=MappingProxyType({("DB", "S", "AGENT_SPECS"): "INTERNAL NO CSE"}),
        absent_stages=frozenset({("DB", "OTHER", "AGENT_SPECS")}),
        missing_stage_privileges=MappingProxyType({("DB", "S", "AGENT_SPECS"): ("WRITE",)}),
        absent_state_tables=(QualifiedName.parse("DB.S.SST_STATE_LOCK"),),
        state_table_kind="hybrid",
        state_table_kinds=((QualifiedName.parse("DB.S.SST_STATE"), "standard"),),
        create_schemas=True,
        missing_state_schemas=frozenset({("DB", "STATE")}),
        missing_cowork_privileges=MappingProxyType({"SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT": ("MODIFY",)}),
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


def test_a_record_written_before_the_stage_facts_reads_as_holding_none() -> None:
    document = _recorded(preflight=_preflight()).as_dict()
    preflight = document["preflight"]
    assert isinstance(preflight, dict)
    fields = (
        "stage_types",
        "absent_stages",
        "missing_stage_privileges",
        "secondary_privileges",
        "unreached_privileges",
        "absent_state_tables",
        "state_table_kind",
        "state_table_kinds",
        "missing_cowork_privileges",
    )
    for field in fields:
        del preflight[field]
    read = RecordedObservation.from_dict(document).preflight
    assert read is not None
    assert (read.stage_types, read.absent_stages, read.missing_stage_privileges) == ({}, frozenset(), {})
    assert read.secondary_privileges == {} and read.unreached_privileges == {}
    assert read.missing_cowork_privileges == {}
    # A record that read no kinds holds no table of the wrong one.
    assert (read.absent_state_tables, read.state_table_kind, read.state_table_kinds) == ((), "standard", ())


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
        ("attachment", [], "observation attachment must be an object"),
        ("attachment", {"attached": {}}, "observation attached must be a list"),
    ],
)
def test_a_malformed_record_is_refused(field: str, value: object, message: str) -> None:
    document = _recorded(preflight=_preflight()).as_dict()
    document[field] = value
    with pytest.raises(ValueError, match=message):
        RecordedObservation.from_dict(document)


def test_a_record_written_before_agent_version_facts_reads_with_none() -> None:
    document = _recorded(preflight=None).as_dict()
    for artifact in document["artifacts"]:  # type: ignore[attr-defined]
        for key in (
            "newest_version",
            "newest_spec",
            "default_version",
            "alias_versions",
            "last_is_eval_model",
            "default_pinned",
        ):
            del artifact[key]
    observed = RecordedObservation.from_dict(json.loads(json.dumps(document))).observation.artifacts["semantic_view:a"]
    assert (observed.newest_version, observed.newest_spec, observed.default_version, observed.alias_versions) == (
        None,
        None,
        None,
        (),
    )
    # A default whose setting was not recorded is unread, which plans no pin.
    assert (observed.last_is_eval_model, observed.default_pinned) == (False, None)


def test_a_default_read_as_following_last_reads_back_as_unpinned() -> None:
    recorded = _recorded(preflight=None)
    artifact = replace(recorded.observation.artifacts["semantic_view:a"], default_pinned=False)
    observation = replace(recorded.observation, artifacts=MappingProxyType({artifact.key: artifact}))
    assert _round_trip(replace(recorded, observation=observation)).observation.artifacts[artifact.key] == artifact


@pytest.mark.parametrize(
    "attachment",
    [
        AttachmentReading(
            "SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT", frozenset({("SNOWFLAKE_INTELLIGENCE", "PROFILES", "A")})
        ),
        AttachmentReading(None),
    ],
)
def test_the_cowork_attachment_reading_reads_back_as_written(attachment: AttachmentReading) -> None:
    # Snowflake private preview (PrPr): CoWork profiles. An account without a CoWork object
    # records that it has none, which differs from a plan that read nothing.
    recorded = replace(_recorded(preflight=None), attachment=attachment)
    assert _round_trip(recorded) == recorded
    assert recorded.as_dict()["attachment"] == {
        "cowork_object": attachment.cowork_object,
        "attached": [list(name) for name in sorted(attachment.attached)],
    }


def test_a_record_without_the_attachment_reading_writes_none_and_reads_back_without_one() -> None:
    document = _recorded(preflight=None).as_dict()
    assert "attachment" not in document
    assert RecordedObservation.from_dict(document).attachment is None
    document["attachment"] = {"cowork_object": "SI"}
    assert RecordedObservation.from_dict(document).attachment == AttachmentReading("SI")
