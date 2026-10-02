"""Deterministic manifest, state, and plan serialization."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType
from typing import Any

import pytest

from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle import Action, Change, ChangeReason, ChangeSet, RenderedArtifact
from snowflake_semantic_tools.domain.state import (
    STATE_SCHEMA_VERSION,
    AppliedEntry,
    AppliedResource,
    ArtifactEntry,
    ImpactIndex,
    LastRun,
    Manifest,
    ResourceStatus,
    SavedChange,
    SavedPlan,
    State,
    canonical_json,
    content_hash,
    migrate_manifest,
    migrate_state,
)
from snowflake_semantic_tools.domain.state.manifest import _manifest_from_dict_unchecked, _object_map
from tests.helpers.manifests import build_minimal_manifest
from tests.helpers.sql_values import statement


def target() -> TargetIdentity:
    return TargetIdentity("verify", "account", Identifier.parse("db"), Identifier.parse("schema"))


def artifact() -> RenderedArtifact:
    return RenderedArtifact.create(
        key="semantic_view:v",
        artifact_type="semantic_view",
        target=QualifiedName.from_parts("db", "schema", "v"),
        ddl=statement("create semantic view db.schema.v as tables (t as db.schema.t) copy grants"),
    )


def manifest() -> Manifest:
    return build_minimal_manifest(
        {artifact().key: artifact()},
        project={"root": ".", "semantic_path": "semantic_models"},
        sources={"semantic_file_count": 1},
        members={},
        files={"views.yml": {"checksum": "x"}},
        impact=ImpactIndex(
            MappingProxyType({"views.yml": ("semantic_view:v",)}),
            MappingProxyType({}),
            MappingProxyType({}),
        ),
        diagnostics_summary={"error": 0, "warning": 0, "info": 0},
    )


def test_canonical_json_is_deterministic_and_rejects_nan() -> None:
    assert canonical_json({"b": 2, "a": 1}) == b'{"a":1,"b":2}'
    assert content_hash({"x": 1}) == content_hash({"x": 1})
    with pytest.raises(ValueError):
        canonical_json({"x": float("nan")})


def test_artifact_entry_and_impact_index_round_trip() -> None:
    entry = ArtifactEntry("semantic_view", "v", "a" * 64, ("v.yml",), ("metric:m",), (), "DB.S.V", 10)
    assert ArtifactEntry.from_dict(entry.as_dict()) == entry
    generic = ArtifactEntry(
        "agent",
        "a",
        "b" * 64,
        ("agent.yml",),
        (),
        ("tool:t",),
        "DB.S.A",
        20,
        "AGENT",
        "json",
        component_fingerprints=(("config", "c" * 64),),
        physical_resources=(("AGENT", "DB.S.A"),),
    )
    assert ArtifactEntry.from_dict(generic.as_dict()) == generic
    impact = ImpactIndex(
        MappingProxyType({"f": ("a",)}),
        MappingProxyType({"m": ("a",)}),
        MappingProxyType({"x": ("a",)}),
    )
    assert ImpactIndex.from_dict(impact.as_dict()) == impact
    with pytest.raises(ValueError):
        ArtifactEntry.from_dict([])
    with pytest.raises(ValueError):
        ArtifactEntry.from_dict({"type": "x"})
    with pytest.raises(ValueError):
        ImpactIndex.from_dict([])
    with pytest.raises(ValueError):
        ImpactIndex.from_dict({"by_file": []})
    malformed = generic.as_dict()
    malformed["component_fingerprints"] = []
    with pytest.raises(ValueError, match="component metadata"):
        ArtifactEntry.from_dict(malformed)
    malformed = generic.as_dict()
    malformed["physical_resources"] = ["DB.S.A"]
    with pytest.raises(ValueError, match="physical resource"):
        ArtifactEntry.from_dict(malformed)


def test_manifest_round_trip_detects_tampering_and_versions() -> None:
    value = manifest()
    assert value.manifest_id == content_hash(value.hash_material())
    assert Manifest.from_dict(value.as_dict()) == value
    tampered = value.as_dict()
    tampered["project"] = {"root": "elsewhere"}
    with pytest.raises(ValueError, match="recomputed"):
        Manifest.from_dict(tampered)
    newer = value.as_dict()
    newer["schema_version"] = 99
    with pytest.raises(ValueError, match="newer"):
        Manifest.from_dict(newer)
    with pytest.raises(ValueError, match="must be an object"):
        Manifest.from_dict([])
    with pytest.raises(ValueError, match="schema_version"):
        Manifest.from_dict({})
    with pytest.raises(ValueError, match="artifacts"):
        Manifest.from_dict({"schema_version": 2})


def test_manifest_v1_migrates_in_memory_and_bad_migrations_fail() -> None:
    value = manifest().as_dict()
    value["schema_version"] = 1
    value.pop("members")
    value.pop("dbt_models")
    value.pop("files")
    value.pop("impact")
    migrated = migrate_manifest(value)
    assert migrated["schema_version"] == 2
    assert Manifest.from_dict(migrated).schema_version == 2
    legacy = manifest().as_dict()
    legacy["schema_version"] = 1
    assert Manifest.from_dict(legacy).schema_version == 2
    with pytest.raises(ValueError, match="no migration"):
        migrate_manifest({"schema_version": 0})


def test_manifest_rejects_malformed_nested_shapes() -> None:
    value = manifest().as_dict()
    value["artifacts"] = []
    with pytest.raises(ValueError, match="artifacts"):
        Manifest.from_dict(value)
    value = manifest().as_dict()
    value["diagnostics_summary"] = {"error": "no"}
    with pytest.raises(ValueError, match="must be an integer"):
        Manifest.from_dict(value)
    value = manifest().as_dict()
    value["project"] = []
    with pytest.raises(ValueError, match="expected an object"):
        Manifest.from_dict(value)
    value = manifest().as_dict()
    value["schema_version"] = "2"
    with pytest.raises(ValueError, match="schema_version"):
        Manifest.from_dict(value)
    with pytest.raises(ValueError, match="artifacts"):
        _manifest_from_dict_unchecked({"schema_version": 2, "artifacts": []})
    with pytest.raises(ValueError, match="schema_version"):
        _manifest_from_dict_unchecked({"schema_version": "2", "artifacts": {}})
    assert _object_map(None) == {}


def test_state_round_trip_and_empty_state() -> None:
    applied = AppliedEntry(
        "a" * 64,
        "DB.S.V",
        "now",
        "run",
        "applied",
        "a" * 64,
        "m",
        "git",
        component_fingerprints=(("dataset", "d" * 64), ("config", "c" * 64)),
        physical_resources=(
            AppliedResource("table", "DB.S.V_SRC", ResourceStatus.RETAINED),
            AppliedResource("DATASET", "DB.S.V", ResourceStatus.UNVERIFIED_AFTER_WRITE),
        ),
    )
    last = LastRun("run", "start", "finish", "1.0", "apply", "ok", "actor")
    state = State(
        STATE_SCHEMA_VERSION, target(), "m", "sst_config.yml", last, MappingProxyType({"semantic_view:v": applied})
    )
    assert State.from_dict(state.as_dict()) == state
    assert State.empty(target()).applied == {}
    with pytest.raises(ValueError, match="must be an object"):
        State.from_dict([])
    with pytest.raises(ValueError, match="not supported"):
        State.from_dict({"schema_version": 99})
    with pytest.raises(ValueError, match="applied"):
        State.from_dict({"schema_version": 1, "target": target().as_dict(), "applied": []})
    with pytest.raises(ValueError, match="applied entry"):
        AppliedEntry.from_dict([])
    without_last = State(STATE_SCHEMA_VERSION, target(), "", "cfg", None, MappingProxyType({}))
    assert State.from_dict(without_last.as_dict()).last_run is None


def test_applied_entry_accepts_legacy_resource_tuples_and_serializes_status() -> None:
    entry = AppliedEntry(
        "a" * 64,
        "DB.S.V",
        "now",
        "run",
        "applied",
        "a" * 64,
        "m",
        component_fingerprints=(("dataset", "d"), ("config", "c")),
        physical_resources=(
            ("table", "DB.S.SRC"),
            ("DATASET", "DB.S.V", ResourceStatus.RETAINED),
            ("STAGE", "DB.S.CFG", "unverified_after_write"),
        ),
    )

    assert entry.component_fingerprints == (("config", "c"), ("dataset", "d"))
    assert entry.physical_resources == (
        AppliedResource("TABLE", "DB.S.SRC"),
        AppliedResource("DATASET", "DB.S.V", ResourceStatus.RETAINED),
        AppliedResource("STAGE", "DB.S.CFG", ResourceStatus.UNVERIFIED_AFTER_WRITE),
    )
    assert entry.as_dict()["physical_resources"] == [
        {"object_type": "TABLE", "qualified_name": "DB.S.SRC", "status": "verified"},
        {"object_type": "DATASET", "qualified_name": "DB.S.V", "status": "retained"},
        {
            "object_type": "STAGE",
            "qualified_name": "DB.S.CFG",
            "status": "unverified_after_write",
        },
    ]
    assert AppliedEntry.from_dict(entry.as_dict()) == entry


def test_applied_resource_defaults_old_v2_documents_to_verified_and_rejects_bad_status() -> None:
    entry = AppliedEntry("a" * 64, "DB.S.V", "now", "run", "applied", "a" * 64, "m")
    document = entry.as_dict()
    document["physical_resources"] = [{"object_type": "TABLE", "qualified_name": "DB.S.V"}]
    assert AppliedEntry.from_dict(document).physical_resources == (AppliedResource("TABLE", "DB.S.V"),)

    document["physical_resources"] = [{"object_type": "TABLE", "qualified_name": "DB.S.V", "status": "unknown"}]
    with pytest.raises(ValueError, match="invalid status"):
        AppliedEntry.from_dict(document)

    with pytest.raises(ValueError, match="must be an object"):
        AppliedResource.from_dict("DB.S.V")
    resource = AppliedResource("table", "DB.S.V")
    assert tuple(resource) == ("TABLE", "DB.S.V")
    with pytest.raises(ValueError, match="two or three values"):
        replace(entry, physical_resources=(("TABLE",),))  # type: ignore[arg-type]  # deliberately malformed
    malformed = entry.as_dict()
    malformed["component_fingerprints"] = []
    with pytest.raises(ValueError, match="component metadata"):
        AppliedEntry.from_dict(malformed)


def test_state_v1_migrates_composite_metadata_without_losing_the_legacy_target() -> None:
    legacy: dict[str, Any] = {
        "schema_version": 1,
        "target": target().as_dict(),
        "manifest_id": "m",
        "config_path": "sst_config.yml",
        "last_run": None,
        "applied": {
            "semantic_view:v": AppliedEntry(
                "a" * 64,
                "DB.S.V",
                "now",
                "run",
                "applied",
                "a" * 64,
                "m",
            ).as_dict()
        },
    }
    legacy["applied"]["semantic_view:v"].pop("component_fingerprints")
    legacy["applied"]["semantic_view:v"].pop("physical_resources")
    migrated = migrate_state(legacy)
    parsed = State.from_dict(legacy)
    assert migrated["schema_version"] == STATE_SCHEMA_VERSION
    assert parsed.applied["semantic_view:v"].component_fingerprints == ()
    assert parsed.applied["semantic_view:v"].physical_resources == (
        AppliedResource("", "DB.S.V", ResourceStatus.VERIFIED),
    )
    assert legacy["schema_version"] == 1
    assert "component_fingerprints" not in legacy["applied"]["semantic_view:v"]


def test_state_and_migration_reject_invalid_schema_and_applied_shapes() -> None:
    with pytest.raises(ValueError, match="not supported"):
        State.from_dict({"schema_version": None})
    with pytest.raises(ValueError, match="state.applied"):
        State.from_dict({"schema_version": STATE_SCHEMA_VERSION, "applied": []})
    with pytest.raises(ValueError, match="no migration"):
        migrate_state({"schema_version": 0})
    with pytest.raises(ValueError, match="state.applied"):
        migrate_state({"schema_version": 1, "applied": []})
    with pytest.raises(ValueError, match="applied entry"):
        migrate_state({"schema_version": 1, "applied": {"x": []}})


def test_saved_plan_is_content_addressed_and_target_guarded() -> None:
    rendered = artifact()
    change = Change(
        rendered.key,
        "semantic_view",
        Action.CREATE,
        ChangeReason.NOT_PRESENT,
        rendered,
        None,
        (),
        100,
    )
    changeset = ChangeSet("m", target(), (change,), DiagnosticBag(), "now")
    saved = SavedPlan.from_changeset(changeset)
    assert len(saved.plan_id) == 64
    assert saved.matches("m", target())
    assert not saved.matches("other", target())
    assert SavedChange.from_change(replace(change, rendered=None, action=Action.PRUNE)).statement_hashes == ()
    assert saved.as_dict()["changes"][0]["action"] == "create"  # type: ignore[index]

    scoped = SavedPlan.from_changeset(
        changeset,
        selected=("v",),
        excluded=("other",),
        include_prune=True,
    )
    assert scoped.selected == ("v",)
    assert scoped.excluded == ("other",)
    assert scoped.include_prune
    # `partial` is recorded only when set, so a plan that is not partial keeps its id.
    assert "partial" not in saved.as_dict()["selection"]  # type: ignore[operator]
    partial = SavedPlan.from_changeset(changeset, partial=True)
    assert partial.partial and partial.as_dict()["selection"]["partial"] is True  # type: ignore[index]
    assert partial.plan_id != saved.plan_id


def _saved_plan_document() -> dict[str, Any]:
    rendered = artifact()
    change = Change(rendered.key, "semantic_view", Action.CREATE, ChangeReason.NOT_PRESENT, rendered, None, (), 100)
    saved = SavedPlan.from_changeset(ChangeSet("m", target(), (change,), DiagnosticBag(), "now"), selected=("v",))
    return saved.as_dict()


def _rehash(document: dict[str, Any]) -> dict[str, Any]:
    body = {key: value for key, value in document.items() if key != "plan_id"}
    return {**body, "plan_id": content_hash(body)}


def test_a_saved_plan_reads_back_what_it_wrote() -> None:
    document = _saved_plan_document()
    restored = SavedPlan.from_dict(document)
    assert restored.as_dict() == document
    assert restored.selected == ("v",) and restored.changes[0].target is not None
    change = {**document["changes"][0], "target": None, "fingerprint": None}
    del change["component_fingerprints"], change["physical_resources"]
    sparse = SavedPlan.from_dict(_rehash({**document, "changes": [change]}))
    assert sparse.changes[0].target is None and sparse.changes[0].component_fingerprints == ()


@pytest.mark.parametrize(
    ("patch", "message"),
    [
        ({"schema_version": 1}, "saved plan schema 1 is not supported"),
        ({"changes": {}}, "saved plan changes must be a list"),
        ({"changes": ["x"]}, "saved plan change must be an object"),
        ({"selection": []}, "saved plan selection must be an object"),
        ({"plan_id": "0" * 64}, "saved plan id 0{64}, recomputed"),
    ],
)
def test_a_saved_plan_that_is_not_one_is_refused(patch: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        SavedPlan.from_dict({**_saved_plan_document(), **patch})
    with pytest.raises(ValueError, match="saved plan must be an object"):
        SavedPlan.from_dict([])


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("component_fingerprints", [], "saved plan component_fingerprints must be an object"),
        ("physical_resources", {}, "saved plan physical_resources must be objects"),
        ("physical_resources", ["x"], "saved plan physical_resources must be objects"),
    ],
)
def test_a_saved_change_refuses_mistyped_metadata(field: str, value: object, message: str) -> None:
    document = _saved_plan_document()
    change = {**document["changes"][0], field: value}
    with pytest.raises(ValueError, match=message):
        SavedPlan.from_dict({**document, "changes": [change]})
