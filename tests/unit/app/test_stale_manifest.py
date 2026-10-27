"""`stale_manifest`: a compile that no longer matches the project names what differs."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

from snowflake_semantic_tools.app.manifest import _changed_section, manifest_for, stale_manifest
from snowflake_semantic_tools.domain.diagnostics import D
from tests.helpers.compile_builders import compiled, with_diagnostics
from tests.helpers.project_inputs import EMPTY_SOURCES


def _reason(before: object, now: object) -> str:
    stale = stale_manifest(before, now, before="plan")  # type: ignore[arg-type]
    assert stale is not None and stale.code == "SST-MAN009"
    return str(stale.context["reason"])


def test_the_same_manifest_is_not_stale() -> None:
    manifest = manifest_for(compiled(), EMPTY_SOURCES)
    assert stale_manifest(manifest, manifest, before="plan") is None


def test_a_changed_input_is_named_before_any_section() -> None:
    now = manifest_for(compiled(), EMPTY_SOURCES)
    files = MappingProxyType({"semantic_models/a.yml": {"checksum": "1"}})
    before = replace(now, files=files).with_computed_id()
    changed = replace(now, files=MappingProxyType({"semantic_models/a.yml": {"checksum": "2"}})).with_computed_id()
    assert _reason(before, changed) == "the project compiles to another manifest now: semantic_models/a.yml changed"


def test_with_the_same_inputs_the_first_differing_section_and_key_are_named() -> None:
    base = manifest_for(compiled(), EMPTY_SOURCES)
    now = replace(base, project=MappingProxyType({**base.project, "dbt_digest": "new"})).with_computed_id()
    digest = replace(base, project=MappingProxyType({**base.project, "dbt_digest": "old"})).with_computed_id()
    assert _reason(digest, now) == "the project compiles to another manifest now: project.dbt_digest differs"
    added = manifest_for(compiled("SALES", "OTHER"), EMPTY_SOURCES)
    assert _reason(base, added).endswith("artifacts.semantic_view:other is new")


def test_a_section_that_is_not_a_mapping_on_both_sides_is_named_whole() -> None:
    assert _changed_section({"warnings": ["a"]}, {"warnings": ["b"]}) == "warnings differs"
    assert _changed_section({"project": {"a": 1}}, {"project": None}) == "project differs"
    assert _changed_section({"project": {"a": 1}}, {"project": {"a": 1}}) is None


def test_a_note_that_the_deferred_manifest_was_reused_does_not_change_the_manifest_id() -> None:
    parsed = compiled()
    reused = with_diagnostics(
        parsed, *parsed.diagnostics, D("SST-DBT035", target="prod", detail="parsed 2 minutes ago")
    )
    # A run that reused the deferred manifest compiles the same project as one that parsed it.
    assert manifest_for(reused, EMPTY_SOURCES).manifest_id == manifest_for(parsed, EMPTY_SOURCES).manifest_id
    assert (
        stale_manifest(manifest_for(reused, EMPTY_SOURCES), manifest_for(parsed, EMPTY_SOURCES), before="plan") is None
    )
