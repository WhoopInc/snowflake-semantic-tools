"""Compiling Cortex Code agent profiles (a Snowflake private preview): pins, instructions, checks, publication.

A profile compiles to a staged AGENT whose specification holds only an orchestration model, its
response instructions, and its skills, each pinned as an agent's are; it publishes no alias,
tags, or display block.
"""

from __future__ import annotations

import json
from dataclasses import replace
from types import MappingProxyType

from snowflake_semantic_tools.app.compile.coco_profiles import (
    COCO_STAGE_FOLDER,
    CompiledCocoProfile,
    for_coco_publication,
    pinned_by_profiles,
)
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.coco_profile import CocoProfileCatalog, owned_entries
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import spec_fingerprint
from snowflake_semantic_tools.domain.model.setting import Setting
from tests.helpers.compile_builders import compiled_as
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.skill_inputs import compile_profiles, profile, profile_catalog, shared
from tests.helpers.sql_values import texts

STAGE = QualifiedName.parse("DB.S.SPECS")


def _compiled(catalog: CocoProfileCatalog, **kwargs: object) -> CompiledCocoProfile:
    result = compile_profiles(catalog, **kwargs)  # type: ignore[arg-type]
    return compiled_as(result, CompiledCocoProfile)


def test_a_profile_pins_its_skills_and_plugins_and_renders_only_what_a_profile_accepts() -> None:
    analyst = profile(
        orchestration_model="auto",
        skills=(*owned_entries("skill", ("month-close",)), *owned_entries("plugin", ("finance-kit",))),
    )
    result = compile_profiles(profile_catalog(analyst))
    compiled = compiled_as(result, CompiledCocoProfile)

    assert json.loads(compiled.payload) == {
        "models": {"orchestration": "auto"},
        "instructions": {"response": "Be careful.\n"},
        "skills": [
            {
                "name": "month-close",
                "source": {"type": "CORTEX_EXTENSION", "path": "DB.S.MONTH_CLOSE", "version": "SST_ABCDEF012345"},
            },
            # A plugin entry is named after the skill it carries, as an agent's is.
            {
                "name": "month-close",
                "source": {"type": "CORTEX_EXTENSION", "path": "DB.S.FINANCE_KIT", "version": "SST_0123456789AB"},
            },
        ],
    }
    assert compiled.resolved.skill_dependencies == ("skill:month-close", "plugin:finance-kit")
    assert compiled.rendered_artifact.depends_on == ("skill:month-close", "plugin:finance-kit")
    # The plugin carries month-close too, which the agents' own check warns of.
    assert codes(result.diagnostics) == ["SST-VAL541", "SST-VAL890"]


def test_a_compiled_profile_names_itself_its_target_and_its_sources() -> None:
    settings = MappingProxyType(
        {"schema": Setting("schema", "CONFIG", "root"), "database": Setting("database", "C", "root")}
    )
    compiled = _compiled(profile_catalog(profile(settings=settings)))

    assert (compiled.name, compiled.artifact_key, compiled.artifact_type) == (
        "analyst",
        "coco_profile:analyst",
        "coco_profile",
    )
    assert compiled.target.sql == "CORTEX_CODE.CONFIG.ANALYST"
    assert compiled.source_files == ("coco_profiles/analyst/coco_profile.yml",)
    assert [item.key for item in compiled.settings] == ["database", "schema"]
    assert compiled.spec_fingerprint == spec_fingerprint(compiled.payload)
    assert compiled.folder == f"{COCO_STAGE_FOLDER}/analyst"
    # Unstaged, it has nowhere to publish from yet.
    assert (compiled.stage_path, compiled.stage) == ("", None)
    assert compiled.rendered_artifact.create_statements == ()


def test_the_shared_layer_comes_first_and_a_skill_the_profile_repeats_is_pinned_once() -> None:
    layer = shared(rules=(("b.md", "Rule b.\n"), ("a.md", "Rule a.\n")), skills=("month-close", "month-close"))
    result = compile_profiles(profile_catalog(profile(prompt="Mine.\n"), shared=layer))
    compiled = compiled_as(result, CompiledCocoProfile)

    payload = json.loads(compiled.payload)
    assert payload["instructions"] == {"response": "Shared rules.\n\nRule a.\n\nRule b.\n\nMine.\n"}
    assert [entry["name"] for entry in payload["skills"]] == ["month-close"]
    assert only(result.diagnostics, "SST-VAL845").context["name"] == "month-close"


def test_a_profile_with_no_instructions_skills_or_model_renders_an_empty_spec() -> None:
    compiled = _compiled(profile_catalog(profile(prompt=None, skills=())))
    assert json.loads(compiled.payload) == {}


def test_a_profile_skill_with_no_version_to_pin_is_val856_and_keeps_the_profile_back() -> None:
    broken = profile(skills=owned_entries("skill", ("draft",)))
    result = compile_profiles(profile_catalog(broken), unpublished={"skill:draft": "it has errors"})

    assert result.compiled == ()
    found = only(result.diagnostics, "SST-VAL856")
    assert (found.subject, found.context["name"], found.context["reason"]) == (
        "coco_profile:analyst",
        "draft",
        "it has errors",
    )


def test_a_model_outside_the_allowlist_is_val894_and_keeps_the_profile_back() -> None:
    result = compile_profiles(profile_catalog(profile(orchestration_model="fast")))
    assert result.compiled == () and only(result.diagnostics, "SST-VAL894").context["found"] == "fast"


def test_a_loader_error_naming_the_profile_keeps_it_back_and_is_reported_first() -> None:
    error = D(
        "SST-PRS003",
        origin=Origin("m", 1),
        subject="coco_profile:analyst",
        artifact="analyst",
        field="model",
        expected="a string",
        found="list",
    )
    result = compile_profiles(profile_catalog(profile(), profile("operator")), diagnostics=DiagnosticBag((error,)))

    assert [item.artifact_key for item in result.compiled] == ["coco_profile:operator"]
    assert codes(result.diagnostics)[0] == "SST-PRS003"


def test_a_disabled_profile_is_not_compiled_or_checked() -> None:
    result = compile_profiles(profile_catalog(profile(enabled=False, orchestration_model="fast"), profile("operator")))
    assert [item.artifact_key for item in result.compiled] == ["coco_profile:operator"]
    assert "SST-VAL894" not in codes(result.diagnostics)


def test_a_profile_outside_cortex_code_config_is_a_rehearsal_and_val891_names_where() -> None:
    result = compile_profiles(profile_catalog(profile()), schema=("DB", "REHEARSAL"))
    assert only(result.diagnostics, "SST-VAL891").context["target"] == "DB.REHEARSAL"
    assert "SST-VAL893" not in codes(result.diagnostics)


def test_several_listed_profiles_need_one_named_default() -> None:
    plain = compile_profiles(profile_catalog(profile(), profile("operator")))
    found = only(plain.diagnostics, "SST-VAL893")
    assert (found.context["count"], found.context["names"]) == (2, "ANALYST, OPERATOR")
    with_default = compile_profiles(profile_catalog(profile(), profile("default")))
    assert "SST-VAL893" not in codes(with_default.diagnostics)


def test_a_spec_over_the_size_limit_is_val511_and_keeps_the_profile_back() -> None:
    result = compile_profiles(profile_catalog(profile(prompt="x" * 100_001)))
    assert result.compiled == () and "SST-VAL511" in codes(result.diagnostics)


def test_publication_stages_the_spec_under_the_profiles_folder_and_creates_the_agent_from_it() -> None:
    compiled = _compiled(profile_catalog(profile()))
    staged = for_coco_publication(compiled, stage=STAGE, git_sha="abc1234")

    assert (staged.stage_path, staged.stage, staged.dirty) == ("DB.S.SPECS/coco-profiles/analyst/abc1234", STAGE, False)
    rendered = staged.rendered_artifact
    assert rendered.upload_path == "@DB.S.SPECS/coco-profiles/analyst/abc1234/agent_spec.yaml"
    assert rendered.upload_content == compiled.payload.encode("utf-8")
    create = texts(rendered.create_statements)
    assert create[0] == "CREATE AGENT CORTEX_CODE.CONFIG.ANALYST\n  FROM @DB.S.SPECS/coco-profiles/analyst/abc1234/"
    assert "git:abc1234 sst-spec:" in create[1]
    statements = "\n".join((*create, *texts(rendered.update_statements), *texts(rendered.update_live_statements)))
    assert "SET TAG" not in statements and "ALIAS" not in statements
    dirty = for_coco_publication(compiled, stage=STAGE, git_sha="abc1234", dirty=True)
    assert "git:abc1234-dirty" in texts(dirty.rendered_artifact.create_statements)[1]


def test_publishing_writes_the_marked_comment_first_and_expects_it_back() -> None:
    compiled = _compiled(profile_catalog(profile()))
    published = for_coco_publication(compiled, stage=STAGE, git_sha="abc1234").rendered_for_publish("0" * 64)

    assert published.expected_marker is not None
    assert published.expected_marker.fingerprint == compiled.definition_fingerprint
    marker = published.expected_marker.text
    assert texts(published.create_statements)[0].startswith(
        f"CREATE AGENT CORTEX_CODE.CONFIG.ANALYST\n  COMMENT = 'Analyst. {marker}'"
    )
    assert (
        texts(published.update_statements)[0]
        == f"ALTER AGENT CORTEX_CODE.CONFIG.ANALYST SET COMMENT = 'Analyst. {marker}'"
    )


def test_the_definition_fingerprint_follows_the_comment_published_name_and_spec() -> None:
    base = _compiled(profile_catalog(profile()))
    renamed = _compiled(profile_catalog(profile(published_name="analyst_dev")))
    recommented = _compiled(profile_catalog(profile(comment="Other.")))
    reprompted = _compiled(profile_catalog(profile(prompt="Other.\n")))

    fingerprints = {item.definition_fingerprint for item in (base, renamed, recommented, reprompted)}
    assert len(fingerprints) == 4
    # Only the specification moves the spec fingerprint each committed version carries.
    assert base.spec_fingerprint == renamed.spec_fingerprint == recommented.spec_fingerprint
    assert base.spec_fingerprint != reprompted.spec_fingerprint


def test_pinned_by_profiles_counts_enabled_profiles_and_the_shared_layer_they_carry() -> None:
    layer = shared(skills=("common",))
    analyst = profile(skills=(*owned_entries("skill", ("month-close",)), *owned_entries("plugin", ("finance-kit",))))
    retired = profile("retired", enabled=False, skills=owned_entries("skill", ("old",)))

    assert pinned_by_profiles(profile_catalog(analyst, retired, shared=layer)) == frozenset(
        ("skill:common", "skill:month-close", "plugin:finance-kit")
    )
    # A shared layer no profile carries pins nothing.
    assert pinned_by_profiles(CocoProfileCatalog((), layer)) == frozenset()
    assert pinned_by_profiles(replace(profile_catalog(analyst), shared=None)) == frozenset(
        ("skill:month-close", "plugin:finance-kit")
    )
