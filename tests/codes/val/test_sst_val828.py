"""SST-VAL828: a publication channel's change has no outcome of its own, or several.

Each channel reports its own outcome and any failure fails the deploy, so the run's result
shows every channel's state. Apply accounts for each skill, plugin and profile change by key
once the waves ran, so a dropped or doubled outcome fails the run instead of reading as success.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.app.compile.skills import CompileSkills
from snowflake_semantic_tools.app.lifecycle.composite import skipped
from snowflake_semantic_tools.app.lifecycle.extensions import ExtensionLifecycleHandler
from snowflake_semantic_tools.app.lifecycle.profiles import ProfileLifecycleHandler
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.app.plan_artifacts import PlanArtifacts
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import ApplyOptions, ApplyOutcome, Change, RenderedArtifact
from snowflake_semantic_tools.domain.model.skill import SkillCatalog
from snowflake_semantic_tools.domain.ports.lifecycle import CompositeLifecycleHandler
from snowflake_semantic_tools.domain.validate.publication import channel_outcome_diagnostics
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.artifact_builders import target
from tests.helpers.clocks import FixedClock
from tests.helpers.publications import compiled_profile, compiled_skill, empty_state, publish_skill, skill
from tests.helpers.snowflake_fake import FakeSnowflake


def test_sst_val828_fires() -> None:
    changes = (("skill:month-close", "skill"), ("profile:analyst", "profile"))
    [missing, doubled] = channel_outcome_diagnostics(changes, ["profile:analyst", "profile:analyst"])
    assert (missing.code, missing.severity, missing.subject) == ("SST-VAL828", Severity.ERROR, "skill:month-close")
    assert missing.message == "skill 'month-close': the catalog channel reported no outcomes"
    assert doubled.message == "skill 'analyst': the stage channel reported 2 outcomes"


def test_sst_val828_fails_a_run_where_one_channel_reports_for_the_other(monkeypatch: pytest.MonkeyPatch) -> None:
    def misfiled(handler: ProfileLifecycleHandler, change: Change, artifact: RenderedArtifact) -> ApplyOutcome:
        return skipped(replace(change, key="skill:month-close"))

    monkeypatch.setattr(ProfileLifecycleHandler, "_publish", misfiled)
    port = FakeSnowflake(existing=())
    extension = compiled_skill()
    profile = compiled_profile(skill())
    handlers: dict[str, CompositeLifecycleHandler] = {
        "skill": ExtensionLifecycleHandler(port, {extension.artifact_key: extension.release}, "skill"),
        "profile": ProfileLifecycleHandler(port, {profile.artifact_key: profile}),
    }
    rendered = {item.artifact_key: item.rendered_artifact for item in (extension, profile)}
    manifest = build_manifest(CompileSkills(SkillCatalog(), None).run_result())
    changeset = PlanArtifacts(port, lifecycle_handlers=handlers).run(
        rendered, manifest, empty_state(), target(), fetched_at="now"
    )
    result = ApplyArtifacts(
        port,
        InMemoryStateStore(),
        FixedClock(),
        state_table=QualifiedName.parse("DB.S.SST_STATE"),
        lifecycle_handlers=handlers,
    ).run(changeset, empty_state(), ApplyOptions())
    # As many outcomes as changes, so only the per-key accounting sees the gap.
    assert "SST-APL900" not in [item.code for item in result.diagnostics]
    assert sorted(item.message for item in result.diagnostics if item.code == "SST-VAL828") == [
        "skill 'analyst': the stage channel reported no outcomes",
        "skill 'month-close': the catalog channel reported 2 outcomes",
    ]
    assert not result.success


def test_sst_val828_silent() -> None:
    assert channel_outcome_diagnostics((("skill:a", "skill"), ("semantic_view:v", "semantic_view")), ["skill:a"]) == ()
    _, result, _ = publish_skill(FakeSnowflake(existing=()), compiled_skill())
    assert result.success, result.outcomes
    assert "SST-VAL828" not in [item.code for item in result.diagnostics]
