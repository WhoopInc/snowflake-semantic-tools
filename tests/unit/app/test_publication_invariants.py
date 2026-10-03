"""Invariants of skill and profile publication that keep several failure shapes from arising.

Each test pins the property that rules a condition out, so a change that broke it would fail
here: versions are named by content, every extension lands in one schema, SST issues no
grant, no raw string reaches DDL, and each channel reports its own outcome.
"""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.app.compile.skills import CatalogChannel, CompiledExtension, CompileSkills
from snowflake_semantic_tools.app.lifecycle.extensions import ExtensionLifecycleHandler
from snowflake_semantic_tools.app.lifecycle.profiles import ProfileLifecycleHandler
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.app.plan_artifacts import PlanArtifacts
from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import ApplyOptions, OutcomeStatus
from snowflake_semantic_tools.domain.model.skill import Skill, SkillCatalog, SkillFile
from snowflake_semantic_tools.domain.ports.lifecycle import CompositeLifecycleHandler
from snowflake_semantic_tools.domain.sql import sql
from snowflake_semantic_tools.domain.validate.config import validate_config
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.artifact_builders import target
from tests.helpers.clocks import FixedClock
from tests.helpers.publications import (
    SKILL_STAGE,
    compiled_profile,
    compiled_skill,
    empty_state,
    publish_skill,
    skill,
)
from tests.helpers.snowflake_fake import FakeSnowflake


def _skill_in(folder: str, name: str, body: bytes = b"Do it.\n") -> Skill:
    files = (SkillFile("SKILL.md", b"---\nname: " + name.encode() + b"\ndescription: D.\n---\n" + body),)
    return Skill(name, folder, name, "D.", body.decode(), files, Origin(f"{folder}/SKILL.md"))


def _extensions(*skills: Skill, prefix: str = "GIT_") -> list[CompiledExtension]:
    channel = CatalogChannel("DB", "S", SKILL_STAGE, version_prefix=prefix)
    result = CompileSkills(SkillCatalog(skills), channel).run_result()
    return [item for item in result.compiled if isinstance(item, CompiledExtension)]


def test_a_version_alias_is_the_prefix_and_the_bundle_digest_so_one_name_means_one_content() -> None:
    [first] = _extensions(_skill_in("skills/a", "close"))
    [again] = _extensions(_skill_in("skills/a", "close"))
    [edited] = _extensions(_skill_in("skills/a", "close", b"Do it twice.\n"))
    assert first.release.alias == f"GIT_{first.release.bundle.digest[:12].upper()}"
    # Re-publishing the same tree, from any deploy, names the same version; any edit, another.
    assert first.release.alias == again.release.alias != edited.release.alias


def test_every_extension_publishes_into_the_one_catalog_schema_whatever_its_folder() -> None:
    compiled = _extensions(_skill_in("skills/finance/close", "close"), _skill_in("skills/ops/triage", "triage"))
    assert {item.release.target.folded[:2] for item in compiled} == {("DB", "S")}
    # Nor can a folder route a skill elsewhere.
    codes = [item.code for item in validate_config({"skills": {"finance": {"+schema": "FINANCE"}}})]
    assert "SST-CFG042" in codes


def test_a_certified_publish_tags_the_version_and_issues_no_grant() -> None:
    port = FakeSnowflake(existing=())
    _, result, _ = publish_skill(port, compiled_skill(certified=True))
    assert result.success, result.outcomes
    statements = [statement.upper() for script in port.scripts for statement in script]
    assert any("SET TAG SNOWFLAKE.CORE.CERTIFICATION_STATUS" in statement for statement in statements)
    assert not any(statement.lstrip().startswith(("GRANT ", "REVOKE ")) for statement in statements)
    codes = [item.code for item in validate_config({"skills": {"+grant_read_to": ["ANALYST"]}})]
    assert "SST-CFG043" in codes


def test_no_raw_text_reaches_ddl_through_the_statement_builder() -> None:
    with pytest.raises(TypeError, match="must be Sql"):
        sql("CREATE CORTEX EXTENSION {name}", name="DB.S.X; DROP TABLE T")  # type: ignore[arg-type]


def test_each_channel_reports_its_own_outcome_and_one_failure_fails_the_deploy() -> None:
    port = FakeSnowflake(existing=())
    extension = compiled_skill()
    profile = compiled_profile(skill())
    # The stage channel's registry write is refused; the catalog channel publishes.
    port.refused = ("MERGE",)
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
    statuses = {outcome.key: outcome.status for outcome in result.outcomes}
    assert statuses == {"skill:month-close": OutcomeStatus.APPLIED, "profile:analyst": OutcomeStatus.FAILED}
    assert not result.success
