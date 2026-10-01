from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.compile.agents import CompiledAgent, for_publication
from snowflake_semantic_tools.domain.model.agent import AgentModel, AgentProfile, ResolvedAgent
from snowflake_semantic_tools.domain.model.diagnostic import Origin
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import Action, ObservedArtifact


def compiled(*, alias: str | None = "promoted") -> CompiledAgent:
    model = AgentModel(
        "agent",
        Origin("agent.yml"),
        ("agent.yml",),
        alias=alias,
        tags=(("DOMAIN", "sales"),),
    )
    return CompiledAgent(
        ResolvedAgent(model, ()),
        QualifiedName.parse("DB.S.AGENT"),
        '{\n  "models": {\n    "orchestration": "auto"\n  }\n}\n',
        "a" * 64,
    )


def test_permanent_agent_create_uses_exact_stage_path_then_alias_and_tags() -> None:
    value = for_publication(
        compiled(),
        stage=QualifiedName.parse("DB.S.AGENT_SPECS"),
        git_sha="abc1234",
    ).rendered_artifact
    assert value.upload_path == "@DB.S.AGENT_SPECS/agent/abc1234/agent_spec.yaml"
    assert value.upload_content == value.ddl.encode("utf-8")
    assert value.statements == (
        "CREATE AGENT DB.S.AGENT\n  FROM @DB.S.AGENT_SPECS/agent/abc1234/",
        'ALTER AGENT DB.S.AGENT\n  MODIFY VERSION "LAST" SET ALIAS = PROMOTED',
        "ALTER AGENT DB.S.AGENT\n  SET TAG DOMAIN = 'sales'",
    )


def test_permanent_agent_update_commits_live_before_add_version_alias_and_tags() -> None:
    value = for_publication(
        compiled(),
        stage=QualifiedName.parse("DB.S.AGENT_SPECS"),
        git_sha="abc1234",
    ).rendered_artifact
    assert value.update_live_statements[0] == "ALTER AGENT DB.S.AGENT COMMIT"
    assert value.update_live_statements[1] == (
        "ALTER AGENT DB.S.AGENT\n  ADD VERSION FROM @DB.S.AGENT_SPECS/agent/abc1234/\n" "  COMMENT = 'git:abc1234'"
    )
    assert value.update_live_statements[2].endswith("ALIAS = PROMOTED")
    assert value.update_live_statements[3].endswith("DOMAIN = 'sales'")
    observed = ObservedArtifact(
        value.key,
        value.target.name.folded,
        value.target,
        "AGENT",
        "OWNER",
        "now",
        None,
        None,
        has_live_version=True,
    )
    update = value.for_action(Action.UPDATE, observed)
    assert update.statements == value.update_live_statements


def test_recorded_port_reports_live_agent_versions() -> None:
    from tests.helpers.recorded_snowflake import RecordedSnowflake

    port = RecordedSnowflake()
    name = QualifiedName.parse("DB.S.AGENT")
    assert not port.agent_has_live_version(name)
    port.live_agents.add(name.sql)
    assert port.agent_has_live_version(name)


def test_temporary_agent_is_inline_and_has_no_upload() -> None:
    value = for_publication(
        compiled(alias=None),
        stage=QualifiedName.parse("DB.S.AGENT_SPECS"),
        git_sha="abc1234",
        temporary=True,
    ).rendered_artifact
    assert value.temporary
    assert value.upload_path is None and value.upload_content is None
    assert value.statements[0].startswith("CREATE OR REPLACE TEMPORARY AGENT DB.S.AGENT")
    assert "FROM SPECIFICATION $$" in value.statements[0]


def test_agent_publish_appends_profile_comment_marker_and_secure_metadata() -> None:
    value = compiled()
    model = value.resolved.model
    value = for_publication(
        replace(
            value,
            resolved=ResolvedAgent(
                replace(
                    model,
                    comment="Scope comment",
                    secure=True,
                    profile=AgentProfile("Agent", "Icon", "blue"),
                ),
                (),
            ),
        ),
        stage=QualifiedName.parse("DB.S.AGENT_SPECS"),
        git_sha="abc1234",
    ).rendered_for_publish("b" * 64)
    assert any("SET PROFILE" in statement for statement in value.statements)
    assert any(f"[sst:{'b' * 64}:{value.fingerprint}] Scope comment" in statement for statement in value.statements)
    assert value.statements[-1] == "ALTER AGENT DB.S.AGENT SET SECURE = TRUE"


def test_agent_update_unsets_removed_aliases_and_tags_and_clears_profile() -> None:
    value = for_publication(
        compiled(alias=None),
        stage=QualifiedName.parse("DB.S.AGENT_SPECS"),
        git_sha="abc1234",
    ).rendered_for_publish("b" * 64)
    observed = ObservedArtifact(
        value.key,
        value.target.name.folded,
        value.target,
        "AGENT",
        "OWNER",
        "now",
        None,
        None,
        aliases=("OLD_ALIAS",),
        tags=("DB.S.OLD_TAG",),
    )
    update = value.for_action(Action.UPDATE, observed)
    assert any("SET PROFILE = '{}'" in statement for statement in value.statements)
    assert any("OLD_ALIAS UNSET ALIAS" in statement for statement in update.statements)
    assert any("UNSET TAG DB.S.OLD_TAG" in statement for statement in update.statements)


def test_profile_with_dollar_delimiter_is_sql_string_escaped() -> None:
    base = compiled(alias=None)
    model = replace(base.resolved.model, profile=AgentProfile("Agent $$", None, None))
    value = for_publication(
        replace(base, resolved=ResolvedAgent(model, ())),
        stage=QualifiedName.parse("DB.S.AGENT_SPECS"),
        git_sha="abc1234",
    ).rendered_for_publish("b" * 64)
    assert any("Agent $$" in statement and "SET PROFILE = '" in statement for statement in value.statements)
