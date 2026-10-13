from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.compile.agents import CompiledAgent, for_publication
from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.agent import AgentModel, AgentProfile, ResolvedAgent
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import Action, ObservedArtifact
from snowflake_semantic_tools.domain.ports.snowflake.catalog import AgentVersion, AgentVersions
from tests.helpers.sql_values import texts


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


def test_permanent_agent_create_leaves_one_committed_version_then_alias_and_tags() -> None:
    value = for_publication(
        compiled(),
        stage=QualifiedName.parse("DB.S.AGENT_SPECS"),
        git_sha="abc1234",
    ).rendered_artifact
    assert value.upload_path == "@DB.S.AGENT_SPECS/agent/abc1234/agent_spec.yaml"
    assert value.upload_content == value.ddl.encode("utf-8")
    # CREATE also opens a live copy of VERSION$1; it is committed as VERSION$2 while the
    # default is pinned to VERSION$1, dropped, and the default handed back to LAST.
    assert texts(value.statements) == (
        "CREATE AGENT DB.S.AGENT\n  FROM @DB.S.AGENT_SPECS/agent/abc1234/",
        "ALTER AGENT DB.S.AGENT SET DEFAULT_VERSION = 'VERSION$1'",
        "ALTER AGENT DB.S.AGENT COMMIT",
        "ALTER AGENT DB.S.AGENT DROP VERSION VERSION$2",
        "ALTER AGENT DB.S.AGENT SET DEFAULT_VERSION = 'LAST'",
        "ALTER AGENT DB.S.AGENT MODIFY VERSION VERSION$1 SET COMMENT = 'git:abc1234'",
        'ALTER AGENT DB.S.AGENT\n  MODIFY VERSION "LAST" SET ALIAS = PROMOTED',
        "ALTER AGENT DB.S.AGENT\n  SET TAG DOMAIN = 'sales'",
    )
    assert value.for_action(Action.CREATE, None).statements == value.statements


def test_permanent_agent_update_adds_exactly_one_version_then_alias_and_tags() -> None:
    value = for_publication(
        compiled(),
        stage=QualifiedName.parse("DB.S.AGENT_SPECS"),
        git_sha="abc1234",
    ).rendered_artifact
    observed = ObservedArtifact(
        value.key, value.target.name.folded, value.target, "AGENT", "OWNER", "now", None, None, aliases=("PROMOTED",)
    )
    assert texts(value.for_action(Action.UPDATE, observed).statements) == (
        "ALTER AGENT DB.S.AGENT\n  ADD VERSION FROM @DB.S.AGENT_SPECS/agent/abc1234/\n  COMMENT = 'git:abc1234'",
        'ALTER AGENT DB.S.AGENT\n  MODIFY VERSION "LAST" SET ALIAS = PROMOTED',
        "ALTER AGENT DB.S.AGENT\n  SET TAG DOMAIN = 'sales'",
    )


def test_permanent_agent_update_writes_a_live_version_and_commits_it_as_the_one_new_version() -> None:
    payload = '{\n  "instructions": {\n    "response": "it\'s \\"one\\"\\nline"\n  }\n}\n'
    value = for_publication(
        replace(compiled(), payload=payload),
        stage=QualifiedName.parse("DB.S.AGENT_SPECS"),
        git_sha="abc1234",
    ).rendered_artifact
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
    # Committing the live version first would publish it as a version of its own, and ADD
    # VERSION beside it fails: the spec goes into it, escaped so its JSON reads back exactly.
    assert texts(update.statements) == (
        "ALTER AGENT DB.S.AGENT\n  MODIFY LIVE VERSION SET SPECIFICATION = "
        '\'{\n  "instructions": {\n    "response": "it\'\'s \\\\"one\\\\"\\\\nline"\n  }\n}\'',
        "ALTER AGENT DB.S.AGENT COMMIT COMMENT = 'git:abc1234'",
        'ALTER AGENT DB.S.AGENT\n  MODIFY VERSION "LAST" SET ALIAS = PROMOTED',
        "ALTER AGENT DB.S.AGENT\n  SET TAG DOMAIN = 'sales'",
    )
    assert not any("ADD VERSION" in text for text in texts(update.statements))


def test_recorded_port_reports_live_agent_versions_and_aliases() -> None:
    from tests.helpers.snowflake_fake import FakeSnowflake

    port = FakeSnowflake()
    name = QualifiedName.parse("DB.S.AGENT")
    assert port.observe_agent_versions(name) == AgentVersions()
    port.agent_version_rows[name.sql] = (AgentVersion(None, None), AgentVersion("VERSION$1", "PROMOTED"))
    observed = port.observe_agent_versions(name)
    assert (observed.has_live_version, observed.aliases) == (True, ("PROMOTED",))


def test_temporary_agent_is_inline_and_has_no_upload() -> None:
    value = for_publication(
        compiled(alias=None),
        stage=QualifiedName.parse("DB.S.AGENT_SPECS"),
        git_sha="abc1234",
        temporary=True,
    ).rendered_artifact
    assert value.temporary
    assert value.upload_path is None and value.upload_content is None
    assert str(value.statements[0]).startswith("CREATE OR REPLACE TEMPORARY AGENT DB.S.AGENT")
    assert "FROM SPECIFICATION $$" in str(value.statements[0])


def test_agent_publish_appends_profile_comment_marker_and_secure_metadata() -> None:
    value = compiled()
    model = value.resolved.model
    published = for_publication(
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
    assert any("SET PROFILE" in str(statement) for statement in published.statements)
    assert any(
        f"Scope comment [sst:{'b' * 64}:{published.fingerprint}]" in str(statement)
        for statement in published.statements
    )
    assert str(published.statements[-1]) == "ALTER AGENT DB.S.AGENT SET SECURE = TRUE"


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
    assert any("SET PROFILE = '{}'" in str(statement) for statement in value.statements)
    assert any("OLD_ALIAS UNSET ALIAS" in str(statement) for statement in update.statements)
    assert any("UNSET TAG DB.S.OLD_TAG" in str(statement) for statement in update.statements)


def test_profile_with_dollar_delimiter_is_sql_string_escaped() -> None:
    base = compiled(alias=None)
    model = replace(base.resolved.model, profile=AgentProfile("Agent $$", None, None))
    value = for_publication(
        replace(base, resolved=ResolvedAgent(model, ())),
        stage=QualifiedName.parse("DB.S.AGENT_SPECS"),
        git_sha="abc1234",
    ).rendered_for_publish("b" * 64)
    assert any("Agent $$" in text and "SET PROFILE = '" in text for text in texts(value.statements))
