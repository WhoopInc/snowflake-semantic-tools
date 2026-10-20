from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from snowflake_semantic_tools.app.compile.agents import CompiledAgent, for_publication
from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.agent import AgentModel, AgentProfile, ResolvedAgent
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ObservedArtifact,
    RenderedArtifact,
    held_spec_fingerprint,
    spec_fingerprint,
)
from snowflake_semantic_tools.domain.ports.snowflake.catalog import AgentVersion, AgentVersions
from tests.helpers.sql_values import texts

PAYLOAD = '{\n  "models": {\n    "orchestration": "auto"\n  }\n}\n'
# What every version SST commits from PAYLOAD at abc1234 carries.
VERSION_COMMENT = f"git:abc1234 sst-spec:{spec_fingerprint(PAYLOAD)}"


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
        PAYLOAD,
        "a" * 64,
    )


def test_permanent_agent_create_keeps_the_live_version_then_alias_and_tags() -> None:
    value = for_publication(
        compiled(),
        stage=QualifiedName.parse("DB.S.AGENT_SPECS"),
        git_sha="abc1234",
    ).rendered_artifact
    assert value.upload_path == "@DB.S.AGENT_SPECS/agent/abc1234/agent_spec.yaml"
    assert value.upload_content == value.ddl.encode("utf-8")
    # CREATE commits VERSION$1 and opens a live copy of it, which is kept: Snowsight shows the
    # live version, and a request naming no version is served it.
    assert texts(value.statements) == (
        "CREATE AGENT DB.S.AGENT\n  FROM @DB.S.AGENT_SPECS/agent/abc1234/",
        f"ALTER AGENT DB.S.AGENT MODIFY VERSION VERSION$1 SET COMMENT = '{VERSION_COMMENT}'",
        'ALTER AGENT DB.S.AGENT\n  MODIFY VERSION "LAST" SET ALIAS = PROMOTED',
        "ALTER AGENT DB.S.AGENT\n  SET TAG DOMAIN = 'sales'",
    )
    assert not any("COMMIT" in text or "DROP VERSION" in text for text in texts(value.statements))
    assert value.for_action(Action.CREATE, None).statements == value.statements


def test_permanent_agent_update_adds_exactly_one_version_and_a_live_copy_then_alias_and_tags() -> None:
    value = for_publication(
        compiled(),
        stage=QualifiedName.parse("DB.S.AGENT_SPECS"),
        git_sha="abc1234",
    ).rendered_artifact
    observed = ObservedArtifact(
        value.key, value.target.name.folded, value.target, "AGENT", "OWNER", "now", None, None, aliases=("PROMOTED",)
    )
    assert texts(value.for_action(Action.UPDATE, observed).statements) == (
        f"ALTER AGENT DB.S.AGENT\n  ADD VERSION FROM @DB.S.AGENT_SPECS/agent/abc1234/\n  COMMENT = '{VERSION_COMMENT}'",
        "ALTER AGENT DB.S.AGENT ADD LIVE VERSION FROM LAST",
        'ALTER AGENT DB.S.AGENT\n  MODIFY VERSION "LAST" SET ALIAS = PROMOTED',
        "ALTER AGENT DB.S.AGENT\n  SET TAG DOMAIN = 'sales'",
        "ALTER AGENT DB.S.AGENT SET DEFAULT_VERSION = 'LAST'",
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
    default = "ALTER AGENT DB.S.AGENT SET DEFAULT_VERSION = 'LAST'"
    assert texts(update.statements) == (*texts(value.update_live_statements), default)
    # Committing the live version first would publish it as a version of its own, and ADD
    # VERSION beside it fails: the spec goes into it, escaped so its JSON reads back exactly.
    # COMMIT closes the live version, so a new one is opened from what was committed.
    assert texts(update.statements) == (
        "ALTER AGENT DB.S.AGENT\n  MODIFY LIVE VERSION SET SPECIFICATION = "
        '\'{\n  "instructions": {\n    "response": "it\'\'s \\\\"one\\\\"\\\\nline"\n  }\n}\'',
        f"ALTER AGENT DB.S.AGENT COMMIT COMMENT = 'git:abc1234 sst-spec:{spec_fingerprint(payload)}'",
        "ALTER AGENT DB.S.AGENT ADD LIVE VERSION FROM LAST",
        'ALTER AGENT DB.S.AGENT\n  MODIFY VERSION "LAST" SET ALIAS = PROMOTED',
        "ALTER AGENT DB.S.AGENT\n  SET TAG DOMAIN = 'sales'",
        default,
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


def published(model: AgentModel, fingerprint: str = "a" * 64) -> RenderedArtifact:
    """The agent as plan publishes it, staged at abc1234, from PAYLOAD whatever `model` changes."""
    agent = replace(compiled(), resolved=ResolvedAgent(model, ()), definition_fingerprint=fingerprint)
    return for_publication(
        agent, stage=QualifiedName.parse("DB.S.AGENT_SPECS"), git_sha="abc1234"
    ).rendered_for_publish("b" * 64)


def holding_the_spec(artifact: RenderedArtifact) -> ObservedArtifact:
    """The agent as an earlier publish left it: VERSION$4 holds PAYLOAD, aliased and live."""
    return ObservedArtifact(
        artifact.key,
        "AGENT",
        artifact.target,
        "AGENT",
        "OWNER",
        "now",
        None,
        None,
        has_live_version=True,
        aliases=("PROMOTED",),
        newest_version="VERSION$4",
        newest_spec=held_spec_fingerprint(VERSION_COMMENT, None),
        alias_versions=(("PROMOTED", "VERSION$4"),),
    )


def test_every_version_sst_commits_carries_its_commit_and_the_specification_fingerprint() -> None:
    artifact = published(compiled().resolved.model)
    comments = [
        text for text in texts((*artifact.create_statements, *artifact.update_statements)) if VERSION_COMMENT in text
    ]
    assert len(comments) == 2
    assert "git:abc1234 sst-spec:" + spec_fingerprint('{"models":{"orchestration":"auto"}}') == VERSION_COMMENT


def test_the_marked_comment_is_written_before_anything_else_a_publish_runs() -> None:
    artifact = published(replace(compiled().resolved.model, comment="Sales"))
    marked = f"Sales [sst:{'b' * 64}:{'a' * 64}]"
    assert texts(artifact.create_statements)[0] == (
        f"CREATE AGENT DB.S.AGENT\n  COMMENT = '{marked}'\n  FROM @DB.S.AGENT_SPECS/agent/abc1234/"
    )
    for program in (artifact.update_statements, artifact.update_live_statements, artifact.update_properties_statements):
        assert texts(program)[0] == f"ALTER AGENT DB.S.AGENT SET COMMENT = '{marked}'"
        assert texts(program)[-2:] == (
            "ALTER AGENT DB.S.AGENT SET PROFILE = '{}'",
            "ALTER AGENT DB.S.AGENT SET SECURE = FALSE",
        )


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        ({"comment": "Answers sales questions."}, "SET COMMENT = 'Answers sales questions. [sst:"),
        ({"tags": (("DOMAIN", "finance"),)}, "SET TAG DOMAIN = 'finance'"),
        ({"profile": AgentProfile("Sales", None, "blue")}, 'SET PROFILE = \'{"display_name":"Sales","color":"blue"}\''),
        ({"alias": "released"}, "MODIFY VERSION VERSION$4 SET ALIAS = RELEASED"),
    ],
)
def test_a_property_only_change_keeps_the_version_holding_the_specification(
    change: dict[str, Any], expected: str
) -> None:
    model = compiled().resolved.model
    before = published(model)
    after = published(replace(model, **change), fingerprint="c" * 64)
    update = after.for_action(Action.UPDATE, holding_the_spec(before))
    statements = texts(update.statements)
    assert any(expected in text for text in statements), statements
    assert not any(
        part in text
        for text in statements
        for part in ("ADD VERSION", "MODIFY LIVE", "COMMIT", "ADD LIVE VERSION", "CREATE AGENT", '"LAST"')
    )
    assert statements[0].startswith("ALTER AGENT DB.S.AGENT SET COMMENT = ")
