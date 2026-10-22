"""A permanent agent's planned update, and the versions the fake lists once it is applied.

For the tests of the pin apply sets after an update commits a version: the update either adds a
version (its newest release holds another specification) or only sets properties.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    Change,
    ChangeReason,
    DesiredMetadata,
    ObservedArtifact,
    PublishShape,
    RenderedArtifact,
    StatementPlan,
    eval_model_comment,
    spec_fingerprint,
    version_comment,
)
from snowflake_semantic_tools.domain.model.registry import GrantPreservation
from snowflake_semantic_tools.domain.ports.snowflake.catalog import AgentVersion
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.sql_values import statement

AGENT = QualifiedName.parse("DB.S.SALES")
SPEC = '{"models": {"orchestration": "auto"}}'
# The statement apply runs after the update, pinning the default to the version it committed.
PIN = "ALTER AGENT DB.S.SALES SET DEFAULT_VERSION = 'VERSION$3'"


def agent_update(*, adds_version: bool = True) -> Change:
    """Plan the update of agent `DB.S.SALES`, whose newest release is VERSION$1, with no live version.

    `adds_version` gives that release another specification, so the update commits a version;
    otherwise it holds the planned one, and the update only sets properties.
    """
    artifact = RenderedArtifact.create(
        key="agent:sales",
        artifact_type="agent",
        target=AGENT,
        ddl=SPEC,
        shape=PublishShape("AGENT", render_dialect="json", grant_preservation=GrantPreservation.NONE),
        statements=StatementPlan(
            default=(statement("create"),),
            update=(statement("add version"),),
            update_properties=(statement("set comment"),),
        ),
        metadata=DesiredMetadata(spec_fingerprint=spec_fingerprint(SPEC)),
    )
    observed = ObservedArtifact(
        artifact.key,
        "SALES",
        AGENT,
        "AGENT",
        "OWNER",
        "now",
        None,
        None,
        newest_version="VERSION$1",
        newest_spec="f" * 64 if adds_version else spec_fingerprint(SPEC),
    )
    effective = artifact.for_action(Action.UPDATE, observed)
    return Change(artifact.key, "agent", Action.UPDATE, ChangeReason.FINGERPRINT_DIFFERS, effective, observed, (), 60)


def committed(port: FakeSnowflake) -> None:
    """List what the update leaves: VERSION$1, an eval-model VERSION$2, and the new release VERSION$3."""
    port.agent_version_rows[AGENT.sql] = (
        AgentVersion(None, None),
        AgentVersion("VERSION$1", None, comment=version_comment("abc1234", "{}")),
        AgentVersion("VERSION$2", "EVAL_CLAUDE_OPUS_4_8", comment=eval_model_comment("abc1234", "{}", "x", "r1")),
        AgentVersion("VERSION$3", None, True, comment=version_comment("def5678", SPEC)),
    )
