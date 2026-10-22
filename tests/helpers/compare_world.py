"""A published agent or CoWork profile in the fake, as `sst apply` leaves it, for the comparison tests.

`compare_subject` renders a subject as plan publishes it: staged under `DB.S.AGENT_SPECS`, its
specification on `claude-sonnet-4-6`, with the alias `RELEASED`. `publish` stages it on a fake
whose versions each ALTER AGENT changes: listed with its marker, released as `VERSION$4` with the
alias and the default pinned there by name, a live version holding the same specification, and
the state entry apply recorded. A plan of the subject is then NOOP.
"""

from __future__ import annotations

import json
from dataclasses import replace

from snowflake_semantic_tools.app.compile.staged_agent import (
    AgentObjectMetadata,
    StagedAgentObject,
    render_agent_object,
    staged_path,
)
from snowflake_semantic_tools.app.evals.compare_lifecycle import CompareSubject
from snowflake_semantic_tools.domain.model.agent import AgentProfile
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    OwnershipMarker,
    ShowRow,
    marked_comment,
    spec_fingerprint,
    version_comment,
)
from snowflake_semantic_tools.domain.ports.snowflake.catalog import AgentVersion
from snowflake_semantic_tools.domain.render.agent import render_spec_json
from snowflake_semantic_tools.domain.state import APPLIED, AppliedEntry
from tests.helpers.snowflake_fake import FakeSnowflake

STAGE = QualifiedName.parse("DB.S.AGENT_SPECS")
MANIFEST_ID = "a" * 64
GIT_SHA = "abc1234"
PINNED = "claude-sonnet-4-6"
RELEASE = "VERSION$4"
ALIAS = "RELEASED"


def payload(model: str = PINNED) -> str:
    """The subject's specification, orchestrated by `model`."""
    return render_spec_json({"models": {"orchestration": model}, "instructions": {"response": "Be brief."}})


def compare_subject(
    name: str = "sales_agent", *, kind: str = "agent", target: str = "DB.S.SALES_AGENT", alias: str | None = ALIAS
) -> CompareSubject:
    """Render one subject as plan publishes it, and describe it as a comparison commits versions of it."""
    qualified = QualifiedName.parse(target)
    folder = name if kind == "agent" else f"cowork-profiles/{name}"
    spec = payload()
    staged = StagedAgentObject(
        key=f"{kind}:{name}",
        artifact_type=kind,
        target=qualified,
        payload=spec,
        definition_fingerprint="d" * 64,
        spec_fingerprint=spec_fingerprint(spec),
        metadata=AgentObjectMetadata(alias=alias, tags=(), profile=AgentProfile(), secure=None),
        depends_on=(),
        references=(),
        folder=folder,
        stage_path=staged_path(STAGE, folder, GIT_SHA),
        stage=STAGE,
    )
    marker = OwnershipMarker(MANIFEST_ID, "d" * 64)
    rendered = render_agent_object(staged, marked_comment(None, marker.text))
    return CompareSubject(
        f"{kind}:{name}", kind, name, qualified, spec, PINNED, STAGE, folder, replace(rendered, expected_marker=marker)
    )


def publish(port: FakeSnowflake, subject: CompareSubject, *, earlier: tuple[AgentVersion, ...] = ()) -> AppliedEntry:
    """Stage the subject on `port` as apply leaves it, its versions modelled; return the state entry.

    Args:
        earlier: Versions an earlier comparison committed, listed after the release.
    """
    target = subject.target
    port.modelled_agents.add(target.sql)
    port.show(
        ShowRow(
            target.name.value,
            target.database.value,
            target.schema.value,
            "OWNER",
            "now",
            comment=marked_comment(None, OwnershipMarker(MANIFEST_ID, "d" * 64).text),
            object_type="AGENT",
        )
    )
    port.agent_version_rows[target.sql] = (
        AgentVersion(None, None, specification=subject.payload),
        *(AgentVersion(f"VERSION${number}", None, comment=version_comment(GIT_SHA, "{}")) for number in (1, 2, 3)),
        AgentVersion(
            RELEASE,
            ALIAS if subject.rendered.desired_alias else None,
            True,
            comment=version_comment(GIT_SHA, subject.payload),
            specification=subject.payload,
        ),
        *earlier,
    )
    port.agent_default_settings[target.sql] = RELEASE
    return AppliedEntry("d" * 64, target.sql, "now", "run", APPLIED, "ddl", MANIFEST_ID, GIT_SHA)


def orchestration(specification: str | None) -> object:
    """Return the orchestration model a version's specification names."""
    return json.loads(specification or "{}").get("models", {}).get("orchestration")
