"""A comparison's versions of a published agent or CoWork profile, committed and restored in the fake.

The fake models the subject's versions, so each test reads what SHOW VERSIONS lists afterwards:
the release keeps its alias and the default, each candidate is on its own `EVAL_` version, and a
live version holds the released specification again.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace

import pytest

from snowflake_semantic_tools.app.evals.compare_lifecycle import CandidateVersion, CompareLifecycle, LifecycleResult
from snowflake_semantic_tools.app.evals.compare_precheck import published_release
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, ExecutionError, eval_model_comment
from snowflake_semantic_tools.domain.ports.snowflake.catalog import AgentVersion, AgentVersions
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import Sql
from tests.helpers.compare_world import GIT_SHA, RELEASE, compare_subject, orchestration, payload, publish
from tests.helpers.snowflake_fake import FakeSnowflake

RUN = "c7f2a19e"
OPUS = "claude-opus-4-8"
GPT = "openai-gpt-5"


def commit(port: FakeSnowflake, *models: str, kind: str = "agent") -> tuple[list[str], LifecycleResult]:
    subject = (
        compare_subject()
        if kind == "agent"
        else compare_subject("concierge", kind="cowork_profile", target="DB.PROFILES.CONCIERGE")
    )
    if subject.target.sql not in port.modeled_agents:
        publish(port, subject)
    result = CompareLifecycle(port, run_id=RUN, git_sha=GIT_SHA).commit(subject, models)
    return [text for script in port.scripts for text in script], result


def comment(label: str, specification: str) -> str:
    return eval_model_comment(GIT_SHA, specification, label, RUN)


def test_a_comparison_holds_commits_each_candidate_and_restores_leaving_the_release_alone() -> None:
    port = FakeSnowflake()
    subject = compare_subject()
    entry = publish(
        port,
        subject,
        earlier=(AgentVersion("VERSION$5", "EVAL_CLAUDE_OPUS_4_8", comment=comment(OPUS, "{}").replace(RUN, "old")),),
    )
    sent, result = commit(port, OPUS, GPT)
    agent = "DB.S.SALES_AGENT"
    directory = "@DB.S.AGENT_SPECS/sales_agent/eval/c7f2a19e"
    assert sent == [
        f"ALTER AGENT {agent} COMMIT COMMENT = '{comment('hold', subject.payload)}'",
        f"ALTER AGENT {agent}\n  ADD VERSION FROM {directory}/claude_opus_4_8/\n"
        f"  COMMENT = '{comment(OPUS, payload(OPUS))}'",
        f"ALTER AGENT {agent} MODIFY VERSION VERSION$5 UNSET ALIAS",
        f"ALTER AGENT {agent} MODIFY VERSION VERSION$7 SET ALIAS = EVAL_CLAUDE_OPUS_4_8",
        f"ALTER AGENT {agent}\n  ADD VERSION FROM {directory}/openai_gpt_5/\n"
        f"  COMMENT = '{comment(GPT, payload(GPT))}'",
        f"ALTER AGENT {agent} MODIFY VERSION VERSION$8 SET ALIAS = EVAL_OPENAI_GPT_5",
        f"ALTER AGENT {agent}\n  ADD VERSION FROM {directory}/restore/\n"
        f"  COMMENT = '{comment('restore', subject.payload)}'",
        f"ALTER AGENT {agent} ADD LIVE VERSION FROM LAST",
    ]
    assert not any("DEFAULT_VERSION" in text or "RELEASED" in text for text in sent)
    # Each candidate's specification differs from the release in its orchestration model alone.
    assert [(path, content.decode()) for path, content in port.uploads] == [
        (f"{directory}/claude_opus_4_8/agent_spec.yaml", payload(OPUS)),
        (f"{directory}/openai_gpt_5/agent_spec.yaml", payload(GPT)),
        (f"{directory}/restore/agent_spec.yaml", subject.payload),
    ]
    assert result.hold == "VERSION$6" and result.restore == "VERSION$9"
    assert result.candidates == (
        CandidateVersion(OPUS, "VERSION$7", "EVAL_CLAUDE_OPUS_4_8", "VERSION$5"),
        CandidateVersion(GPT, "VERSION$8", "EVAL_OPENAI_GPT_5"),
    )
    assert (result.failed, result.diagnostics, result.restored) == ((), (), True)
    versions = port.observe_agent_versions(subject.target)
    assert versions.has_live_version and versions.default_name == RELEASE
    assert dict(versions.alias_versions) == {
        "RELEASED": RELEASE,
        "EVAL_CLAUDE_OPUS_4_8": "VERSION$7",
        "EVAL_OPENAI_GPT_5": "VERSION$8",
    }
    live = next(item for item in versions.versions if item.name is None)
    assert orchestration(live.specification) == "claude-sonnet-4-6"
    # A plan of the same project is NOOP, so the next comparison may start, on the same release.
    assert published_release(port, subject, entry, fetched_at="now") == RELEASE


def test_a_cowork_profile_is_compared_in_its_own_stage_folder() -> None:
    port = FakeSnowflake()
    sent, result = commit(port, OPUS, kind="cowork_profile")
    assert "@DB.S.AGENT_SPECS/cowork-profiles/concierge/eval/c7f2a19e/claude_opus_4_8/" in sent[1]
    assert result.candidates == (CandidateVersion(OPUS, "VERSION$6", "EVAL_CLAUDE_OPUS_4_8"),)
    assert port.observe_agent_versions(QualifiedName.parse("DB.PROFILES.CONCIERGE")).has_live_version


def test_a_refused_hold_commits_nothing_and_restores_nothing() -> None:
    port = FakeSnowflake()
    port.refused = ("COMMIT COMMENT",)
    sent, result = commit(port, OPUS)
    assert len(sent) == 1
    assert [item.code for item in result.diagnostics] == ["SST-APL109"]
    assert result.diagnostics[0].message.startswith(
        "agent:sales_agent: the model comparison could not commit the hold copy: COMMIT failed: recorded refusal"
    )
    assert result.failed == ((OPUS, "the hold copy did not commit"),)


class LostReply(FakeSnowflake):
    """A fake whose COMMIT takes effect but reports a failure, as a reply lost in transit does."""

    def execute_script(self, statements: Sequence[Sql]) -> ExecResult:
        result = super().execute_script(statements)
        if "COMMIT COMMENT" in str(statements[0]):
            return ExecResult(False, error=ExecutionError("connection reset"))
        return result


def test_a_hold_that_failed_but_committed_is_still_restored() -> None:
    port = LostReply()
    sent, result = commit(port, OPUS)
    assert sent[-1] == "ALTER AGENT DB.S.SALES_AGENT ADD LIVE VERSION FROM LAST"
    assert result.candidates == () and result.restored
    assert port.observe_agent_versions(QualifiedName.parse("DB.S.SALES_AGENT")).has_live_version


class UnreadAfterHold(FakeSnowflake):
    """A fake whose hold is refused, then whose versions cannot be read."""

    def observe_agent_versions(self, qualified_name: QualifiedName) -> AgentVersions:
        if any("COMMIT COMMENT" in text for script in self.scripts for text in script):
            raise SnowflakePortError("SHOW VERSIONS refused")
        return super().observe_agent_versions(qualified_name)


def test_a_failed_hold_whose_versions_cannot_be_read_is_restored_regardless() -> None:
    port = UnreadAfterHold()
    port.refused = ("COMMIT COMMENT",)
    sent, result = commit(port, OPUS)
    # The restore beside the live version fails, which says how to repair whatever is left.
    assert "/restore/" in sent[-1]
    assert [item.code for item in result.diagnostics] == ["SST-APL109", "SST-APL110"]


def test_a_candidate_whose_upload_fails_is_left_out_and_the_rest_still_commit() -> None:
    port = FakeSnowflake()
    port.fail("upload", SnowflakePortError("PUT refused"), times=1)
    _, result = commit(port, OPUS, GPT)
    assert result.failed == (
        (
            OPUS,
            "staging @DB.S.AGENT_SPECS/sales_agent/eval/c7f2a19e/claude_opus_4_8/agent_spec.yaml failed: PUT refused",
        ),
    )
    assert [item.model for item in result.candidates] == [GPT]
    assert result.restored


@pytest.mark.parametrize(
    ("refused", "detail"),
    [
        ("/claude_opus_4_8/", "ADD VERSION failed: recorded refusal"),
        ("SET ALIAS = EVAL_CLAUDE", "SET ALIAS failed: recorded refusal"),
    ],
)
def test_a_candidate_whose_statement_is_refused_reports_which(refused: str, detail: str) -> None:
    port = FakeSnowflake()
    port.refused = (refused,)
    _, result = commit(port, OPUS)
    [diagnostic] = result.diagnostics
    assert diagnostic.code == "SST-APL109"
    assert f"could not commit the candidate {OPUS}: {detail}" in diagnostic.message
    assert result.restored


class Unlisted(FakeSnowflake):
    """A fake whose SHOW VERSIONS lists nothing a comparison committed, or fails, after the hold."""

    def __init__(self, *, raises: bool) -> None:
        super().__init__()
        self.raises = raises

    def observe_agent_versions(self, qualified_name: QualifiedName) -> AgentVersions:
        if any("ADD VERSION" in text for script in self.scripts for text in script):
            if self.raises:
                raise SnowflakePortError("SHOW refused")
            return AgentVersions()
        return super().observe_agent_versions(qualified_name)


@pytest.mark.parametrize(
    ("raises", "detail"),
    [
        (True, "reading SHOW VERSIONS IN AGENT failed: SHOW refused"),
        (False, "the agent's versions list none this comparison committed for claude-opus-4-8"),
    ],
)
def test_a_candidate_whose_version_is_not_read_back_is_left_out(raises: bool, detail: str) -> None:
    port = Unlisted(raises=raises)
    _, result = commit(port, OPUS)
    assert result.failed == ((OPUS, detail),)
    # The restore committed, though its name could not be read.
    assert result.restore is None and result.restored


@pytest.mark.parametrize("refused", ["/restore/", "ADD LIVE VERSION"])
def test_a_failed_restore_says_how_to_repair_the_subject(refused: str) -> None:
    port = FakeSnowflake()
    port.refused = (refused,)
    _, result = commit(port, OPUS)
    [diagnostic] = result.diagnostics
    assert diagnostic.code == "SST-APL110"
    assert diagnostic.message.startswith(
        "agent:sales_agent: restoring the live version of DB.S.SALES_AGENT after the model comparison failed"
    )
    assert not result.restored and result.restore is None
    assert [item.model for item in result.candidates] == [OPUS]


def test_a_failed_session_mid_statement_is_a_failed_step() -> None:
    port = FakeSnowflake()
    port.fail("execute_script", SnowflakePortError("session gone"), times=1)
    _, result = commit(port, OPUS)
    assert result.diagnostics[0].message.endswith("COMMIT failed: session gone")


class Interrupted(FakeSnowflake):
    """A fake interrupted while it stages the first candidate."""

    def upload(self, stage_path: str, content: bytes) -> None:
        if "claude_opus" in stage_path:
            raise KeyboardInterrupt
        super().upload(stage_path, content)


def test_an_interrupt_mid_candidate_still_restores_before_it_propagates() -> None:
    port = Interrupted()
    with pytest.raises(KeyboardInterrupt):
        commit(port, OPUS)
    assert port.scripts[-1] == ("ALTER AGENT DB.S.SALES_AGENT ADD LIVE VERSION FROM LAST",)


def test_a_specification_sst_did_not_render_commits_no_candidate() -> None:
    port = FakeSnowflake()
    subject = compare_subject()
    publish(port, subject)
    broken = replace(subject, payload="[1]")
    result = CompareLifecycle(port, run_id=RUN, git_sha=GIT_SHA, dirty=True).commit(broken, (OPUS,))
    assert result.failed[0][1].startswith("the released specification is not one SST rendered")
    assert "git:abc1234-dirty" in port.scripts[0][0]


def test_a_subject_with_no_candidate_left_is_neither_held_nor_restored() -> None:
    port = FakeSnowflake()
    sent, result = commit(port)
    assert sent == [] and result.restored and result.candidates == ()
