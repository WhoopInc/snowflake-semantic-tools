"""Each eval's subject, described as a comparison commits versions of it: staged as plan publishes it."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.compare_subjects import compare_subjects
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from tests.helpers.agent_builders import agent, compile_agents
from tests.helpers.artifact_builders import target
from tests.helpers.cowork_profiles import compile_profiles, cowork_profile, described_agent, subagent
from tests.helpers.eval_builders import compiled_eval_of, profile_eval_of


def whole(*results: CompileResult) -> CompileResult:
    return CompileResult(tuple(item for result in results for item in result.compiled), DiagnosticBag())


def test_an_agents_subject_is_staged_on_the_spec_stage_under_the_commit() -> None:
    agents = compile_agents(agent("sales_agent"))
    evaluation = compiled_eval_of()
    subjects = compare_subjects(
        whole(agents),
        (evaluation,),
        build_manifest(agents),
        apply_config={"agent_spec_stage": {"stage": "SPECS"}},
        target=target(),
        git_sha="abc1234",
        dirty=True,
    )
    subject = subjects[evaluation.artifact_key]
    assert (subject.key, subject.kind, subject.folder, subject.pinned_model) == (
        "agent:sales_agent",
        "agent",
        "sales_agent",
        "claude",
    )
    assert subject.stage.sql == "DB.SCHEMA.SPECS" and subject.target.sql == "DB.S.SALES_AGENT"
    assert subject.rendered.upload_path == "@DB.SCHEMA.SPECS/sales_agent/abc1234/agent_spec.yaml"
    assert "git:abc1234-dirty" in " ".join(str(item) for item in subject.rendered.statements)


def test_a_profiles_subject_is_staged_in_its_own_folder_and_an_eval_without_one_is_left_out() -> None:
    profiles = compile_profiles(cowork_profile("concierge", subagent()), agents=(described_agent(),))
    evaluation = profile_eval_of()
    orphan: CompiledEval = replace(compiled_eval_of(), resolved=replace(compiled_eval_of().resolved))
    subjects = compare_subjects(
        whole(profiles),
        (evaluation, orphan),
        build_manifest(profiles),
        apply_config={},
        target=target(),
        git_sha="abc1234",
    )
    assert list(subjects) == [evaluation.artifact_key]
    subject = subjects[evaluation.artifact_key]
    assert (subject.kind, subject.folder, subject.stage.sql) == (
        "cowork_profile",
        "cowork-profiles/concierge",
        "DB.SCHEMA.AGENT_SPECS",
    )
