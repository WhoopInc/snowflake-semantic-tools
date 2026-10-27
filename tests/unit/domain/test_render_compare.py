"""The pure builders of a comparison's versions: candidate specifications, stage paths, statements."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import eval_model_comment, eval_model_run
from snowflake_semantic_tools.domain.render.compare import (
    add_version_statement,
    compare_run_name,
    hold_statement,
    model_slug,
    open_live_statement,
    set_alias_statement,
    staged_spec_path,
    unset_alias_statement,
    version_directory,
    with_orchestration_model,
)

AGENT = QualifiedName.parse("DB.S.SALES")
STAGE = QualifiedName.parse("DB.S.AGENT_SPECS")


def test_a_candidate_specification_changes_the_orchestration_model_alone() -> None:
    released = '{\n  "models": {\n    "orchestration": "auto",\n    "x": 1\n  },\n  "tools": []\n}\n'
    assert with_orchestration_model(released, "claude-opus-4-8") == (
        '{\n  "models": {\n    "orchestration": "claude-opus-4-8",\n    "x": 1\n  },\n  "tools": []\n}\n'
    )
    assert '"orchestration": "gpt-5"' in with_orchestration_model("{}", "gpt-5")


@pytest.mark.parametrize("payload", ["[]", '{"models": []}'])
def test_a_specification_that_is_not_an_agents_is_refused(payload: str) -> None:
    with pytest.raises(ValueError, match="JSON object"):
        with_orchestration_model(payload, "gpt-5")


def test_each_run_is_named_by_its_subject_comparison_model_and_number() -> None:
    assert compare_run_name("jaffle_analyst", "c7f2a19e", "claude-sonnet-4-6", 2) == (
        "JAFFLE_ANALYST_CMP_C7F2A19E_CLAUDE_SONNET_4_6_2"
    )


def test_each_version_is_staged_under_the_runs_directory_by_its_slug() -> None:
    assert model_slug("openai-gpt-5.4") == "openai_gpt_5_4"
    assert version_directory("sales", "r1", "restore") == "sales/eval/r1/restore/"
    assert (
        staged_spec_path(STAGE, "sales", "r1", "restore") == "@DB.S.AGENT_SPECS/sales/eval/r1/restore/agent_spec.yaml"
    )


def test_the_statements_name_the_agent_its_version_and_the_staged_directory() -> None:
    assert str(hold_statement(AGENT, "c")) == "ALTER AGENT DB.S.SALES COMMIT COMMENT = 'c'"
    assert str(add_version_statement(AGENT, STAGE, "sales/eval/r1/restore/", "c")) == (
        "ALTER AGENT DB.S.SALES\n  ADD VERSION FROM @DB.S.AGENT_SPECS/sales/eval/r1/restore/\n  COMMENT = 'c'"
    )
    assert (
        str(unset_alias_statement(AGENT, "version$5")) == "ALTER AGENT DB.S.SALES MODIFY VERSION VERSION$5 UNSET ALIAS"
    )
    assert str(set_alias_statement(AGENT, "VERSION$6", "EVAL_GPT_5")) == (
        "ALTER AGENT DB.S.SALES MODIFY VERSION VERSION$6 SET ALIAS = EVAL_GPT_5"
    )
    assert str(open_live_statement(AGENT)) == "ALTER AGENT DB.S.SALES ADD LIVE VERSION FROM LAST"


@pytest.mark.parametrize("version", ["LAST", "VERSION$0", "EVAL_GPT_5"])
def test_only_a_system_version_name_is_written_as_a_version(version: str) -> None:
    with pytest.raises(ValueError, match="system name"):
        unset_alias_statement(AGENT, version)


def test_an_eval_model_comment_reads_back_its_label_and_run() -> None:
    assert eval_model_run(eval_model_comment("abc", "{}", "claude-opus-4-8", "c7f2")) == ("claude-opus-4-8", "c7f2")
    assert eval_model_run("git:abc sst-spec:" + "e" * 64) is None
    assert eval_model_run(None) is None
