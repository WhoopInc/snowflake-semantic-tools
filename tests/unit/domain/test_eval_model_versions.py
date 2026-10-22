"""The versions an eval-model comparison commits, told apart from SST's releases.

A comparison commits a hold copy of the released specification, one version per candidate
model, and a restore copy, each commented `sst-eval-model:<label>` and each candidate aliased
`EVAL_<MODEL>`. None of them is SST's newest release.
"""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.model.lifecycle import (
    EVAL_ALIAS_PREFIX,
    EVAL_MODEL_TOKEN,
    commented_spec_fingerprint,
    eval_model_alias,
    eval_model_comment,
    is_eval_alias,
    is_eval_model_comment,
    spec_fingerprint,
    version_comment,
)
from snowflake_semantic_tools.domain.ports.snowflake.catalog import AgentVersion, AgentVersions

SPEC = '{"models": {"orchestration": "auto"}}'


def test_an_eval_model_version_comment_keeps_the_release_tokens_and_adds_its_label_and_run() -> None:
    comment = eval_model_comment("abc1234", SPEC, "claude-opus-4-8", "r1")
    assert comment == f"{version_comment('abc1234', SPEC)} {EVAL_MODEL_TOKEN}claude-opus-4-8 run:r1"
    assert commented_spec_fingerprint(comment) == spec_fingerprint(SPEC)
    assert eval_model_comment("abc1234", SPEC, "hold", "r1", dirty=True).startswith("git:abc1234-dirty ")
    assert is_eval_model_comment(comment)
    assert is_eval_model_comment(comment.replace(EVAL_MODEL_TOKEN, "SST-EVAL-MODEL:"))
    for other in (version_comment("abc1234", SPEC), None, "xsst-eval-model:hold", "sst-eval-model: hold"):
        assert not is_eval_model_comment(other)


@pytest.mark.parametrize(("label", "run_id"), [("", "r1"), ("claude opus", "r1"), ("hold", ""), ("hold", "r 1")])
def test_an_eval_model_comment_refuses_a_label_or_run_that_would_not_read_back(label: str, run_id: str) -> None:
    with pytest.raises(ValueError, match="single tokens"):
        eval_model_comment("abc1234", SPEC, label, run_id)


def test_each_model_gets_a_plain_alias_under_the_reserved_prefix() -> None:
    assert eval_model_alias("claude-opus-4-8") == "EVAL_CLAUDE_OPUS_4_8"
    assert eval_model_alias("openai-gpt.5 mini") == "EVAL_OPENAI_GPT_5_MINI"
    assert eval_model_alias("modèle") == "EVAL_MOD_LE"
    assert EVAL_ALIAS_PREFIX == "EVAL_"
    assert is_eval_alias("EVAL_CLAUDE_OPUS_4_8") and is_eval_alias(" eval_x")
    assert not is_eval_alias("EVALUATED") and not is_eval_alias("PROMOTED")


def _versions(*rows: tuple[str | None, str | None]) -> AgentVersions:
    return AgentVersions(tuple(AgentVersion(name, None, comment=comment) for name, comment in rows))


def test_the_newest_release_passes_over_eval_model_versions_and_last_does_not() -> None:
    release = version_comment("abc1234", SPEC)
    candidate = eval_model_comment("abc1234", SPEC, "claude-opus-4-8", "r1")
    versions = _versions((None, None), ("VERSION$3", candidate), ("VERSION$10", candidate), ("VERSION$2", release))
    assert versions.newest is not None and versions.newest.name == "VERSION$2"
    assert versions.last is not None and versions.last.name == "VERSION$10"
    assert versions.last_is_eval_model
    released = _versions(("VERSION$1", candidate), ("VERSION$2", release))
    assert not released.last_is_eval_model
    only_candidates = _versions(("VERSION$1", candidate))
    assert only_candidates.newest is None and only_candidates.last is not None
    assert not _versions((None, None)).last_is_eval_model
