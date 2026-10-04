"""What the per-code RND tests share: an eval, a skill and a tool member to render."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.eval import EvalGroundTruth, EvalQuestion, ResolvedEval
from snowflake_semantic_tools.domain.model.skill import Skill, SkillFile
from snowflake_semantic_tools.domain.model.tool import ToolMember, ToolOwnership
from tests.helpers.eval_builders import ORIGIN, resolved_eval


def with_truth(truth: EvalGroundTruth) -> ResolvedEval:
    """The resolved eval with one question whose ground truth is `truth`."""
    value = resolved_eval()
    return replace(value, dataset=replace(value.dataset, questions=(EvalQuestion(ORIGIN, "Question", truth),)))


def skill(*files: SkillFile, body: str = "Body.\n") -> Skill:
    """Skill `close` with `files` and `body`."""
    return Skill("close", "skills/close", "close", "Close.", body, files, Origin("skills/close/SKILL.md"))


def member(kind: str, **fields: object) -> ToolMember:
    """A defined tool member `platform.lookup` of `kind` with `fields`."""
    return ToolMember("platform", "lookup", kind, ToolOwnership.DEFINE, Origin("tools/t.yml"), "tools/t.yml", **fields)  # type: ignore[arg-type]
