"""Check what spans artifact types: shared questions, and agents against their evals and tools.

Each check reads values the compilers already produced -- verified queries, agents, CoWork
profiles, evals, and compiled tools -- so `CompileProject` runs them once every compiler has.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.agent import AgentModel
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.cowork_profile import CoworkProfileModel
from snowflake_semantic_tools.domain.model.eval import EvalDefaults, ResolvedEval
from snowflake_semantic_tools.domain.model.tool import ToolKind, ToolMember, vocabulary_key
from snowflake_semantic_tools.domain.validate.relative_date import relative_date


def shared_questions(
    verified: Iterable[str], sampled: Iterable[str], evaluated: Iterable[str]
) -> tuple[Diagnostic, ...]:
    """Report each question asked word for word as a verified query, a sample question, and an eval row.

    Questions compare with surrounding whitespace stripped and are reported in sorted order.

    Diagnostics:
        SST-VAL417: one question text is in all three sets.
    """
    sets = [{text.strip() for text in texts if text.strip()} for texts in (verified, sampled, evaluated)]
    return tuple(D("SST-VAL417", value=text) for text in sorted(sets[0] & sets[1] & sets[2]))


def blocking(evaluation: ResolvedEval, defaults: EvalDefaults) -> bool:
    """Report whether an eval blocks: its `eval_tier` setting, else the `evals:` root's, is `blocking`."""
    tier = evaluation.options.eval_tier or defaults.eval_tier
    return (tier or "report").casefold() == "blocking"


def auto_model_problems(model: AgentModel | CoworkProfileModel, gated: bool) -> list[Diagnostic]:
    """Report an agent or CoWork profile, its settings applied, whose orchestration model is `auto`.

    One diagnostic at most: a gated subject is refused, whatever `allow_auto_model` says, and
    any other is warned unless `allow_auto_model` keeps `auto` deliberately.

    Args:
        gated: Whether a blocking eval scores the subject.

    Diagnostics:
        SST-VAL544: the model is `auto` and a blocking eval scores it, so a score change
            cannot be told from a model change.
        SST-VAL560: the model is `auto`, no blocking eval scores it, and `allow_auto_model` is
            not true.
    """
    if model.orchestration_model.casefold() != "auto" or (model.allow_auto_model and not gated):
        return []
    kind, block = ("agent", "agents") if isinstance(model, AgentModel) else ("cowork profile", "cowork_profiles")
    code = "SST-VAL544" if gated else "SST-VAL560"
    return [D(code, type=kind, artifact=model.name, block=block, subject=model.key, origin=model.origin)]


def agent_eval_problems(agent: AgentModel, evaluation: ResolvedEval) -> list[Diagnostic]:
    """Check one agent's sample questions against the eval written for it.

    Diagnostics:
        SST-VAL537: a sample question is relative-dated and word for word an eval question.
    """
    found: list[Diagnostic] = []
    rows = {row.question.strip() for row in evaluation.dataset.questions if row.question}
    for question in agent.sample_questions:
        if question.strip() in rows and relative_date(question) is not None:
            found.append(D("SST-VAL537", artifact=agent.name, value=question, subject=agent.key, origin=agent.origin))
    return found


def reachable_tool_problems(
    agent: AgentModel, dependencies: Sequence[str], tools: Mapping[str, ToolMember], gated: bool
) -> list[Diagnostic]:
    """Check the defined tool members an agent's tools publish after, with their defaults applied.

    Args:
        dependencies: The artifact keys the agent publishes after, in order.
        tools: Each compiled member, by its artifact key.
        gated: Whether a blocking eval scores the agent.

    Diagnostics:
        SST-VAL610: a procedure the agent reaches runs as its owner.
        SST-VAL611: a search service a gated agent reads pins no embedding model.
    """
    found: list[Diagnostic] = []
    for key in dependencies:
        member = tools.get(key)
        if member is None:
            continue
        if member.type == ToolKind.PROCEDURE.value and vocabulary_key(member.execute_as or "") == "owner":
            found.append(D("SST-VAL610", name=member.name, artifact=agent.name, subject=key, origin=member.origin))
        if gated and member.type == ToolKind.CORTEX_SEARCH_SERVICE.value and not member.embedding_model:
            found.append(D("SST-VAL611", name=member.name, subject=key, origin=member.origin))
    return found


def publish_order(member: ToolMember, source: str, later: Mapping[str, str]) -> Diagnostic | None:
    """Report a search service whose source relation an artifact publishing no earlier creates.

    Args:
        source: The relation the service indexes.
        later: The target of each artifact that publishes at the tools' position or after, by
            the target casefolded; its artifact key is the value.

    Diagnostics:
        SST-VAL614: the source relation is created by an artifact that publishes at or after the
            search service, so the service would be created over a relation that is not there.
    """
    if member.type != ToolKind.CORTEX_SEARCH_SERVICE.value or source.casefold() not in later:
        return None
    return D(
        "SST-VAL614",
        name=member.name,
        value=source,
        subject=artifact_key("tool", member.name.casefold()),
        origin=member.origin,
    )
