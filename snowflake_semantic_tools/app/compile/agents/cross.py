"""Run the checks that span artifact types once the views, tools, agents, and evals have compiled.

The checks are pure and live in `domain.validate.cross_artifact`, and for CoWork profiles (a
Snowflake private preview) in `domain.validate.cowork_profile`; this module reads what each
compiler produced and hands them the parts they compare.
"""

from __future__ import annotations

from collections.abc import Mapping

from snowflake_semantic_tools.app.compile import CompiledView
from snowflake_semantic_tools.app.compile.agents.compiled import CompiledAgent
from snowflake_semantic_tools.app.compile.base import CompiledArtifact, CompileResult
from snowflake_semantic_tools.app.compile.cowork_profiles.compiled import CompiledCoworkProfile
from snowflake_semantic_tools.app.compile.tools import CompiledTool
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.eval import EvalCatalog
from snowflake_semantic_tools.domain.validate.cowork_profile import schema_problems
from snowflake_semantic_tools.domain.validate.cross_artifact import (
    agent_eval_problems,
    auto_model_problems,
    blocking,
    publish_order,
    reachable_tool_problems,
    shared_questions,
)


def cross_artifact_result(
    results: tuple[CompileResult, ...], evals: EvalCatalog, positions: Mapping[str, int]
) -> CompileResult:
    """Check the compiled artifacts of `results` against one another, compiling nothing.

    Reports, in order: the questions all three question sets share; each agent's orchestration
    model, then the agent against its eval, then against the tools it reaches, in agent order;
    then each CoWork profile's orchestration model, in profile order; then each search service
    whose source an artifact publishing no earlier creates; then each agent publishing where
    CoWork lists profiles, and each CoWork profile sharing an agent's schema. A tool member
    reached by several agents is reported once.

    Args:
        evals: The eval catalog the eval compiler read.
        positions: Each artifact type's position in the publish order.

    Diagnostics:
        Those of `domain.validate.cross_artifact`.
        Those of `domain.validate.cowork_profile.schema_problems`.
    """
    compiled = tuple(item for result in results for item in result.compiled)
    agents = tuple(item for item in compiled if isinstance(item, CompiledAgent))
    tools = tuple(item for item in compiled if isinstance(item, CompiledTool))
    profiles = tuple(item for item in compiled if isinstance(item, CompiledCoworkProfile))
    diagnostics = [
        *_shared_questions(compiled, agents, evals),
        *_agent_problems(agents, tools, evals),
        *_profile_problems(profiles, evals),
        *_publish_order(compiled, tools, positions),
        *schema_problems(
            tuple((agent.resolved.model, agent.target) for agent in agents),
            tuple((profile.resolved.model, profile.target) for profile in profiles),
        ),
    ]
    unique: dict[tuple[str, str, str | None], Diagnostic] = {}
    for item in diagnostics:
        unique.setdefault((item.code, item.message, item.subject), item)
    return CompileResult((), DiagnosticBag(tuple(unique.values())))


def _shared_questions(
    compiled: tuple[CompiledArtifact, ...], agents: tuple[CompiledAgent, ...], evals: EvalCatalog
) -> tuple[Diagnostic, ...]:
    """Compare the verified queries' questions with the agents' sample questions and the eval rows."""
    views = tuple(item for item in compiled if isinstance(item, CompiledView))
    return shared_questions(
        (query.question for view in views for query in view.view.verified_queries),
        (question for agent in agents for question in agent.resolved.model.sample_questions),
        (row.question or "" for evaluation in evals.evals for row in evaluation.dataset.questions),
    )


def _agent_problems(
    agents: tuple[CompiledAgent, ...], tools: tuple[CompiledTool, ...], evals: EvalCatalog
) -> list[Diagnostic]:
    """Check each agent's model and its eval, then the defined tool members it reaches."""
    # A CoWork profile's eval checks no agent, even one sharing the profile's name.
    evaluations = {evaluation.name.casefold(): evaluation for evaluation in evals.evals if evaluation.agent is not None}
    members = {item.artifact_key: item.member for item in tools}
    found: list[Diagnostic] = []
    for agent in agents:
        model = agent.resolved.model
        evaluation = evaluations.get(model.name.casefold())
        gated = evaluation is not None and blocking(evaluation, evals.defaults)
        found.extend(auto_model_problems(model, gated))
        if evaluation is not None:
            found.extend(agent_eval_problems(model, evaluation))
        found.extend(reachable_tool_problems(model, agent.resolved.depends_on, members, gated))
    return found


def _profile_problems(profiles: tuple[CompiledCoworkProfile, ...], evals: EvalCatalog) -> list[Diagnostic]:
    """Check each CoWork profile's orchestration model against whether a blocking eval scores it."""
    # Keyed by the subject's artifact key, so an agent's eval never gates a profile of its name.
    evaluations = {evaluation.subject.key: evaluation for evaluation in evals.evals if evaluation.agent is None}
    found: list[Diagnostic] = []
    for profile in profiles:
        model = profile.resolved.model
        evaluation = evaluations.get(model.key)
        found.extend(auto_model_problems(model, evaluation is not None and blocking(evaluation, evals.defaults)))
    return found


def _publish_order(
    compiled: tuple[CompiledArtifact, ...], tools: tuple[CompiledTool, ...], positions: Mapping[str, int]
) -> list[Diagnostic]:
    """Check each search service's source against what publishes at the tools' position or later."""
    later = {
        item.rendered_artifact.target.sql.casefold(): item.artifact_key
        for item in compiled
        if positions.get(item.artifact_type, 0) >= positions.get("tool", 0)
    }
    return [
        found
        for tool in tools
        for _, relation in tool.dbt_relations
        if (found := publish_order(tool.member, relation, later)) is not None
    ]
