"""Describe each eval's subject as a comparison commits versions of it: as plan publishes it.

The subject is the eval's agent or CoWork profile from the whole compile, staged for publication
exactly as plan stages it -- on `apply.agent_spec_stage`, under the project's commit, its
versions `-dirty` when the tree has uncommitted changes -- so the precheck classifies what plan
would, and every version the comparison commits sits beside the subject's own on its stage.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.agents import CompiledAgent, for_publication
from snowflake_semantic_tools.app.compile.cowork_profiles import CompiledCoworkProfile, for_cowork_publication
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.compile.staged_agent import agent_spec_stage
from snowflake_semantic_tools.app.evals.compare_lifecycle import CompareSubject
from snowflake_semantic_tools.domain.model.identifier import TargetIdentity
from snowflake_semantic_tools.domain.state import Manifest


def compare_subjects(
    result: CompileResult,
    evals: Sequence[CompiledEval],
    manifest: Manifest,
    *,
    apply_config: Mapping[str, object],
    target: TargetIdentity,
    git_sha: str,
    dirty: bool = False,
) -> dict[str, CompareSubject]:
    """Return each eval's subject by eval key; an eval whose subject did not compile is left out.

    Args:
        result: The whole compile, which holds every agent and profile, whatever the selection.
        apply_config: The project's `apply:` block, which names the specification stage.
    """
    stage = agent_spec_stage(apply_config, target)
    compiled: dict[str, CompiledAgent | CompiledCoworkProfile] = {
        item.artifact_key: item for item in result.compiled if isinstance(item, (CompiledAgent, CompiledCoworkProfile))
    }
    subjects: dict[str, CompareSubject] = {}
    for item in evals:
        subject = compiled.get(item.resolved.evaluated.key)
        if isinstance(subject, CompiledAgent):
            agent = for_publication(subject, stage=stage, git_sha=git_sha, dirty=dirty)
            subjects[item.artifact_key] = CompareSubject(
                agent.artifact_key,
                "agent",
                agent.name,
                agent.target,
                agent.payload,
                agent.resolved.model.orchestration_model,
                stage,
                agent.name,
                agent.rendered_for_publish(manifest.manifest_id),
            )
        elif isinstance(subject, CompiledCoworkProfile):
            profile = for_cowork_publication(subject, stage=stage, git_sha=git_sha, dirty=dirty)
            subjects[item.artifact_key] = CompareSubject(
                profile.artifact_key,
                "cowork_profile",
                profile.name,
                profile.target,
                profile.payload,
                profile.resolved.model.orchestration_model,
                stage,
                profile.folder,
                profile.rendered_for_publish(manifest.manifest_id),
            )
    return subjects
