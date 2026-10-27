"""What an eval evaluates: one agent, or one Snowflake CoWork profile.

CoWork profiles are a Snowflake private preview (PrPr) feature; a profile's eval follows the
preview's contract, as these pages describe it:
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/set-up-cowork-profile-routing

`EvalSubject` is the one view every eval rule reads its subject through: its kind, name and
keys, its folder and `evals:` block, its sample questions, and, for a profile, the subagents
its router may choose. An agent's eval keeps the key `eval:<agent>`, which baselines and state
recorded before profiles had evals use; a profile's is `eval:cowork_profile:<profile>`, so an
agent and a profile of one name never share one. `SubjectTarget` is where a subject publishes
on one target, with the qualified names a profile's subagents resolve to there.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.agent import AgentEvalFiles, AgentModel
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.cowork_profile import (
    ROUTER_NAME,
    CoworkProfileModel,
    CoworkSubagent,
    ResolvedCoworkProfile,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName

EVAL_SUBJECT_AGENT = "agent"
# Snowflake private preview (PrPr): CoWork profiles.
EVAL_SUBJECT_COWORK_PROFILE = "cowork_profile"


def subagent_reference(function: str, name: str) -> str:
    """Identify one `{{ agent() }}` or `{{ tool() }}` call: `<function>:<casefolded name>`.

    A profile's subagent and a dataset row's expected subagent are the same call when their
    references are equal.
    """
    return f"{function}:{name.casefold()}"


@dataclass(frozen=True, slots=True)
class EvalSubject:
    """The agent or CoWork profile an eval evaluates, read the same way for either.

    Attributes:
        model: The agent, or the profile, as loading resolved it.
    """

    model: AgentModel | CoworkProfileModel

    @property
    def kind(self) -> str:
        """`agent`, or `cowork_profile` for a CoWork profile."""
        return EVAL_SUBJECT_AGENT if isinstance(self.model, AgentModel) else EVAL_SUBJECT_COWORK_PROFILE

    @property
    def is_profile(self) -> bool:
        """Report whether the subject is a CoWork profile."""
        return isinstance(self.model, CoworkProfileModel)

    @property
    def name(self) -> str:
        """The subject's name as written."""
        return self.model.name

    @property
    def key(self) -> str:
        """The subject's own artifact key, `agent:<name>` or `cowork_profile:<name>`."""
        return self.model.key

    @property
    def eval_key(self) -> str:
        """The key of the subject's eval: `eval:<agent>`, or `eval:cowork_profile:<profile>`.

        Both names are casefolded. An agent's is unchanged from before profiles had evals, so
        the baselines and state recorded under it still match.
        """
        folded = self.model.name.casefold()
        if self.is_profile:
            return artifact_key("eval", artifact_key(EVAL_SUBJECT_COWORK_PROFILE, folded))
        return artifact_key("eval", folded)

    @property
    def origin(self) -> Origin:
        """Where the subject's file starts."""
        return self.model.origin

    @property
    def folder(self) -> tuple[str, ...]:
        """The subject file's directory below its kind's directory; `evals:` routes fold along it."""
        return self.model.folder

    @property
    def evals(self) -> AgentEvalFiles | None:
        """The subject's `evals:` block; None when it declares none."""
        return self.model.evals

    @property
    def sample_questions(self) -> tuple[str, ...]:
        """The subject's own sample questions, which an eval question should not repeat."""
        return self.model.sample_questions

    @property
    def tool_names(self) -> tuple[str, ...] | None:
        """A profile's trace-facing tool names: its router, then each tool by its name or type.

        None for an agent, whose tool names only compiling it resolves.
        """
        if not isinstance(self.model, CoworkProfileModel):
            return None
        return (ROUTER_NAME, *(tool.name or tool.type for tool in self.model.tools))

    @property
    def subagents(self) -> tuple[CoworkSubagent, ...]:
        """A profile's subagents as authored, in order; none for an agent."""
        return self.model.subagents if isinstance(self.model, CoworkProfileModel) else ()

    @property
    def agent(self) -> AgentModel | None:
        """The agent, when the subject is one."""
        return self.model if isinstance(self.model, AgentModel) else None


@dataclass(frozen=True, slots=True)
class SubjectTarget:
    """Where an eval's subject publishes on one target, and what a profile routes to there.

    Attributes:
        target: The AGENT object the subject publishes as.
        subagents: For a CoWork profile, each subagent's qualified name by its
            `subagent_reference`, as the profile's router names it on this target; empty for an
            agent.
    """

    target: QualifiedName
    subagents: Mapping[str, QualifiedName] = field(default_factory=lambda: MappingProxyType({}))


def profile_subject_target(target: QualifiedName, resolved: ResolvedCoworkProfile) -> SubjectTarget:
    """Return a compiled profile's target with its subagents' qualified names on that target.

    A subagent through `agent('<name>')` is found by the agent it depends on, and one through
    `tool('<name>')` by the tool member it resolved through; a subagent that did not resolve is
    absent.
    """
    names: dict[str, QualifiedName] = {}
    for subagent in resolved.subagents:
        if subagent.depends_on is not None:
            names[subagent_reference("agent", subagent.depends_on.partition(":")[2])] = subagent.fqn
        elif subagent.member is not None:
            names[subagent_reference("tool", subagent.member)] = subagent.fqn
    return SubjectTarget(target, MappingProxyType(names))
