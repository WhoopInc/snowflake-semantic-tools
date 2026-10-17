"""`sst ask`: run an agent's compiled specification once, as a temporary agent, publishing nothing.

The specification is the one `sst compile --emit-agent-spec` writes, so an edit to the agent
itself -- its instructions, a tool's description, its tool list -- is what runs. Every object
it names is reached by its published name, so an edit to one of them is not tried: each such
dependency whose compiled fingerprint the target's state does not record is reported. Nothing
here takes a lock, and nothing writes state.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.agents import CompiledAgent
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.config_schema import setting_value, target_text
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope, TargetIdentity
from snowflake_semantic_tools.domain.ports.snowflake.agent_run import AgentReply, AgentRunPort
from snowflake_semantic_tools.domain.state import APPLIED, AppliedEntry

# How long one question may run when the command line says nothing: well past the 9 to 26
# seconds the measured runs took, and within what an agent's own budget usually allows.
DEFAULT_RUN_TIMEOUT_SECONDS = 300
_SCRATCH_KEY = "agents.+scratch_schema"


def compiled_agents(result: CompileResult) -> tuple[CompiledAgent, ...]:
    """Return the agents among the compiled artifacts, in compile order."""
    return tuple(item for item in result.compiled if isinstance(item, CompiledAgent))


def find_agent(result: CompileResult, name: str) -> CompiledAgent | Diagnostic:
    """Return the compiled agent `name` names, by its name or its `agent:<name>` key, casefolded.

    Diagnostics:
        SST-ASK003: no compiled agent has that name; the diagnostic lists those that compile.
    """
    agents = compiled_agents(result)
    wanted = name.casefold().removeprefix("agent:")
    found = next((agent for agent in agents if agent.name.casefold() == wanted), None)
    if found is not None:
        return found
    names = ", ".join(sorted(agent.name for agent in agents)) or "no agent"
    return D("SST-ASK003", name=name, detail=names)


def agent_scope(agent: CompiledAgent, target: TargetIdentity) -> SchemaScope | Diagnostic:
    """Return where the agent's temporary agent is created: `agents.+scratch_schema`, else its own schema.

    The setting's `{{ target.* }}` templates are rendered for `target`. A value the
    configuration check refused never reaches here; one only a template made unusable does.

    Diagnostics:
        SST-CFG008: the setting renders to no `DATABASE.SCHEMA`.
    """
    written = setting_value(agent.resolved.model.settings, "scratch_schema")
    rendered = target_text(written, target, None) if written is not None else None
    if not rendered:
        return SchemaScope.from_qualified_name(agent.target)
    try:
        return SchemaScope.parse(rendered)
    except ValueError:
        return D(
            "SST-CFG008",
            subject=agent.artifact_key,
            key=_SCRATCH_KEY,
            found=repr(rendered),
            expected="two-part names, DATABASE.SCHEMA",
        )


def stale_dependencies(
    agent: CompiledAgent, compiled: CompileResult, applied: Mapping[str, AppliedEntry] | None
) -> tuple[Diagnostic, ...]:
    """Warn of each project artifact the agent calls whose edit an unpublished run does not see.

    Each artifact the agent depends on, in dependency order, is compared with the target's
    state: an entry that is absent, not applied, at another fingerprint, or for another
    object means the run calls what is published, not what compiles. An external object the
    agent names, which SST does not publish, is not checked.

    Args:
        compiled: The whole compile, which holds every artifact the agent depends on.
        applied: The target's state entries, by artifact key; None when they could not be read.

    Diagnostics:
        SST-ASK001: once per such dependency, naming why.
    """
    artifacts = {item.artifact_key: item for item in compiled.compiled}
    found: list[Diagnostic] = []
    for key in agent.resolved.depends_on:
        artifact = artifacts.get(key)
        if artifact is None:
            continue
        rendered = artifact.rendered_artifact
        reason = _unpublished_reason(applied, key, rendered.fingerprint, rendered.target)
        if reason is not None:
            found.append(
                D(
                    "SST-ASK001",
                    subject=agent.artifact_key,
                    artifact=agent.artifact_key,
                    value=f"{key} ({rendered.target.sql})",
                    detail=reason,
                )
            )
    return tuple(found)


def _unpublished_reason(
    applied: Mapping[str, AppliedEntry] | None, key: str, fingerprint: str, target: QualifiedName
) -> str | None:
    """Say why the target may not hold `key` as it compiles now; None when state records it so."""
    if applied is None:
        return "the target's state could not be read, so whether it is published as compiled is unknown"
    entry = applied.get(key)
    if entry is None or entry.outcome != APPLIED:
        return "it is not published to this target"
    if entry.fingerprint != fingerprint or QualifiedName.parse(entry.qualified_name).folded != target.folded:
        return "what is published differs from what compiles now"
    return None


@dataclass(frozen=True, slots=True)
class AskOutcome:
    """What one question to an unpublished agent got back.

    Attributes:
        scope: Where the temporary agent was created.
        diagnostics: The notes the caller passed, then SST-ASK002 when the agent answered with
            an error.
    """

    agent: CompiledAgent
    scope: SchemaScope
    question: str
    reply: AgentReply
    diagnostics: DiagnosticBag

    @property
    def answered(self) -> bool:
        """Report whether the agent replied rather than answering with an error."""
        return self.reply.error is None


class AskAgent:
    """Ask a compiled agent one question through a temporary agent; nothing is published or recorded."""

    def __init__(self, port: AgentRunPort) -> None:
        self._port = port

    def run(
        self,
        agent: CompiledAgent,
        question: str,
        *,
        scope: SchemaScope,
        timeout: int = DEFAULT_RUN_TIMEOUT_SECONDS,
        notes: tuple[Diagnostic, ...] = (),
    ) -> AskOutcome:
        """Run the agent's specification as a temporary agent in `scope`, and report its reply.

        Args:
            notes: What the caller found before asking, such as `stale_dependencies`' warnings;
                reported first.

        Raises:
            SnowflakePortError: the temporary agent could not be created, or the run failed or
                timed out.
            ValueError: the specification holds `$$`.

        Diagnostics:
            SST-ASK002: the agent answered with an error message.
        """
        reply = self._port.run_inline(scope, agent.name, agent.payload, question, timeout)
        failed = (
            ()
            if reply.error is None
            else (D("SST-ASK002", subject=agent.artifact_key, artifact=agent.artifact_key, detail=reply.error),)
        )
        return AskOutcome(agent, scope, question, reply, DiagnosticBag((*notes, *failed)))


def reply_json(reply: AgentReply) -> dict[str, object]:
    """Project a reply for the CLI: the answer, each tool call and result, the error, and the time taken."""
    return {
        "answer": reply.answer,
        "tool_uses": [{"name": use.name, "type": use.type, "input": dict(use.input)} for use in reply.tool_uses],
        "tool_results": [{"name": result.name, "status": result.status} for result in reply.tool_results],
        "error": reply.error,
        "duration_ms": reply.duration_ms,
    }
