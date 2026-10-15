"""A scripted `CortexModelsPort`, and a compile holding one agent, for the orchestration-model check."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.agents import CompiledAgent
from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.app.validate import ValidateArtifacts
from snowflake_semantic_tools.app.validate_models import ModelCheck
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Origin
from snowflake_semantic_tools.domain.model.agent import AgentModel, ResolvedAgent
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.ports.snowflake.cortex_models import (
    AgentProbe,
    CortexBaseModel,
    ProbeOutcome,
)
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.snowflake_fake import FakeSnowflake

ACCEPTED = AgentProbe(ProbeOutcome.ACCEPTED)


class FakeCortexModels:
    """List scripted base models, and answer each probe from `probes`; an unscripted model is accepted.

    Attributes:
        probed: Every probe asked, as (scope, model), in order.
    """

    def __init__(
        self,
        listed: Sequence[CortexBaseModel] = (),
        probes: Mapping[str, AgentProbe | SnowflakePortError] | None = None,
        *,
        show_error: SnowflakePortError | None = None,
    ) -> None:
        self._listed = tuple(listed)
        self._probes = dict(probes or {})
        self._show_error = show_error
        self.probed: list[tuple[SchemaScope, str]] = []
        self.shown = 0

    def base_models(self) -> tuple[CortexBaseModel, ...]:
        self.shown += 1
        if self._show_error is not None:
            raise self._show_error
        return self._listed

    def probe_orchestration_model(self, scope: SchemaScope, model: str) -> AgentProbe:
        self.probed.append((scope, model))
        answer = self._probes.get(model, ACCEPTED)
        if isinstance(answer, SnowflakePortError):
            raise answer
        return answer


def ga(name: str) -> CortexBaseModel:
    """A generally available base model."""
    return CortexBaseModel(name, "GA")


def agent_compile(*names: str) -> CompileResult:
    """A compile publishing one agent per name, each as `DB.<NAME>_S.<NAME>`."""
    return CompileResult(
        tuple(
            CompiledAgent(
                ResolvedAgent(AgentModel(name, Origin(f"{name}.yml"), (f"{name}.yml",)), ()),
                QualifiedName.from_parts("db", f"{name}_s", name),
                "{}\n",
                "a" * 64,
            )
            for name in names
        )
    )


def checked(models: Sequence[str], port: FakeCortexModels, compiled: CompileResult | None = None) -> list[Diagnostic]:
    """Run connected validation with the orchestration-model check alone, and return what it found."""
    check = ModelCheck(tuple(models), Fanout(port))
    result = ValidateArtifacts(FakeSnowflake(), target="prod", models=check).run(
        compiled if compiled is not None else agent_compile("sales"), strict=False, connected=True
    )
    return [item for item in result.diagnostics if item.code in MODEL_CODES]


# The codes the orchestration-model check raises.
MODEL_CODES = frozenset(f"SST-VAL55{digit}" for digit in range(1, 7))
