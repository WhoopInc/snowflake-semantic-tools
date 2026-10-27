"""The fake's `CortexModelsPort`: a model list in which every probe agent answers.

By default the account lists every orchestration model the reference fixture allows as GA,
so a connected run over a fixture project finds nothing to report about its models; a test
of the check itself sets `listed_models` or `refused_models`.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from snowflake_semantic_tools.domain.ports.snowflake.cortex_models import (
    AgentProbe,
    CortexBaseModel,
    ProbeOutcome,
)
from tests.helpers.snowflake_fake.world import SnowflakeWorld

# The reference fixture's `snowflake.orchestration_models`, `auto` aside.
FIXTURE_MODELS = (
    *("claude-haiku-4-5", "claude-opus-4-5", "claude-opus-4-6", "claude-opus-4-7", "claude-opus-4-8"),
    *("claude-opus-5", "claude-opus-5-5", "claude-sonnet-4-5", "claude-sonnet-4-6", "claude-sonnet-5"),
    *("gemini-3.1-pro", "gemini-3.5-flash", "gemini-3.7-flash", "gemini-3.8-flash", "grok-4.6"),
    *("openai-1p-gpt-5.4", "openai-gpt-4.1", "openai-gpt-5", "openai-gpt-5-mini", "openai-gpt-5.1"),
    *("openai-gpt-5.2", "openai-gpt-5.4", "openai-gpt-5.5"),
)


class FakeCortexModels(SnowflakeWorld):
    """The model reads: `listed_models` as SHOW rows, and every probe accepted but `refused_models`'.

    Attributes:
        listed_models: The SHOW rows; None lists `FIXTURE_MODELS`, each GA.
        refused_models: The detail each probe of a model is refused with, by model name.
        probed_models: Every probe asked, as (scope, model), in order.
    """

    listed_models: tuple[CortexBaseModel, ...] | None = None
    refused_models: dict[str, str] | None = None

    @property
    def probed_models(self) -> list[tuple[SchemaScope, str]]:
        """Every probe asked so far, as (scope, model), in order."""
        probed: list[tuple[SchemaScope, str]] = self.__dict__.setdefault("_probed_models", [])
        return probed

    def base_models(self) -> tuple[CortexBaseModel, ...]:
        self._check("base_models")
        if self.listed_models is not None:
            return self.listed_models
        return tuple(CortexBaseModel(model, "GA") for model in FIXTURE_MODELS)

    def probe_orchestration_model(self, scope: SchemaScope, model: str) -> AgentProbe:
        self._check("probe_orchestration_model")
        self.probed_models.append((scope, model))
        refused = (self.refused_models or {}).get(model)
        return AgentProbe(ProbeOutcome.REJECTED, refused) if refused is not None else AgentProbe(ProbeOutcome.ACCEPTED)
