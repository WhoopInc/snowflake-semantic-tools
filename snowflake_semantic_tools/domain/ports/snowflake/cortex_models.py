"""The Cortex models an account lists, and whether an agent runs on one.

Snowflake keeps no list of which models Cortex Agents accept: `CREATE AGENT` takes any model
name, and a model agents refuse fails only when the agent runs. So the connected check of
`snowflake.orchestration_models` runs a throwaway agent on each model, and reads each model's
lifecycle from `SHOW CORTEX BASE MODELS`, which lists every model and marks none as one
agents accept. This port is apart from `SnowflakePort`, so the doubles of one need not
implement the other.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from snowflake_semantic_tools.domain.model.identifier import SchemaScope


@dataclass(frozen=True, slots=True)
class CortexBaseModel:
    """One `SHOW CORTEX BASE MODELS` row.

    Attributes:
        name: The model's name as Cortex spells it, such as `claude-sonnet-4-6`.
        lifecycle_status: `GA`, `PUPR`, `PRPR`, `LEGACY`, `EOL` or `INTERNAL`, as SHOW prints
            it; None when the row has none.
        legacy_date: When the model became, or becomes, legacy, as SHOW prints it; None for none.
        eol_date: When the model reaches end-of-life, as SHOW prints it; None for none.
    """

    name: str
    lifecycle_status: str | None = None
    legacy_date: str | None = None
    eol_date: str | None = None


class ProbeOutcome(Enum):
    """What running a throwaway agent on one model showed."""

    ACCEPTED = "accepted"
    REJECTED = "rejected"
    UNPROBED = "unprobed"


@dataclass(frozen=True, slots=True)
class AgentProbe:
    """What one throwaway agent run showed about its orchestration model.

    Attributes:
        outcome: ACCEPTED when the agent answered; REJECTED when Snowflake refused the model
            when the agent ran; UNPROBED when the agent could not be created or run at all,
            which says nothing about the model.
        detail: Snowflake's reason, the first line of its message, for REJECTED and UNPROBED;
            empty for ACCEPTED.
    """

    outcome: ProbeOutcome
    detail: str = ""


# SHOW's preview statuses, by what each is called in a message.
PREVIEW_STATUSES = {"PUPR": "public preview", "PRPR": "private preview"}


def lifecycle_concern(row: CortexBaseModel | None) -> str | None:
    """Say what about a model's `SHOW CORTEX BASE MODELS` row makes it a risk to depend on.

    Args:
        row: The model's row; None when SHOW does not list it.

    Returns:
        A phrase completing "model 'x' is ...": not listed, INTERNAL, past end-of-life,
        legacy, or in preview; None for a GA model, or a status SST does not know.
    """
    if row is None:
        return "not in SHOW CORTEX BASE MODELS"
    status = (row.lifecycle_status or "").upper()
    if status == "INTERNAL":
        return "marked INTERNAL by SHOW CORTEX BASE MODELS"
    if status == "EOL":
        return f"past end-of-life ({row.eol_date or 'no date listed'})"
    if status == "LEGACY":
        return f"legacy; end-of-life: {row.eol_date or 'no date announced'}"
    if status in PREVIEW_STATUSES:
        return f"in {PREVIEW_STATUSES[status]}"
    return None


class BaseModelsPort(Protocol):
    """Read the account's Cortex base models."""

    def base_models(self) -> tuple[CortexBaseModel, ...]:
        """Return every row `SHOW CORTEX BASE MODELS IN SCHEMA SNOWFLAKE.MODELS` prints.

        Never writes.

        Raises:
            SnowflakePortError: the SHOW failed.
        """
        ...


class CortexModelsPort(BaseModelsPort, Protocol):
    """Read the account's Cortex base models, and run an agent on one model."""

    def probe_orchestration_model(self, scope: SchemaScope, model: str) -> AgentProbe:
        """Run a throwaway agent orchestrated by `model` once, and report whether it answered.

        The agent is TEMPORARY, created in `scope` on this session alone, given no tools and
        a small budget, asked for one word, and dropped. A refusal to create or run it is
        reported as UNPROBED, never raised.

        Raises:
            SnowflakePortError: `model` is not a plain Cortex model name.
        """
        ...
