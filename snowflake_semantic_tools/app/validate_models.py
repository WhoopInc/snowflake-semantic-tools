"""The connected check of `snowflake.orchestration_models`: does each model work for agents here.

Snowflake keeps no list of the models Cortex Agents accept, so each model the project allows,
`auto` aside, is run once in a throwaway agent, as the session's role, in the schema of the
first agent the project publishes. `SHOW CORTEX BASE MODELS` is read once beside that for each
model's lifecycle: it lists every model, but marks none as one agents accept, so it cannot
stand in for the run. The runs are independent, so a `Fanout` runs them concurrently, and the
findings come back in model order whatever the thread count.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.agents.compiled import CompiledAgent
from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from snowflake_semantic_tools.domain.ports.snowflake.cortex_models import (
    AgentProbe,
    CortexBaseModel,
    CortexModelsPort,
    ProbeOutcome,
)
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError

# What every diagnostic here is about: the config list it checks.
SUBJECT = "config:snowflake.orchestration_models"
# The value Snowflake resolves to a model itself, which no probe can check.
AUTO = "auto"
# SHOW's lifecycle statuses, and what each means for an agent naming the model.
_PREVIEW = {"PUPR": "public preview", "PRPR": "private preview"}
_NOT_OFFERED = "INTERNAL"


@dataclass(frozen=True, slots=True)
class ModelCheck:
    """What the orchestration-model check needs beyond the compile.

    Attributes:
        models: `snowflake.orchestration_models` as configured, `auto` included.
        probes: Sessions to run the probe agents on, concurrently when it has a pool.
    """

    models: tuple[str, ...]
    probes: Fanout[CortexModelsPort]


def configured_models(config: Mapping[str, object]) -> tuple[str, ...]:
    """Return `snowflake.orchestration_models` from the config tree; unset, it holds only `auto`."""
    snowflake = config.get("snowflake")
    raw = snowflake.get("orchestration_models") if isinstance(snowflake, Mapping) else None
    return tuple(str(item) for item in raw) if isinstance(raw, list) else (AUTO,)


def probe_scope(compiled: CompileResult) -> SchemaScope | None:
    """Return the schema the probe agents are created in: the first published agent's, by key.

    The role that publishes agents there may create one, so it may create the probes too.
    None when the compile publishes no agent.
    """
    agents = sorted(
        (item for item in compiled.compiled if isinstance(item, CompiledAgent)), key=lambda item: item.artifact_key
    )
    return SchemaScope.from_qualified_name(agents[0].target) if agents else None


def check_orchestration_models(check: ModelCheck, compiled: CompileResult, *, target: str) -> list[Diagnostic]:
    """Run each allowed model in a throwaway agent, and report what fails or is at risk, by model.

    Each distinct model, compared ignoring case, is checked once; `auto` is never checked.
    A model whose run is refused is not also reported as one Snowflake does not list.

    Args:
        target: The `profiles.yml` target being validated, which a refused model names.

    Diagnostics:
        SST-VAL551: an agent run refused the model.
        SST-VAL552: Snowflake lists the model as INTERNAL, or does not list it at all.
        SST-VAL553: the model is past end-of-life.
        SST-VAL554: the model is legacy.
        SST-VAL555: the model is in preview.
        SST-VAL556: the model could not be fully checked: no agent schema, a probe or the
            SHOW refused.
    """
    firsts: dict[str, str] = {}
    for model in check.models:
        if model.casefold() != AUTO:
            firsts.setdefault(model.casefold(), model)
    distinct = tuple(firsts.values())
    if not distinct:
        return []
    listed, unread = _listed(check.probes.port)
    scope = probe_scope(compiled)
    probes: Sequence[AgentProbe | None]
    if scope is None:
        probes = (None,) * len(distinct)
    else:
        probes = check.probes.map(lambda port, model: _probe(port, scope, model), distinct)
    found: list[Diagnostic] = []
    for model, probe in zip(distinct, probes, strict=True):
        found.extend(_findings(model, probe, listed, unread, target=target))
    return found


def _listed(port: CortexModelsPort) -> tuple[Mapping[str, CortexBaseModel], str | None]:
    """Return SHOW's rows by folded model name, else no rows and why SHOW failed."""
    try:
        rows = port.base_models()
    except SnowflakePortError as exc:
        return {}, f"the list of Cortex base models could not be read: {exc}"
    return {row.name.casefold(): row for row in rows}, None


def _probe(port: CortexModelsPort, scope: SchemaScope, model: str) -> AgentProbe:
    """Run one model's probe agent, reporting a model name the port refuses as unprobed."""
    try:
        return port.probe_orchestration_model(scope, model)
    except SnowflakePortError as exc:
        return AgentProbe(ProbeOutcome.UNPROBED, str(exc))


def _findings(
    model: str,
    probe: AgentProbe | None,
    listed: Mapping[str, CortexBaseModel],
    unread: str | None,
    *,
    target: str,
) -> list[Diagnostic]:
    """Report one model: its agent run, then its lifecycle as SHOW lists it."""
    found: list[Diagnostic] = []
    rejected = probe is not None and probe.outcome is ProbeOutcome.REJECTED
    if probe is None:
        reason = "the project publishes no agent, so there is no schema to run one in"
        found.append(D("SST-VAL556", model=model, reason=reason, subject=SUBJECT))
    elif rejected:
        found.append(D("SST-VAL551", model=model, target=target, detail=probe.detail, subject=SUBJECT))
    elif probe.outcome is ProbeOutcome.UNPROBED:
        reason = f"the probe agent could not run: {probe.detail}"
        found.append(D("SST-VAL556", model=model, reason=reason, subject=SUBJECT))
    if unread is not None:
        found.append(D("SST-VAL556", model=model, reason=unread, subject=SUBJECT))
        return found
    row = listed.get(model.casefold())
    status = (row.lifecycle_status or "").upper() if row is not None else ""
    if row is None or status == _NOT_OFFERED:
        if not rejected:
            reason = (
                "it is not in SHOW CORTEX BASE MODELS" if row is None else "SHOW CORTEX BASE MODELS marks it INTERNAL"
            )
            found.append(D("SST-VAL552", model=model, reason=reason, subject=SUBJECT))
    elif status == "EOL":
        found.append(D("SST-VAL553", model=model, date=row.eol_date or "no date listed", subject=SUBJECT))
    elif status == "LEGACY":
        found.append(D("SST-VAL554", model=model, date=row.eol_date or "no date announced", subject=SUBJECT))
    elif status in _PREVIEW:
        found.append(D("SST-VAL555", model=model, kind=_PREVIEW[status], subject=SUBJECT))
    return found
