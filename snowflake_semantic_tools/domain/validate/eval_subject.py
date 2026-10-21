"""Check what only one kind of eval subject may write: a CoWork profile's routing, or an agent's.

CoWork profiles are a Snowflake private preview (PrPr) feature; a profile's eval follows the
preview's contract, as these pages describe it:
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/set-up-cowork-profile-routing

A profile's eval is scored on which subagent its router chose: each row names the subagents
it expects in `ground_truth_subagents`, and `metrics.routing` gates on `routing_accuracy`.
Snowflake's two tool metrics never see a router call, so they, and the tool calls a row
expects, are refused on a profile's eval. An agent routes nothing, so its eval may write
neither routing field. Where a profile's eval may publish is checked here too.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.cowork_profile import COWORK_PROFILE_SCHEMA, listed_by_cowork
from snowflake_semantic_tools.domain.model.eval.model import (
    ROUTER_BLIND_METRICS,
    EvalRoutingMetric,
    ResolvedEval,
)
from snowflake_semantic_tools.domain.model.eval.subject import subagent_reference
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.setting import ROUTE_PREFIX
from snowflake_semantic_tools.domain.validate.shared import Emitter

# The bounds every routing metric's score lies within.
_ROUTING_RANGE = (0.0, 1.0)


def subject_rules(resolved: ResolvedEval) -> tuple[Diagnostic, ...]:
    """Check the routing fields of one eval against the kind of subject it evaluates.

    Diagnostics:
        SST-VAL767: an agent's eval config names a routing metric.
        SST-VAL768: an agent's eval dataset row sets `ground_truth_subagents`.
        SST-VAL881: a profile's row expects a call that is not one of the profile's subagents.
        SST-VAL882: a profile's eval config enables tool selection or tool execution accuracy.
        SST-VAL887: a profile's eval config inherits one of those two, which is left out (info).
        SST-VAL883: a profile's eval dataset row sets `ground_truth_invocations`.
        SST-VAL884: no row of a profile's eval dataset sets `ground_truth_subagents`.
        SST-VAL885: no row of a profile's eval dataset expects the profile to decline (info).
        SST-VAL769: a routing metric's threshold has a bound outside [0, 1].
    """
    if resolved.subject.is_profile:
        return (*_profile_config(resolved), *_profile_dataset(resolved))
    return _agent_routing(resolved)


def cowork_listed_placement(resolved: ResolvedEval, target: QualifiedName) -> tuple[Diagnostic, ...]:
    """Report a CoWork profile's eval whose profile publishes where CoWork lists profiles.

    Such an eval does not compile for the target, so it creates nothing there: CoWork's schema
    holds the profiles CoWork lists, and eval tables, datasets and stages in it are unmeasured.
    The profile's rehearsal routes the same way.

    Diagnostics:
        SST-VAL886: the eval's subject is a profile published to SNOWFLAKE_INTELLIGENCE.PROFILES.
    """
    if not resolved.subject.is_profile or not listed_by_cowork(target):
        return ()
    return (
        D(
            "SST-VAL886",
            artifact=resolved.name,
            target=".".join(COWORK_PROFILE_SCHEMA).upper(),
            origin=resolved.config.origin,
            subject=resolved.key,
        ),
    )


def _agent_routing(resolved: ResolvedEval) -> tuple[Diagnostic, ...]:
    """Refuse the routing fields on an agent's eval: its routing metrics, then its rows'."""
    found: list[Diagnostic] = [
        D(
            "SST-VAL767",
            artifact=resolved.name,
            name=metric.name or "",
            origin=metric.origin,
            subject=resolved.key,
        )
        for metric in resolved.config.routing_metrics
    ]
    for index, row in enumerate(resolved.dataset.questions):
        truth = row.ground_truth
        if truth is not None and truth.subagents is not None:
            found.append(
                D(
                    "SST-VAL768",
                    artifact=resolved.dataset.source_file,
                    index=index,
                    origin=truth.origin,
                    subject=resolved.key,
                )
            )
    return tuple(found)


def _profile_config(resolved: ResolvedEval) -> tuple[Diagnostic, ...]:
    """Refuse the system metrics blind to a router, note those left out, then check routing bounds."""
    found: list[Diagnostic] = [
        D("SST-VAL882", artifact=resolved.name, name=metric.name, origin=metric.origin, subject=resolved.key)
        for metric in resolved.config.system_metrics
        if metric.name in ROUTER_BLIND_METRICS
    ]
    inherited = resolved.settings.get("metrics")
    written = _written_at(inherited.source) if inherited is not None else "evals.+metrics"
    found.extend(
        D(
            "SST-VAL887",
            artifact=resolved.name,
            name=metric.name,
            field=written,
            origin=metric.origin,
            subject=resolved.key,
        )
        for metric in resolved.config.inherited_blind_metrics
    )
    for metric in resolved.config.routing_metrics:
        found.extend(_routing_bounds(resolved, metric))
    return tuple(found)


def _written_at(source: str) -> str:
    """Name where an inherited setting was written: the block root's `+metrics`, or a route's."""
    if source.startswith(ROUTE_PREFIX):
        return f"evals.{source.removeprefix(ROUTE_PREFIX).replace('/', '.')}.+metrics"
    return "evals.+metrics"


def _routing_bounds(resolved: ResolvedEval, metric: EvalRoutingMetric) -> tuple[Diagnostic, ...]:
    """Report a routing threshold bound outside [0, 1], the range every routing score lies in."""
    threshold = metric.threshold
    if threshold is None:
        return ()
    low, high = _ROUTING_RANGE
    return tuple(
        D(
            "SST-VAL769",
            artifact=resolved.name,
            name=metric.name or "",
            key=key,
            found=bound,
            origin=metric.origin,
            subject=resolved.key,
        )
        for key, bound in (("min", threshold.min), ("max", threshold.max))
        if bound is not None and not low <= bound <= high
    )


def _profile_dataset(resolved: ResolvedEval) -> tuple[Diagnostic, ...]:
    """Check each row's routing expectations, then that the rows expect a route and a decline."""
    emit = Emitter(subject=resolved.key, artifact=resolved.dataset.source_file, value=resolved.name)
    subagents = resolved.subject.subagents
    known = {subagent_reference(subagent.reference, subagent.name) for subagent in subagents}
    listed = ", ".join(f"{subagent.reference}('{subagent.name}')" for subagent in subagents) or "none"
    routed = declined = False
    for index, row in enumerate(resolved.dataset.questions):
        truth = row.ground_truth
        if truth is None:
            continue
        if truth.invocations is not None:
            emit("SST-VAL883", index=index, origin=truth.origin)
        if truth.subagents is None:
            continue
        routed = True
        declined = declined or not truth.subagents
        for expected in truth.subagents:
            if expected.reference not in known:
                emit(
                    "SST-VAL881",
                    index=index,
                    function=expected.function,
                    name=expected.name,
                    detail=listed,
                    origin=expected.origin,
                )
    if not routed:
        emit("SST-VAL884", origin=resolved.dataset.origin)
    elif not declined:
        emit("SST-VAL885", origin=resolved.dataset.origin)
    return emit.diagnostics
