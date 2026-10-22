"""Decide whether a comparison may start on an eval's agent or CoWork profile: is it published as compiled?

A comparison commits versions beside the release and restores the live version from the
released specification, so it must start from exactly what `sst apply` left: the plan of the
subject is NOOP, a live version is open, the alias is on the version holding the specification,
and the default is pinned by name to that version. The subject is observed and classified as
plan observes and classifies it, so a comparison never starts on what a plan would change.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.evals.compare_lifecycle import CompareSubject
from snowflake_semantic_tools.app.observe import ObserveOptions, observe
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.lifecycle import Action
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.plan import classify
from snowflake_semantic_tools.domain.ports.snowflake.catalog import CatalogPort
from snowflake_semantic_tools.domain.state import APPLIED, AppliedEntry

# What the precheck reads beyond each listing: no grants, no live definitions to diff.
_READS = ObserveOptions(grants=False, diff=False)


def published_release(
    port: CatalogPort, subject: CompareSubject, recorded: AppliedEntry | None, *, fetched_at: str
) -> str | Diagnostic:
    """Return the release a subject serves, once it is published exactly as the manifest renders it.

    Args:
        recorded: What state records for the subject; None when nothing does.
        fetched_at: When the observation is taken, as the clock reports it.

    Returns:
        The release's system name, such as `VERSION$4`, which the comparison runs the pinned
        model on; else why the comparison may not start.

    Diagnostics:
        SST-APL108: state records no apply of the subject; reading it failed; plan would change
            it; or its default is not read as pinned by name to the release.
    """
    if recorded is None or recorded.outcome != APPLIED:
        return _refused(subject, "state records no apply of it on this target")
    artifact = subject.rendered
    observation, read = observe(
        port,
        SEMANTIC_REGISTRY,
        (subject.target,),
        fetched_at=fetched_at,
        artifact_types=frozenset({artifact.artifact_type}),
        desired_artifacts={artifact.key: artifact},
        options=_READS,
        kind_targets={artifact.artifact_type: (subject.target,)},
    )
    if read.has_errors:
        return _refused(subject, f"reading it failed: {_first_message(read)}")
    observed = observation.artifacts.get(artifact.key)
    decision = classify(
        artifact.key,
        artifact,
        SEMANTIC_REGISTRY,
        observed=observed,
        recorded=recorded,
        validation=DiagnosticBag(),
        composite=None,
    )
    change = decision.change
    if change.action is not Action.NOOP or observed is None or observed.newest_version is None:
        return _refused(subject, f"`sst plan` shows it as {change.action.value} ({change.reason.value})")
    release = observed.newest_version
    if observed.default_pinned is not True or (observed.default_version or "").casefold() != release.casefold():
        setting = {None: "was not read", False: "follows a shortcut such as LAST"}.get(observed.default_pinned, "")
        detail = f"its default {setting}" if setting else f"its default is {observed.default_version}, not {release}"
        return _refused(subject, f"{detail}, where only a default pinned by name to {release} holds still")
    return release


def _refused(subject: CompareSubject, detail: str) -> Diagnostic:
    return D("SST-APL108", artifact=subject.key, detail=detail, subject=subject.key)


def _first_message(found: DiagnosticBag) -> str:
    return next((item.message for item in found if item.blocks), "")
