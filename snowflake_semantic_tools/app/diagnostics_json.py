"""Diagnostics as JSON: one diagnostic as `--output json` lists it, and the summary that counts them.

The CLI's envelope is built from these, and so is anything else that reports diagnostics as data,
so every caller reports one diagnostic the same way. Pure: what a run's baseline holds is passed in.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Diagnostic
from snowflake_semantic_tools.domain.diagnostics.baseline import stable_fingerprint
from snowflake_semantic_tools.domain.model.artifact_key import split_artifact_key
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY

# A subject whose type is one of these names an artifact; any other `<type>:<name>` a member of one.
ARTIFACT_SUBJECTS = frozenset(SEMANTIC_REGISTRY.artifacts)


def diagnostic_json(value: Diagnostic, *, baselined: bool = False) -> dict[str, object]:
    """Return one diagnostic as the envelope lists it, with its registered severity and suggestion.

    A subject that names an artifact type, or `profile`, is reported as `artifact`; any
    other `<type>:<name>` subject is a `member` of one. A code that declares `internal_detail`
    reports its `detail` value, the same text its message shows, on its own as
    `internal_detail`; every other code reports None.
    """
    origin = getattr(value, "origin", None)
    spec = ERROR_REGISTRY[value.code]
    subject_parts = split_artifact_key(value.subject) if value.subject and ":" in value.subject else None
    return {
        "code": value.code,
        "severity": value.severity.name.lower(),
        "declared_severity": spec.severity.name.lower(),
        "promoted_from": value.promoted_from.name.lower() if value.promoted_from is not None else None,
        "demoted_from": value.demoted_from.name.lower() if value.demoted_from is not None else None,
        "baselined": baselined,
        "message": value.message,
        "suggestion": value.suggestion,
        "phase": value.phase,
        "help_url": value.help_url,
        "fingerprint": stable_fingerprint(value),
        "params": dict(value.context),
        "artifact": (
            {"type": subject_parts[0], "name": subject_parts[1]}
            if subject_parts and subject_parts[0] in ARTIFACT_SUBJECTS
            else None
        ),
        "member": (
            {"type": subject_parts[0], "name": subject_parts[1]}
            if subject_parts and subject_parts[0] not in ARTIFACT_SUBJECTS
            else None
        ),
        "location": (
            {
                "file": origin.file,
                "line": origin.line,
                "column": origin.col,
                "end_line": None,
                "end_column": None,
            }
            if origin is not None
            else None
        ),
        "related": [{"file": item.file, "line": item.line, "column": item.col} for item in value.related],
        "internal_detail": value.context.get("detail") if spec.internal_detail else None,
        "caused_by": value.caused_by,
    }


def summary_counts(diagnostics: Iterable[Diagnostic], baselined: frozenset[str] = frozenset()) -> dict[str, int]:
    """Count the diagnostics by severity, and those the baseline holds apart from every severity.

    A baselined diagnostic is counted once, under `baselined`, so a run whose every error the
    baseline holds -- a promoted warning under `--strict` -- reports no errors, as its exit says.

    Args:
        baselined: The stable fingerprints the run's baseline holds.
    """
    counts: Counter[str] = Counter()
    for item in diagnostics:
        counts["baselined" if stable_fingerprint(item) in baselined else item.severity.name.lower()] += 1
    return {key: counts[key] for key in ("error", "warning", "info", "baselined")}
