"""How apply names a failure: the classified error, the failed or skipped outcome, and its diagnostic.

`classify_error` turns what Snowflake reported into the `ClassifiedError` an outcome carries;
the composite lifecycle handlers classify their own failures with it. The outcome builders keep
every refusal shaped alike, and `_outcome_diagnostic` reports a failed outcome under the code
its error names.
"""

from __future__ import annotations

from ...domain.model.diagnostic import D, Diagnostic
from ...domain.model.lifecycle import (
    ApplyOutcome,
    Change,
    ClassifiedError,
    ErrorKind,
    ExecResult,
    GrantCheck,
    OutcomeStatus,
)


def classify_error(message: str, *, sqlstate: str | None = None) -> ClassifiedError:
    """Classify a Snowflake failure by its SQLSTATE, else by the words of its message.

    The classes are tried in a fixed order and the first that matches wins: privilege, missing
    object, name conflict, syntax, transient fault. Only a transient fault is retryable.

    Returns:
        SST-SNO004 for a privilege failure, SST-SNO003 for a missing object, SST-SNO002 for a
        name conflict, SST-SNO009 for a syntax error, SST-SNO022 for a transient fault, and
        SST-SNO001 for anything else.
    """
    state = sqlstate or ""
    upper = message.upper()
    if state.startswith("28") or "INSUFFICIENT PRIVILEGE" in upper or "NOT AUTHORIZED" in upper:
        return ClassifiedError("SST-SNO004", message, ErrorKind.PRIVILEGE, False, sqlstate)
    if state in {"02000", "42S02"} or "DOES NOT EXIST" in upper:
        return ClassifiedError("SST-SNO003", message, ErrorKind.NOT_FOUND, False, sqlstate)
    # Checked before the syntax class, which shares the 42 prefix: a name conflict is not a typo.
    if state == "42710" or "ALREADY EXISTS" in upper:
        return ClassifiedError("SST-SNO002", message, ErrorKind.UNKNOWN, False, sqlstate)
    if state.startswith("42") or "SYNTAX ERROR" in upper:
        return ClassifiedError("SST-SNO009", message, ErrorKind.SYNTAX, False, sqlstate)
    if state.startswith("08") or state in {"57014", "57P01"} or "TIMEOUT" in upper:
        return ClassifiedError("SST-SNO022", message, ErrorKind.TRANSIENT, True, sqlstate)
    return ClassifiedError("SST-SNO001", message, ErrorKind.UNKNOWN, False, sqlstate)


def _exception_error(exc: BaseException) -> ClassifiedError:
    """Classify an exception by its message and, when it carries one, its SQLSTATE."""
    return classify_error(str(exc), sqlstate=getattr(exc, "sqlstate", None))


def _script_error(result: ExecResult, fallback: str) -> ClassifiedError:
    """Classify a failed script by the error it reported; `fallback` is the message when it gave none."""
    message = result.error.message if result.error else fallback
    return classify_error(message, sqlstate=result.error.sqlstate if result.error else None)


def _rendered_ddl(change: Change) -> str:
    """Return the change's rendered text; empty when nothing is rendered, as for a prune."""
    return change.rendered.ddl if change.rendered else ""


def _failed(
    change: Change,
    error: ClassifiedError,
    ddl: str,
    *,
    duration_ms: int = 0,
    grants: GrantCheck = GrantCheck.NOT_APPLICABLE,
) -> ApplyOutcome:
    """Report a change that failed before any of its statements ran, so nothing was written."""
    return ApplyOutcome(change.key, change.action, OutcomeStatus.FAILED, 0, duration_ms, ddl, error, grants)


def _skipped(change: Change, ddl: str) -> ApplyOutcome:
    """Report a change apply did not run."""
    return ApplyOutcome(change.key, change.action, OutcomeStatus.SKIPPED, 0, 0, ddl)


def _outcome_diagnostic(change: Change, outcome: ApplyOutcome) -> Diagnostic:
    """Report a failed outcome as the diagnostic its error code stands for.

    A code without a diagnostic of its own, such as a classified Snowflake failure, reports as
    SST-APL001 naming the action and the error.

    Diagnostics:
        SST-APL001: any other failure, with the action and the error message.
        SST-APL004: a replace omits COPY GRANTS.
        SST-APL008: the grants could not be read before the replace.
        SST-APL009: an explicit grant present before the replace is absent after it.
        SST-APL012: the object changed since the plan, or the write left no ownership marker.
        SST-APL016: a partial write left a composite artifact unusable.
        SST-APL022: a dataset could not be published.
        SST-APL028: the plan's own diagnostic for an eval config stage with the wrong file format.
        SST-INT902: an SST-APL028 failure the plan never recorded.
    """
    assert outcome.error is not None
    code = outcome.error.code
    if code in ("SST-APL004", "SST-APL008"):
        return D(code, artifact=change.key)
    if code == "SST-APL009":
        return D(code, artifact=change.key, value=outcome.error.message)
    if code == "SST-APL012":
        target = change.observed.qualified_name.sql if change.observed else change.key
        return D(code, artifact=change.key, value=target)
    if code in ("SST-APL016", "SST-APL022"):
        return D(code, artifact=change.key, detail=outcome.error.message)
    if code == "SST-APL028":
        if change.diagnostics and change.diagnostics[0].code == "SST-APL028":
            return change.diagnostics[0]
        return D("SST-INT902", detail=outcome.error.message)
    return D(
        "SST-APL001",
        artifact=change.key,
        value=change.action.value,
        detail=outcome.error.message,
    )
