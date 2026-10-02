"""How apply names a failure: the classified error, the failed or skipped outcome, and its diagnostic.

`classify_error` turns what Snowflake reported into the `ClassifiedError` an outcome carries,
by the signature table in `domain.diagnostics.signatures`; the composite lifecycle handlers
classify their own failures with it. The outcome builders keep every refusal shaped alike,
`_outcome_diagnostic` reports a failed outcome under the code its error names, and
`_cause_diagnostic` reports the Snowflake refusal behind it under its SNO code.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.diagnostics.signatures import (
    match_signature,
    signature_codes,
    snowflake_diagnostic,
)
from snowflake_semantic_tools.domain.model.lifecycle import (
    ApplyOutcome,
    Change,
    ClassifiedError,
    ExecResult,
    GrantCheck,
    OutcomeStatus,
)


def classify_error(message: str, *, sqlstate: str | None = None, errno: int | None = None) -> ClassifiedError:
    """Classify a Snowflake failure by the signature table: its number, its SQLSTATE, then its wording.

    Returns:
        The SNO code of the most specific signature `match_signature` finds, with that
        signature's kind and retryability; SST-SNO001 when no signature matches.
    """
    signature = match_signature(message, errno=errno, sqlstate=sqlstate)
    return ClassifiedError(signature.code, message, signature.kind, signature.retryable, sqlstate)


def _exception_error(exc: BaseException) -> ClassifiedError:
    """Classify an exception by its message and, when it carries them, its SQLSTATE and number."""
    errno = getattr(exc, "errno", None)
    return classify_error(
        str(exc), sqlstate=getattr(exc, "sqlstate", None), errno=errno if isinstance(errno, int) else None
    )


def _script_error(result: ExecResult, fallback: str) -> ClassifiedError:
    """Classify a failed script by the error it reported; `fallback` is the message when it gave none."""
    if result.error is None:
        return classify_error(fallback)
    return classify_error(result.error.message, sqlstate=result.error.sqlstate, errno=result.error.errno)


def _cause_diagnostic(change: Change, outcome: ApplyOutcome) -> Diagnostic | None:
    """Report why Snowflake refused a failed change, under its SNO code; None for any other failure.

    Diagnostics:
        Any SNO code the signature table maps a driver error to, SST-SNO001 included.
    """
    if outcome.error is None or outcome.error.code not in signature_codes():
        return None
    target = change.rendered.target.sql if change.rendered else change.key
    return snowflake_diagnostic(outcome.error.code, outcome.error.message, value=target, subject=change.key)


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
    SST-APL001 naming the action and the error. A code whose message names something besides
    the artifact reads it from the error's `value`.

    Diagnostics:
        SST-APL001: any other failure, with the action and the error message.
        SST-APL004: a replace omits COPY GRANTS.
        SST-APL007: the tag a publish applies after creating could not be applied.
        SST-APL008: the grants could not be read before the replace.
        SST-APL009: an explicit grant present before the replace is absent after it.
        SST-APL012: the object changed since the plan, or the write left no ownership marker.
        SST-APL016: a partial write left a composite artifact unusable.
        SST-APL017: one channel of a multi-channel publish failed while another succeeded.
        SST-APL018: a registry pointer write failed after its upload.
        SST-APL019: a file the published version must no longer hold is still in it.
        SST-APL020: the version name a publish mints was minted by another deploy.
        SST-APL021: grants are in place on an artifact whose certification did not succeed.
        SST-APL022: a dataset version could not be added.
        SST-APL027: a metadata table a publish writes is absent or the wrong shape.
        SST-APL028: the plan's own diagnostic for an eval config stage with the wrong file format.
        SST-INT902: an SST-APL028 failure the plan never recorded.
    """
    assert outcome.error is not None
    error = outcome.error
    code = error.code
    if code in ("SST-APL004", "SST-APL008", "SST-APL021"):
        return D(code, artifact=change.key)
    if code == "SST-APL009":
        return D(code, artifact=change.key, value=error.message)
    if code == "SST-APL012":
        target = change.observed.qualified_name.sql if change.observed else change.key
        return D(code, artifact=change.key, value=target)
    if code in ("SST-APL007", "SST-APL016", "SST-APL017", "SST-APL022"):
        return D(code, artifact=change.key, detail=error.message)
    if code == "SST-APL018":
        return D(code, artifact=change.key, path=error.value, detail=error.message)
    if code == "SST-APL019":
        return D(code, artifact=change.key, path=error.value)
    if code == "SST-APL020":
        return D(code, artifact=change.key, value=error.value)
    if code == "SST-APL027":
        return D(code, value=error.value, detail=error.message)
    if code == "SST-APL028":
        if change.diagnostics and change.diagnostics[0].code == "SST-APL028":
            return change.diagnostics[0]
        return D("SST-INT902", detail=error.message)
    return D(
        "SST-APL001",
        artifact=change.key,
        value=change.action.value,
        detail=error.message,
    )
