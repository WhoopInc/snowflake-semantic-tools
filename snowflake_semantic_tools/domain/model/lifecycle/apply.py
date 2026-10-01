"""How apply runs and what it reports: its options, each artifact's outcome, and the run's result.

`ApplyOptions` says how apply handles failure and retries; each executed change yields an
`ApplyOutcome`, with a `ClassifiedError` when it failed, and the run an `ApplyResult`. An
outcome's status and grant check are reported by value in the apply JSON, so those values
never change once released.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from snowflake_semantic_tools.domain.model.diagnostic import DiagnosticBag
from snowflake_semantic_tools.domain.model.lifecycle.action import Action
from snowflake_semantic_tools.domain.model.lifecycle.observation import ArtifactKey


class FailurePolicy(Enum):
    """What apply does after a change fails.

    STOP_ALL skips everything not yet run, STOP_DEPENDENTS skips only the changes that depend
    on the failure, and CONTINUE runs the rest; only CONTINUE applies a plan with blocked changes.
    """

    STOP_ALL = "stop_all"
    STOP_DEPENDENTS = "stop_dependents"
    CONTINUE = "continue"


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """How often apply retries a statement that failed with a transient error, and how long it waits.

    Attributes:
        max_attempts: The attempts in all, the first included.
        backoff_ms: The wait in milliseconds before each retry, in order; the last one repeats.
    """

    max_attempts: int = 3
    backoff_ms: tuple[int, ...] = (1000, 2000)

    def delay_after(self, attempt: int) -> int:
        """Return how many milliseconds to wait after the given failed attempt, counting from 1.

        Raises:
            ValueError: the attempt is below 1, or it is the last one, after which nothing is retried.
        """
        if attempt < 1 or attempt >= self.max_attempts:
            raise ValueError("retry delay requested outside retryable attempts")
        return self.backoff_ms[min(attempt - 1, len(self.backoff_ms) - 1)]


@dataclass(frozen=True, slots=True)
class ApplyOptions:
    """How one apply run executes its plan.

    Attributes:
        parallelism: How many independent changes run at once, unless the policy is STOP_ALL,
            which runs them one at a time.
        allow_prune: Whether a planned prune executes; without it the prune is skipped.
        break_stale_lock: Whether to take over a state lock whose holder has expired.
    """

    parallelism: int = 4
    on_failure: FailurePolicy = FailurePolicy.STOP_DEPENDENTS
    retry: RetryPolicy = RetryPolicy()
    allow_prune: bool = False
    break_stale_lock: bool = False


class GrantCheck(Enum):
    """What apply learned about an object's explicit grants after replacing it."""

    NOT_APPLICABLE = "not_applicable"
    PRESERVED = "preserved"
    UNREADABLE = "unreadable"


class OutcomeStatus(Enum):
    """How one change ended: applied, skipped without running, or failed."""

    APPLIED = "applied"
    SKIPPED = "skipped"
    FAILED = "failed"


class ErrorKind(Enum):
    """The class of a failure apply reports, such as a missing privilege or a transient fault."""

    PRIVILEGE = "privilege"
    NOT_FOUND = "not_found"
    SYNTAX = "syntax"
    TRANSIENT = "transient"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class ClassifiedError:
    """A failure apply classified, with the diagnostic code it reports.

    Attributes:
        retryable: Whether retrying the same statements may succeed.
        sqlstate: The SQLSTATE the failure carried; None when it carried none.
    """

    code: str
    message: str
    kind: ErrorKind
    retryable: bool = False
    sqlstate: str | None = None


@dataclass(frozen=True, slots=True)
class ApplyOutcome:
    """How one change ended when apply ran it.

    Attributes:
        attempts: How many times the statements ran; 0 when they never did.
        duration_ms: How long the change took, in milliseconds.
        ddl: The rendered text of the artifact; empty when there is none, as for a prune.
        error: Why the change failed; None unless the status is FAILED.
        write_succeeded: Whether anything was written, even if the change then failed; state
            keeps the object as SST's when it was.
        component_fingerprints: What a composite handler verified it published; empty when
            state should record the rendered artifact's own.
        physical_resources: The objects a composite handler verified, as `(object type, name)`;
            state records exactly these for a composite artifact, even when there are none.
    """

    key: ArtifactKey
    action: Action
    status: OutcomeStatus
    attempts: int
    duration_ms: int
    ddl: str
    error: ClassifiedError | None = None
    grants: GrantCheck = GrantCheck.NOT_APPLICABLE
    write_succeeded: bool = False
    component_fingerprints: tuple[tuple[str, str], ...] = ()
    physical_resources: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class ApplyResult:
    """The result of one apply run: every outcome, the diagnostics, and whether state was written.

    Attributes:
        started_at, finished_at: When the run started and finished, as the clock reported it.
        state_written: Whether the run recorded its outcomes in state.
    """

    outcomes: tuple[ApplyOutcome, ...]
    diagnostics: DiagnosticBag
    run_id: str
    started_at: str
    finished_at: str
    state_written: bool

    @property
    def success(self) -> bool:
        """Report whether the run had no error diagnostic and no failed outcome."""
        return not self.diagnostics.has_errors and all(
            outcome.status is not OutcomeStatus.FAILED for outcome in self.outcomes
        )
