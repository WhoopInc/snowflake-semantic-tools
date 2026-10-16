"""The apply run: refuse or lock, verify state, preflight, run the waves, account, and persist.

`ApplyArtifacts.run` holds the run lease, the local and the remote lock, from the moment it
takes it until the run ends. Under the lock it first re-reads the state table, and refuses a
plan whose state changed since it was read. It writes state only after every wave ran, fenced
by the lease's token: remote first, then local, so local state never claims what the state
table does not hold. Changes run on sessions leased from a pool, one per worker, never on the
command's own session, and the heartbeat on one of its own; losing the lock halts them all.
Each change reads the lock again before it starts, and each write statement on a worker's
session before it runs, so a run whose lock another run broke while it was paused finds out
before its next write rather than at its next heartbeat. An interrupt stops the waves, not the
run: what finished is recorded before the lock is released. A session Snowflake ended mid-run
is recovered from as `recovery` says.
"""

from __future__ import annotations

from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from types import MappingProxyType
from typing import Protocol

from snowflake_semantic_tools.app.apply.errors import _exception_error, _failed, _rendered_ddl, _skipped
from snowflake_semantic_tools.app.apply.lock import LockPolicy, RunLease
from snowflake_semantic_tools.app.apply.one import ChangeApplier
from snowflake_semantic_tools.app.apply.recovery import Reconnect, Reconnection, connection_lost
from snowflake_semantic_tools.app.apply.stages import UploadStages
from snowflake_semantic_tools.app.apply.state import EntryStamp, _applied_after, _run_outcome
from snowflake_semantic_tools.app.apply.temporary import temporary_evals, temporary_notes, temporary_refusal
from snowflake_semantic_tools.app.apply.waves import _WaveRun, lost_lock
from snowflake_semantic_tools.app.lifecycle.ports import CatalogPublicationPort
from snowflake_semantic_tools.app.state import table_state
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ApplyOptions,
    ApplyOutcome,
    ApplyResult,
    Change,
    ChangeSet,
    ClassifiedError,
    ErrorKind,
    FailurePolicy,
)
from snowflake_semantic_tools.domain.ports.clock import ClockPort
from snowflake_semantic_tools.domain.ports.lifecycle import CompositeLifecycleHandler
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.execution import SessionPool, WriteGuardPort
from snowflake_semantic_tools.domain.ports.snowflake.state import StatePort
from snowflake_semantic_tools.domain.ports.state import StateStore
from snowflake_semantic_tools.domain.ports.timing import NO_PHASES, PhaseTimer
from snowflake_semantic_tools.domain.state import SST_VERSION, STATE_SCHEMA_VERSION, LastRun, State
from snowflake_semantic_tools.domain.state.lock import LockClaim, LockFence, state_write
from snowflake_semantic_tools.domain.validate.publication import channel_outcome_diagnostics

_DEFAULT_LOCK_POLICY = LockPolicy()


class ApplyPort(CatalogPublicationPort, StatePort, Protocol):
    """The Snowflake roles apply uses: a catalog publication port that also keeps the state table."""


class WorkerPort(ApplyPort, WriteGuardPort, Protocol):
    """A session a worker leases: it publishes, reads the run lock, and checks the lock before each write."""


class ApplyArtifacts:
    """Execute a reviewed ChangeSet against Snowflake and record what it wrote in state.

    A `ChangeApplier` applies each change, re-checking what plan saw before it writes. The use
    case keeps nothing between runs; the run lease keeps two runs from applying at once.

    Args:
        sessions: Where workers lease their sessions, one per change; None runs every change on
            `port`, one statement at a time. A leased session reads the run lock before each
            write statement it runs.
        heartbeat: The session the lock heartbeat extends the lock on, used for nothing else;
            None extends it on `port`.
        host: The machine the run lock records; empty when it is unknown.
        phases: Times `lock` (taking it), `publish` (the preflight and the waves), and
            `state_write`.
        reconnect: Opens a fresh connection, used only once Snowflake ended the run's own
            session, to write state and release the lock; None lets that failure propagate.
    """

    def __init__(
        self,
        port: ApplyPort,
        state_store: StateStore,
        clock: ClockPort,
        *,
        state_table: QualifiedName,
        git_sha: str = "",
        actor: str = "",
        host: str = "",
        lifecycle_handlers: Mapping[str, CompositeLifecycleHandler] | None = None,
        sessions: SessionPool[WorkerPort] | None = None,
        heartbeat: StatePort | None = None,
        lock_policy: LockPolicy = _DEFAULT_LOCK_POLICY,
        phases: PhaseTimer = NO_PHASES,
        reconnect: Reconnect | None = None,
    ) -> None:
        self._port = port
        self._state_store = state_store
        self._clock = clock
        self._state_table = state_table
        self._git_sha = git_sha
        self._actor = actor
        self._host = host
        self._lifecycle_handlers = dict(lifecycle_handlers or {})
        self._sessions = sessions
        self._heartbeat = heartbeat
        self._lock_policy = lock_policy
        self._phases = phases
        self._reconnect = reconnect

    def run(self, changeset: ChangeSet, previous: State, options: ApplyOptions = ApplyOptions()) -> ApplyResult:
        """Execute the plan's writes under the apply lock and record their outcomes in state.

        The run goes through these phases in order:

        1. Refusal, before the lock: a plan with an error diagnostic is refused with it, and so
           is a plan with blocked changes unless the policy is CONTINUE, and a `--temporary`
           run against a production-like target.
        2. Lock: the local lock, then the target's lock in Snowflake; either held by another
           run refuses the run, and an expired one is taken over only when
           `options.break_stale_lock` allows it. A heartbeat extends the remote lock while
           the run holds it.
        3. Verify: the state table is read again under the lock; when its entries for the
           target differ from `previous`, which the plan was made from, the run is refused.
        4. Preflight: when a relation some create or update requires does not exist, every
           change fails with SST-PRT005 and nothing runs.
        5. Waves: the dependency waves run in order, as `_WaveRun` describes.
        6. Accounting: the outcomes must account for every planned change.
        7. Persist: unless the plan has neither writes nor report-only prunes, the state the run
           leaves is written to the state table, fenced by the run's lock, then locally.

        An interrupt -- `KeyboardInterrupt`, which Ctrl-C raises and the CLI raises for SIGTERM
        and SIGHUP -- during the waves ends them as `_WaveRun` describes; the run then accounts
        and persists as usual, and its result is `interrupted`. An interrupt in any other phase
        propagates, the lock released first.

        A refused run never takes the lock, or releases it at once; once taken, it is released
        however the run ends, including when writing state raises. The moment the run loses
        its lock -- its heartbeat finds it broken, or so does the read of it before a change or
        a worker's write statement -- its worker sessions refuse every further statement and no
        further change starts; it then writes no state, since the run that broke the lock owns
        the target's state. When Snowflake ended the run's own session, the state write and the
        release go once more on one fresh connection, as `recovery` says. State otherwise
        keeps every object a statement wrote, even when its change then failed, so a partial
        write stays SST's and the next plan sees it.
        The plan's own diagnostics come first in the result, then each phase's in order.

        Raises:
            ValueError: the changes' dependencies form a cycle.
            SnowflakePortError: reading or writing the state table failed; local state is not
                written.
            KeyboardInterrupt: an interrupt outside the waves, or a third one while the run
                waited for its running changes.

        Diagnostics:
            SST-APL002: a change was skipped because a change it depends on failed or was skipped.
            SST-APL003: the plan has a blocked change and the policy is not CONTINUE.
            SST-APL008: an object's grants could not be verified.
            SST-APL010: the run took over an expired lock.
            SST-APL011: another run holds the lock, or broke it while this run held it.
            SST-APL012: the target's state changed between the plan and the lock.
            SST-APL013: a temporary artifact would publish to a production-like target.
            SST-APL014: a temporary artifact now shadows the permanent object of its name.
            SST-APL015: a temporary artifact's alias was ignored.
            SST-APL105: an eval of an agent published as temporary was skipped.
            SST-APL103: Snowflake ended a session the run used, after it took its lock.
            SST-APL900: the outcomes do not account for every planned change.
            SST-VAL828: a skill, plugin or profile change, a publication channel's, has no
                outcome or several, so the run's result would not show that channel's state.
            Each failed change reports the diagnostic its error names; see `_outcome_diagnostic`.
            A change Snowflake refused also reports the refusal under its SNO code; see
            `_cause_diagnostic`.
        """
        run_id = self._clock.new_run_id()
        started = self._clock.now_iso()
        refusal = _refusal(changeset, options)
        if refusal is not None:
            return self._refused(refusal, run_id, started)
        lease = RunLease(
            self._state_store,
            self._port,
            self._state_table,
            changeset.target.name,
            LockClaim(run_id, self._actor, self._host, self._lock_policy.ttl_seconds),
            self._lock_policy,
            heartbeat_port=self._heartbeat,
            on_lost=self._halt,
        )
        recovery = Reconnection(self._reconnect)
        # Released from the moment the lock may be taken: an interrupt that lands between taking
        # it and the run's first statement must not leave it held. Releasing a lease that was
        # refused releases nothing another run holds.
        try:
            try:
                with self._phases.phase("lock"):
                    locked, lock_diagnostics = lease.acquire(break_stale=options.break_stale_lock)
                reported = (*changeset.diagnostics, *temporary_notes(changeset), *lock_diagnostics)
                if not locked:
                    return self._refused(reported, run_id, started)
                changed = self._state_changed(changeset, previous)
                if changed is not None:
                    return self._refused((*reported, changed), run_id, started)
                return self._run_locked(changeset, previous, options, reported, run_id, started, lease, recovery)
            finally:
                _release(lease, recovery)
        finally:
            recovery.close()

    def _halt(self, reason: str) -> None:
        """Stop every worker session before its next statement, once the run lost its lock."""
        if self._sessions is not None:
            self._sessions.halt(f"the run lock was lost: {reason} holds it")

    def _halt_interrupted(self) -> None:
        """Stop every worker session before its next statement, once the run was interrupted again."""
        if self._sessions is not None:
            self._sessions.halt("the run was interrupted")

    def _state_changed(self, changeset: ChangeSet, previous: State) -> Diagnostic | None:
        """Read the target's state again under the lock; SST-APL012 when it is not the state the plan read.

        Diagnostics:
            SST-APL012: the state table's entries for the target changed since the plan, or
                it can no longer be read.
        """
        current = table_state(self._port, self._state_table, changeset.target, previous.config_path)
        if current is not None and current.applied == previous.applied:
            return None
        value = f"its state in {self._state_table.sql}"
        return D("SST-APL012", subject=changeset.target.name, artifact=f"target {changeset.target.name}", value=value)

    def _refused(self, diagnostics: tuple[Diagnostic, ...], run_id: str, started: str) -> ApplyResult:
        """Report a run that executed nothing and wrote no state."""
        return ApplyResult((), DiagnosticBag(diagnostics), run_id, started, self._clock.now_iso(), False)

    def _run_locked(
        self,
        changeset: ChangeSet,
        previous: State,
        options: ApplyOptions,
        reported: tuple[Diagnostic, ...],
        run_id: str,
        started: str,
        lease: RunLease,
        recovery: Reconnection,
    ) -> ApplyResult:
        """Run the phases that need the lock: preflight, waves, accounting, then persist.

        The waves share one `UploadStages`, so each stage the run uploads to is created at most once.
        An eval of an agent the run publishes as temporary is skipped, writing nothing.
        """
        with self._phases.phase("publish"):
            outcomes = self._preflight_relations(changeset.changes, options.parallelism)
        wave_diagnostics: tuple[Diagnostic, ...] = ()
        stages = UploadStages()

        def lost() -> str | None:
            return lease.lost_reason if lease.lost else None

        skipped = temporary_evals(changeset)

        def apply_one(change: Change, options: ApplyOptions) -> ApplyOutcome:
            if change.key in skipped:
                return _skipped(change, _rendered_ddl(change))
            return self._apply_one(change, options, stages, lease)

        waves = _WaveRun(
            changeset, options, apply_one, lost, verify=lease.verify, on_second_interrupt=self._halt_interrupted
        )
        if outcomes is None:
            with self._phases.phase("publish"):
                outcomes, wave_diagnostics = waves.run()
        accounted = (
            *_unaccounted(changeset, outcomes),
            *channel_outcome_diagnostics(
                ((change.key, change.artifact_type) for change in changeset.changes),
                [outcome.key for outcome in outcomes],
            ),
        )
        diagnostics = DiagnosticBag((*reported, *wave_diagnostics, *accounted))
        assert lease.fence is not None
        with self._phases.phase("state_write"):
            result = self._persist(changeset, previous, outcomes, diagnostics, (run_id, started), lease.fence, recovery)
        return replace(result, interrupted=waves.interrupted)

    def _apply_one(self, change: Change, options: ApplyOptions, stages: UploadStages, lease: RunLease) -> ApplyOutcome:
        """Apply one change: on `port` without a pool, else on a session leased for it alone.

        On a leased session the change's composite handler runs there too, and the session
        reads the run lock again before each write statement, so a publish of several
        statements stops at the first one after another run took the lock. A session that
        cannot be opened fails the change with nothing written.
        """
        if self._sessions is None:
            return ChangeApplier(self._port, self._clock, self._lifecycle_handlers, stages=stages).apply(
                change, options
            )
        try:
            with self._sessions.lease() as port:
                port.guard_writes(lambda: _guard_write(lease, port))
                try:
                    return ChangeApplier(port, self._clock, self._lifecycle_handlers, leased=True, stages=stages).apply(
                        change, options
                    )
                finally:
                    port.guard_writes(None)
        except SnowflakePortError as exc:
            return _failed(change, _exception_error(exc), _rendered_ddl(change))

    def _preflight_relations(self, changes: tuple[Change, ...], parallelism: int) -> tuple[ApplyOutcome, ...] | None:
        """Fail every change when a relation some create or update requires does not exist.

        The relations are checked in name order, at once on worker sessions when there is a
        pool, so the check waits for one lookup rather than one after another. Returns None
        when all of them exist.
        """
        relations = sorted(
            {
                relation
                for change in changes
                if change.action in (Action.CREATE, Action.UPDATE) and change.rendered is not None
                for relation in change.rendered.required_relations
            }
        )
        present = self._relations_exist(relations, parallelism)
        missing = tuple(relation for relation, exists in zip(relations, present, strict=True) if not exists)
        if not missing:
            return None
        detail = "missing required relations: " + ", ".join(relation.sql for relation in missing)
        error = ClassifiedError("SST-PRT005", detail, ErrorKind.NOT_FOUND)
        return tuple(_failed(change, error, _rendered_ddl(change)) for change in changes)

    def _relations_exist(self, relations: list[QualifiedName], parallelism: int) -> tuple[bool, ...]:
        """Whether each relation exists, in order: on `port` without a pool, else on leased sessions at once."""
        sessions = self._sessions
        if sessions is None or len(relations) < 2:
            return tuple(self._port.object_exists("TABLE OR VIEW", relation) for relation in relations)

        def exists(relation: QualifiedName) -> bool:
            with sessions.lease() as port:
                return port.object_exists("TABLE OR VIEW", relation)

        pool = ThreadPoolExecutor(max_workers=parallelism)
        try:
            return tuple(pool.map(exists, relations))
        finally:
            pool.shutdown(wait=False, cancel_futures=True)

    def _persist(
        self,
        changeset: ChangeSet,
        previous: State,
        outcomes: tuple[ApplyOutcome, ...],
        diagnostics: DiagnosticBag,
        run: tuple[str, str],
        fence: LockFence,
        recovery: Reconnection,
    ) -> ApplyResult:
        """Write the state the run leaves, remote then local, unless the plan had nothing to record.

        `run` is the run's id and when it started. A state write that fails because Snowflake
        ended the run's session is sent once more, on the fresh connection `recovery` opens.

        Raises:
            SnowflakePortError: the state write failed, and sending it again could not help or
                failed too.

        Diagnostics:
            SST-APL011: the state table refused the write because the run no longer holds
                its lock; reported unless the waves already reported the loss.
            SST-APL103: Snowflake ended a session the run used; as `connection_lost` says.
        """
        run_id, started = run
        # A report-only prune executes nothing, but state still records the run, and the
        # object plan observed when no entry names it yet.
        if not changeset.writes and not changeset.report_only:
            return ApplyResult(outcomes, diagnostics, run_id, started, self._clock.now_iso(), False)
        state = self._finish_state(changeset, previous, outcomes, run_id, started)
        try:
            written = self._write_remote_state(self._port, changeset, previous, state, fence)
        except SnowflakePortError as exc:
            written = self._write_remote_state(recovery.recover(exc), changeset, previous, state, fence)
        diagnostics = DiagnosticBag((*diagnostics, *connection_lost(outcomes, reconnected=recovery.used)))
        if not written:
            refused = () if any(item.code == "SST-APL011" for item in diagnostics) else (lost_lock(),)
            return ApplyResult(
                outcomes, DiagnosticBag((*diagnostics, *refused)), run_id, started, self._clock.now_iso(), False
            )
        self._state_store.write_local(state)
        return ApplyResult(
            outcomes,
            diagnostics,
            run_id,
            started,
            state.last_run.finished_at if state.last_run else self._clock.now_iso(),
            True,
        )

    def _finish_state(
        self,
        changeset: ChangeSet,
        previous: State,
        outcomes: tuple[ApplyOutcome, ...],
        run_id: str,
        started: str,
    ) -> State:
        """Build the state this run leaves: the entries `_applied_after` keeps, and the run itself."""
        finished = self._clock.now_iso()
        stamp = EntryStamp(run_id, finished, self._git_sha)
        applied = _applied_after(changeset, previous, outcomes, stamp, self._lifecycle_handlers)
        last_run = LastRun(run_id, started, finished, SST_VERSION, "apply", _run_outcome(outcomes), self._actor)
        return State(
            STATE_SCHEMA_VERSION,
            changeset.target,
            changeset.manifest_id,
            self._state_store.config_path,
            last_run,
            MappingProxyType(applied),
        )

    def _write_remote_state(
        self, port: StatePort, changeset: ChangeSet, previous: State, state: State, fence: LockFence
    ) -> bool:
        """Write to the state table, on `port`, only the entries the run changed or retired, and its manifest.

        Returns:
            Whether the write committed; False when the run's fence no longer holds the lock.

        Raises:
            SnowflakePortError: the write failed.
        """
        return port.write_state(
            self._state_table,
            changeset.target.name,
            changeset.manifest_id,
            state_write(previous.applied, state.applied),
            fence,
        )


def _release(lease: RunLease, recovery: Reconnection) -> None:
    """Release the run's locks; once more on the fresh connection when Snowflake ended the run's session.

    Raises:
        SnowflakePortError: releasing the remote lock failed, and releasing it again could not
            help or failed too; the lock then expires on its own.
    """
    try:
        lease.release()
    except SnowflakePortError as exc:
        lease.release(recovery.recover(exc))


def _guard_write(lease: RunLease, port: WorkerPort) -> None:
    """Read the run lock on a worker's session before its next write; losing it halts the session.

    The read is made on every write: no clock decides it may be skipped, since a monotonic
    clock stops while the machine sleeps, which is how a run comes to resume past its lock.

    Raises:
        SnowflakePortError: the lock row could not be read; the write is not sent.
    """
    lease.verify(port)


def _refusal(changeset: ChangeSet, options: ApplyOptions) -> tuple[Diagnostic, ...] | None:
    """Return what a plan is refused with before the lock is taken; None when it may run.

    Diagnostics:
        SST-APL003: a blocked change, unless the policy is CONTINUE; one per blocked change.
        SST-APL013: a temporary artifact against a production-like target; one per artifact.
    """
    if changeset.diagnostics.has_errors:
        return tuple(changeset.diagnostics)
    refused_temporary = temporary_refusal(changeset, options)
    if refused_temporary:
        return (*changeset.diagnostics, *refused_temporary)
    if changeset.blocked and options.on_failure is not FailurePolicy.CONTINUE:
        return (
            *changeset.diagnostics,
            *(D("SST-APL003", artifact=change.key, count=len(change.diagnostics) or 1) for change in changeset.blocked),
        )
    return None


def _unaccounted(changeset: ChangeSet, outcomes: tuple[ApplyOutcome, ...]) -> tuple[Diagnostic, ...]:
    """Report SST-APL900 when the outcomes do not account for every planned change, else nothing."""
    if len(outcomes) != len(changeset.changes):
        return (D("SST-APL900", found=len(outcomes), expected=len(changeset.changes)),)
    return ()
