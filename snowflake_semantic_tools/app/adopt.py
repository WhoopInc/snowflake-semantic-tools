"""`sst state adopt`: hand SST an object it did not publish, once its files compile to what is live.

A live object state does not record is unmanaged (SST-PLN024), and plan never touches it.
Adopt is the one way across: for each selected artifact that plan blocks as unmanaged, it
reads the live object as `sst plan`'s member diff does (`app.plan_diff.COMPARISONS`) and
compares it with the compiled artifact. Where they agree, or the user accepts the
differences, it records a state entry whose outcome is `adopted`, under the run lock as
apply holds it, and writes no object. The next plan updates the object, which writes SST's
ownership marker; an object that changed after it was adopted is blocked instead (SST-PLN014).

A composite artifact -- a skill, plugin, profile or eval -- holds no single object to read
this way, and an object carrying SST's marker is `sst state recover`'s; neither is adopted.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from types import MappingProxyType

from snowflake_semantic_tools.app.apply.lock import LockPolicy, RunLease
from snowflake_semantic_tools.app.apply.state import EntryStamp
from snowflake_semantic_tools.app.compile import CompiledArtifact, CompileResult
from snowflake_semantic_tools.app.diff import name_map
from snowflake_semantic_tools.app.drift import written_digest
from snowflake_semantic_tools.app.plan_diff import COMPARISONS, Comparison, live_object
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import Change, ChangeReason, ChangeSet
from snowflake_semantic_tools.domain.model.lifecycle.member import MemberDiff
from snowflake_semantic_tools.domain.plan.member_diff import diff_definitions, normalize, summary
from snowflake_semantic_tools.domain.ports.clock import ClockPort
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePort
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.state import StateStore
from snowflake_semantic_tools.domain.state import ADOPTED, AppliedEntry
from snowflake_semantic_tools.domain.state.lock import LockClaim, StateWrite

# The types adopt compares: those whose live object `COMPARISONS` reads whole.
ADOPTABLE_TYPES = ("semantic_view", "tool", "agent", "cowork_profile")
_DEFAULT_LOCK_POLICY = LockPolicy()
# How many changed members a refusal names before it counts the rest.
_LISTED = 6


@dataclass(frozen=True, slots=True)
class AdoptedArtifact:
    """One selected artifact: what adopt found, and whether it records it.

    Attributes:
        found: `matches`, `differs` (recorded only with accepted differences), or why it is
            not adoptable.
        differences: The members that differ, as `sst plan` names them.
        recorded: Whether adopt records it.
    """

    key: str
    qualified_name: str
    found: str
    differences: tuple[str, ...] = ()
    recorded: bool = False


@dataclass(frozen=True, slots=True)
class AdoptResult:
    """What one adopt did: each selected artifact in key order, and what it wrote."""

    artifacts: tuple[AdoptedArtifact, ...]
    diagnostics: DiagnosticBag
    wrote: tuple[str, ...] = ()
    refused: bool = False

    @property
    def pending(self) -> tuple[AdoptedArtifact, ...]:
        """The artifacts adopt records, or would without a lost lock."""
        return tuple(item for item in self.artifacts if item.recorded)


class AdoptObjects:
    """Compare each unmanaged object with its compiled artifact, and record those that may be adopted.

    Args:
        state_table: The target's state table, where the run lock also lives.
        actor, host: Who and where, as the run lock records them.
        git_sha: The commit adopt ran from, which each entry carries.
    """

    def __init__(
        self,
        port: SnowflakePort,
        state_store: StateStore,
        clock: ClockPort,
        *,
        state_table: QualifiedName,
        actor: str = "",
        host: str = "",
        git_sha: str = "",
        lock_policy: LockPolicy = _DEFAULT_LOCK_POLICY,
        comparisons: Mapping[str, Comparison] = COMPARISONS,
    ) -> None:
        self._port = port
        self._state_store = state_store
        self._clock = clock
        self._state_table = state_table
        self._actor = actor
        self._host = host
        self._git_sha = git_sha
        self._lock_policy = lock_policy
        self._comparisons = comparisons

    def compare(
        self, changeset: ChangeSet, result: CompileResult, *, accept_differences: bool = False
    ) -> tuple[tuple[AdoptedArtifact, ...], tuple[Diagnostic, ...]]:
        """Decide each planned artifact: adoptable and matching, differing, or not adoptable.

        Diagnostics:
            SST-IMP020: the live object differs from the compiled artifact; refused unless
                `accept_differences`, when it is a warning-free record and the diff is listed.
            SST-IMP022: the artifact is not an unmanaged object adopt can compare.
        """
        compiled = {item.artifact_key: item for item in result.compiled}
        names = name_map(result)
        found: list[AdoptedArtifact] = []
        diagnostics: list[Diagnostic] = []
        for change in sorted(changeset.changes, key=lambda item: item.key):
            item, problem = self._decide(change, compiled.get(change.key), names, accept_differences)
            found.append(item)
            if problem is not None:
                diagnostics.append(problem)
        return tuple(found), tuple(diagnostics)

    def record(
        self,
        artifacts: Sequence[AdoptedArtifact],
        changeset: ChangeSet,
        target_name: str,
        *,
        break_stale: bool = False,
    ) -> AdoptResult:
        """Record each artifact `compare` let through, under the run lease; write no object.

        Diagnostics:
            SST-IMP021: one per artifact recorded.
            SST-APL011: another run holds the lock, or broke it before the write.
            SST-APL010: an expired lock was taken over, with `break_stale`.
        """
        pending = {item.key: item for item in artifacts if item.recorded}
        if not pending:
            return AdoptResult(tuple(artifacts), DiagnosticBag())
        run_id = self._clock.new_run_id()
        stamp = EntryStamp(run_id, self._clock.now_iso(), self._git_sha)
        changes = {change.key: change for change in changeset.changes}
        entries = {
            key: _adopted_entry(changes[key], changeset.manifest_id, stamp, self._digest(changes[key]))
            for key in pending
        }
        lease = RunLease(
            self._state_store,
            self._port,
            self._state_table,
            target_name,
            LockClaim(run_id, self._actor, self._host, self._lock_policy.ttl_seconds),
            self._lock_policy,
        )
        locked, reported = lease.acquire(break_stale=break_stale)
        if not locked or lease.fence is None:
            return AdoptResult(tuple(artifacts), DiagnosticBag(reported), refused=True)
        try:
            written = self._write(entries, changeset.manifest_id, target_name, lease)
        finally:
            lease.release()
        if written is None:
            return AdoptResult(tuple(artifacts), DiagnosticBag(reported), refused=True)
        kept = tuple(
            item if item.key in written or not item.recorded else replace(item, recorded=False) for item in artifacts
        )
        recorded = tuple(
            D("SST-IMP021", subject=item.key, artifact=item.key, value=item.qualified_name)
            for item in kept
            if item.key in written
        )
        return AdoptResult(kept, DiagnosticBag((*reported, *recorded)), wrote=written)

    def _digest(self, change: Change) -> str | None:
        """Digest the adopted object as apply digests what it writes, so plan can tell it changed since.

        Adopt takes only an object at the artifact's own name, so the rendered target is the live one.
        """
        return written_digest(self._port, change.rendered) if change.rendered is not None else None

    def _decide(
        self,
        change: Change,
        compiled: CompiledArtifact | None,
        names: Mapping[tuple[str, str, str], str],
        accept_differences: bool,
    ) -> tuple[AdoptedArtifact, Diagnostic | None]:
        """Decide one artifact: not adoptable, matching, or differing, with what it reports."""
        observed, rendered = change.observed, change.rendered
        name = observed.qualified_name.sql if observed is not None else rendered.target.sql if rendered else ""
        reason = _not_adoptable(change)
        comparison = self._comparisons.get(change.artifact_type)
        if reason is None and (comparison is None or compiled is None):
            reason = "it did not compile, so there is nothing to compare the live object with"
        if reason is not None or comparison is None or compiled is None:
            text = reason or "it cannot be compared"
            return AdoptedArtifact(change.key, name, text), D(
                "SST-IMP022", subject=change.key, artifact=change.key, reason=text
            )
        desired = comparison.desired(compiled)
        if desired is None:
            text = "SST does not compare this kind of object member by member"
            return AdoptedArtifact(change.key, name, text), D(
                "SST-IMP022", subject=change.key, artifact=change.key, reason=text
            )
        try:
            live = comparison.live(self._port, compiled, live_object(change, None))
        except SnowflakePortError as exc:
            text = f"its live definition could not be read: {exc}"
            return AdoptedArtifact(change.key, name, text), D(
                "SST-IMP022", subject=change.key, artifact=change.key, reason=text
            )
        differences = _described(diff_definitions(normalize(live, names), normalize(desired, names)))
        if not differences:
            return AdoptedArtifact(change.key, name, "matches", recorded=True), None
        listed = ", ".join(differences[:_LISTED]) + (
            f", and {len(differences) - _LISTED} more" if len(differences) > _LISTED else ""
        )
        diagnostic = D("SST-IMP020", subject=change.key, artifact=change.key, value=name, detail=listed)
        if accept_differences:
            return AdoptedArtifact(change.key, name, "differs", tuple(differences), recorded=True), None
        return AdoptedArtifact(change.key, name, "differs", tuple(differences)), diagnostic

    def _write(
        self, entries: Mapping[str, AppliedEntry], manifest_id: str, target_name: str, lease: RunLease
    ) -> tuple[str, ...] | None:
        """Write the entries state still lacks, remote then local; None when the lock was lost."""
        if lease.fence is None:
            return None
        current = self._port.read_state(self._state_table, target_name) or {}
        missing = {key: entry for key, entry in entries.items() if key not in current}
        if not missing:
            return ()
        manifest = self._port.read_state_manifest(self._state_table, target_name) or manifest_id
        if not self._port.write_state(
            self._state_table, target_name, manifest, StateWrite(MappingProxyType(missing)), lease.fence
        ):
            return None
        cached = self._state_store.read_local()
        if cached is not None:
            kept = {**cached.applied, **{key: entry for key, entry in missing.items() if key not in cached.applied}}
            self._state_store.write_local(replace(cached, applied=MappingProxyType(kept)))
        return tuple(sorted(missing))


def _described(diffs: tuple[MemberDiff, ...]) -> tuple[str, ...]:
    """Name each differing member as plan does, with the properties of a changed one: `live -> project`."""
    named = summary(diffs)
    described = []
    for label, diff in zip(named, diffs, strict=True):
        if diff.op != "changed" or not diff.properties:
            described.append(label)
            continue
        changes = "; ".join(f"{item.name}: {_shown(item.before)} -> {_shown(item.after)}" for item in diff.properties)
        described.append(f"{label} ({changes})")
    return tuple(described)


def _shown(value: object) -> str:
    text = "unset" if value in (None, "") else str(value)
    return text if len(text) <= 60 else text[:57] + "..."


def _not_adoptable(change: Change) -> str | None:
    """Say why a planned artifact is not one adopt may record; None when it is."""
    if change.artifact_type not in ADOPTABLE_TYPES:
        return f"adopt records {', '.join(ADOPTABLE_TYPES)}; a {change.artifact_type} has no single object to compare"
    if change.rendered is None:
        return "it does not compile for this target"
    if change.observed is None:
        return "no object holds its name on the target; `sst apply` creates it"
    if change.reason is not ChangeReason.UNMANAGED_OBJECT:
        return "state records it already"
    if change.observed.marker is not None:
        return "it carries SST's ownership marker; `sst state recover` records it"
    return None


def _adopted_entry(change: Change, manifest_id: str, stamp: EntryStamp, digest: str | None) -> AppliedEntry:
    """Record an adopted object: the compiled definition, at the live object, as it read when adopted."""
    rendered, observed = change.rendered, change.observed
    if rendered is None or observed is None:
        raise ValueError(f"{change.key} names no object to adopt")
    return AppliedEntry(
        fingerprint=rendered.fingerprint,
        qualified_name=observed.qualified_name.sql,
        applied_at=stamp.applied_at,
        run_id=stamp.run_id,
        outcome=ADOPTED,
        ddl_sha256=rendered.fingerprint,
        manifest_id=manifest_id,
        git_sha=stamp.git_sha,
        physical_resources=((observed.object_type, observed.qualified_name.sql),),
        live_digest=digest if digest is not None else observed.live_digest,
    )
