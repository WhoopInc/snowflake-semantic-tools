"""The run lock in Snowflake, and the change a run makes to the state table.

One row per target in the lock table beside the state table says which run may apply to the
target. A run claims it with a `LockClaim`; `LockAcquisition` says what the claim found.
`StateWrite` is what a run's state write changes: the entries it inserts or replaces, and
the keys it removes; `state_write` derives it from the state before and after the run.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.state.applied import AppliedEntry

# How long a claim holds the lock without a heartbeat, and how often a heartbeat extends it.
LOCK_TTL_SECONDS = 30 * 60
LOCK_HEARTBEAT_SECONDS = 5 * 60


def lock_table_for(state_table: QualifiedName) -> QualifiedName:
    """Return the lock table beside a state table: its name with `_LOCK` appended, in its schema."""
    name = state_table.name
    return QualifiedName(state_table.database, state_table.schema, Identifier(f"{name.value}_LOCK", quoted=name.quoted))


@dataclass(frozen=True, slots=True)
class LockClaim:
    """What a run records when it takes the lock.

    Attributes:
        run_id: The run that holds the lock; only it may extend or release it.
        owner: Who ran it, such as the role; empty when it is unknown.
        host: The machine it ran on; empty when it is unknown.
        ttl_seconds: How long after it is taken, or last extended, the lock expires.
    """

    run_id: str
    owner: str = ""
    host: str = ""
    ttl_seconds: int = LOCK_TTL_SECONDS


@dataclass(frozen=True, slots=True)
class RunLock:
    """The lock row as Snowflake holds it: the claim that holds it, and when it was taken and expires.

    Attributes:
        acquired_at, expires_at: As Snowflake recorded them, by its own clock.
        expired: Whether `expires_at` had passed when the row was read.
    """

    run_id: str
    owner: str
    host: str
    acquired_at: str
    expires_at: str
    expired: bool

    def describe(self) -> str:
        """Name the holder as a diagnostic reports it: the run, then who and where, when known."""
        where = " on ".join(part for part in (self.owner, self.host) if part)
        expiry = "expired" if self.expired else "expires"
        return f"run {self.run_id}" + (f" ({where})" if where else "") + f", {expiry} {self.expires_at}"


@dataclass(frozen=True, slots=True)
class LockAcquisition:
    """What claiming the lock found.

    Attributes:
        acquired: Whether the claim now holds the lock.
        holder: The lock that was there before the claim: the run that holds it when the claim
            failed, the expired one it replaced when it broke one; None when the lock was free.
        broke_stale: Whether the claim took over an expired lock.
    """

    acquired: bool
    holder: RunLock | None = None
    broke_stale: bool = False


@dataclass(frozen=True, slots=True)
class StateWrite:
    """One run's change to a target's state-table rows, applied in one transaction.

    Attributes:
        upserts: The entries to insert or replace, by artifact key.
        deletes: The artifact keys whose entries are removed, sorted.
    """

    upserts: Mapping[str, AppliedEntry]
    deletes: tuple[str, ...] = ()


def state_write(before: Mapping[str, AppliedEntry], after: Mapping[str, AppliedEntry]) -> StateWrite:
    """Return what turns the entries `before` into the entries `after`: changed entries and removed keys.

    An entry equal to the one before is not rewritten, so a run writes only what it touched.
    """
    upserts = {key: entry for key, entry in sorted(after.items()) if before.get(key) != entry}
    deletes = tuple(sorted(key for key in before if key not in after))
    return StateWrite(MappingProxyType(upserts), deletes)
