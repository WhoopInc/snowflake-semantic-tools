"""The interfaces application use cases reach Snowflake through, one protocol per role.

`CatalogPort` reads what exists and how it is defined, `ExecutionPort` runs SQL,
`StagePort` moves files on stages and in extension versions, `ProfileRegistryPort` keeps
CoCo Desktop's profile registry, and `StatePort` keeps the state table. `SnowflakePort` is
their union, which an adapter satisfies structurally; a use case can ask for only the roles
it uses. `ClockPort` and `StateStore` are the clock and the local state file.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from snowflake_semantic_tools.domain.model.diagnostic import Diagnostic
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, GrantRow, OwnershipMarker, QueryResult, ShowRow
from snowflake_semantic_tools.domain.sql import Sql
from snowflake_semantic_tools.domain.state import AppliedEntry, State


class SnowflakePortError(RuntimeError):
    """A port operation Snowflake refused or could not be reached for, or whose answer SST cannot use.

    A command that lets one escape exits as a connection failure, reporting `diagnostic`
    when there is one and the message otherwise.

    Attributes:
        sqlstate: The SQLSTATE of the driver failure behind the error; None when there is none.
        errno: The driver's error number for that failure; None when there is none.
        diagnostic: What the command reports for the failure; None when the adapter did not
            recognise it.
    """

    def __init__(
        self,
        message: str,
        *,
        sqlstate: str | None = None,
        errno: int | None = None,
        diagnostic: Diagnostic | None = None,
    ) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate
        self.errno = errno
        # What the command reports, when the adapter recognised the failure.
        self.diagnostic = diagnostic


@dataclass(frozen=True, slots=True)
class StageObservation:
    """Whether a stage exists and, when it does, the file format it declares.

    Attributes:
        file_format: The format as `CatalogPort.describe_stage_file_format` returns it; None
            when the stage does not exist or declares none.
    """

    exists: bool
    file_format: str | None = None


@dataclass(frozen=True, slots=True)
class StagedFileMetadata:
    """What LIST reports for the one file at a stage path.

    Attributes:
        stage_path: The path observed, exactly as the caller gave it.
        name: The path without its leading `@`, whatever form LIST printed the name in.
        size: The file's size in bytes; 0 when LIST reports none.
        md5: The MD5 LIST reports; None when it reports none.
        last_modified: The modification time as LIST prints it; None when it reports none.
    """

    stage_path: str
    name: str
    size: int
    md5: str | None = None
    last_modified: str | None = None


@dataclass(frozen=True, slots=True)
class ExtensionObservation:
    """One `SHOW CORTEX EXTENSIONS` row."""

    qualified_name: QualifiedName
    extension_type: str
    comment: str | None
    owner: str
    effective_version: str | None = None
    latest_certified_version: str | None = None


@dataclass(frozen=True, slots=True)
class ExtensionVersion:
    """One `SHOW VERSIONS IN CORTEX EXTENSION` row, addressed by its system name."""

    name: str
    alias: str | None
    location: str
    is_default: bool = False
    certification_status: str | None = None


class CatalogPort(Protocol):
    """Read what exists in Snowflake and how it is defined, and who the session is.

    No method writes. Absence is a value where the method says so; a read Snowflake refuses
    raises `SnowflakePortError`.
    """

    def show_objects(self, object_type: str, scope: SchemaScope) -> tuple[ShowRow, ...]:
        """Return every object of one type in a schema, with its names as SHOW prints them.

        Built-in routines, which SHOW FUNCTIONS and SHOW PROCEDURES also list, are left out.
        Never writes.

        Returns:
            One row per object; empty when the schema holds none.

        Raises:
            SnowflakePortError: the SHOW failed, or SST does not observe that object type.
        """
        ...

    def show_grants(
        self,
        object_type: str,
        qualified_name: QualifiedName,
        routine_signature: tuple[str, ...] = (),
    ) -> tuple[GrantRow, ...]:
        """Return every grant on one object, as SHOW GRANTS lists them.

        `routine_signature` holds a procedure's or function's argument types, which address
        the overload; any other object ignores it. Never writes.

        Raises:
            SnowflakePortError: SHOW GRANTS failed, including for an object that does not exist.
        """
        ...

    def describe_marker(
        self,
        qualified_name: QualifiedName,
        object_type: str = "SEMANTIC VIEW",
    ) -> OwnershipMarker | None:
        """Return the SST ownership marker in the comment of one object of `object_type`.

        Never writes.

        Returns:
            The marker; None when no such object exists or its comment carries no marker.

        Raises:
            SnowflakePortError: the SHOW that finds the object failed.
        """
        ...

    def object_exists(self, object_type: str, qualified_name: QualifiedName) -> bool:
        """Report whether an object of one type exists under a qualified name.

        `object_type` is a type SHOW lists, `DATASET`, `TABLE` for a base table only, or
        `TABLE OR VIEW` for either. Never writes.

        Raises:
            SnowflakePortError: the lookup failed.
        """
        ...

    def dataset_exists(self, qualified_name: QualifiedName) -> bool:
        """Report whether a dataset exists under a qualified name, each part compared casefolded.

        Never writes.

        Raises:
            SnowflakePortError: SHOW DATASETS failed.
        """
        ...

    def table_columns(self, qualified_name: QualifiedName) -> tuple[tuple[str, str], ...] | None:
        """Return a base table's columns as `(name, type)` pairs, both uppercase, in table order.

        Never writes.

        Returns:
            The columns; None when no base table exists under the name.

        Raises:
            SnowflakePortError: the lookup or DESCRIBE TABLE failed.
        """
        ...

    def observe_stage(self, qualified_name: QualifiedName) -> StageObservation:
        """Report whether a stage exists and, when it does, the file format it declares.

        Never writes.

        Returns:
            `exists=False` when there is no stage; otherwise the stage's file format, as
            `describe_stage_file_format` returns it.

        Raises:
            SnowflakePortError: the lookup failed.
        """
        ...

    def describe_stage_file_format(self, qualified_name: QualifiedName) -> str | None:
        """Return the file format a stage declares, as text a caller compares once normalized.

        The text is the named file format the stage references, else its inline format
        options. Never writes.

        Returns:
            The file format; None when the stage declares none.

        Raises:
            SnowflakePortError: DESCRIBE STAGE failed, including for a stage that does not exist.
        """
        ...

    def stage_type(self, qualified_name: QualifiedName) -> str | None:
        """Return a stage's type as SHOW STAGES reports it, such as `INTERNAL NO CSE`.

        Never writes.

        Returns:
            The type; None when no stage exists under the name, and empty when SHOW reports none.

        Raises:
            SnowflakePortError: SHOW STAGES failed.
        """
        ...

    def observe_extension(self, qualified_name: QualifiedName) -> ExtensionObservation | None:
        """Return what SHOW CORTEX EXTENSIONS reports for one Cortex extension.

        Never writes.

        Returns:
            The extension's row; None when no extension exists under the name.

        Raises:
            SnowflakePortError: the SHOW failed.
        """
        ...

    def extension_versions(self, qualified_name: QualifiedName) -> tuple[ExtensionVersion, ...]:
        """Return a Cortex extension's versions as SHOW VERSIONS lists them.

        Never writes.

        Raises:
            SnowflakePortError: the listing failed, including for an extension that does not exist.
        """
        ...

    def agent_has_live_version(self, qualified_name: QualifiedName) -> bool:
        """Report whether an agent has a live version, which an update must commit first.

        Never writes.

        Raises:
            SnowflakePortError: SHOW VERSIONS IN AGENT failed.
        """
        ...

    def resolve_agent_version(self, qualified_name: QualifiedName, selector: str) -> str:
        """Resolve a version selector to the committed agent version it names, such as `VERSION$3`.

        A `VERSION$<n>` selector resolves to itself, uppercased, without a read; `committed`
        names the version aliased LAST, and `alias:<name>` the version with that alias.
        Never writes.

        Raises:
            SnowflakePortError: the selector names no committed version, or DESCRIBE AGENT failed.
        """
        ...

    def current_role(self) -> str:
        """Return the session's current role.

        Never writes.

        Raises:
            SnowflakePortError: the query failed.
        """
        ...

    def current_account_locator(self) -> str:
        """Return the locator of the account the session is connected to.

        Never writes.

        Raises:
            SnowflakePortError: the query failed.
        """
        ...


class ExecutionPort(Protocol):
    """Run SQL on the session: a statement whose rows the caller reads, or a script of writes.

    `query` and `query_in_context` raise when a statement fails; `execute_script` and
    `try_execute` report the failure in their result instead.
    """

    def query(self, sql: Sql, params: Sequence[object] | Mapping[str, object] | None = None) -> QueryResult:
        """Run one statement with `params` bound to its placeholders, and return its rows.

        Writes what the statement writes, and nothing else.

        Returns:
            The column names and rows; both empty for a statement that returns no rows.

        Raises:
            SnowflakePortError: the statement failed, or Snowflake could not be reached.
        """
        ...

    def query_in_context(
        self,
        scope: SchemaScope,
        sql: Sql,
        params: Sequence[object] | Mapping[str, object] | None = None,
    ) -> QueryResult:
        """Run one statement as `query` does, with `scope` as the session's current schema.

        For SQL that resolves unqualified names against the current schema. `scope` may stay
        current on the session afterwards.

        Raises:
            SnowflakePortError: switching to `scope` or the statement failed.
        """
        ...

    def execute_script(self, statements: Sequence[Sql]) -> ExecResult:
        """Run statements in order on one session, stopping at the first failure.

        Never raises for a SQL error; the result carries it.

        Args:
            statements: Complete statements, each executed separately (never split on ``;``).

        Returns:
            ``ok=True`` with one query id per statement, or ``ok=False`` with the error and the
            ids of the statements that already ran, which the caller must treat as a partial write.
        """
        ...

    def try_execute(self, sql: Sql) -> ExecResult:
        """Run one statement as a one-statement script, which reports a failure in its result.

        Never raises for a SQL error, as `execute_script` does not.
        """
        ...


class StagePort(Protocol):
    """Read and write files on a stage or in a Cortex extension version, by path.

    A stage path is `@<db>.<schema>.<stage>/<path>`; an extension version path is
    `snow://cortex_extension/<db>.<schema>.<name>/versions/<version>/<path>`. A path with an
    unsafe segment raises `SnowflakePortError` before anything runs.
    """

    def observe_staged_file(self, stage_path: str) -> StagedFileMetadata | None:
        """Return what LIST reports for the one file at a stage path.

        Never writes.

        Returns:
            The file's size, MD5, and modification time; None when no file is at the path.

        Raises:
            SnowflakePortError: the path is unsafe, LIST failed, or it reported the file twice
                or with an unreadable size.
        """
        ...

    def stage_file_exists(self, stage_path: str) -> bool:
        """Report whether a file is at a stage path, as `observe_staged_file` finds it.

        Never writes.

        Raises:
            SnowflakePortError: as `observe_staged_file` raises it.
        """
        ...

    def read_staged_file(self, stage_path: str) -> bytes | None:
        """Download the file at a stage path and return its bytes.

        Never writes to Snowflake, and keeps no local copy.

        Returns:
            The file's content; None when no file is at the path.

        Raises:
            SnowflakePortError: the path is unsafe, or the download failed.
        """
        ...

    def upload(self, stage_path: str, content: bytes) -> None:
        """Write a file to a stage path or into an extension's live version, replacing any there.

        Raises:
            SnowflakePortError: the path is unsafe, or the upload failed.
        """
        ...

    def list_location(self, location: str) -> tuple[str, ...]:
        """Return the paths of the files below a stage directory or an extension version, sorted.

        `location` is a stage path ending in `/`, or the root of an extension version; each
        path returned is relative to it. Never writes.

        Returns:
            The relative paths; empty when there are none.

        Raises:
            SnowflakePortError: the location is unsafe, or LIST failed.
        """
        ...


class ProfileRegistryPort(Protocol):
    """Keep CoCo Desktop's profile registry: one row per profile, keyed by CONFIG_NAME.

    A row write is guarded by the VERSION the caller observed and returns the rows it
    changed, so a count of 0 reveals a concurrent writer. Rows read back with their column
    names uppercase.
    """

    def ensure_profile_registry(self, qualified_name: QualifiedName) -> None:
        """Create the registry table with CoCo Desktop's columns unless it exists.

        Idempotent: an existing table is left as it is, whatever its columns.

        Raises:
            SnowflakePortError: the CREATE failed.
        """
        ...

    def read_profile_row(self, registry: QualifiedName, name: str) -> Mapping[str, object] | None:
        """Return the row for one profile, active or not.

        Never writes.

        Returns:
            The row; None when no row has the name.

        Raises:
            SnowflakePortError: the query failed, or more than one row has the name.
        """
        ...

    def merge_profile_row(
        self,
        registry: QualifiedName,
        row: Mapping[str, object],
        *,
        expected_version: str | None,
    ) -> int:
        """Insert a profile's row, or update it while its VERSION is still `expected_version`.

        The row is keyed by its CONFIG_NAME and left active.

        Args:
            expected_version: The VERSION the caller observed; None when it observed no row,
                so an existing row is left alone.

        Returns:
            The rows written: 0 when a stored row's VERSION is not `expected_version`.

        Raises:
            SnowflakePortError: the MERGE failed.
        """
        ...

    def deactivate_profile_row(self, registry: QualifiedName, name: str, *, expected_version: str) -> int:
        """Mark a profile's row inactive when it is active at `expected_version`.

        Returns:
            The rows changed: 0 when the row is absent, already inactive, or at another VERSION.

        Raises:
            SnowflakePortError: the UPDATE failed.
        """
        ...

    def desktop_profile_rows(self, registry: QualifiedName) -> tuple[Mapping[str, object], ...]:
        """Return the active rows by CONFIG_NAME, exactly as CoCo Desktop reads the registry.

        Never writes.

        Raises:
            SnowflakePortError: the query failed.
        """
        ...


class StatePort(Protocol):
    """Keep the state table: what apply recorded for each artifact, by target and artifact key.

    Plan trusts a live object only when its entry here matches the object's ownership marker.
    """

    def read_state(self, state_table: QualifiedName, target_name: str) -> Mapping[str, AppliedEntry] | None:
        """Return the entries apply recorded for one target, by artifact key.

        Never writes. An entry that does not decode raises rather than reading as absent.

        Returns:
            The entries; empty when the state table does not exist, and None when it exists
            but cannot be read, which the caller must not mistake for no entries.

        Raises:
            SnowflakePortError: checking whether the state table exists failed.
        """
        ...

    def write_state(
        self,
        state_table: QualifiedName,
        target_name: str,
        manifest_id: str,
        applied: Mapping[str, AppliedEntry],
    ) -> None:
        """Replace every entry recorded for one target with `applied`, in one transaction.

        Creates or migrates the table first, as `ensure_state_table` does. `manifest_id` is
        not stored: each entry records its own.

        Raises:
            SnowflakePortError: the write failed; its transaction rolls back, so the target
                keeps the entries it had.
        """
        ...

    def ensure_state_table(self, state_table: QualifiedName) -> None:
        """Create the state table unless it exists, and add any column an older table lacks.

        Idempotent.

        Raises:
            SnowflakePortError: the CREATE or ALTER failed.
        """
        ...

    def delete_state(self, state_table: QualifiedName, target_name: str, artifact_key: str) -> int:
        """Delete one artifact's entry for one target.

        Returns:
            The rows deleted: 0 when there was no entry.

        Raises:
            SnowflakePortError: the DELETE failed.
        """
        ...

    def upsert_state(
        self,
        state_table: QualifiedName,
        target_name: str,
        artifact_key: str,
        entry: AppliedEntry,
    ) -> int:
        """Insert or replace one artifact's entry for one target.

        Returns:
            The rows written, as the driver counts them.

        Raises:
            SnowflakePortError: the MERGE failed.
        """
        ...


class SnowflakePort(CatalogPort, ExecutionPort, StagePort, ProfileRegistryPort, StatePort, Protocol):
    """Everything application use cases reach Snowflake for: the union of the role protocols.

    It declares nothing of its own, so each method's contract lives on its role. An adapter
    satisfies it structurally, without subclassing it.
    """


class ClockPort(Protocol):
    """The time and run ids a use case reads, injected so that a test can fix them.

    Wall time stamps what state and eval records store; monotonic time measures durations.
    """

    def now_iso(self) -> str:
        """Return the current time in UTC as ISO 8601 text ending in `Z`.

        State and eval records store their timestamps in this form, and the state table reads
        its timestamps back in it, so an entry compares equal to the one SST wrote.
        """
        ...

    def monotonic_ms(self) -> int:
        """Return a monotonic clock reading in whole milliseconds, for measuring a duration.

        Only the difference between two readings means anything, and a later reading is
        never smaller.
        """
        ...

    def sleep(self, milliseconds: int) -> None:
        """Block the caller for `milliseconds`: a retry's backoff, or the wait between two polls."""
        ...

    def new_run_id(self) -> str:
        """Return a new identifier, unique to one run, which names it in state and as the lock holder."""
        ...


class StateStore(Protocol):
    """The local cache of one target's state, and the lock that lets one run at a time use it.

    The state table is authoritative; the cache only mirrors it. The lock is held by run id,
    and only its holder releases it.
    """

    @property
    def config_path(self) -> str:
        """Return the configuration path recorded in each `State` a use case builds for this store."""
        ...

    def read_local(self) -> State | None:
        """Return the cached state; None when there is no cache.

        Never writes. A cache that exists and cannot be used raises rather than reading as absent.

        Raises:
            ProjectError: the cache is unreadable (SST-MAN022) or declares a schema SST does not
                support (SST-MAN023).
            OSError: the cache exists and cannot be opened.
        """
        ...

    def write_local(self, value: State) -> None:
        """Replace the cache with `value` atomically, so a reader sees the old state or the new one.

        Creates the cache's directory when it is missing.

        Raises:
            OSError: the cache or its directory cannot be written.
        """
        ...

    def acquire_lock(self, run_id: str, *, break_stale: bool) -> tuple[bool, str | None, bool]:
        """Take the lock for `run_id` unless another run holds it, without waiting.

        A lock older than the store's time limit is stale, and `break_stale` takes it over; a
        lock whose holder or age cannot be read is never stale.

        Returns:
            `(acquired, holder, broke_stale)`: whether `run_id` now holds the lock; the run
            that held it, None when the lock was free or its holder is unreadable; and whether
            a stale lock was taken over.
        """
        ...

    def release_lock(self, run_id: str) -> None:
        """Release the lock when `run_id` holds it, and otherwise leave it alone.

        Idempotent: releasing a lock that is absent or held by another run does nothing.
        """
        ...
