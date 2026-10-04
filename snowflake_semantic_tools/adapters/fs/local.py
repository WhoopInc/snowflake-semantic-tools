"""Atomic local persistence for manifest, state, plan, and apply locks."""

from __future__ import annotations

import contextlib
import hashlib
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Generic, TypeVar

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.json_files import JsonFileError, parse_json, read_json_file
from snowflake_semantic_tools.adapters.paths import create_within, remove_within, write_within
from snowflake_semantic_tools.domain.diagnostics import D
from snowflake_semantic_tools.domain.file_names import file_name
from snowflake_semantic_tools.domain.model.config_schema import CONFIG_FILE
from snowflake_semantic_tools.domain.plan.recorded import RecordedObservation
from snowflake_semantic_tools.domain.state import Manifest, SavedPlan, State, StoredDocumentError, canonical_json

T = TypeVar("T")

# Far more than the run id and time a lock file records.
_LOCK_READ_BYTES = 4096


class JsonStore(Generic[T]):
    """One JSON document on disk, read through `parser` and replaced atomically in canonical form.

    A file that exists and cannot be used raises `ProjectError` with the code that names why:
    the one a `StoredDocumentError` carries, else the store's `unreadable_code`. Without such a
    code it raises `ValueError` instead. Writes stay inside `root` (by default the file's
    folder), as `adapters.paths.write_within` keeps them.
    """

    # The code for a file that exists and cannot be used, when the store has one.
    unreadable_code: str | None = None

    def __init__(self, path: Path, parser: Callable[[object], T], *, root: Path | None = None) -> None:
        self.path = path
        self.root = path.parent if root is None else root
        self._parser = parser

    def read(self) -> T | None:
        """Read and parse the file; None when it does not exist.

        Raises:
            ProjectError: The file cannot be used and the store can name why, with one diagnostic:
                the code of a `StoredDocumentError` the parser raised, else `unreadable_code` for
                bytes that are not JSON (or are too large or nested too deeply, as
                `adapters.json_files` bounds them) or a document the parser cannot read.
            ValueError: The file cannot be used and the store has no code for it; a document of the
                wrong shape is reported as `<path> has the wrong shape: <detail>`.
            OSError: The file exists and cannot be opened.
        """
        try:
            document = read_json_file(self.path)
        except FileNotFoundError:
            return None
        except JsonFileError as exc:
            if self.unreadable_code is None:
                raise ValueError(f"{self.path}: {exc}") from exc
            diagnostic = D(self.unreadable_code, path=str(self.path), detail=str(exc))
            raise ProjectError(diagnostic.message, diagnostics=(diagnostic,)) from exc
        try:
            return self._parser(document)
        except StoredDocumentError as exc:
            diagnostic = D(exc.code, path=str(self.path), **exc.context)
        except ValueError as exc:
            if self.unreadable_code is None:
                raise
            diagnostic = D(self.unreadable_code, path=str(self.path), detail=str(exc))
        except (KeyError, TypeError, OverflowError) as exc:
            # Valid JSON of the wrong shape: the parser indexed a key the file lacks or
            # converted a mistyped value. That is the file's fault, not an SST invariant.
            detail = _shape_problem(exc)
            if self.unreadable_code is None:
                raise ValueError(f"{self.path} has the wrong shape: {detail}") from exc
            diagnostic = D(self.unreadable_code, path=str(self.path), detail=detail)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))

    def write(self, value: object) -> None:
        """Replace the file atomically with `value` as canonical JSON and a trailing newline.

        A value with `as_dict()` is written as what that returns. The bytes go to a hidden
        temporary file beside the target, are synced, and replace the target in one rename, so a
        reader sees the old document or the new one; missing parent directories are created.

        Raises:
            UnsafeWrite: The file lies outside the store's root, or a link is on the way to it.
            OSError: The directory or the file cannot be written; the temporary file is removed.
            TypeError: The value holds something JSON cannot encode.
            ValueError: The value holds NaN or an infinity.
        """
        payload = value.as_dict() if hasattr(value, "as_dict") else value
        write_within(self.root, self.path, canonical_json(payload) + b"\n")


class ManifestFileStore(JsonStore[Manifest]):
    """The compiled manifest file, read as a `Manifest` whose id must be the hash of its content.

    `read` raises `ProjectError` for a manifest that exists and cannot be used.

    Diagnostics:
        SST-MAN002: when the file is not JSON, not an object, or not a manifest's shape.
        SST-MAN003: when it lacks an integer `schema_version` or an `artifacts` object.
        SST-MAN203: when its schema is newer than this binary reads.
        SST-MAN202: when its older schema has no migration.
        SST-MAN005: when its `manifest_id` is not the hash of its content.
    """

    unreadable_code = "SST-MAN002"

    def __init__(self, path: Path, *, root: Path | None = None) -> None:
        super().__init__(path, Manifest.from_dict, root=root)


class PlanFileStore(JsonStore[SavedPlan]):
    """A saved plan file, read as a `SavedPlan` whose `plan_id` must be the hash of its content.

    No registered code names an unusable plan, so `read` raises `ValueError` for a plan that
    exists and cannot be used, never `ProjectError`.
    """

    def __init__(self, path: Path, *, root: Path | None = None) -> None:
        super().__init__(path, SavedPlan.from_dict, root=root)


STATE_FILE_GLOB = "state.*.json"


def observation_file(directory: Path, target_name: str) -> Path:
    """Return where a plan records what it read of one target, in a build directory.

    The target's name is written as `domain.file_names.file_name` writes it, so no name, one
    holding a separator, `..`, a NUL, or a leading dot among them, places the file elsewhere.
    """
    return directory / f"observation.{file_name(target_name)}.json"


class ObservationFileStore(JsonStore[RecordedObservation]):
    """The observation `sst plan` recorded of one target, which `--use-cached-state` plans from.

    `read` raises `ProjectError` for a file that exists and cannot be used.

    Diagnostics:
        SST-PRT009: when the file is not JSON, or not a recorded observation of this schema.
        SST-MAN022, SST-MAN023: when the state it records is not a state document of a
            schema this release reads.
    """

    unreadable_code = "SST-PRT009"

    def __init__(self, path: Path, *, root: Path | None = None) -> None:
        super().__init__(path, RecordedObservation.from_dict, root=root)


def state_file(target_dir: Path, target_name: str) -> Path:
    """Return the local state file for one target, under the project's SST target directory.

    The target's name is written as `observation_file` writes it.
    """
    return target_dir / f"state.{file_name(target_name)}.json"


class StateFileStore(JsonStore[State]):
    """One target's local state file, which caches the state table, and the apply lock beside it.

    The lock is the sibling file `<state file>.lock`, created exclusively and holding the run id
    and the time it was taken; it goes stale `LOCK_TTL_SECONDS` (30 minutes) after that time, by
    the `now` clock, which is UTC by default. Removing it, to release or to take over a stale
    one, first takes that claim's break token, `<lock>.<digest>.break`, so no run ever removes
    a claim it did not judge. `config_path` is only reported back, never read.
    """

    LOCK_TTL_SECONDS = 30 * 60
    unreadable_code = "SST-MAN022"

    def __init__(
        self,
        path: Path,
        *,
        config_path: str = CONFIG_FILE,
        now: Callable[[], datetime] | None = None,
        root: Path | None = None,
    ) -> None:
        super().__init__(path, State.from_dict, root=root)
        self._config_path = config_path
        self._lock_path = path.with_suffix(path.suffix + ".lock")
        self._now = now or (lambda: datetime.now(UTC))

    @property
    def config_path(self) -> str:
        """Return the configuration path this store was given, which the states built for it record."""
        return self._config_path

    @property
    def location(self) -> str:
        """Return the state file's path."""
        return str(self.path)

    def read_local(self) -> State | None:
        """Read the state file; None when there is none.

        Raises:
            ProjectError: The file exists and cannot be used; its one diagnostic is listed below.
            OSError: The file exists and cannot be opened.

        Diagnostics:
            SST-MAN022: when the file is not JSON, not an object, or not a state document's shape.
            SST-MAN023: when its schema is not an integer, is newer, or has no migration.
        """
        return self.read()

    def write_local(self, value: State) -> None:
        """Replace the state file with `value` atomically, as `JsonStore.write` does."""
        self.write(value)

    def acquire_lock(self, run_id: str, *, break_stale: bool) -> tuple[bool, str | None, bool]:
        """Take the apply lock for `run_id` unless another run holds it, without waiting.

        The lock file is created exclusively, so of two runs racing for a free lock one wins.
        With `break_stale`, a stale lock is taken over; a lock file that cannot be read, or
        records no time, is never stale. Taking one over never removes a lock that is not the
        stale one judged: only the holder of that lock's break token may remove it, as
        `_removing` explains, and the run then creates its own exclusively like any other.

        Returns:
            `(True, None, False)` when the lock was free; `(True, holder, True)` when a stale lock
            was taken over; `(False, holder, False)` otherwise, including when another run took
            the lock, or a stale one over, first. `holder` is the run id the lock file records,
            None when it records none or cannot be read.

        Raises:
            OSError: The lock file cannot be created, written, or removed.
        """
        now = self._now()
        payload = canonical_json({"run_id": run_id, "created_at": now.isoformat()})
        if self._create_lock(payload):
            return True, None, False
        instance = self._lock_bytes()
        if instance is None:
            # Released since; one more exclusive create decides, as for a free lock.
            return (True, None, False) if self._create_lock(payload) else (False, self._lock_status(now)[0], False)
        holder, stale = _lock_status(instance, now, self.LOCK_TTL_SECONDS)
        if not (stale and break_stale):
            return False, holder, False
        with self._removing(instance) as removable:
            if not removable:
                return False, self._lock_status(now)[0], False
            remove_within(self.root, self._lock_path)
            if self._create_lock(payload):
                return True, holder, True
        return False, self._lock_status(now)[0], False

    def _create_lock(self, payload: bytes) -> bool:
        """Create the lock file exclusively with `payload`; False when one exists."""
        try:
            create_within(self.root, self._lock_path, payload)
        except FileExistsError:
            return False
        return True

    def _lock_bytes(self) -> bytes | None:
        """Read the lock file's bytes, at most `_LOCK_READ_BYTES` and one more; None when there is none.

        A lock SST writes is far shorter. A longer one is cut there, so it never parses: it is held
        by no run SST can name and is never stale, like any lock file that cannot be read.
        """
        try:
            with self._lock_path.open("rb") as handle:
                return handle.read(_LOCK_READ_BYTES + 1)
        except FileNotFoundError:
            return None

    @contextlib.contextmanager
    def _removing(self, instance: bytes) -> Iterator[bool]:
        """Hold the right to remove the lock file whose bytes are `instance`; yield whether it is still there.

        The right is a break token beside the lock, named by a digest of `instance` and created
        exclusively, so one caller at a time holds it; a caller that cannot take it yields
        False. A lock file's bytes name one claim, run id and time, so while the token is held
        the lock file is `instance` until its holder removes it: nobody else may remove that
        claim, and nobody can create a lock file while one exists. Reading the file, then
        removing it, therefore removes exactly the claim judged. The token is removed when the
        block ends; a process killed in between leaves it, and that claim can then be released
        or broken only once the token file is deleted by hand.

        Raises:
            OSError: the token cannot be created for another reason than that it exists.
        """
        token = self._lock_path.with_name(f"{self._lock_path.name}.{hashlib.sha256(instance).hexdigest()[:16]}.break")
        try:
            create_within(self.root, token, b"")
        except FileExistsError:
            yield False
            return
        try:
            yield self._lock_bytes() == instance
        finally:
            remove_within(self.root, token)

    def _lock_status(self, now: datetime) -> tuple[str | None, bool]:
        """Read the lock file's holder, and whether it is stale; `(None, False)` when it cannot be read."""
        instance = self._lock_bytes()
        return _lock_status(instance, now, self.LOCK_TTL_SECONDS) if instance is not None else (None, False)

    def release_lock(self, run_id: str) -> None:
        """Delete the lock file when `run_id` holds the lock; otherwise leave it as it is.

        A lock another run is taking over at that moment is left to that run.
        """
        instance = self._lock_bytes()
        if instance is None or _lock_status(instance, self._now(), self.LOCK_TTL_SECONDS)[0] != run_id:
            return
        with self._removing(instance) as removable:
            if removable:
                remove_within(self.root, self._lock_path)


def _lock_status(instance: bytes, now: datetime, ttl_seconds: int) -> tuple[str | None, bool]:
    """Return the holder a lock file's bytes record, and whether its claim is older than `ttl_seconds`.

    Bytes that do not decode, are longer than `_LOCK_READ_BYTES`, or record no time, name no holder
    and are never stale.
    """
    if len(instance) > _LOCK_READ_BYTES:
        return None, False
    try:
        value = parse_json(instance)
        holder = value.get("run_id") if isinstance(value, dict) else None
        created = value.get("created_at") if isinstance(value, dict) else None
        timestamp = datetime.fromisoformat(created) if isinstance(created, str) else None
    except ValueError:
        return None, False
    stale = timestamp is not None and (now - timestamp).total_seconds() > ttl_seconds
    return holder if isinstance(holder, str) else None, stale


def _shape_problem(exc: Exception) -> str:
    # A KeyError's own text is only the quoted key.
    return f"missing key {exc.args[0]!r}" if isinstance(exc, KeyError) and exc.args else str(exc)
