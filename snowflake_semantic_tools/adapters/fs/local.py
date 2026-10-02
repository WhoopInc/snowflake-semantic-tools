"""Atomic local persistence for manifest, state, plan, and apply locks."""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Generic, TypeVar

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.diagnostics import D
from snowflake_semantic_tools.domain.model.config_schema import CONFIG_FILE
from snowflake_semantic_tools.domain.state import Manifest, SavedPlan, State, StoredDocumentError, canonical_json

T = TypeVar("T")


def write_bytes_atomic(path: Path, data: bytes) -> None:
    """Replace a file with `data` in one rename, so a reader sees the old bytes or the new ones.

    The bytes go to a hidden temporary file beside the target, are synced, and replace the
    target; the directory is synced after. Missing parent directories are created.

    Raises:
        OSError: The directory or the file cannot be written; the temporary file is removed.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, raw_path = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temp = Path(raw_path)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temp.unlink(missing_ok=True)


def write_text_atomic(path: Path, text: str) -> None:
    """Replace a file with `text` encoded as UTF-8, atomically, as `write_bytes_atomic` does.

    Raises:
        OSError: The directory or the file cannot be written.
    """
    write_bytes_atomic(path, text.encode("utf-8"))


class JsonStore(Generic[T]):
    """One JSON document on disk, read through `parser` and replaced atomically in canonical form.

    A file that exists and cannot be used raises `ProjectError` with the code that names why:
    the one a `StoredDocumentError` carries, else the store's `unreadable_code`. Without such a
    code it raises `ValueError` instead.
    """

    # The code for a file that exists and cannot be used, when the store has one.
    unreadable_code: str | None = None

    def __init__(self, path: Path, parser: Callable[[object], T]) -> None:
        self.path = path
        self._parser = parser

    def read(self) -> T | None:
        """Read and parse the file; None when it does not exist.

        Raises:
            ProjectError: The file cannot be used and the store can name why, with one diagnostic:
                the code of a `StoredDocumentError` the parser raised, else `unreadable_code` for
                bytes that are not JSON or a document the parser cannot read.
            ValueError: The file cannot be used and the store has no code for it; a document of the
                wrong shape is reported as `<path> has the wrong shape: <detail>`.
            OSError: The file exists and cannot be opened.
        """
        try:
            raw = self.path.read_bytes()
        except FileNotFoundError:
            return None
        try:
            return self._parser(json.loads(raw))
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
            OSError: The directory or the file cannot be written; the temporary file is removed.
            TypeError: The value holds something JSON cannot encode.
            ValueError: The value holds NaN or an infinity.
        """
        payload = value.as_dict() if hasattr(value, "as_dict") else value
        write_bytes_atomic(self.path, canonical_json(payload) + b"\n")


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

    def __init__(self, path: Path) -> None:
        super().__init__(path, Manifest.from_dict)


class PlanFileStore(JsonStore[SavedPlan]):
    """A saved plan file, read as a `SavedPlan` whose `plan_id` must be the hash of its content.

    No registered code names an unusable plan, so `read` raises `ValueError` for a plan that
    exists and cannot be used, never `ProjectError`.
    """

    def __init__(self, path: Path) -> None:
        super().__init__(path, SavedPlan.from_dict)


STATE_FILE_GLOB = "state.*.json"


def state_file(target_dir: Path, target_name: str) -> Path:
    """Return the local state file for one target, under the project's SST target directory."""
    return target_dir / f"state.{target_name}.json"


class StateFileStore(JsonStore[State]):
    """One target's local state file, which caches the state table, and the apply lock beside it.

    The lock is the sibling file `<state file>.lock`, created exclusively and holding the run id
    and the time it was taken; it goes stale `LOCK_TTL_SECONDS` (30 minutes) after that time, by
    the `now` clock, which is UTC by default. `config_path` is only reported back, never read.
    """

    LOCK_TTL_SECONDS = 30 * 60
    unreadable_code = "SST-MAN022"

    def __init__(
        self,
        path: Path,
        *,
        config_path: str = CONFIG_FILE,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        super().__init__(path, State.from_dict)
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

        The lock file is created exclusively, so of two runs racing for a free lock one wins. With
        `break_stale`, a stale lock is deleted and taken over; a lock file that cannot be read, or
        records no time, is never stale.

        Returns:
            `(True, None, False)` when the lock was free; `(True, holder, True)` when a stale lock
            was taken over; `(False, holder, False)` otherwise. `holder` is the run id the lock
            file records, None when it records none or cannot be read.

        Raises:
            OSError: The lock file cannot be created or written, including `FileExistsError` when
                another run takes the lock between this one deleting a stale lock and taking it.
        """
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        now = self._now()
        payload = canonical_json({"run_id": run_id, "created_at": now.isoformat()})
        try:
            descriptor = os.open(self._lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            holder, stale = self._lock_status(now)
            if stale and break_stale:
                self._lock_path.unlink(missing_ok=True)
                descriptor = os.open(self._lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                os.write(descriptor, payload)
                os.fsync(descriptor)
                os.close(descriptor)
                return True, holder, True
            return False, holder, False
        os.write(descriptor, payload)
        os.fsync(descriptor)
        os.close(descriptor)
        return True, None, False

    def _lock_status(self, now: datetime) -> tuple[str | None, bool]:
        try:
            value = json.loads(self._lock_path.read_text(encoding="utf-8"))
            holder = value.get("run_id") if isinstance(value, dict) else None
            created = value.get("created_at") if isinstance(value, dict) else None
            timestamp = datetime.fromisoformat(created) if isinstance(created, str) else None
        except (OSError, json.JSONDecodeError, ValueError):
            return None, False
        stale = timestamp is not None and (now - timestamp).total_seconds() > self.LOCK_TTL_SECONDS
        return holder if isinstance(holder, str) else None, stale

    def release_lock(self, run_id: str) -> None:
        """Delete the lock file when `run_id` holds the lock; otherwise leave it as it is."""
        holder, _ = self._lock_status(self._now())
        if holder == run_id:
            self._lock_path.unlink(missing_ok=True)


def _shape_problem(exc: Exception) -> str:
    # A KeyError's own text is only the quoted key.
    return f"missing key {exc.args[0]!r}" if isinstance(exc, KeyError) and exc.args else str(exc)
