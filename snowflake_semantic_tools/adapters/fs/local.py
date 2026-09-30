"""Atomic local persistence for manifest, state, plan, and apply locks."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Generic, TypeVar

from ...domain.model.diagnostic import D
from ...domain.state.model import Manifest, SavedPlan, State, StoredDocumentError, canonical_json
from ..project import ProjectError

T = TypeVar("T")


class JsonStore(Generic[T]):
    # The code for a file that exists and cannot be used, when the store has one.
    unreadable_code: str | None = None

    def __init__(self, path: Path, parser: Callable[[object], T]) -> None:
        self.path = path
        self._parser = parser

    def read(self) -> T | None:
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
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = value.as_dict() if hasattr(value, "as_dict") else value
        data = canonical_json(payload) + b"\n"
        fd, raw_path = tempfile.mkstemp(prefix=f".{self.path.name}.", dir=self.path.parent)
        temp = Path(raw_path)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp, self.path)
            directory_fd = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            temp.unlink(missing_ok=True)


class ManifestFileStore(JsonStore[Manifest]):
    unreadable_code = "SST-MAN002"

    def __init__(self, path: Path) -> None:
        super().__init__(path, Manifest.from_dict)


class PlanFileStore(JsonStore[SavedPlan]):
    def __init__(self, path: Path) -> None:
        super().__init__(path, SavedPlan.from_dict)


STATE_FILE_GLOB = "state.*.json"


def state_file(target_dir: Path, target_name: str) -> Path:
    """Return the local state file for one target, under the project's SST target directory."""
    return target_dir / f"state.{target_name}.json"


class StateFileStore(JsonStore[State]):
    LOCK_TTL_SECONDS = 30 * 60
    unreadable_code = "SST-MAN022"

    def __init__(
        self,
        path: Path,
        *,
        config_path: str = "sst_config.yml",
        now: Callable[[], datetime] | None = None,
    ) -> None:
        super().__init__(path, State.from_dict)
        self._config_path = config_path
        self._lock_path = path.with_suffix(path.suffix + ".lock")
        self._now = now or (lambda: datetime.now(timezone.utc))

    @property
    def config_path(self) -> str:
        return self._config_path

    def read_local(self) -> State | None:
        return self.read()

    def write_local(self, value: State) -> None:
        self.write(value)

    def acquire_lock(self, run_id: str, *, break_stale: bool) -> tuple[bool, str | None, bool]:
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
        holder, _ = self._lock_status(self._now())
        if holder == run_id:
            self._lock_path.unlink(missing_ok=True)


def _shape_problem(exc: Exception) -> str:
    # A KeyError's own text is only the quoted key.
    return f"missing key {exc.args[0]!r}" if isinstance(exc, KeyError) and exc.args else str(exc)
