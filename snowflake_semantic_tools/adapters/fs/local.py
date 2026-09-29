"""Atomic local persistence for manifest, state, plan, and apply locks."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Generic, TypeVar

from ...domain.state.model import Manifest, SavedPlan, State, canonical_json

T = TypeVar("T")


class JsonStore(Generic[T]):
    def __init__(self, path: Path, parser: Callable[[object], T]) -> None:
        self.path = path
        self._parser = parser

    def read(self) -> T | None:
        try:
            raw = self.path.read_bytes()
        except FileNotFoundError:
            return None
        return self._parser(json.loads(raw))

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
    def __init__(self, path: Path) -> None:
        super().__init__(path, Manifest.from_dict)


class PlanFileStore(JsonStore[SavedPlan]):
    def __init__(self, path: Path) -> None:
        super().__init__(path, _saved_plan_from_dict)


class StateFileStore(JsonStore[State]):
    LOCK_TTL_SECONDS = 30 * 60

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


def _saved_plan_from_dict(value: object) -> SavedPlan:
    from ...domain.model.identifier import TargetIdentity
    from ...domain.state.model import PLAN_SCHEMA_VERSION, SavedChange, content_hash

    if not isinstance(value, dict):
        raise ValueError("saved plan must be an object")
    version = value.get("schema_version")
    if version != PLAN_SCHEMA_VERSION:
        raise ValueError(f"saved plan schema {version} is not supported")
    raw_changes = value.get("changes")
    if not isinstance(raw_changes, list):
        raise ValueError("saved plan changes must be a list")
    changes = []
    for item in raw_changes:
        if not isinstance(item, dict):
            raise ValueError("saved plan change must be an object")
        changes.append(
            SavedChange(
                key=str(item["key"]),
                artifact_type=str(item["artifact_type"]),
                action=str(item["action"]),
                reason=str(item["reason"]),
                target=str(item["target"]) if item.get("target") is not None else None,
                fingerprint=(str(item["fingerprint"]) if item.get("fingerprint") is not None else None),
                previous_marker=(str(item["previous_marker"]) if item.get("previous_marker") is not None else None),
                statement_hashes=tuple(str(element) for element in item.get("statement_hashes", [])),
                depends_on=tuple(str(element) for element in item.get("depends_on", [])),
                order=int(item["order"]),
                component_fingerprints=tuple(
                    sorted(
                        (str(key), str(element))
                        for key, element in _saved_mapping(item.get("component_fingerprints")).items()
                    )
                ),
                physical_resources=tuple(
                    (str(element["object_type"]), str(element["qualified_name"]))
                    for element in _saved_resources(item.get("physical_resources"))
                ),
                prune_executable=bool(item.get("prune_executable", True)),
            )
        )
    raw_selection = value.get("selection", {})
    if not isinstance(raw_selection, dict):
        raise ValueError("saved plan selection must be an object")
    plan = SavedPlan(
        schema_version=version,
        plan_id=str(value.get("plan_id") or ""),
        manifest_id=str(value.get("manifest_id") or ""),
        target=TargetIdentity.from_dict(value.get("target")),
        observation_at=str(value.get("observation_at") or ""),
        observation_fingerprint=str(value.get("observation_fingerprint") or ""),
        changes=tuple(changes),
        selected=tuple(str(item) for item in raw_selection.get("selected", [])),
        excluded=tuple(str(item) for item in raw_selection.get("excluded", [])),
        include_prune=bool(raw_selection.get("include_prune", False)),
        partial=bool(raw_selection.get("partial", False)),
    )
    recorded = str(value.get("plan_id") or "")
    expected_body = dict(value)
    expected_body.pop("plan_id", None)
    expected = content_hash(expected_body)
    if recorded != expected:
        raise ValueError(f"saved plan id {recorded}, recomputed {expected}")
    return plan


def _saved_mapping(value: object) -> dict[str, object]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("saved plan component_fingerprints must be an object")
    return value


def _saved_resources(value: object) -> list[dict[str, object]]:
    if value is None:
        return []
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        raise ValueError("saved plan physical_resources must be objects")
    return value
