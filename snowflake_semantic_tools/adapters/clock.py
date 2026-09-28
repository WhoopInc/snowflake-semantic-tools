"""System and deterministic clock adapters."""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone


class SystemClock:
    def now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    def monotonic_ms(self) -> int:
        return int(time.monotonic() * 1000)

    def sleep(self, milliseconds: int) -> None:
        time.sleep(milliseconds / 1000)

    def new_run_id(self) -> str:
        return str(uuid.uuid4())


class FixedClock:
    def __init__(self, instant: str = "2026-01-01T00:00:00Z") -> None:
        self.instant = instant
        self.milliseconds = 0
        self.sleeps: list[int] = []

    def now_iso(self) -> str:
        return self.instant

    def monotonic_ms(self) -> int:
        return self.milliseconds

    def sleep(self, milliseconds: int) -> None:
        self.sleeps.append(milliseconds)
        self.milliseconds += milliseconds

    def new_run_id(self) -> str:
        return "00000000-0000-0000-0000-000000000001"
