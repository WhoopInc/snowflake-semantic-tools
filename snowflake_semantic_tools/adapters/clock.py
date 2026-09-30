"""The system clock: the one place outside the connector that reads time or makes run ids."""

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
