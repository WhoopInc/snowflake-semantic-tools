"""The system clock: the one place outside the connector that reads time or makes run ids."""

from __future__ import annotations

import time
import uuid
from datetime import UTC, datetime

from snowflake_semantic_tools.domain.ports.snowflake import ClockPort


class SystemClock(ClockPort):
    """`ClockPort` on the real system: UTC wall time, `time.monotonic`, and real sleeps.

    `now_iso` carries microseconds, which `datetime.isoformat` leaves out when they are zero,
    and a run id is a random UUID in its 36-character hyphenated form. It holds no state.
    """

    def now_iso(self) -> str:
        return datetime.now(UTC).isoformat().replace("+00:00", "Z")

    def monotonic_ms(self) -> int:
        return int(time.monotonic() * 1000)

    def sleep(self, milliseconds: int) -> None:
        time.sleep(milliseconds / 1000)

    def new_run_id(self) -> str:
        return str(uuid.uuid4())
