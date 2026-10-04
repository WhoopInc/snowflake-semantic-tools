"""The in-memory local state store the application use cases take, with its lock.

`InMemoryStateStore` is a real implementation that records each write, not a mock. The
Snowflake ports are `tests.helpers.snowflake_fake`; the clock is `tests.helpers.clocks`.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.state import State


class InMemoryStateStore:
    def __init__(self, state: State | None = None) -> None:
        self.state = state
        self.locked = False
        self.holder: str | None = None
        self.stale = False
        self.writes: list[State] = []

    @property
    def config_path(self) -> str:
        return "sst_config.yml"

    @property
    def location(self) -> str:
        return "target/sst/state.verify.json"

    def read_local(self) -> State | None:
        return self.state

    def write_local(self, value: State) -> None:
        self.state = value
        self.writes.append(value)

    def acquire_lock(self, run_id: str, *, break_stale: bool) -> tuple[bool, str | None, bool]:
        if not self.locked:
            self.locked = True
            self.holder = run_id
            return True, None, False
        if self.stale and break_stale:
            old = self.holder
            self.holder = run_id
            return True, old, True
        return False, self.holder, False

    def release_lock(self, run_id: str) -> None:
        if self.holder == run_id:
            self.locked = False
            self.holder = None
