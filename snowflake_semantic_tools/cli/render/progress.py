"""Progress on stderr while a long command runs: one live line on a terminal, plain lines in CI.

The display reads only what `RUN_TIMINGS` records -- the phase in flight, the Snowflake
statements and logins finished and in flight, the dbt processes -- and what the `Progress`
port counts, so no use case prints anything. Nothing shows for the first second, so a fast run
looks as it did. While the live line shows, every other write to stderr clears it first and
it is drawn again after, and `paused` stops it for a prompt. A phase that took a while leaves
one finished line behind (`✓ observed Snowflake · 12.3s`).

Without a terminal the display prints only where `$CI` is set: a line as each slow phase ends,
and one every `HEARTBEAT_SECONDS` while a phase runs, so a CI log shows the run is alive.
"""

from __future__ import annotations

import contextlib
import sys
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from typing import TextIO, cast

from snowflake_semantic_tools.adapters.timings import RUN_TIMINGS, LiveCounts, RunTimings
from snowflake_semantic_tools.cli.render.style import Style
from snowflake_semantic_tools.cli.render.terminal import Terminal

DELAY_SECONDS = 1.0
INTERVAL_SECONDS = 0.1
HEARTBEAT_SECONDS = 15.0
# A phase shorter than this leaves no finished line behind.
_FINISHED_AT = 1.0
# Each phase `RUN_TIMINGS` names: what the live line says while it runs, and once it ended.
PHASE_LABELS: dict[str, tuple[str, str]] = {
    "load": ("reading the project", "read the project"),
    "state_read": ("reading state", "read state"),
    "composites": ("reading agents and evals", "read agents and evals"),
    "observe": ("observing Snowflake", "observed Snowflake"),
    "classify": ("deciding the changes", "decided the changes"),
    "validate": ("checking against Snowflake", "checked against Snowflake"),
    "lock": ("taking the run lock", "took the run lock"),
    "publish": ("publishing", "published"),
    "state_write": ("recording state", "recorded state"),
    "smoke": ("running smoke probes", "ran smoke probes"),
    "disconnect": ("closing sessions", "closed sessions"),
}
_ACTIVE: list[ProgressDisplay] = []


class ProgressDisplay:
    """The progress of one run, drawn on stderr; also the `Progress` port its use cases count into.

    Args:
        live: Redraw one line in place (stderr is a terminal); False prints plain lines.
        stream: Where progress goes; the process's stderr when None, read when the display starts.
    """

    def __init__(
        self,
        style: Style,
        *,
        live: bool,
        stream: TextIO | None = None,
        timings: RunTimings = RUN_TIMINGS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._style = style
        self._live = live
        self._stream = stream
        self._timings = timings
        self._clock = clock
        self._lock = threading.RLock()
        self._started = 0.0
        self._shown = False
        self._drawn = ""
        self._paused = 0
        self._frame = 0
        self._counts: dict[str, tuple[int, int]] = {}
        self._phase_started: dict[str, float] = {}
        self._last_heartbeat = 0.0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._saved_stderr: TextIO | None = None

    def start(self) -> None:
        """Start showing progress: listen to the run's phases, and draw from a background thread."""
        self._stream = self._stream or sys.stderr
        self._started = self._last_heartbeat = self._clock()
        self._timings.enable()
        self._timings.watch(self)
        _ACTIVE.append(self)
        if self._live:
            self._saved_stderr = sys.stderr
            sys.stderr = cast(TextIO, _ClearingStream(sys.stderr, self))
        self._thread = threading.Thread(target=self._loop, name="sst-progress", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop showing progress, clearing the live line; safe to call more than once."""
        self._stop.set()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=1.0)
        self._thread = None
        self._timings.unwatch(self)
        with self._lock:
            self._clear()
            if self._saved_stderr is not None:
                sys.stderr = self._saved_stderr
                self._saved_stderr = None
        if self in _ACTIVE:
            _ACTIVE.remove(self)

    def line(self, text: str) -> None:
        """Print `text` as a finished line above the live one, which is drawn again after."""
        with self._lock:
            self._clear()
            self._write(text + "\n")
            self._redraw()

    def write_through(self, stream: TextIO, text: str) -> int:
        """Write `text` to `stream` with the live line cleared, drawing it again after a full line."""
        with self._lock:
            self._clear()
            written = stream.write(text)
            if text.endswith("\n"):
                self._redraw()
            return written

    @contextlib.contextmanager
    def paused(self) -> Iterator[None]:
        """Clear the live line and draw nothing until the block ends, as a prompt needs."""
        with self._lock:
            self._paused += 1
            self._clear()
        try:
            yield
        finally:
            with self._lock:
                self._paused -= 1

    def expect(self, step: str, total: int) -> None:
        """Count `step`'s items from zero toward `total`."""
        with self._lock:
            self._counts[step] = (0, total)

    def advance(self, step: str, count: int = 1) -> None:
        """Count `count` more of `step`'s items done."""
        with self._lock:
            done, total = self._counts.get(step, (0, 0))
            self._counts[step] = (done + count, total)

    def phase_started(self, name: str) -> None:
        """Note when phase `name` started, for its heartbeat lines."""
        with self._lock:
            self._phase_started[name] = self._clock()

    def phase_finished(self, name: str, seconds: float) -> None:
        """Leave a finished line for a phase that took a while, once progress is showing."""
        if seconds < _FINISHED_AT or (self._live and not self._shown):
            return
        label = PHASE_LABELS.get(name, (name, name))[1]
        counted = self._counts.get(name)
        detail = f"{self._style.sep}{counted[0]}/{counted[1]}" if counted and counted[1] else ""
        mark = self._style(self._style.symbol("ok"), "success")
        self.line(f"{mark} {label}{detail}{self._style.sep}{seconds:.1f}s")

    def describe(self, counts: LiveCounts) -> str:
        """Return the live line's text, without its spinner: what runs now, its counts, the time.

        Example:
            `observing Snowflake · 151 queries · 16 sessions · 12.3s`
        """
        sep = self._style.sep
        if counts.phases:
            label = PHASE_LABELS.get(counts.phases[-1], (counts.phases[-1].replace("_", " "), ""))[0]
        elif counts.dbt:
            label = "running dbt"
        elif counts.connecting:
            label = "connecting to Snowflake"
        else:
            label = "working"
        parts = [label]
        counted = self._counts.get(counts.phases[-1]) if counts.phases else None
        if counted and counted[1]:
            parts.append(f"{counted[0]}/{counted[1]}")
        if counts.queries:
            parts.append(f"{counts.queries} {'query' if counts.queries == 1 else 'queries'}")
        if counts.connects > 1:
            parts.append(f"{counts.connects} sessions")
        parts.append(f"{self._clock() - self._started:.1f}s")
        return sep.join(parts)

    def _loop(self) -> None:
        while not self._stop.wait(INTERVAL_SECONDS):
            now = self._clock()
            if now - self._started < DELAY_SECONDS:
                continue
            with self._lock:
                if self._live:
                    self._shown = True
                    self._redraw()
                elif now - self._last_heartbeat >= HEARTBEAT_SECONDS:
                    self._last_heartbeat = now
                    counts = self._timings.live()
                    if counts.phases or counts.dbt or counts.connecting:
                        self._write(f"... {self.describe(counts)}\n")

    def _redraw(self) -> None:
        """Draw the live line again; the caller holds the lock."""
        if not self._live or not self._shown or self._paused or self._stop.is_set():
            return
        self._frame = (self._frame + 1) % len(self._style.spinner)
        spinner = self._style(self._style.spinner[self._frame], "info")
        text = self._fit(f"{spinner} {self.describe(self._timings.live())}")
        self._write(f"\r{text}\x1b[K")
        self._drawn = text

    def _clear(self) -> None:
        """Erase the live line, if one is drawn; the caller holds the lock."""
        if self._drawn:
            self._write("\r\x1b[K")
            self._drawn = ""

    def _fit(self, text: str) -> str:
        width = self._style.terminal.width - 1
        return text if len(text) <= width else text[: width - 3] + "..."

    def _write(self, text: str) -> None:
        # The stream was read before stderr was wrapped, so this never writes through the wrapper.
        stream = self._stream
        if stream is None:
            return
        with contextlib.suppress(OSError, ValueError):
            stream.write(text)
            stream.flush()


class _ClearingStream:
    """stderr while a live line shows: each write clears the line first, and it is drawn again after."""

    def __init__(self, wrapped: TextIO, display: ProgressDisplay) -> None:
        self._wrapped = wrapped
        self._display = display

    def write(self, text: str) -> int:
        return self._display.write_through(self._wrapped, text)

    def flush(self) -> None:
        self._wrapped.flush()

    def __getattr__(self, name: str) -> object:
        return getattr(self._wrapped, name)


def progress_display(terminal: Terminal, environ: Mapping[str, str]) -> ProgressDisplay | None:
    """Return the display a run shows: live on a terminal, plain lines in CI, else None."""
    if terminal.live:
        return ProgressDisplay(Style(terminal), live=True)
    if environ.get("CI", "") not in ("", "0", "false"):
        return ProgressDisplay(Style(terminal), live=False)
    return None


def active_display() -> ProgressDisplay | None:
    """Return the progress display this run shows; None when it shows none."""
    return _ACTIVE[-1] if _ACTIVE else None


@contextlib.contextmanager
def progress_paused() -> Iterator[None]:
    """Stop drawing progress for the block, as a prompt on stderr needs; a no-op with none shown."""
    display = active_display()
    if display is None:
        yield
        return
    with display.paused():
        yield
