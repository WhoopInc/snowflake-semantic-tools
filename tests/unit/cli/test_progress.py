"""Progress shows on a terminal or in CI and never in a test or a pipe; library logs stay quiet."""

from __future__ import annotations

import io
import logging
import threading
import time
from collections.abc import Callable, Iterator

import pytest

from snowflake_semantic_tools.adapters.timings import LiveCounts, RunTimings
from snowflake_semantic_tools.cli import output
from snowflake_semantic_tools.cli.output import (
    RenderPolicy,
    announce,
    configure_logging,
    print_progress,
    use_render_policy,
)
from snowflake_semantic_tools.cli.render import progress
from snowflake_semantic_tools.cli.render.progress import (
    ProgressDisplay,
    active_display,
    progress_display,
    progress_paused,
)
from snowflake_semantic_tools.cli.render.style import Style
from snowflake_semantic_tools.cli.render.terminal import PLAIN, Terminal


class _Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


class _Listener:
    def __init__(self) -> None:
        self.events: list[tuple[str, str]] = []

    def phase_started(self, name: str) -> None:
        self.events.append(("start", name))

    def phase_finished(self, name: str, seconds: float) -> None:
        self.events.append(("end", name))


@pytest.fixture
def policy() -> Iterator[None]:
    use_render_policy(RenderPolicy(terminal=PLAIN))
    yield
    use_render_policy(RenderPolicy(terminal=PLAIN))


def test_live_counts_name_the_phases_and_calls_in_flight() -> None:
    timings = RunTimings(enabled=True)
    with timings.phase("observe"), timings.phase("observe"):
        with timings.round_trip(), timings.login():
            assert timings.live() == LiveCounts(phases=("observe", "observe"), querying=1, connecting=1)
        with timings.dbt_process():
            assert timings.live().dbt == 1
    assert timings.live() == LiveCounts(queries=1, connects=1)


def test_a_listener_hears_each_phase_until_it_stops_watching() -> None:
    timings, listener = RunTimings(enabled=True), _Listener()
    timings.watch(listener)
    with timings.phase("load"):
        pass
    timings.unwatch(listener)
    timings.unwatch(listener)
    with timings.phase("observe"):
        pass
    assert listener.events == [("start", "load"), ("end", "load")]


def test_a_reset_forgets_what_is_in_flight() -> None:
    timings = RunTimings(enabled=True)
    with timings.phase("observe"):
        timings.reset()
        timings.enable()
    assert timings.live() == LiveCounts()


def _display(*, live: bool = False, unicode: bool = True) -> tuple[ProgressDisplay, io.StringIO, _Clock, RunTimings]:
    stream, clock, timings = io.StringIO(), _Clock(), RunTimings()
    display = ProgressDisplay(Style(Terminal(unicode=unicode)), live=live, stream=stream, timings=timings, clock=clock)
    return display, stream, clock, timings


def test_the_live_text_says_what_runs_what_it_counted_and_the_time() -> None:
    display, _, clock, _ = _display()
    display.start()
    try:
        clock.now += 12.34
        display.expect("publish", 29)
        display.advance("publish", 3)
        counts = LiveCounts(phases=("publish",), queries=151, connects=16)
        assert display.describe(counts) == "publishing · 3/29 · 151 queries · 16 sessions · 12.3s"
        assert display.describe(LiveCounts(queries=1, connecting=1)) == "connecting to Snowflake · 1 query · 12.3s"
        assert display.describe(LiveCounts(dbt=1)) == "running dbt · 12.3s"
        assert display.describe(LiveCounts(phases=("some_step",))) == "some step · 12.3s"
        assert display.describe(LiveCounts()) == "working · 12.3s"
    finally:
        display.stop()


def test_a_slow_phase_leaves_a_finished_line_and_a_quick_one_none() -> None:
    display, stream, _, timings = _display()
    display.start()
    try:
        display.phase_finished("load", 0.2)
        display.expect("publish", 2)
        display.advance("publish", 2)
        display.phase_finished("publish", 3.25)
    finally:
        display.stop()
    assert stream.getvalue() == "✓ published · 2/2 · 3.2s\n"


def test_plain_lines_beat_while_a_phase_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(progress, "INTERVAL_SECONDS", 0.005)
    display, stream, clock, timings = _display()
    display.start()
    try:
        with timings.phase("observe"):
            clock.now += progress.HEARTBEAT_SECONDS + 1
            _wait_for(lambda: "observing Snowflake" in stream.getvalue())
    finally:
        display.stop()
    assert stream.getvalue().startswith("... observing Snowflake")


def test_a_live_line_is_cleared_for_every_other_write(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(progress, "INTERVAL_SECONDS", 0.005)
    display, stream, clock, timings = _display(live=True)
    display.start()
    try:
        clock.now += progress.DELAY_SECONDS + 1
        _wait_for(lambda: "working" in stream.getvalue())
        display.write_through(stream, "a finding\n")
        assert "\r\x1b[Ka finding\n" in stream.getvalue()
        with progress_paused():
            stream.truncate(0)
            stream.seek(0)
            time.sleep(0.05)
            assert stream.getvalue() == ""
    finally:
        display.stop()
    assert active_display() is None


def test_nothing_draws_before_the_first_second(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(progress, "INTERVAL_SECONDS", 0.005)
    display, stream, _, _ = _display(live=True)
    display.start()
    time.sleep(0.05)
    display.stop()
    display.stop()
    assert stream.getvalue() == ""


def test_a_display_shows_on_a_terminal_or_in_ci_and_nowhere_else() -> None:
    live = progress_display(Terminal(live=True), {})
    lines = progress_display(Terminal(), {"CI": "true"})
    assert live is not None and lines is not None
    assert progress_display(Terminal(), {}) is None
    assert progress_display(Terminal(), {"CI": "false"}) is None


@pytest.mark.usefixtures("policy")
def test_progress_lines_go_above_the_live_line(capsys: pytest.CaptureFixture[str]) -> None:
    print_progress("eval queued")
    announce("connected to X", "target prod")
    assert capsys.readouterr().err == "eval queued\n"
    display, stream, _, _ = _display()
    display.start()
    try:
        print_progress("eval running", warning=True)
        announce("connected to X", "target prod")
    finally:
        display.stop()
    assert stream.getvalue() == "warn  eval running\n✓ connected to X · target prod\n"


def test_quiet_and_machine_formats_show_no_progress() -> None:
    assert not RenderPolicy(output="json", terminal=PLAIN).progress
    assert not RenderPolicy(output="agent", terminal=PLAIN).progress
    assert not RenderPolicy(quiet=True, terminal=PLAIN).progress
    assert RenderPolicy(output="plain", terminal=PLAIN).progress


@pytest.fixture
def root_logging() -> Iterator[logging.Logger]:
    root = logging.getLogger()
    level, handlers = root.level, list(root.handlers)
    yield root
    root.setLevel(level)
    root.handlers[:] = handlers


@pytest.mark.usefixtures("policy")
def test_library_chatter_is_quiet_by_default_and_heard_when_asked(
    root_logging: logging.Logger, capsys: pytest.CaptureFixture[str]
) -> None:
    configure_logging("warn")
    configure_logging("warn")
    assert sum(type(item).__name__ == "_LogHandler" for item in root_logging.handlers) == 1
    logging.getLogger("snowflake.connector.connection").info("Snowflake Connector for Python Version")
    logging.getLogger("snowflake.connector").warning("retrying")
    assert capsys.readouterr().err == "warn  snowflake.connector: retrying\n"
    configure_logging("info")
    logging.getLogger("snowflake.connector.connection").info("noted %s", "this")
    assert capsys.readouterr().err == "info  snowflake.connector.connection: noted this\n"
    configure_logging("debug")
    logging.getLogger("snowflake.connector").debug("a request")
    assert capsys.readouterr().err == "debug snowflake.connector: a request\n"


@pytest.mark.usefixtures("policy")
def test_a_log_record_carrying_a_credential_is_refused(
    root_logging: logging.Logger, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    configure_logging("warn")
    monkeypatch.setattr(output, "_leaks", lambda text: "hunter2" in text)
    logging.getLogger("x").warning("password=hunter2")
    err = capsys.readouterr().err
    assert "hunter2" not in err and "SST-PRT012" in err


@pytest.mark.usefixtures("policy")
def test_a_record_that_does_not_format_does_not_end_the_run(
    root_logging: logging.Logger, monkeypatch: pytest.MonkeyPatch
) -> None:
    configure_logging("warn")
    monkeypatch.setattr(logging, "raiseExceptions", False)
    logging.getLogger("x").warning("%d", "not a number")


def _wait_for(condition: Callable[[], bool], timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("timed out")
        threading.Event().wait(0.005)
