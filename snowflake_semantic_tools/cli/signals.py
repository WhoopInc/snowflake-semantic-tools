"""End a run on SIGTERM or SIGHUP as Ctrl-C ends it, so every `finally` releases what it holds.

By default either signal ends the process at once: no `finally` runs, so a run lock stays held
until it expires, and an apply records nothing it published. `terminated_as_interrupt` raises
the first one as `KeyboardInterrupt` instead, which a command handles as it handles Ctrl-C:
apply records what it finished, every lock is released, and the run exits 130 (INTERRUPTED)
with SST-PRT107.

Once any of the three has interrupted the run, SIGTERM and SIGHUP are ignored until it ends, so
the signal a CI runner or container sends after its first cannot cut that cleanup short. Ctrl-C
still raises each time it is pressed, which is how a user stops waiting; SIGKILL is never caught.
A signal the process inherited as ignored, as `nohup` leaves SIGHUP, stays ignored.
"""

from __future__ import annotations

import signal
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from types import FrameType
from typing import NoReturn

# The signals a run treats as an interrupt besides SIGINT; SIGHUP does not exist on Windows.
_STOP_SIGNALS = tuple(
    getattr(signal, name) for name in ("SIGTERM", "SIGHUP") if isinstance(getattr(signal, name, None), signal.Signals)
)
# The name of the first signal that interrupted the current run; empty before one did.
_received: list[str] = []


@contextmanager
def terminated_as_interrupt() -> Iterator[None]:
    """Raise SIGTERM and SIGHUP in the block as `KeyboardInterrupt`, ignoring them after any interrupt.

    Ctrl-C keeps Python's own behaviour, raising each time; SIGINT is not taken over when it is
    ignored, as it is for a job a non-interactive shell started in the background, and SIGTERM
    or SIGHUP is not taken over when it is ignored, as `nohup` leaves SIGHUP. Each replaced
    handler is restored when the block ends. Outside the main thread no handler can be set, so
    the block runs unchanged.
    """
    _received.clear()
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    previous = {
        number: signal.signal(number, _raise_interrupt)
        for number in _STOP_SIGNALS
        if signal.getsignal(number) is not signal.SIG_IGN
    }
    if signal.getsignal(signal.SIGINT) is signal.default_int_handler:
        previous[signal.SIGINT] = signal.signal(signal.SIGINT, _raise_interrupt)
    try:
        yield
    finally:
        for number, handler in previous.items():
            signal.signal(number, signal.SIG_DFL if handler is None else handler)


def received_signal() -> str:
    """Return the name of the signal that first interrupted the run, such as `SIGTERM`; empty for none."""
    return _received[0] if _received else ""


def _raise_interrupt(signum: int, frame: FrameType | None) -> NoReturn:
    del frame
    for number in _STOP_SIGNALS:
        signal.signal(number, signal.SIG_IGN)
    if not _received:
        _received.append(signal.Signals(signum).name)
    raise KeyboardInterrupt
