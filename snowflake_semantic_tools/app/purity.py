"""Hold the pure ring to its contract at run time: no file, socket, or process from a domain call.

`pure_phase` marks a call into `domain/` as pure for the current context. An audit hook, installed
once when this module is imported and inert outside a pure phase, raises `ImpureCall` for a
filesystem, network, or process event inside one; the caller reports it as SST-INT002. An open the
import system makes, reading a module's source or writing its bytecode cache, is not I/O the domain
performs, so it is allowed; directory listings are not watched, because importing lists directories
too.

The hook runs no Python for an event it does not watch. It is `getattr(_WATCHED, event, args)`,
which CPython evaluates in C: an unwatched event names no attribute, so the call returns its
default. That matters because CPython 3.11 holds one process-wide flag while it runs the audit hooks
for `sys.settrace`; Python in a hook lets another thread run inside that window, and a tracer such as
coverage or a debugger calls `sys.settrace` as each thread starts, so the new thread would die before
its target ran and a pool waiting on it would wait forever.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from functools import partial

_PHASE: ContextVar[str | None] = ContextVar("sst_pure_phase", default=None)
_IO_EVENTS = frozenset(
    {
        *("open", "os.remove", "os.rmdir", "os.truncate", "shutil.copyfile", "shutil.rmtree"),
        *("socket.connect", "socket.bind", "socket.getaddrinfo", "urllib.Request"),
        *("subprocess.Popen", "os.system", "os.exec", "os.posix_spawn", "os.fork"),
    }
)
_IMPORT_SYSTEM = frozenset({"importlib._bootstrap", "importlib._bootstrap_external", "zipimport"})


class ImpureCall(Exception):
    """A call inside `pure_phase` reached a file, a socket, or a process.

    Attributes:
        phase: What the pure phase was named.
        event: The audit event it raised, such as ``open``.
    """

    def __init__(self, phase: str, event: str) -> None:
        super().__init__(f"{phase} raised the audit event {event!r}")
        self.phase = phase
        self.event = event


@contextmanager
def pure_phase(name: str) -> Iterator[None]:
    """Run the block as the pure phase `name`, raising `ImpureCall` at its first I/O."""
    token = _PHASE.set(name)
    try:
        yield
    finally:
        _PHASE.reset(token)


class _Watched:
    """One attribute per watched audit event; reading it checks the event against the pure phase."""

    __slots__ = ()


def _refusal(event: str) -> property:
    """Return the attribute for `event`: None outside a pure phase, else `ImpureCall` raised.

    The frame one above the getter is the Python code that raised the event, since the audit hook
    and `getattr` add no Python frame between them.
    """

    def check(_: _Watched) -> None:
        phase = _PHASE.get()
        if phase is None:
            return None
        if event == "open" and sys._getframe(1).f_globals.get("__name__") in _IMPORT_SYSTEM:
            return None
        raise ImpureCall(phase, event)

    return property(check)


for _event in _IO_EVENTS:
    setattr(_Watched, _event, _refusal(_event))

_WATCHED = _Watched()
_HOOK = partial(getattr, _WATCHED)
sys.addaudithook(_HOOK)
