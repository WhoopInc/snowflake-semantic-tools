"""Hold the pure ring to its contract at run time: no file, socket, or process from a domain call.

`pure_phase` marks a call into `domain/` as pure for the current context. A Python audit hook,
installed once when this module is imported and inert outside a pure phase, raises
`ImpureCall` for a filesystem, network, or process event inside one; the caller reports it as
SST-INT002. Importing a module reads its source and writes its bytecode cache, which is not I/O
the domain performs, so those opens are allowed; directory listings are not watched, because
importing lists directories too.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

_PHASE: ContextVar[str | None] = ContextVar("sst_pure_phase", default=None)
_IO_EVENTS = frozenset(
    {
        *("open", "os.remove", "os.rmdir", "os.truncate", "shutil.copyfile", "shutil.rmtree"),
        *("socket.connect", "socket.bind", "socket.getaddrinfo", "urllib.Request"),
        *("subprocess.Popen", "os.system", "os.exec", "os.posix_spawn", "os.fork"),
    }
)
_IMPORT_SUFFIXES = (".py", ".pyc", ".so", ".pyd")
_BYTECODE_CACHE = "__pycache__"


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


def _audit(event: str, args: tuple[object, ...]) -> None:
    phase = _PHASE.get()
    if phase is None or event not in _IO_EVENTS:
        return
    if event == "open" and args:
        path = str(args[0])
        if path.endswith(_IMPORT_SUFFIXES) or _BYTECODE_CACHE in path:
            return
    raise ImpureCall(phase, event)


sys.addaudithook(_audit)
