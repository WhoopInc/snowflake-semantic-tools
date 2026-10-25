"""What one SST run learned from dbt: each `dbt parse` it ran, and each manifest it decoded.

A command reads the project through several inputs, each with its own source, and each source
used to run dbt and decode the manifest for itself, so a deferred plan parsed the deferred
target twice and decoded a manifest six times. One `DbtCache` shared by a run's sources does
each once.

A parse is keyed by everything that decides what dbt writes and where: the project, the target,
the manifest path, the command, the profiles directory and the runner. A run that changed the
project's files and then read it again would need a new parse; no command does, and a decoded
manifest is keyed by the file's size and modification time, so a manifest that was rewritten
is decoded again.
"""

from __future__ import annotations

from collections.abc import Callable, Hashable
from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog


class DbtCache:
    """The dbt parses and decoded manifests one run shares across its sources."""

    def __init__(self) -> None:
        self._parses: dict[Hashable, tuple[Diagnostic, ...]] = {}
        self._catalogs: dict[Hashable, DbtCatalog] = {}
        self._notes: dict[Hashable, tuple[Diagnostic, ...]] = {}

    def note(self, key: Hashable, diagnostic: Diagnostic) -> None:
        """Keep `diagnostic` for the deferral `key` names, for each of the run's sources to report."""
        self._notes[key] = (*self._notes.get(key, ()), diagnostic)

    def notes(self, key: Hashable) -> tuple[Diagnostic, ...]:
        """Return what was noted for `key`, in the order noted."""
        return self._notes.get(key, ())

    def parse(self, key: Hashable, run: Callable[[], tuple[Diagnostic, ...]]) -> tuple[Diagnostic, ...]:
        """Run the parse `key` names the first time it is asked for; return its warnings every time.

        A parse that raises is not kept, so asking again runs it again.
        """
        if key not in self._parses:
            self._parses[key] = run()
        return self._parses[key]

    def catalog(self, path: Path, options: Hashable, read: Callable[[], DbtCatalog]) -> DbtCatalog:
        """Return the manifest at `path` decoded with `options`, decoding it only when it changed.

        A manifest that cannot be read is not kept: `read` raises, as it would uncached.
        """
        try:
            stat = path.stat()
        except OSError:
            return read()
        key = (path.resolve(), stat.st_mtime_ns, stat.st_size, options)
        if key not in self._catalogs:
            self._catalogs[key] = read()
        return self._catalogs[key]
