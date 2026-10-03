"""The ports the golden suite reads committed goldens through, and rewrites them through.

A golden is addressed by a `GoldenPath`: in the semantic view DDL directory the suite was
given, or in a directory beside it named for an artifact type. The store owns where those
directories are and how a report names a golden, so the suite compares text and never
touches a file. Only `--update-golden` writes, through a `GoldenWriter`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class GoldenPath:
    """Where one golden lives, relative to the golden directories.

    Attributes:
        beside: The directory beside the DDL directory that holds the golden, such as
            `agent` or `eval`; None for the DDL directory itself.
        parts: The path below that directory, one segment per part, joined in order.
    """

    beside: str | None
    parts: tuple[str, ...]


class GoldenStore(Protocol):
    """Committed goldens, read by location; nothing is ever written."""

    def exists(self, path: GoldenPath) -> bool:
        """Report whether a golden file is at `path`.

        Never raises for a missing directory: that is a golden that does not exist.
        """
        ...

    def read(self, path: GoldenPath) -> str | None:
        """Return the golden at `path` as UTF-8 text.

        Returns:
            The text; None when no golden file is at the path.

        Raises:
            ValueError: the file is not valid UTF-8.
            OSError: the file exists but cannot be read.
        """
        ...

    def name(self, path: GoldenPath) -> str:
        """Return how a report names the golden at `path`: its path, as the directory was given.

        Pure; the same location always has the same name, whether or not a file is there.
        """
        ...


class GoldenWriter(GoldenStore, Protocol):
    """Committed goldens that `sst test --update-golden` may also rewrite."""

    def write(self, path: GoldenPath, text: str) -> None:
        """Write `text` as the golden at `path`, in UTF-8, creating its directory when missing.

        Raises:
            OSError: the file or its directory cannot be written.
        """
        ...
