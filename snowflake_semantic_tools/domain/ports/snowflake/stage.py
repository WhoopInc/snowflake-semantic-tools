"""The port that reads and writes files on a stage or in a Cortex extension version, by path."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class StagedFileMetadata:
    """What LIST reports for the one file at a stage path.

    Attributes:
        stage_path: The path observed, exactly as the caller gave it.
        name: The path without its leading `@`, whatever form LIST printed the name in.
        size: The file's size in bytes; 0 when LIST reports none.
        md5: The MD5 LIST reports; None when it reports none.
        last_modified: The modification time as LIST prints it; None when it reports none.
    """

    stage_path: str
    name: str
    size: int
    md5: str | None = None
    last_modified: str | None = None


class StagePort(Protocol):
    """Read and write files on a stage or in a Cortex extension version, by path.

    A stage path is `@<db>.<schema>.<stage>/<path>`; an extension version path is
    `snow://cortex_extension/<db>.<schema>.<name>/versions/<version>/<path>`. A path with an
    unsafe segment raises `SnowflakePortError` before anything runs.
    """

    def observe_staged_file(self, stage_path: str) -> StagedFileMetadata | None:
        """Return what LIST reports for the one file at a stage path.

        Never writes.

        Returns:
            The file's size, MD5, and modification time; None when no file is at the path.

        Raises:
            SnowflakePortError: the path is unsafe, LIST failed, or it reported the file twice
                or with an unreadable size.
        """
        ...

    def stage_file_exists(self, stage_path: str) -> bool:
        """Report whether a file is at a stage path, as `observe_staged_file` finds it.

        Never writes.

        Raises:
            SnowflakePortError: as `observe_staged_file` raises it.
        """
        ...

    def read_staged_file(self, stage_path: str) -> bytes | None:
        """Download the file at a stage path and return its bytes.

        Never writes to Snowflake, and keeps no local copy.

        Returns:
            The file's content; None when no file is at the path.

        Raises:
            SnowflakePortError: the path is unsafe, or the download failed.
        """
        ...

    def upload(self, stage_path: str, content: bytes) -> None:
        """Write a file to a stage path or into an extension's live version, replacing any there.

        Raises:
            SnowflakePortError: the path is unsafe, or the upload failed.
        """
        ...

    def list_location(self, location: str) -> tuple[str, ...]:
        """Return the paths of the files below a stage directory or an extension version, sorted.

        `location` is a stage path ending in `/`, or the root of an extension version; each
        path returned is relative to it. Never writes.

        Returns:
            The relative paths; empty when there are none.

        Raises:
            SnowflakePortError: the location is unsafe, or LIST failed.
        """
        ...
