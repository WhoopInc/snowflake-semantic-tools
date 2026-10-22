"""Where a comparison's report is written: the port the command line hands a writer through.

The report is the `sst.compare/1` object (`domain.evaluate.compare_json`). A writer puts it in a
directory as `compare.json`, and may render a page for people to read beside it.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Protocol

# The file every writer writes the report object to, which `--refresh-cost` reads back.
COMPARE_JSON = "compare.json"


class CompareReportWriter(Protocol):
    """Write a comparison's report into a directory."""

    def write(self, directory: Path, report: Mapping[str, object]) -> Path:
        """Write `report` into `directory`, created when absent, replacing what an earlier write left.

        Writes `compare.json`, the report object, and whatever page the writer renders from it.

        Returns:
            The file a reader opens first: the rendered page, else `compare.json`.

        Raises:
            OSError: the directory or a file could not be written.
        """
        ...
