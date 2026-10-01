"""The process exit codes `sst` uses, and the table `sst docs` renders them from."""

from __future__ import annotations

OK = 0
ERROR = 1
CHANGES = 2
USAGE = 3
CONFIG = 4
CONNECTION = 5
INTERRUPTED = 130

EXIT_CODE_DOCS: tuple[tuple[int, str, str], ...] = (
    (OK, "OK", "Success. For `sst plan`, nothing to change."),
    (ERROR, "ERROR", "Errors were reported, or an apply, a test suite, or a check failed."),
    (CHANGES, "CHANGES", "`sst plan` found changes, or `sst migrate refs` found rewrites to make."),
    (USAGE, "USAGE", "The command line is invalid."),
    (CONFIG, "CONFIG", "The project, its configuration, or a saved plan cannot be used."),
    (CONNECTION, "CONNECTION", "Snowflake could not be reached."),
    (INTERRUPTED, "INTERRUPTED", "The run was interrupted."),
)
