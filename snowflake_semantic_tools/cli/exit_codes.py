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
    (
        OK,
        "OK",
        "Success. For `sst plan`, nothing to change: every live object carries the marker state "
        "records, and the properties plan compares are as apply read them back.",
    ),
    (
        ERROR,
        "ERROR",
        "Errors were reported, an apply, a test suite, or a check failed, or an apply skipped a blocked change.",
    ),
    (
        CHANGES,
        "CHANGES",
        "Declared and actual state differ: `sst plan` found changes, `sst diff` found the two states "
        "differ, or `enrich --check`, `format --check`, `docs --check`, `migrate refs`, or `migrate split` "
        "found something to rewrite.",
    ),
    (USAGE, "USAGE", "The command line is invalid."),
    (CONFIG, "CONFIG", "The project, its configuration, or a saved plan cannot be used."),
    (
        CONNECTION,
        "CONNECTION",
        "Snowflake could not be reached, rejected the credential, refused a privilege, or did not finish "
        "in time; the diagnostic code says which, so a retry fixes only some of them.",
    ),
    (
        INTERRUPTED,
        "INTERRUPTED",
        "The run was interrupted: Ctrl-C, SIGTERM or SIGHUP, end of input, or a declined prompt. Any "
        "command can return it.",
    ),
)
