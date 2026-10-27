"""The process exit codes `sst` uses, and the table `sst docs` renders them from."""

from __future__ import annotations

OK = 0
ERROR = 1
CHANGES = 2
USAGE = 3
CONFIG = 4
CONNECTION = 5
INTERRUPTED = 130

# A refusal whose only findings are selectors that named nothing is the command line's fault.
_SELECTOR_MISSES = frozenset({"SST-DIS010", "SST-DIS015"})


def refusal_exit(codes: tuple[str, ...], default: int) -> int:
    """Return USAGE for a refusal whose every finding is a selector that matched nothing, else `default`.

    `--select` that names nothing is an argument naming nothing, as `sst lineage <node>` reports
    an unknown node, so every command exits 3 for it.
    """
    return USAGE if codes and all(code in _SELECTOR_MISSES for code in codes) else default


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
        (
            "Errors were reported, an apply, a smoke probe, an eval gate, or a check failed, "
            "or an apply skipped a blocked change."
        ),
    ),
    (
        CHANGES,
        "CHANGES",
        "Declared and actual state differ: `sst plan` found changes, `sst diff` found the two states "
        "differ, or `enrich --check`, `format --check`, `docs --check`, `migrate refs`, or `migrate split` "
        "found something to rewrite, or `sst import` found files to write without --write.",
    ),
    (
        USAGE,
        "USAGE",
        "The command line is invalid, or names nothing: an empty `--select`, or one that matches no artifact.",
    ),
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
