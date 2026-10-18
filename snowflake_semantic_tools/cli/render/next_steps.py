"""The next step a run suggests, from how it ended: one sentence a person or an agent can act on.

A command may name its own steps; these are the defaults every command gets from its exit code
and what it reported. A step never tells anyone to change Snowflake on their own: `apply`,
`drop` and `baseline add` are only ever named as something to run when the user asks.
"""

from __future__ import annotations

from snowflake_semantic_tools.cli.exit_codes import CHANGES, CONFIG, CONNECTION, ERROR, INTERRUPTED, OK, USAGE


def default_next_steps(command: str, exit_code: int, *, errors: int, warnings: int) -> tuple[str, ...]:
    """Return the next steps for a run of `command` that ended with `exit_code`; () for none.

    Args:
        errors, warnings: What the run reported, as its summary counts them.
    """
    if exit_code == USAGE:
        return (f"see `sst {command} --help` for what the command accepts",)
    if exit_code == CONFIG:
        return (f"fix the setup problem reported, then run `sst {command}` again; `sst debug` shows what resolved",)
    if exit_code == CONNECTION:
        return ("check the connection, role and grants with `sst debug`, then retry",)
    if exit_code == INTERRUPTED:
        return ("run `sst plan` to see what remains",) if command == "apply" else ()
    if exit_code == ERROR and errors:
        return (_fix(command, errors),)
    if exit_code == CHANGES and command == "plan":
        return ("review the plan; `sst apply` makes these changes in Snowflake, so run it only when you mean to",)
    if exit_code == OK and command == "validate" and not errors:
        return ("`sst plan` shows what would change in Snowflake",) if not warnings else ()
    return ()


def _fix(command: str, errors: int) -> str:
    noun = "error" if errors == 1 else "errors"
    return f"fix the {noun}, then run `sst {command}` again"
