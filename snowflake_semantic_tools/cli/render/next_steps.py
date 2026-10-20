"""The next step a run suggests, from how it ended: one sentence a person or an agent can act on.

A command may name its own steps; these are the defaults every command gets from its exit code
and what it reported. A step never tells an agent to change Snowflake on its own: `apply`,
`drop`, the baseline's writes and `state` repairs are only ever named as the user's to run.
"""

from __future__ import annotations

from snowflake_semantic_tools.cli.exit_codes import CHANGES, CONFIG, CONNECTION, ERROR, INTERRUPTED, OK, USAGE

# Commands that change Snowflake, the state table, or the baseline: an agent is never told to
# run one again, only that it is the user's to run.
RISKY = frozenset(
    (
        "apply",
        "drop",
        "baseline add",
        "baseline prune",
        "baseline renew",
        "state recover",
        "state convert",
        "state unlock",
    )
)


def default_next_steps(
    command: str, exit_code: int, *, errors: int, warnings: int, audience: str = "person"
) -> tuple[str, ...]:
    """Return the next steps for a run of `command` that ended with `exit_code`; () for none.

    Args:
        errors, warnings: What the run reported, as its summary counts them.
        audience: `person`, or `agent` for the agent document and the JSON envelope: a step
            for an agent leaves every change to Snowflake to the user, and says where the
            detail is.
    """
    agent = audience == "agent"
    if exit_code == USAGE:
        return (f"see `sst {command} --help` for what the command accepts",)
    if exit_code == CONFIG:
        return (f"fix the setup problem reported, then {_again(command, agent)}; `sst debug` shows what resolved",)
    if exit_code == CONNECTION:
        return ("check the connection, role and grants with `sst debug`, then retry",)
    if exit_code == INTERRUPTED:
        return ("run `sst plan` to see what remains",) if command == "apply" else ()
    if exit_code == ERROR and errors:
        fix = f"fix the {'error' if errors == 1 else 'errors'}, then {_again(command, agent)}"
        if agent:
            return (fix, "`sst explain <CODE>` explains a code", f"`sst -o json {command}` has every field")
        return (fix,)
    if exit_code == CHANGES and command == "plan":
        if agent:
            return ("show the user this plan; `sst apply` changes Snowflake, so run it only when they ask",)
        return ("review the plan; `sst apply` makes these changes in Snowflake, so run it only when you mean to",)
    if exit_code == OK and command == "validate" and not errors:
        return ("`sst plan` shows what would change in Snowflake",) if not warnings else ()
    return ()


def _again(command: str, agent: bool) -> str:
    """Say to run `command` again; to an agent, a command that changes what SST manages is the user's."""
    if agent and command in RISKY:
        return f"tell the user: `sst {command}` changes what SST manages, so it runs again only when they ask"
    return f"run `sst {command}` again"


def capture_baseline_step(eval_keys: tuple[str, ...]) -> str:
    """Name the capture that replaces the baselines a gate could not compare with, as the user's to run.

    Running the suite again can never compare against those baselines; a capture writes the
    ones every later gate compares with, so it is run only once the change is intended.
    """
    selected = " ".join(f"--select {key}" for key in eval_keys)
    return (
        f'once the change is intended, `sst test --suite evals {selected} --capture-baseline --reason "<why>"` '
        "captures new baselines; it writes what later gates compare with, so run it only when you mean to"
    )
