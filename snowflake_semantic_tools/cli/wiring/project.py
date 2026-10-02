"""Bind a project directory to its files, its build directory, its commit, and its Snowflake target.

Two names are looked up when they are needed rather than when this module is imported,
so replacing either one changes what every command sees: the commit comes from this
module's `git_sha`, and every connection is opened through `cli.main.SnowflakeConnector`.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from snowflake_semantic_tools.adapters.dbt.profiles import ProfileTarget, load_profile_target
from snowflake_semantic_tools.adapters.fs.local import StateFileStore, state_file
from snowflake_semantic_tools.adapters.project_source import YamlProjectInputs
from snowflake_semantic_tools.adapters.snowflake.connector import SnowflakeConnector
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError


def target_dir(project_dir: Path) -> Path:
    """Return where SST writes its build artifacts: `target/sst` under the project."""
    return project_dir / "target" / "sst"


def git_sha(project_dir: Path) -> str:
    """Return the project's commit as a 7-character sha, or `WORKTREE` when git cannot say."""
    import subprocess

    completed = subprocess.run(
        ["git", "-C", str(project_dir), "rev-parse", "--short=7", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    value = completed.stdout.strip()
    return value if completed.returncode == 0 and value else "WORKTREE"


def project_inputs(project_dir: Path, target_name: str | None, manifest_path: Path | None) -> YamlProjectInputs:
    """Bind the project's files as the inputs every use case reads.

    The commit is asked for through this module's `git_sha`, looked up when a use case
    needs it, so replacing `git_sha` here changes the commit every use case sees.
    """
    return YamlProjectInputs(
        project_dir,
        target_name=target_name,
        manifest_path=manifest_path,
        git_sha=lambda: git_sha(project_dir),
    )


def open_connector(connection_params: dict[str, object]) -> SnowflakeConnector:
    """Open a Snowflake session through `cli.main.SnowflakeConnector`, as it is bound right now.

    The recorded-Snowflake helpers and the reference project replace that one attribute to
    run every command against a double, so the class is looked up on each call.
    """
    # Imported here, not at module level: `cli.main` imports the commands that import this module.
    from snowflake_semantic_tools.cli import main as entry

    return entry.SnowflakeConnector(connection_params)


def connect(project_dir: Path, target_name: str | None) -> tuple[ProfileTarget, SnowflakeConnector]:
    """Connect to the project's target, and bind it to the live session's account and role.

    An error raised once the connection is open closes it again.

    Raises:
        SnowflakePortError: connecting failed, or the session's role is not the target's role.
    """
    profile = load_profile_target(project_dir, target_name)
    port = open_connector(profile.connection_params)
    try:
        live_account = port.current_account_locator()
        live_role = port.current_role()
        if profile.identity.role and live_role.upper() != profile.identity.role.upper():
            raise SnowflakePortError(
                f"connected role {live_role!r} differs from configured role {profile.identity.role!r}"
            )
        identity = dataclasses.replace(
            profile.identity,
            account_locator=live_account,
            role=live_role,
        )
        resolved = ProfileTarget(
            profile_name=profile.profile_name,
            target_name=profile.target_name,
            connection_params=profile.connection_params,
            identity=identity,
            state_table=profile.state_table,
        )
        return resolved, port
    except Exception:
        port.close()
        raise


@contextmanager
def closed_on_error(port: SnowflakeConnector) -> Iterator[None]:
    """Close `port` if the block raises; a block that completes leaves the connection to its owner."""
    try:
        yield
    except BaseException:
        # Also on an interrupt or a declined prompt: nothing else will close the session.
        port.close()
        raise


def state_store(project_dir: Path, target_name: str) -> StateFileStore:
    """Return the local state file of `target_name`, under the project's build directory."""
    return StateFileStore(state_file(target_dir(project_dir), target_name))
