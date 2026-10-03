"""Bind a resolved project to its files, its build directory, its commit, and its Snowflake target.

Two names are looked up when they are needed rather than when this module is imported,
so replacing either one changes what every command sees: the commit comes from this
module's `git_sha`, and every connection is opened through `cli.main.SnowflakeConnector`.
A connection opened here is closed again if anything fails before it is handed back.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path

import click

from snowflake_semantic_tools.adapters.dbt.profiles import ProfileTarget, load_profile_target
from snowflake_semantic_tools.adapters.fs.local import StateFileStore, state_file
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.project_source import YamlProjectInputs
from snowflake_semantic_tools.adapters.resolved_config import resolved_config
from snowflake_semantic_tools.adapters.snowflake.connector import SnowflakeConnector
from snowflake_semantic_tools.adapters.yaml.documents import LoadCache
from snowflake_semantic_tools.cli.output import register_secrets
from snowflake_semantic_tools.domain.model.config_schema import config_block
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError

# Where a command's shared `LoadCache` is kept, in the click context's `meta`.
LOAD_CACHE = "sst.load_cache"


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


def project_inputs(
    files: ProjectPaths, target_name: str | None, manifest_path: Path | None, *, database: str | None = None
) -> YamlProjectInputs:
    """Bind the project's files as the inputs every use case reads.

    The commit is asked for through this module's `git_sha`, looked up when a use case
    needs it, so replacing `git_sha` here changes the commit every use case sees. Under a click
    command, every input the command builds shares one load cache, so a file read twice in the
    run is parsed once.

    Args:
        database: Resolve refs against this database instead of the target's; None keeps it.
    """
    context = click.get_current_context(silent=True)
    return YamlProjectInputs(
        files,
        target_name=target_name,
        manifest_path=manifest_path,
        git_sha=lambda: git_sha(files.project_dir),
        database=database,
        load_cache=context.meta.setdefault(LOAD_CACHE, LoadCache()) if context is not None else None,
    )


def open_connector(connection_params: dict[str, object]) -> SnowflakeConnector:
    """Open a Snowflake session through `cli.main.SnowflakeConnector`, as it is bound right now.

    The recorded-Snowflake helpers and the reference project replace that one attribute to
    run every command against a double, so the class is looked up on each call.
    """
    # Imported here, not at module level: `cli.main` imports the commands that import this module.
    from snowflake_semantic_tools.cli import main as entry

    return entry.SnowflakeConnector(connection_params)


def connect(
    files: ProjectPaths, target_name: str | None, *, profile: str | None = None
) -> tuple[ProfileTarget, SnowflakeConnector]:
    """Connect to the project's target, and bind it to the live session's account and role.

    `profile` names the `profiles.yml` profile instead of the project. An error raised once the
    connection is open closes it again. The target's credentials are
    registered with the output first, so no report of this run can print one.

    Raises:
        ProjectError: the target cannot be connected to as declared, as `connection_params` says.
        SnowflakePortError: connecting failed, or the session's role is not the target's role.
    """
    resolved_profile = load_profile_target(
        files, target_name, profile=profile, state=config_block(resolved_config(files, target_name).tree.get("state"))
    )
    register_secrets(resolved_profile.secrets)
    port = open_connector(resolved_profile.connection_params)
    with ExitStack() as cleanup:
        cleanup.callback(port.close)
        live_account = port.current_account_locator()
        live_role = port.current_role()
        if resolved_profile.identity.role and live_role.upper() != resolved_profile.identity.role.upper():
            raise SnowflakePortError(
                f"connected role {live_role!r} differs from configured role {resolved_profile.identity.role!r}"
            )
        identity = dataclasses.replace(
            resolved_profile.identity,
            account_locator=live_account,
            role=live_role,
        )
        resolved = ProfileTarget(
            profile_name=resolved_profile.profile_name,
            target_name=resolved_profile.target_name,
            connection_params=resolved_profile.connection_params,
            identity=identity,
            state_table=resolved_profile.state_table,
        )
        # Connected and bound: the caller owns the session from here, so nothing closes it.
        cleanup.pop_all()
    return resolved, port


@contextmanager
def closed_on_error(port: SnowflakeConnector) -> Iterator[None]:
    """Close `port` if the block raises; a block that completes leaves the connection to its owner.

    The close is registered before the block runs and released only once it completes, so an
    interrupt or a declined prompt closes the session too: nothing else would.
    """
    with ExitStack() as cleanup:
        cleanup.callback(port.close)
        yield
        cleanup.pop_all()


def state_store(files: ProjectPaths, target_name: str) -> StateFileStore:
    """Return the local state file of `target_name`, under the project's build directory."""
    return StateFileStore(state_file(target_dir(files.project_dir), target_name), config_path=files.config_name)
