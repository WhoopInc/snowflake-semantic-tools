"""What a project's target has deployed: each artifact as the state table `sst apply` writes has it.

`deployed` reads the state table, never writes, and compares each entry with what the session
compiled as `sst plan` does: an artifact is deployed when the fingerprint apply recorded is the
one it compiles to now, changed when another is recorded, and not deployed when none is.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from snowflake_semantic_tools.adapters.snowflake.connector import SnowflakeConnector
from snowflake_semantic_tools.api.session import ProjectSession
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.state import StatePort
from snowflake_semantic_tools.domain.state import AppliedEntry

DEPLOYED, CHANGED, NOT_DEPLOYED, REMOVED = "deployed", "changed", "not_deployed", "removed"


@dataclass(frozen=True, slots=True)
class ConnectionParams:
    """How to open a Snowflake session as one user, with an OAuth access token or a key pair.

    Exactly one of `token` and `private_key_file` is set; neither is ever shown by `repr`.

    Attributes:
        account: The account identifier, such as `myorg-myaccount`.
        user: The user to connect as.
        role: The role to use; None takes the user's default role.
        warehouse: The warehouse to use; None takes the user's default warehouse.
        token: An OAuth access token, sent with the `oauth` authenticator.
        private_key_file: The path to a PEM private key, for key-pair authentication.
        host: The account's host name, when it is not the one `account` implies.
    """

    account: str
    user: str
    role: str | None = None
    warehouse: str | None = None
    token: str | None = field(default=None, repr=False)
    private_key_file: str | None = field(default=None, repr=False)
    host: str | None = None

    def __post_init__(self) -> None:
        if (self.token is None) == (self.private_key_file is None):
            raise ValueError("ConnectionParams takes exactly one of token and private_key_file")

    def connector_params(self) -> dict[str, object]:
        """Return the arguments `snowflake.connector.connect` takes for this session."""
        params: dict[str, object] = {"account": self.account, "user": self.user}
        if self.token is not None:
            params |= {"authenticator": "oauth", "token": self.token}
        else:
            params["private_key_file"] = self.private_key_file
        for name, value in (("role", self.role), ("warehouse", self.warehouse), ("host", self.host)):
            if value is not None:
                params[name] = value
        return params


@dataclass(frozen=True, slots=True)
class Deployed:
    """One artifact as the target has it: compiled, recorded by apply, or both.

    Attributes:
        key: Its artifact key.
        status: `deployed`, `changed`, or `not_deployed` for an artifact the project compiles;
            `removed` for one apply recorded that the project no longer compiles.
        fingerprint: The fingerprint it compiles to now; None when it is `removed`.
        deployed_fingerprint: The fingerprint apply recorded; None when it is `not_deployed`.
        qualified_name: The object apply published it to; None when it is `not_deployed`.
        applied_at: When apply recorded it; None when it is `not_deployed`.
        git_sha: The commit apply ran from; empty when it is unknown or `not_deployed`.
        outcome: How apply's entry came to be, such as `applied`; None when it is `not_deployed`.
    """

    key: str
    status: str
    fingerprint: str | None
    deployed_fingerprint: str | None = None
    qualified_name: str | None = None
    applied_at: str | None = None
    git_sha: str = ""
    outcome: str | None = None


def deployed(session: ProjectSession, connection: ConnectionParams | StatePort) -> tuple[Deployed, ...]:
    """Return what the session's target has deployed, read from its state table, for every artifact.

    Each artifact the session compiled comes first, in the order apply publishes them, then each
    one apply recorded that it no longer compiles, by key. `connection` is how to open a session,
    which is closed again before this returns, or a port to read through, which is left open.

    Raises:
        ProjectError: the target's `profiles.yml` entry cannot be read, as `state_table` raises.
        SnowflakePortError: connecting failed, or the state table exists but cannot be read.
    """
    table, target_name = session.state_table()
    if isinstance(connection, ConnectionParams):
        connector = SnowflakeConnector(connection.connector_params())
        try:
            recorded = _read(connector, table, target_name)
        finally:
            connector.close()
    else:
        recorded = _read(connection, table, target_name)
    compiled = session.catalog()
    rows = [_compared(item.key, item.fingerprint, recorded.get(item.key)) for item in compiled]
    keys = {item.key for item in compiled}
    rows += [_compared(key, None, recorded[key]) for key in sorted(recorded) if key not in keys]
    return tuple(rows)


def _read(port: StatePort, table: str, target_name: str) -> Mapping[str, AppliedEntry]:
    recorded = port.read_state(QualifiedName.parse(table), target_name)
    if recorded is None:
        raise SnowflakePortError(f"the state table {table} exists but cannot be read")
    return recorded


def _compared(key: str, fingerprint: str | None, entry: AppliedEntry | None) -> Deployed:
    """One artifact's row: its status by `sst plan`'s rule, and what apply recorded of it."""
    if entry is None:
        return Deployed(key=key, status=NOT_DEPLOYED, fingerprint=fingerprint)
    if fingerprint is None:
        status = REMOVED
    elif entry.fingerprint == fingerprint:
        status = DEPLOYED
    else:
        status = CHANGED
    return Deployed(
        key=key,
        status=status,
        fingerprint=fingerprint,
        deployed_fingerprint=entry.fingerprint,
        qualified_name=entry.qualified_name,
        applied_at=entry.applied_at,
        git_sha=entry.git_sha,
        outcome=entry.outcome,
    )
