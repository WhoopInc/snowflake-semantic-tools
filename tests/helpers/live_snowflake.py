"""A live Snowflake account for the tests marked `live`: credentials, the run's scratch schema, the sweep.

Credentials come only from the environment, as `SST_TEST_SNOWFLAKE_*`, and authenticate with a key
pair, so a run needs no browser and no credential is written into a file. A profile `sst` reads
names the variables, never their values.

Every live run works in a schema of its own, `SST_IT_<UTC timestamp>_<run>_<worker>`, created with
the comment `SCRATCH_MARKER`. The timestamp makes a schema's age readable from its name, the run
token lets a CI job drop exactly the schemas it created, and `sweepable` drops only a schema with
both the prefix and the marker -- so neither can reach a schema anything else created.
"""

from __future__ import annotations

import os
import re
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from snowflake_semantic_tools.domain.model.identifier import Identifier, SchemaScope
from snowflake_semantic_tools.domain.ports.snowflake.execution import ExecutionPort
from snowflake_semantic_tools.domain.sql import literal, scope, sql

ENV_PREFIX = "SST_TEST_SNOWFLAKE_"
REQUIRED = ("ACCOUNT", "USER", "PRIVATE_KEY_PATH", "ROLE", "WAREHOUSE", "DATABASE")
SCRATCH_PREFIX = "SST_IT_"
SCRATCH_MARKER = "sst test suite scratch schema, dropped by its run or by the sweep"
_STAMP = "%Y%m%d%H%M%S"
_SCRATCH_NAME = re.compile(r"^SST_IT_(?P<stamp>\d{14})_(?P<run>[A-Z0-9]+)_(?P<worker>[A-Z0-9]+)$")


class LiveConfigurationError(Exception):
    """Some `SST_TEST_SNOWFLAKE_*` variables are set and others are not."""


@dataclass(frozen=True, slots=True)
class LiveAccount:
    """The account, key-pair user, role, warehouse and scratch database the live tests run in."""

    account: str
    user: str
    private_key_path: str
    role: str
    warehouse: str
    database: str

    @classmethod
    def from_environment(cls, environ: Mapping[str, str]) -> LiveAccount | None:
        """Read the account from `environ`; None when no account is configured.

        Raises:
            LiveConfigurationError: the account is set but another required variable is not.
        """
        values = {name: environ.get(ENV_PREFIX + name, "") for name in REQUIRED}
        if not values["ACCOUNT"]:
            return None
        missing = [ENV_PREFIX + name for name, value in values.items() if not value]
        if missing:
            raise LiveConfigurationError(f"set {', '.join(missing)} as well, or unset {ENV_PREFIX}ACCOUNT")
        return cls(*(values[name] for name in REQUIRED))

    def connection_params(self) -> dict[str, object]:
        """The driver settings `SnowflakeConnector` connects with, by key pair."""
        return {
            "account": self.account,
            "user": self.user,
            "private_key_file": self.private_key_path,
            "authenticator": "SNOWFLAKE_JWT",
            "role": self.role,
            "warehouse": self.warehouse,
            "database": self.database,
        }


def profile_target(schema: str) -> dict[str, object]:
    """A dbt profile target in `schema` whose every credential is an `env_var` reference."""
    target: dict[str, object] = {"type": "snowflake", "authenticator": "snowflake_jwt", "threads": 4}
    for name in REQUIRED:
        key = "private_key_path" if name == "PRIVATE_KEY_PATH" else name.lower()
        target[key] = "{{ env_var('" + ENV_PREFIX + name + "') }}"
    target["schema"] = schema
    return target


def run_token(environ: Mapping[str, str]) -> str:
    """The run's token: `SST_TEST_RUN_ID` when set, as CI sets it, else this process's start."""
    raw = environ.get("SST_TEST_RUN_ID") or f"L{int(time.time())}P{os.getpid()}"
    return re.sub(r"[^A-Z0-9]", "X", raw.upper())


def scratch_schema_name(run: str, worker: str, now: datetime) -> str:
    """The scratch schema for one worker of one run, stamped with `now` in UTC."""
    worker_token = re.sub(r"[^A-Z0-9]", "X", worker.upper()) or "MAIN"
    return f"{SCRATCH_PREFIX}{now.astimezone(UTC):{_STAMP}}_{run}_{worker_token}"


def scratch_created_at(name: str) -> datetime | None:
    """When the scratch schema `name` was created, read from its name; None if it is not one."""
    found = _SCRATCH_NAME.match(name)
    if found is None:
        return None
    return datetime.strptime(found["stamp"], _STAMP).replace(tzinfo=UTC)


def sweepable(
    schemas: Iterable[tuple[str, str | None]], now: datetime, *, older_than: timedelta, run: str | None = None
) -> tuple[str, ...]:
    """The scratch schemas to drop from `(name, comment)` pairs, in name order.

    A schema qualifies only with the scratch prefix, a parseable stamp, and the marker comment;
    of those, `run` selects the given run's schemas whatever their age, and otherwise a schema is
    dropped once it is at least `older_than` old.
    """
    chosen = []
    for name, comment in schemas:
        created = scratch_created_at(name)
        if created is None or comment != SCRATCH_MARKER:
            continue
        found = _SCRATCH_NAME.match(name)
        assert found is not None
        if (run is not None and found["run"] == run) or (run is None and now - created >= older_than):
            chosen.append(name)
    return tuple(sorted(chosen))


def scratch_scope(account: LiveAccount, name: str) -> SchemaScope:
    """The scratch schema `name` in the account's scratch database."""
    return SchemaScope(Identifier.parse(account.database), Identifier.parse(name))


def create_scratch(port: ExecutionPort, schema: SchemaScope) -> None:
    """Create `schema` with the marker the sweep recognises; refuse one that already exists.

    Raises:
        RuntimeError: Snowflake refused the statement.
    """
    created = port.execute_script(
        (sql("CREATE SCHEMA {schema} COMMENT = {marker}", schema=scope(schema), marker=literal(SCRATCH_MARKER)),)
    )
    if not created.ok:
        raise RuntimeError(f"could not create the scratch schema {schema.sql}: {created.error}")


def drop_scratch(port: ExecutionPort, schema: SchemaScope) -> bool:
    """Drop `schema` and everything in it; report whether Snowflake accepted the statement.

    Raises:
        ValueError: `schema` is not a scratch schema, so it is never dropped.
    """
    if scratch_created_at(schema.schema.value) is None:
        raise ValueError(f"refusing to drop {schema.sql}: it is not an {SCRATCH_PREFIX} scratch schema")
    return port.execute_script((sql("DROP SCHEMA IF EXISTS {schema} CASCADE", schema=scope(schema)),)).ok
