"""A live Snowflake account for the tests marked `live`: credentials, the run's scratch schema, the sweep.

Where the tests connect, and as whom, is configuration only (`tests/helpers/live_config.py`): the
environment, a git-ignored local file, or a named connection. A profile `sst` reads names the
`SST_TEST_SNOWFLAKE_*` variables, never their values.

Every live run works in schemas of its own, `SST_IT_<UTC timestamp>_<run>_<worker>`, in the
configured database, created with the comment `SCRATCH_MARKER`. The timestamp makes a schema's age
readable from its name, the run token lets a CI job drop exactly the schemas it created, and
`sweepable` drops only a schema with both the prefix and the marker -- so neither can reach a
schema anything else created. `scratch_scope` is the one way a live helper names a schema to write
to, and it refuses any name without the prefix and the configured reference schema itself.
"""

from __future__ import annotations

import os
import re
import time
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime, timedelta

from snowflake_semantic_tools.domain.model.identifier import Identifier, SchemaScope
from snowflake_semantic_tools.domain.ports.snowflake.execution import ExecutionPort
from snowflake_semantic_tools.domain.sql import literal, scope, sql
from tests.helpers.live_config import (
    ENV_PREFIX,
    LiveAccount,
    LiveConfigurationError,
    load_live_account,
    not_configured_reason,
)

__all__ = [
    "ENV_PREFIX",
    "SCRATCH_MARKER",
    "SCRATCH_PREFIX",
    "LiveAccount",
    "LiveConfigurationError",
    "create_scratch",
    "drop_scratch",
    "load_live_account",
    "not_configured_reason",
    "profile_target",
    "run_token",
    "scratch_created_at",
    "scratch_schema_name",
    "scratch_scope",
    "sweepable",
]

SCRATCH_PREFIX = "SST_IT_"
SCRATCH_MARKER = "sst test suite scratch schema, dropped by its run or by the sweep"
_STAMP = "%Y%m%d%H%M%S"
_SCRATCH_NAME = re.compile(r"^SST_IT_(?P<stamp>\d{14})_(?P<run>[A-Z0-9]+)_(?P<worker>[A-Z0-9]+)$")


def profile_target(schema: str, *, key_pair: bool = True) -> dict[str, object]:
    """A dbt profile target in `schema` whose every account value is an `env_var` reference.

    By key pair it names the private key path; otherwise it names the authenticator, and an
    `sst` subprocess signs in the way that authenticator does.
    """
    target: dict[str, object] = {"type": "snowflake", "threads": 4}
    names = ("ACCOUNT", "USER", "PRIVATE_KEY_PATH" if key_pair else "AUTHENTICATOR", "ROLE", "WAREHOUSE", "DATABASE")
    if key_pair:
        target["authenticator"] = "snowflake_jwt"
    for name in names:
        target[name.lower()] = "{{ env_var('" + ENV_PREFIX + name + "') }}"
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
    """The scratch schema `name` in the account's configured database.

    Raises:
        ValueError: `name` lacks the scratch prefix, or is the configured reference schema.
    """
    folded = name.upper()
    if not folded.startswith(SCRATCH_PREFIX) or folded == (account.schema or "").upper():
        raise ValueError(
            f"refusing to write to {account.database}.{name}: it is not an {SCRATCH_PREFIX} scratch schema"
        )
    return SchemaScope(Identifier.parse(account.database), Identifier.parse(name))


def create_scratch(port: ExecutionPort, schema: SchemaScope) -> None:
    """Create `schema` with the marker the sweep recognises; refuse one that already exists.

    Raises:
        ValueError: `schema` is not a scratch schema, so it is never created.
        RuntimeError: Snowflake refused the statement.
    """
    if scratch_created_at(schema.schema.value) is None:
        raise ValueError(f"refusing to create {schema.sql}: it is not an {SCRATCH_PREFIX} scratch schema")
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
