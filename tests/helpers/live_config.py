"""Where the live tests connect, and as whom: the environment, a local file, and a named connection.

Nothing here names a real account. Every value comes from one of three sources, highest first:

1. the process environment, as `SST_TEST_SNOWFLAKE_<KEY>`;
2. `tests/live.local.env`, a git-ignored file of the same `SST_TEST_SNOWFLAKE_<KEY>=value` lines,
   which a shell can also `source` so dbt and `sst` see the same values
   (`tests/live.example.env` is the committed template);
3. with `SST_TEST_SNOWFLAKE_CONNECTION=<name>`, that connection in the Snowflake driver's own files
   under `$SNOWFLAKE_HOME` (default `~/.snowflake`): `[<name>]` in `connections.toml`, else
   `[connections.<name>]` in `config.toml`. It supplies the account, user, role, warehouse and
   authenticator, and every other driver setting it holds, such as a cached-credential switch.

Authentication is by key pair when a private key path is set (directly, or as the connection's
`private_key_file`), and otherwise by the authenticator, such as `externalbrowser` or
`oauth_authorization_code`. Nothing else is accepted, so a run never falls back to a password.
"""

from __future__ import annotations

import os
import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tests.helpers.reference_project import REPO_ROOT

ENV_PREFIX = "SST_TEST_SNOWFLAKE_"
LOCAL_NAME = "live.local.env"
LOCAL_FILE = REPO_ROOT / "tests" / LOCAL_NAME
EXAMPLE_FILE = REPO_ROOT / "tests" / "live.example.env"
GUIDE = "docs/guides/testing-live.md"
# Every key a source may set, without the prefix. The first five must resolve.
REQUIRED = ("ACCOUNT", "USER", "ROLE", "WAREHOUSE", "DATABASE")
OPTIONAL = ("SCHEMA", "PRIVATE_KEY_PATH", "AUTHENTICATOR", "CONNECTION", "GRANTEE_ROLE")
KEYS = REQUIRED + OPTIONAL
# What a named connection may supply, by its driver key; the rest pass through to the driver.
_FROM_CONNECTION = {
    "account": "ACCOUNT",
    "user": "USER",
    "role": "ROLE",
    "warehouse": "WAREHOUSE",
    "database": "DATABASE",
    "authenticator": "AUTHENTICATOR",
    "private_key_file": "PRIVATE_KEY_PATH",
    "private_key_path": "PRIVATE_KEY_PATH",
}
# Driver settings the account itself decides, never taken from a connection as written.
_CONTROLLED = frozenset((*_FROM_CONNECTION, "schema"))
_LINE = re.compile(r"^(?:export\s+)?(?P<key>[A-Z0-9_]+)\s*=\s*(?P<value>.*)$")


class LiveConfigurationError(Exception):
    """The live configuration is started but incomplete, or one of its sources cannot be read."""


@dataclass(frozen=True, slots=True)
class LiveAccount:
    """The account, user, role, warehouse and scratch database the live tests run in.

    `schema` is the reference project's own schema, where the evals suite and the fixture's
    `verify` target deploy; the live layers never write to it. `driver_settings` are the named
    connection's other settings, passed to the driver as they are.
    """

    account: str
    user: str
    role: str
    warehouse: str
    database: str
    schema: str | None = None
    private_key_path: str | None = None
    authenticator: str | None = None
    grantee_role: str | None = None
    driver_settings: tuple[tuple[str, object], ...] = ()

    @property
    def key_pair(self) -> bool:
        """Whether the account signs in with a private key rather than an authenticator."""
        return self.private_key_path is not None

    @classmethod
    def from_environment(cls, environ: Mapping[str, str]) -> LiveAccount | None:
        """Read the account from `environ` alone; None when no account is configured.

        Raises:
            LiveConfigurationError: the account is started but incomplete.
        """
        return cls.from_values({key: environ.get(ENV_PREFIX + key, "") for key in KEYS})

    @classmethod
    def from_values(cls, values: Mapping[str, str], connection: Mapping[str, Any] | None = None) -> LiveAccount | None:
        """Build the account from unprefixed `values` over a named `connection`'s settings.

        Raises:
            LiveConfigurationError: a required value, or any way to authenticate, is missing.
        """
        merged = {key: value for key, value in values.items() if value}
        if not merged.get("ACCOUNT") and not merged.get("CONNECTION"):
            return None
        settings: dict[str, object] = {}
        for name, value in (connection or {}).items():
            if name in _FROM_CONNECTION:
                merged.setdefault(_FROM_CONNECTION[name], str(value))
            elif name not in _CONTROLLED:
                settings[name] = value
        missing = [ENV_PREFIX + key for key in REQUIRED if not merged.get(key)]
        if missing:
            raise LiveConfigurationError(f"set {', '.join(missing)} as well (see {GUIDE})")
        if not merged.get("PRIVATE_KEY_PATH") and not merged.get("AUTHENTICATOR"):
            raise LiveConfigurationError(
                f"set {ENV_PREFIX}PRIVATE_KEY_PATH, {ENV_PREFIX}AUTHENTICATOR or {ENV_PREFIX}CONNECTION"
                f" so the tests can sign in (see {GUIDE})"
            )
        return cls(
            account=merged["ACCOUNT"],
            user=merged["USER"],
            role=merged["ROLE"],
            warehouse=merged["WAREHOUSE"],
            database=merged["DATABASE"],
            schema=merged.get("SCHEMA"),
            private_key_path=os.path.expanduser(key) if (key := merged.get("PRIVATE_KEY_PATH")) else None,
            authenticator=None if merged.get("PRIVATE_KEY_PATH") else merged.get("AUTHENTICATOR"),
            grantee_role=merged.get("GRANTEE_ROLE"),
            driver_settings=tuple(sorted(settings.items())),
        )

    def connection_params(self) -> dict[str, object]:
        """The driver settings `SnowflakeConnector` connects with."""
        params: dict[str, object] = dict(self.driver_settings)
        params.update(
            account=self.account, user=self.user, role=self.role, warehouse=self.warehouse, database=self.database
        )
        if self.private_key_path is not None:
            params.update(private_key_file=self.private_key_path, authenticator="SNOWFLAKE_JWT")
        else:
            params["authenticator"] = self.authenticator
        return params

    def environment(self) -> dict[str, str]:
        """The resolved values as `SST_TEST_SNOWFLAKE_*`, for an `sst` subprocess's profile to read."""
        values = {
            "ACCOUNT": self.account,
            "USER": self.user,
            "ROLE": self.role,
            "WAREHOUSE": self.warehouse,
            "DATABASE": self.database,
            "SCHEMA": self.schema,
            "PRIVATE_KEY_PATH": self.private_key_path,
            "AUTHENTICATOR": self.authenticator,
        }
        return {ENV_PREFIX + key: value for key, value in values.items() if value is not None}


def load_live_account(
    environ: Mapping[str, str], local_file: Path | None = None, snowflake_home: Path | None = None
) -> LiveAccount | None:
    """The live account from the environment over the local file over the named connection.

    `local_file` defaults to `LOCAL_FILE`, and `snowflake_home` to `$SNOWFLAKE_HOME` or
    `~/.snowflake`. None when no source configures an account.

    Raises:
        LiveConfigurationError: the configuration is incomplete, or a source cannot be read.
    """
    values = read_local_file(LOCAL_FILE if local_file is None else local_file)
    values.update({key: environ[ENV_PREFIX + key] for key in KEYS if environ.get(ENV_PREFIX + key)})
    name = values.get("CONNECTION")
    if not name:
        return LiveAccount.from_values(values)
    home = snowflake_home or Path(environ.get("SNOWFLAKE_HOME") or Path.home() / ".snowflake")
    return LiveAccount.from_values(values, named_connection(home, name))


def not_configured_reason() -> str:
    """Why a live test is skipped when no source configures an account."""
    return (
        f"no live Snowflake account: set {ENV_PREFIX}ACCOUNT or {ENV_PREFIX}CONNECTION, in the environment"
        f" or in tests/{LOCAL_NAME} (copy tests/{EXAMPLE_FILE.name}; see {GUIDE})"
    )


def read_local_file(path: Path) -> dict[str, str]:
    """The unprefixed values a local `KEY=value` file sets; empty when the file is absent.

    Blank lines and `#` comments are skipped, an `export ` prefix is allowed, and a value may be
    wrapped in matching quotes.

    Raises:
        LiveConfigurationError: a line is not `SST_TEST_SNOWFLAKE_<KEY>=value` for a known key.
    """
    if not path.is_file():
        return {}
    values: dict[str, str] = {}
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        found = _LINE.match(text)
        key = found["key"].removeprefix(ENV_PREFIX) if found else ""
        if found is None or not found["key"].startswith(ENV_PREFIX) or key not in KEYS:
            raise LiveConfigurationError(f"{path.name}:{number}: expected {ENV_PREFIX}<KEY>=value, one of {KEYS}")
        values[key] = _unquoted(found["value"].strip())
    return values


def named_connection(home: Path, name: str) -> dict[str, Any]:
    """The settings of connection `name` in the driver's files under `home`.

    Raises:
        LiveConfigurationError: neither file holds the connection, or one cannot be parsed.
    """
    for path, table in ((home / "connections.toml", ()), (home / "config.toml", ("connections",))):
        if not path.is_file():
            continue
        try:
            document: Any = tomllib.loads(path.read_text(encoding="utf-8"))
        except tomllib.TOMLDecodeError as error:
            raise LiveConfigurationError(f"cannot read {path}: {error}") from None
        for key in table:
            document = document.get(key, {}) if isinstance(document, dict) else {}
        found = document.get(name) if isinstance(document, dict) else None
        if isinstance(found, dict):
            return found
    raise LiveConfigurationError(f"no connection named {name!r} in {home}/connections.toml or {home}/config.toml")


def _unquoted(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
        return value[1:-1]
    return value
