"""The live configuration, offline: the local file, the named connection, and which source wins.

None of this connects. Every account, path and connection here is synthetic.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from tests.helpers import live_config
from tests.helpers.live_config import (
    ENV_PREFIX,
    LiveAccount,
    LiveConfigurationError,
    load_live_account,
    named_connection,
    not_configured_reason,
    read_local_file,
)
from tests.helpers.reference_project import REPO_ROOT

BROWSER_CONNECTION = """\
[connections.sst-test]
account = "conn-acct"
user = "conn.user@example.com"
authenticator = "oauth_authorization_code"
client_store_temporary_credential = true
role = "CONN_ROLE"
warehouse = "CONN_WH"
schema = "IGNORED"
"""


def write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def test_nothing_configured_is_no_account_and_the_skip_names_what_to_set(tmp_path: Path) -> None:
    assert load_live_account({}, local_file=tmp_path / "absent.env", snowflake_home=tmp_path) is None
    reason = not_configured_reason()
    for named in (f"{ENV_PREFIX}ACCOUNT", f"{ENV_PREFIX}CONNECTION", "tests/live.local.env", "live.example.env"):
        assert named in reason


def test_the_local_file_reads_prefixed_lines_and_the_environment_overrides_it(tmp_path: Path) -> None:
    local = write(
        tmp_path / "live.local.env",
        "# a comment\n\n"
        f"export {ENV_PREFIX}ACCOUNT=file-acct\n"
        f"{ENV_PREFIX}USER='file.user'\n"
        f'{ENV_PREFIX}ROLE="FILE_ROLE"\n'
        f"{ENV_PREFIX}WAREHOUSE=FILE_WH\n"
        f"{ENV_PREFIX}DATABASE=FILE_DB\n"
        f"{ENV_PREFIX}SCHEMA=FILE_SCHEMA\n"
        f"{ENV_PREFIX}PRIVATE_KEY_PATH=~/keys/test.p8\n",
    )
    assert read_local_file(local)["USER"] == "file.user"
    account = load_live_account({f"{ENV_PREFIX}ROLE": "ENV_ROLE"}, local_file=local, snowflake_home=tmp_path)
    assert account == LiveAccount(
        "file-acct",
        "file.user",
        "ENV_ROLE",
        "FILE_WH",
        "FILE_DB",
        schema="FILE_SCHEMA",
        private_key_path=os.path.expanduser("~/keys/test.p8"),
    )
    assert account.key_pair and account.connection_params()["authenticator"] == "SNOWFLAKE_JWT"


@pytest.mark.parametrize(
    "line", ["ACCOUNT=acct", f"{ENV_PREFIX}PASSWORD=x", f"{ENV_PREFIX}ACCOUNT", "not a line"], ids=str
)
def test_the_local_file_refuses_a_line_it_does_not_know(tmp_path: Path, line: str) -> None:
    with pytest.raises(LiveConfigurationError, match=r"live.local.env:2: expected SST_TEST_SNOWFLAKE_<KEY>=value"):
        read_local_file(write(tmp_path / "live.local.env", f"# header\n{line}\n"))


def test_a_named_connection_supplies_what_the_other_sources_leave_unset(tmp_path: Path) -> None:
    write(tmp_path / "home" / "config.toml", BROWSER_CONNECTION)
    local = write(tmp_path / "live.local.env", f"{ENV_PREFIX}CONNECTION=sst-test\n{ENV_PREFIX}DATABASE=TEST_DB\n")
    environ = {f"{ENV_PREFIX}WAREHOUSE": "ENV_WH"}
    account = load_live_account(environ, local_file=local, snowflake_home=tmp_path / "home")
    assert account is not None and not account.key_pair
    assert account.connection_params() == {
        "client_store_temporary_credential": True,
        "account": "conn-acct",
        "user": "conn.user@example.com",
        "role": "CONN_ROLE",
        "warehouse": "ENV_WH",
        "database": "TEST_DB",
        "authenticator": "oauth_authorization_code",
    }
    assert account.environment() == {
        f"{ENV_PREFIX}ACCOUNT": "conn-acct",
        f"{ENV_PREFIX}USER": "conn.user@example.com",
        f"{ENV_PREFIX}ROLE": "CONN_ROLE",
        f"{ENV_PREFIX}WAREHOUSE": "ENV_WH",
        f"{ENV_PREFIX}DATABASE": "TEST_DB",
        f"{ENV_PREFIX}AUTHENTICATOR": "oauth_authorization_code",
    }


def test_connections_toml_is_read_before_config_toml_and_snowflake_home_is_honoured(tmp_path: Path) -> None:
    write(tmp_path / "config.toml", BROWSER_CONNECTION)
    write(tmp_path / "connections.toml", '[sst-test]\naccount = "toml-acct"\nprivate_key_file = "/keys/k.p8"\n')
    assert named_connection(tmp_path, "sst-test") == {"account": "toml-acct", "private_key_file": "/keys/k.p8"}
    environ = {
        "SNOWFLAKE_HOME": str(tmp_path),
        **{f"{ENV_PREFIX}{key}": "X" for key in ("CONNECTION", "USER", "ROLE", "WAREHOUSE", "DATABASE")},
    }
    environ[f"{ENV_PREFIX}CONNECTION"] = "sst-test"
    account = load_live_account(environ, local_file=tmp_path / "absent.env")
    assert account is not None and account.private_key_path == "/keys/k.p8" and account.authenticator is None


def test_a_missing_or_unreadable_connection_is_refused(tmp_path: Path) -> None:
    with pytest.raises(LiveConfigurationError, match="no connection named 'absent'"):
        named_connection(tmp_path, "absent")
    write(tmp_path / "config.toml", "connections = 3\n")
    with pytest.raises(LiveConfigurationError, match="no connection named 'absent'"):
        named_connection(tmp_path, "absent")
    write(tmp_path / "connections.toml", "[broken\n")
    with pytest.raises(LiveConfigurationError, match="cannot read"):
        named_connection(tmp_path, "absent")


def test_an_account_that_cannot_sign_in_or_is_incomplete_is_refused() -> None:
    values = {"ACCOUNT": "acct", "USER": "u", "ROLE": "R", "WAREHOUSE": "W", "DATABASE": "D"}
    with pytest.raises(LiveConfigurationError, match="PRIVATE_KEY_PATH, SST_TEST_SNOWFLAKE_AUTHENTICATOR or"):
        LiveAccount.from_values(values)
    with pytest.raises(LiveConfigurationError, match=f"set {ENV_PREFIX}USER as well"):
        LiveAccount.from_values({**values, "USER": "", "AUTHENTICATOR": "externalbrowser"})
    browser = LiveAccount.from_values({**values, "AUTHENTICATOR": "externalbrowser", "GRANTEE_ROLE": "G"})
    assert browser is not None and browser.grantee_role == "G"
    assert browser.connection_params()["authenticator"] == "externalbrowser"


def test_an_offline_test_sees_no_live_configuration() -> None:
    assert not [name for name in os.environ if name.startswith(ENV_PREFIX)]
    assert not live_config.LOCAL_FILE.exists()
    assert load_live_account(os.environ) is None


def test_the_committed_example_parses_documents_every_key_and_is_ignored_once_copied() -> None:
    text = live_config.EXAMPLE_FILE.read_text(encoding="utf-8")
    assert sorted(read_local_file(live_config.EXAMPLE_FILE)) == sorted(
        ("DATABASE", "SCHEMA", "ACCOUNT", "USER", "ROLE", "WAREHOUSE", "PRIVATE_KEY_PATH")
    )
    assert [key for key in live_config.KEYS if f"{ENV_PREFIX}{key}=" not in text] == []
    try:
        ignored = {
            path: subprocess.run(["git", "check-ignore", "-q", path], cwd=REPO_ROOT, check=False).returncode == 0
            for path in ("tests/live.example.env", "tests/live.local.env")
        }
    except OSError as error:
        pytest.skip(f"needs git: {error}")
    assert ignored == {"tests/live.example.env": False, "tests/live.local.env": True}
