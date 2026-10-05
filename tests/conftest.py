"""Suite-wide pytest configuration: the Hypothesis profiles property tests run under.

`ci` is deterministic, so a property failure on a pull request reproduces on rerun, and has
no deadline, because shared runners are too noisy for per-example timing to mean anything.
`nightly` searches at random and much deeper, for the scheduled run that has time to.
`dev`, the default, keeps Hypothesis's random search and its local example database. Pick
one with HYPOTHESIS_PROFILE or pytest's `--hypothesis-profile`.

Every test also runs with an empty home directory and none of the environment variables `sst`
reads, so a developer's own `~/.dbt/profiles.yml` or exported `SST_*` setting cannot change what
a test sees. An offline test also sees no live configuration: no `SST_TEST_SNOWFLAKE_*` variable
and no local file.

A test fails when a thread it started is still running shortly after it returns: a run-lease
heartbeat or a pool worker that outlives its test holds locks and ports across tests.

No test outside the `live` marker reaches the network: for the whole session, a connection or a
name lookup to anything but a Unix socket or a loopback address raises, and a refusal the code under
test swallowed still fails the test (`tests/helpers/network_guard.py`). A live test, from the setup
of the fixtures it brings in to its teardown, runs with the guard lifted.

A test marked `live` connects to the Snowflake account the live configuration describes -- the
`SST_TEST_SNOWFLAKE_*` environment, the git-ignored `tests/live.local.env`, or a named connection
(`tests/helpers/live_config.py`). It keeps the real home directory, where the driver finds its
connection files and cached credentials, and its `sst` subprocesses see the resolved values as
`SST_TEST_SNOWFLAKE_*`. It runs only when the marker expression selects live tests (`-m live`), so a
run that clears `addopts` never connects. With no account it is skipped, and with
`--require-snowflake` it fails instead, so a gate that could not connect is never green. Live tests
share one connector per
worker and work only in scratch schemas that `scratch_schema` creates and drops
(`tests/helpers/live_snowflake.py`).
"""

from __future__ import annotations

import os
import threading
from collections.abc import Callable, Generator, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from hypothesis import settings

from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from tests.helpers import live_config, network_guard
from tests.helpers.live_snowflake import (
    ENV_PREFIX,
    LiveAccount,
    LiveConfigurationError,
    create_scratch,
    drop_scratch,
    load_live_account,
    not_configured_reason,
    run_token,
    scratch_schema_name,
    scratch_scope,
)

if TYPE_CHECKING:
    from snowflake_semantic_tools.adapters.snowflake.connector import SnowflakeConnector

_SST_ENVIRONMENT = (
    "SST_PROJECT_DIR",
    "SST_CONFIG",
    "SST_PROFILES_DIR",
    "SST_TARGET",
    "SST_DEFER_TARGET",
    "SST_STATE_DIR",
    "SST_THREADS",
    "SST_OUTPUT",
    "SST_LOG_LEVEL",
    "SST_STRICT",
    "SST_NO_COLOR",
    "NO_COLOR",
    "DBT_PROFILES_DIR",
)
# How long a thread a test started may take to finish after the test returns, before it counts as
# left running.
_THREAD_GRACE_SECONDS = 2.0
# Read before any test replaces HOME, so a live test finds the driver's connection files.
_REAL_HOME = Path.home()


@pytest.fixture(autouse=True)
def _isolated_environment(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch
) -> Iterator[None]:
    for name in _SST_ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)
    if "live" not in request.keywords:
        monkeypatch.setenv("HOME", str(tmp_path_factory.mktemp("home")))
        for name in [name for name in os.environ if name.startswith(ENV_PREFIX)]:
            monkeypatch.delenv(name)
        monkeypatch.setattr(live_config, "LOCAL_FILE", tmp_path_factory.mktemp("live") / "absent.env")
    yield


@pytest.fixture(scope="session", autouse=True)
def _no_network() -> Iterator[None]:
    with network_guard.installed():
        yield


@pytest.fixture(autouse=True)
def _no_swallowed_network_refusal(request: pytest.FixtureRequest) -> Iterator[None]:
    if "live" in request.keywords:
        yield
        return
    before = network_guard.refusal_count()
    yield
    refused = network_guard.refusals_since(before)
    assert not refused, f"the test tried to reach the network: {refused}"


@pytest.hookimpl(wrapper=True)
def pytest_runtest_protocol(item: pytest.Item, nextitem: pytest.Item | None) -> Generator[None, object, object]:
    # Wider-scoped fixtures a live test brings in are set up inside its protocol, so they connect too.
    if "live" not in item.keywords:
        return (yield)
    with network_guard.allowed():
        return (yield)


@pytest.fixture(autouse=True)
def _no_leaked_threads(request: pytest.FixtureRequest) -> Iterator[None]:
    # A live test runs the Snowflake driver, whose own threads outlive any one statement.
    if "live" in request.keywords:
        yield
        return
    before = set(threading.enumerate())
    yield
    started = [thread for thread in threading.enumerate() if thread not in before]
    for thread in started:
        thread.join(timeout=_THREAD_GRACE_SECONDS)
    running = sorted(thread.name for thread in started if thread.is_alive())
    assert not running, f"the test left threads running: {running}"


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--require-snowflake",
        action="store_true",
        help="Fail, rather than skip, every live test when no Snowflake account is configured.",
    )


def _configured_account() -> LiveAccount | None:
    home = Path(os.environ.get("SNOWFLAKE_HOME") or _REAL_HOME / ".snowflake")
    return load_live_account(os.environ, snowflake_home=home)


def pytest_runtest_setup(item: pytest.Item) -> None:
    if "live" not in item.keywords:
        return
    # A configured account must not turn an ordinary run, such as one that clears `addopts`, into
    # a connected one: a live test runs only when the marker expression asks for live tests.
    if "live" not in (item.config.getoption("markexpr") or ""):
        pytest.skip("live tests run only when selected with -m live")
    try:
        account = _configured_account()
    except LiveConfigurationError as error:
        pytest.fail(f"the live Snowflake configuration is incomplete: {error}")
    if account is not None:
        return
    reason = not_configured_reason()
    if item.config.getoption("--require-snowflake"):
        pytest.fail(f"--require-snowflake, but {reason}")
    pytest.skip(reason)


@pytest.fixture(scope="session")
def live_account() -> Iterator[LiveAccount]:
    """The configured account, with its values exported as `SST_TEST_SNOWFLAKE_*` for `sst` subprocesses."""
    account = _configured_account()
    assert account is not None, "pytest_runtest_setup lets a live test run only with an account"
    with pytest.MonkeyPatch.context() as patch:
        for name, value in account.environment().items():
            patch.setenv(name, value)
        yield account


@pytest.fixture(scope="session")
def live_connector(live_account: LiveAccount) -> Iterator[SnowflakeConnector]:
    from snowflake_semantic_tools.adapters.snowflake.connector import SnowflakeConnector

    connector = SnowflakeConnector(live_account.connection_params())
    try:
        yield connector
    finally:
        # Session teardown runs with the last test, which need not be a live one.
        with network_guard.allowed():
            connector.close()


@pytest.fixture(scope="session")
def scratch_schema(
    live_account: LiveAccount, live_connector: SnowflakeConnector
) -> Iterator[Callable[[str], SchemaScope]]:
    """Create a scratch schema per label on first use; drop every one when the session ends."""
    run = run_token(os.environ)
    worker = os.environ.get("PYTEST_XDIST_WORKER", "main")
    created: dict[str, SchemaScope] = {}

    def schema(label: str) -> SchemaScope:
        if label not in created:
            name = scratch_schema_name(run, f"{worker}{label}", datetime.now(UTC))
            created[label] = scratch_scope(live_account, name)
            create_scratch(live_connector, created[label])
        return created[label]

    try:
        yield schema
    finally:
        with network_guard.allowed():
            for scope in created.values():
                drop_scratch(live_connector, scope)


settings.register_profile("ci", deadline=None, derandomize=True, database=None, print_blob=True)
settings.register_profile("nightly", deadline=None, max_examples=1000, database=None, print_blob=True)
settings.register_profile("dev", deadline=None)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "dev"))
