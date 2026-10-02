"""Suite-wide pytest configuration: the Hypothesis profiles property tests run under.

`ci` is deterministic, so a property failure on a pull request reproduces on rerun, and has
no deadline, because shared runners are too noisy for per-example timing to mean anything.
`dev`, the default, keeps Hypothesis's random search and its local example database. Pick
one with HYPOTHESIS_PROFILE or pytest's `--hypothesis-profile`.

Every test also runs with an empty home directory and none of the environment variables `sst`
reads, so a developer's own `~/.dbt/profiles.yml` or exported `SST_*` setting cannot change what
a test sees.

A test fails when a thread it started is still running shortly after it returns: a run-lease
heartbeat or a pool worker that outlives its test holds locks and ports across tests.
"""

from __future__ import annotations

import os
import threading
from collections.abc import Iterator

import pytest
from hypothesis import settings

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


@pytest.fixture(autouse=True)
def _isolated_environment(tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("HOME", str(tmp_path_factory.mktemp("home")))
    for name in _SST_ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)
    yield


@pytest.fixture(autouse=True)
def _no_leaked_threads() -> Iterator[None]:
    before = set(threading.enumerate())
    yield
    started = [thread for thread in threading.enumerate() if thread not in before]
    for thread in started:
        thread.join(timeout=_THREAD_GRACE_SECONDS)
    running = sorted(thread.name for thread in started if thread.is_alive())
    assert not running, f"the test left threads running: {running}"


settings.register_profile("ci", deadline=None, derandomize=True, database=None, print_blob=True)
settings.register_profile("dev", deadline=None)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "dev"))
