"""`observe` on concurrent sessions: the same observation and diagnostics as one at a time.

Every read here waits a random moment first, so concurrent reads finish in a different order
each run; the observation, its key order, and its diagnostics must not depend on that order.
"""

from __future__ import annotations

import random
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager

import pytest

from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.app.observe import observe
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import GrantRow, ShowRow, SnowflakeObservation
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.ports.snowflake.catalog import AgentVersion, AgentVersions, CatalogPort
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.artifact_builders import rendered
from tests.helpers.snowflake_fake import FakeSnowflake


class _Jittery(FakeSnowflake):
    """A catalog each read of which waits a random moment, recording how many ran at once."""

    def __init__(self) -> None:
        super().__init__()
        self.running = 0
        self.most_running = 0
        self.refused_listing: tuple[str, str] | None = None
        self.refused_grants: str | None = None
        self.broken_agent: str | None = None
        self._guard = threading.Lock()

    @contextmanager
    def _reading(self) -> Iterator[None]:
        with self._guard:
            self.running += 1
            self.most_running = max(self.most_running, self.running)
        try:
            time.sleep(random.uniform(0, 0.004))
            yield
        finally:
            with self._guard:
                self.running -= 1

    def show_objects(self, object_type: str, scope: SchemaScope) -> tuple[ShowRow, ...]:
        with self._reading():
            if self.refused_listing == (object_type.upper(), scope.sql):
                raise SnowflakePortError(f"listing {object_type} refused")
            return super().show_objects(object_type, scope)

    def show_grants(
        self, object_type: str, qualified_name: QualifiedName, routine_signature: tuple[str, ...] = ()
    ) -> tuple[GrantRow, ...]:
        with self._reading():
            if qualified_name.sql == self.refused_grants:
                raise SnowflakePortError(f"grants on {qualified_name.sql} refused")
            return super().show_grants(object_type, qualified_name, routine_signature)

    def observe_agent_versions(self, qualified_name: QualifiedName) -> AgentVersions:
        with self._reading():
            if qualified_name.sql == self.broken_agent:
                raise RuntimeError(f"versions of {qualified_name.sql} broke")
            return super().observe_agent_versions(qualified_name)


class _Pool:
    """Lends the one catalog to every worker: the fake is safe to read from many threads."""

    def __init__(self, port: CatalogPort) -> None:
        self._port = port

    @contextmanager
    def lease(self) -> Iterator[CatalogPort]:
        yield self._port

    def halt(self, reason: str) -> None:
        del reason


def _catalog() -> tuple[_Jittery, dict[str, object]]:
    port = _Jittery()
    views = [rendered(f"V{index}") for index in range(8)]
    for index, view in enumerate(views):
        port.show(ShowRow(view.target.name.folded, "DB", "SCHEMA", "OWNER", "now", f"view {index}"))
        port.grants[view.target.sql] = (GrantRow("SELECT", "ROLE", f"R{index % 3}"), GrantRow("SELECT", "ROLE", "A"))
    for index in range(4):
        port.show(ShowRow(f"W{index}", "DB", "OTHER", "OWNER", "now"))
        name = f"DB.SCHEMA.AGENT{index}"
        port.show(ShowRow(f"AGENT{index}", "DB", "SCHEMA", "OWNER", "now", object_type="AGENT"))
        port.agent_version_rows[name] = (AgentVersion(None, None), AgentVersion(f"VERSION${index}", "LAST"))
    port.refused_grants = views[3].target.sql
    port.refused_listing = ("CORTEX SEARCH SERVICE", "DB.OTHER")
    arguments: dict[str, object] = {
        "targets": (views[0].target, QualifiedName.parse("DB.OTHER.X")),
        "desired_artifacts": {view.key: view for view in views},
    }
    return port, arguments


def _observed(port: CatalogPort, workers: int, arguments: dict[str, object]) -> tuple[object, ...]:
    observation, diagnostics = observe(
        port,
        SEMANTIC_REGISTRY,
        arguments["targets"],  # type: ignore[arg-type]  # built by _catalog
        fetched_at="now",
        desired_artifacts=arguments["desired_artifacts"],  # type: ignore[arg-type]  # built by _catalog
        readers=Fanout(port, _Pool(port), workers),
    )
    return _comparable(observation, diagnostics)


def _comparable(observation: SnowflakeObservation, diagnostics: DiagnosticBag) -> tuple[object, ...]:
    return tuple(observation.artifacts.items()), tuple(diagnostics), observation.fetched_at


def test_concurrent_observation_is_the_serial_one_whatever_order_reads_finish_in() -> None:
    port, arguments = _catalog()
    serial = _observed(port, 1, arguments)
    assert port.most_running == 1
    artifacts, diagnostics, _ = serial
    assert len(artifacts) == 16  # type: ignore[arg-type]  # a tuple of items
    # The grants read of a view listed first, then the listing of a type observed after views.
    [grants, listing] = [item.message for item in diagnostics]  # type: ignore[attr-defined]  # Diagnostics
    assert "grants on DB.SCHEMA.V3" in grants
    assert "CORTEX SEARCH SERVICE in DB.OTHER" in listing
    for _ in range(5):
        assert _observed(port, 6, arguments) == serial
    assert port.most_running > 1


def test_observing_without_readers_is_observing_one_at_a_time() -> None:
    port, arguments = _catalog()
    observation, diagnostics = observe(
        port,
        SEMANTIC_REGISTRY,
        arguments["targets"],  # type: ignore[arg-type]  # built by _catalog
        fetched_at="now",
        desired_artifacts=arguments["desired_artifacts"],  # type: ignore[arg-type]  # built by _catalog
    )
    assert _comparable(observation, diagnostics) == _observed(port, 1, arguments)


@pytest.mark.parametrize("workers", [1, 4])
def test_a_read_that_raises_propagates_however_many_sessions_read(workers: int) -> None:
    port, arguments = _catalog()
    port.broken_agent = "DB.SCHEMA.AGENT2"
    with pytest.raises(RuntimeError, match="versions of DB.SCHEMA.AGENT2 broke"):
        _observed(port, workers, arguments)
