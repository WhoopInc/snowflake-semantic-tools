"""The one Snowflake fake the suite runs against: the narrow ports, in memory, and the driver.

- `FakeSnowflake` (`port.py`) implements every Snowflake port the application takes, one
  module per role (`catalog`, `execution`, `stage`, `profile_registry`, `state`, `preflight`)
  over one shared account (`world.py`): one statement log, one scripted-response mechanism,
  and failure injection. It answers only what the real connector answers, from what the test
  staged; a test that needs
  another answer subclasses or wraps it.
- `ReadOnlySnowflake` wraps a port so that every write is refused.
- `FakeDriverSession` (`driver.py`) stands below the real connector, in place of the
  Snowflake driver's connection and cursor, for the connector's own tests; `FakeDriverConnector`
  is the real connector on one.

The `run_recorded_*` scripts beside this package import it by bare name (`snowflake_fake`).
"""

from __future__ import annotations

from tests.helpers.snowflake_fake.port import FakeSnowflake, failed
from tests.helpers.snowflake_fake.profile_registry import PROFILE_REGISTRY_SHAPE
from tests.helpers.snowflake_fake.read_only import ReadOnlySnowflake
from tests.helpers.snowflake_fake.run_locks import InMemoryRunLocks
from tests.helpers.snowflake_fake.world import PreflightAnswers, Sent

__all__ = [
    "PROFILE_REGISTRY_SHAPE",
    "FakeSnowflake",
    "InMemoryRunLocks",
    "PreflightAnswers",
    "ReadOnlySnowflake",
    "Sent",
    "failed",
]
