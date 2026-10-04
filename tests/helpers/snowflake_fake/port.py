"""`FakeSnowflake`: every narrow Snowflake port over one in-memory account."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, ExecutionError
from tests.helpers.snowflake_fake.catalog import FakeCatalog
from tests.helpers.snowflake_fake.execution import FakeExecution
from tests.helpers.snowflake_fake.preflight import FakePreflight
from tests.helpers.snowflake_fake.profile_registry import FakeProfileRegistry
from tests.helpers.snowflake_fake.stage import FakeStage
from tests.helpers.snowflake_fake.state import FakeState


class FakeSnowflake(FakeCatalog, FakeExecution, FakeStage, FakeProfileRegistry, FakeState, FakePreflight):
    """`SnowflakePort` and `PreflightPort` in memory, each role over the same account.

    A test stages what the account holds through the constructor and the attributes
    `SnowflakeWorld` documents, scripts answers and failures, runs the use case, and reads the
    log (`log`, or its `queries`, `scripts`, and `uploads` views) and the account it left.
    A test that needs an answer the account cannot model subclasses this and answers it.
    """


def failed(message: str, sqlstate: str | None = None) -> ExecResult:
    """A script outcome that failed with `message`, as `execute_results` scripts one."""
    return ExecResult(False, error=ExecutionError(message, sqlstate))
