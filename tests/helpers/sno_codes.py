"""Driver errors the per-code SNO tests raise, carried through connector and apply as a real refusal is.

`refusal` scripts the connector's driver session to raise one error for a create, takes the
result `execute_script` returns, and has apply run the create against it, so the SNO
diagnostic a test reads is the one a user would see. `enrich_orders` is the enrich run whose
own failures are SNO codes.
"""

from __future__ import annotations

from threading import RLock

from snowflake.connector.errors import ProgrammingError

from snowflake_semantic_tools.adapters.snowflake.connector import SnowflakeConnector
from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.app.enrich import EnrichProject, EnrichRequest
from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.diagnostics.signatures import signature_codes
from snowflake_semantic_tools.domain.enrich import resolve_options
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtColumn, DbtModel
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, RetryPolicy
from snowflake_semantic_tools.domain.sql import sql
from tests.helpers.app_ports import FixedClock, InMemorySnowflake, InMemoryStateStore
from tests.helpers.artifact_builders import change, changeset, rendered, state
from tests.helpers.enrich_ports import InMemoryFiles, ScriptedEnrich
from tests.helpers.project_inputs import InMemoryProjectInputs


class _RefusingSession:
    """A driver session whose every statement raises one error."""

    def __init__(self, error: BaseException) -> None:
        self.error = error
        self.sfqid = "query-id"

    def cursor(self, *args: object) -> _RefusingSession:
        return self

    def execute(self, statement: str, params: object = None, *, num_statements: int | None = None) -> None:
        raise self.error

    def close(self) -> None:
        pass


class _RefusingConnector(SnowflakeConnector):
    def __init__(self, error: BaseException) -> None:
        self._lock = RLock()
        self._connection = _RefusingSession(error)  # type: ignore[assignment]  # a double, not a driver connection


def driver_error(message: str, *, errno: int | None = None, sqlstate: str | None = None) -> ProgrammingError:
    """A driver error as the Snowflake connector raises it, with its number and SQLSTATE when given."""
    if errno is None and sqlstate is None:
        return ProgrammingError(msg=message)
    return ProgrammingError(msg=message, errno=errno, sqlstate=sqlstate)


def driver_result(error: BaseException) -> ExecResult:
    """What the connector's `execute_script` returns when the driver raises `error`."""
    return _RefusingConnector(error).execute_script((sql("CREATE SEMANTIC VIEW DB.SCHEMA.V"),))


def refusal(error: BaseException) -> list[Diagnostic]:
    """Apply one create the driver refuses with `error`; return the SNO diagnostics apply reports."""
    port = InMemorySnowflake()
    # A transient refusal is retried; every attempt meets the same refusal.
    port.execute_results = [driver_result(error)] * RetryPolicy().max_attempts
    artifact = rendered()
    applied = ApplyArtifacts(
        port, InMemoryStateStore(), FixedClock(), state_table=rendered("STATE").target, actor="TEST_ROLE"
    ).run(changeset(change(artifact)), state())
    return [item for item in applied.diagnostics if item.code in signature_codes()]


ORDERS_COLUMNS = [("ORDER_ID", "TEXT"), ("TOTAL", "NUMBER")]


def enrich_orders(port: ScriptedEnrich) -> EnrichProject:
    """`sst enrich` over one dbt model, `orders` at DB.SCH.ORDERS, reading through `port`."""
    model = DbtModel(
        "model.shop.orders",
        "orders",
        "DB.SCH.ORDERS",
        ("order_id",),
        (),
        (DbtColumn("order_id", None, None, None), DbtColumn("total", None, None, None)),
        package_name="shop",
        raw_relation_name="db.sch.orders",
        patch_file="models/orders.yml",
        original_file_path="models/orders.sql",
        description="The orders.",
    )
    inputs = InMemoryProjectInputs(dbt=DbtCatalog("v12", "1.11.2", "shop", (model,)))
    files = InMemoryFiles({"models/orders.yml": "version: 2\nmodels:\n  - name: orders\n"})
    return EnrichProject(inputs, port, files)


def enrich_types() -> EnrichRequest:
    """The default enrich run: column types only."""
    return EnrichRequest(options=resolve_options(frozenset(), frozenset()))
