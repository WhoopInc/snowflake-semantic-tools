"""SST-SNO025: a DESCRIBE result does not have the columns SST reads from it."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.snowflake_fake.driver import FakeDriverConnector, FakeDriverSession

TABLE = QualifiedName.parse("DB.SCH.ORDERS")


class _Described(FakeDriverConnector):
    """A connector whose table exists, and whose DESCRIBE TABLE returns the rows given."""

    def __init__(self, rows: dict[str, tuple[dict[str, object], ...]]) -> None:
        super().__init__(FakeDriverSession(rows=rows))

    def object_exists(self, object_type: str, qualified_name: QualifiedName) -> bool:
        return True


def test_sst_sno025_fires() -> None:
    connector = _Described({"DESCRIBE TABLE": ({"column": "ID", "kind": "NUMBER"},)})
    with pytest.raises(SnowflakePortError) as raised:
        connector.table_columns(TABLE)
    diagnostic = raised.value.diagnostic
    assert diagnostic is not None
    assert (diagnostic.code, diagnostic.severity) == ("SST-SNO025", Severity.ERROR)
    assert diagnostic.message == "DESCRIBE TABLE DB.SCH.ORDERS returned an unexpected shape"


def test_sst_sno025_silent() -> None:
    connector = _Described({"DESCRIBE TABLE": ({"name": "ID", "type": "NUMBER"},)})
    assert connector.table_columns(TABLE) == (("ID", "NUMBER"),)
