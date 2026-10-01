"""The connector's enrich reads, and the offline double the use case is tested with."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.adapters.snowflake.connector import SnowflakeConnector
from snowflake_semantic_tools.adapters.snowflake.connector.cortex import completion_sql, object_constant
from snowflake_semantic_tools.adapters.snowflake.connector.profiler import (
    COLUMNS_PER_QUERY,
    columns_sql,
    distinct_values_sql,
)
from snowflake_semantic_tools.domain.model.enrich import TABLE_SYNONYMS_SCHEMA, WarehouseColumn
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import QueryResult
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePortError
from tests.helpers.enrich_ports import ScriptedEnrich

ORDERS = QualifiedName.parse('db.sch."Orders"')


class _Recorded(SnowflakeConnector):
    """The connector with its query layer replaced: each statement takes the next scripted result."""

    def __init__(self, *results: QueryResult | Exception) -> None:
        self._results = list(results)
        self.statements: list[tuple[str, object]] = []

    def query(self, sql, params=None):  # type: ignore[no-untyped-def]
        self.statements.append((sql, params))
        result = self._results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def test_columns_are_read_from_the_databases_information_schema_by_stored_name() -> None:
    port = _Recorded(QueryResult(("COLUMN_NAME", "DATA_TYPE"), (("ORDER_ID", "TEXT"), ("Total", "NUMBER"))))
    assert port.relation_columns(ORDERS) == (WarehouseColumn("ORDER_ID", "TEXT"), WarehouseColumn("Total", "NUMBER"))
    assert port.statements == [(columns_sql(ORDERS), ("SCH", "Orders"))]
    assert columns_sql(ORDERS).startswith("SELECT COLUMN_NAME, DATA_TYPE FROM DB.INFORMATION_SCHEMA.COLUMNS ")


@pytest.mark.parametrize(
    "error",
    [
        SnowflakePortError("missing", errno=2003),
        SnowflakePortError("missing", sqlstate="02000"),
        SnowflakePortError("Database 'DB' does not exist or not authorized."),
    ],
)
def test_a_relation_the_role_cannot_see_has_no_columns(error: SnowflakePortError) -> None:
    assert _Recorded(error).relation_columns(ORDERS) is None
    assert _Recorded(QueryResult(("COLUMN_NAME", "DATA_TYPE"), ())).relation_columns(ORDERS) is None


def test_any_other_failure_to_read_columns_propagates() -> None:
    with pytest.raises(SnowflakePortError, match="warehouse suspended"):
        _Recorded(SnowflakePortError("warehouse suspended", sqlstate="57P03")).relation_columns(ORDERS)


def test_values_are_sampled_in_groups_of_columns_and_mapped_back_by_position() -> None:
    columns = [f"C{index}" for index in range(COLUMNS_PER_QUERY + 1)]
    first = QueryResult(("COLUMN_INDEX", "VALUE", "ROW_COUNT"), ((0, "b", 3), (0, "a", 1), (2, None, 1)))
    second = QueryResult(("COLUMN_INDEX", "VALUE", "ROW_COUNT"), ((0, "z", 9),))
    port = _Recorded(first, second)
    values = port.distinct_values(ORDERS, columns, 26)
    assert values["C0"] == ("b", "a") and values["C2"] == () and values[columns[-1]] == ("z",)
    assert len(port.statements) == 2
    assert port.statements[1] == (distinct_values_sql(ORDERS, columns[-1:], 26), None)


def test_the_sampling_statement_quotes_names_and_orders_deterministically() -> None:
    sql = distinct_values_sql(ORDERS, ["STATUS", "Mixed Case"], 26)
    branches = sql.split("\nUNION ALL\n")
    assert branches[0] == (
        '(SELECT 0 AS COLUMN_INDEX, TO_VARCHAR(STATUS) AS VALUE, COUNT(*) AS ROW_COUNT FROM DB.SCH."Orders" '
        "WHERE STATUS IS NOT NULL GROUP BY STATUS ORDER BY ROW_COUNT DESC, VALUE LIMIT 26)"
    )
    assert '"Mixed Case"' in branches[1]
    assert sql.endswith("\nORDER BY COLUMN_INDEX, ROW_COUNT DESC, VALUE")


def test_cortex_is_asked_through_a_bound_statement_with_the_schema_as_a_constant() -> None:
    port = _Recorded(QueryResult(("RESPONSE",), (('{"synonyms": ["sales"]}',),)))
    assert port.complete_json("claude-sonnet-4-6", "prompt", TABLE_SYNONYMS_SCHEMA) == {"synonyms": ["sales"]}
    sql, params = port.statements[0]
    assert params == ("claude-sonnet-4-6", "prompt")
    assert sql == completion_sql(TABLE_SYNONYMS_SCHEMA)
    assert "{'temperature': 0}" in sql and "'additionalProperties': FALSE" in sql
    assert _Recorded(QueryResult(("RESPONSE",), ())).complete_json("m", "p", {}) is None


def test_a_model_that_is_not_a_plain_name_is_refused_before_any_statement() -> None:
    port = _Recorded()
    with pytest.raises(SnowflakePortError, match="not a Cortex model name"):
        port.complete_json("mistral'); DROP TABLE x; --", "p", {})
    assert port.statements == []


def test_object_constants_quote_every_string_and_double_percent_signs() -> None:
    value = {"it's": ["a%b", 1, 2.5, True, False, None]}
    assert object_constant(value) == "{'it''s': ['a%%b', 1, 2.5, TRUE, FALSE, NULL]}"
    with pytest.raises(SnowflakePortError, match="cannot write set"):
        object_constant({"bad": {1}})


def test_the_offline_double_follows_the_profiler_contract() -> None:
    double = ScriptedEnrich(
        columns={ORDERS.sql: [("ORDER_ID", "TEXT")]},
        values={ORDERS.sql: {"ORDER_ID": ["a", "b", "c"]}},
        answer=lambda prompt, schema: {"echo": prompt, "required": schema["required"]},
        failures={"DB.SCH.BROKEN": SnowflakePortError("broken")},
    )
    assert double.relation_columns(ORDERS) == (WarehouseColumn("ORDER_ID", "TEXT"),)
    assert double.relation_columns(QualifiedName.parse("db.sch.missing")) is None
    assert double.distinct_values(ORDERS, ["ORDER_ID", "OTHER"], 2) == {"ORDER_ID": ("a", "b"), "OTHER": ()}
    with pytest.raises(SnowflakePortError, match="broken"):
        double.relation_columns(QualifiedName.parse("db.sch.broken"))
    assert double.complete_json("m", "hi", TABLE_SYNONYMS_SCHEMA) == {"echo": "hi", "required": ["synonyms"]}
    failing = ScriptedEnrich(cortex_failure=SnowflakePortError("no model"))
    with pytest.raises(SnowflakePortError, match="no model"):
        failing.complete_json("m", "p", {})
    assert failing.prompts == ["p"] and double.calls[0] == ("columns", ORDERS.sql)
