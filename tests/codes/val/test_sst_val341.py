"""SST-VAL341: a sample value Snowflake could not read as its column's data type.

Snowflake casts each `SAMPLE_VALUES` entry to the member's type when it creates the view and
refuses the view when one fails; only a value no reading of the type accepts is reported offline.
"""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.dbt import DbtColumn
from tests.helpers.val_codes import dbt_column_findings


def _values(data_type: str, *values: str, **changes: object) -> list[str]:
    column = DbtColumn("c", "C.", data_type, "dimension", sample_values=values, **changes)  # type: ignore[arg-type]
    return [item.context["value"] for item in dbt_column_findings(column, "SST-VAL341")]


def test_sst_val341_fires() -> None:
    column = DbtColumn("ended_at", "E.", "timestamp_ntz", "time_dimension", sample_values=("2024-01-01", "NaT"))
    [found] = dbt_column_findings(column, "SST-VAL341")
    assert found.severity is Severity.ERROR
    assert found.message == (
        "dbt_model:m: 'ended_at'.sample_values holds 'NaT', which Snowflake cannot read as timestamp_ntz: "
        "it is a missing value written as text"
    )
    assert found.subject == "dbt_column:m.ended_at"


@pytest.mark.parametrize(
    ("data_type", "value"),
    [
        ("FLOAT", "0,2"),
        ("NUMBER(38,0)", "nan"),
        ("NUMBER", "$5"),
        ("FLOAT", "NaT"),
        ("FLOAT", " "),
        ("DATE", "NaT"),
        ("TIME", "noon"),
        ("TIMESTAMP_TZ", ""),
        ("BOOLEAN", "maybe"),
        ("BOOLEAN", "2"),
    ],
)
def test_sst_val341_fires_on_each_type(data_type: str, value: str) -> None:
    assert _values(data_type, value) == [value]


@pytest.mark.parametrize(
    ("data_type", "value"),
    [
        ("FLOAT", "nan"),
        ("DOUBLE", "-inf"),
        ("FLOAT", "1e3"),
        ("NUMBER(38,0)", "1.5"),
        ("NUMBER", " 12 "),
        ("NUMBER", "e3"),
        ("TIMESTAMP_NTZ", "1700000000"),
        ("DATE", "1/2/2024"),
        ("TIMESTAMP_NTZ", "2024-13-45"),
        ("BOOLEAN", " Yes"),
        ("BOOLEAN", "off"),
        ("VARCHAR", "NaT"),
        ("VARIANT", ""),
    ],
)
def test_sst_val341_silent(data_type: str, value: str) -> None:
    assert _values(data_type, value) == []


def test_sst_val341_silent_where_the_values_never_publish() -> None:
    assert _values("FLOAT", "0,2", excluded=True) == []
    assert _values("FLOAT", "0,2", pii_tagged=True) == []
    assert dbt_column_findings(DbtColumn("c", "C.", "FLOAT", None, sample_values=("0,2",)), "SST-VAL341") == []


def test_sst_val341_reports_a_repeated_value_once() -> None:
    assert _values("DATE", "NaT", "NaT") == ["NaT"]
