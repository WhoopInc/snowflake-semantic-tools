"""SST-VAL340: a committed sample value is shaped like personal data."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity
from snowflake_semantic_tools.domain.model.dbt import DbtColumn
from tests.helpers.val_codes import dbt_column_findings


def test_sst_val340_fires() -> None:
    column = DbtColumn("anonymous_id", "Visitor.", "VARCHAR", "dimension", sample_values=("a1", "jane@example.com"))
    [found] = dbt_column_findings(column, "SST-VAL340")
    assert found.severity is Severity.ERROR
    assert found.message == "model 'm': column 'anonymous_id' has a sample value that holds an email address"
    assert found.subject == "dbt_column:m.anonymous_id"
    assert "jane" not in found.message


def test_sst_val340_fires_on_a_tagged_column_too() -> None:
    column = DbtColumn("phone", "P.", "VARCHAR", "dimension", sample_values=("(555) 123-4567",), pii_tagged=True)
    [found] = dbt_column_findings(column, "SST-VAL340")
    assert found.message.endswith("holds a phone number")


def test_sst_val340_cannot_be_demoted_or_baselined() -> None:
    assert ERROR_REGISTRY["SST-VAL340"].demotable is False


def test_sst_val340_silent() -> None:
    column = DbtColumn("firmware_version", "F.", "VARCHAR", "dimension", sample_values=("4.10.2.1", "5.0.0.12"))
    assert dbt_column_findings(column, "SST-VAL340") == []
