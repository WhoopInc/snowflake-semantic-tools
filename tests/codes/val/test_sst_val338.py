"""SST-VAL338: a column carries a sensitive tag in Snowflake that its YAML does not record."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.app.validate import ValidateArtifacts
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.config_schema import governance_config
from snowflake_semantic_tools.domain.model.dbt import DbtColumn
from snowflake_semantic_tools.domain.sql import Sql
from tests.helpers.sql_values import statement
from tests.helpers.val_codes import TaggedWarehouse, tagged_models

GOVERNANCE = governance_config({"governance": {"sensitive_tags": ["SEC.GOV.*"], "read_column_tags": True}})
EMAIL = (("EMAIL", "SEC.GOV.EMAIL", "COLUMN", "MANUAL"),)


def _found(*columns: DbtColumn) -> list[Diagnostic]:
    view = tagged_models(*columns)
    ddl: Sql = statement(f"CREATE SEMANTIC VIEW {view.fqn}")
    validate = ValidateArtifacts(TaggedWarehouse({"DB.S.ORDERS": EMAIL}), governance=GOVERNANCE)
    result = validate.run(CompileResult((CompiledView(view, ddl),)), strict=False, connected=True)
    return [item for item in result.diagnostics if item.code == "SST-VAL338"]


def test_sst_val338_fires() -> None:
    [found] = _found(DbtColumn("email", None, "TEXT", "dimension"))
    assert found.severity is Severity.WARNING
    assert found.message == (
        "model 'orders': column 'email' carries governance tag SEC.GOV.EMAIL in Snowflake, which its YAML "
        "does not record"
    )
    assert found.subject == "dbt_column:orders.email"


def test_sst_val338_silent() -> None:
    assert _found(DbtColumn("email", None, "TEXT", "dimension", governance_tags=("sec.gov.email",))) == []
    assert _found(DbtColumn("email", None, "TEXT", "dimension", pii_tagged=True)) == []
