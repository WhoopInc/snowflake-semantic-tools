"""The connected column-tag check: what it reads, and which tags it reports unrecorded (SST-VAL338)."""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.app.validate import ValidateArtifacts
from snowflake_semantic_tools.app.validate_tags import TagAnswer, read_tags, tag_findings
from snowflake_semantic_tools.domain.model.config_schema import governance_config
from snowflake_semantic_tools.domain.model.dbt import DbtColumn, DbtModel
from snowflake_semantic_tools.domain.model.governance import TagPattern
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import QueryResult
from snowflake_semantic_tools.domain.model.semantic_view import SemanticView, Table
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.sql_values import statement
from tests.helpers.val_codes import TaggedWarehouse, tagged_models

PATTERNS = (TagPattern.parse("SEC.GOV.*"),)
GOVERNANCE = governance_config({"governance": {"sensitive_tags": ["SEC.GOV.*"], "read_column_tags": True}})


def _compiled(*views: SemanticView) -> CompileResult:
    return CompileResult(tuple(CompiledView(view, statement(f"CREATE SEMANTIC VIEW {view.fqn}")) for view in views))


def test_a_refused_read_or_an_answer_naming_no_tag_is_kept_as_the_error() -> None:
    refused = FakeSnowflake()
    refused.fail("query", SnowflakePortError("does not exist or not authorized", errno=2003))
    assert isinstance(read_tags(refused, "DB.S.ORDERS"), SnowflakePortError)

    class Shapeless(FakeSnowflake):
        def query(self, sql: object, params: object = None) -> QueryResult:
            return QueryResult(("NAME",), ())

    shapeless = read_tags(Shapeless(), "DB.S.ORDERS")
    assert isinstance(shapeless, SnowflakePortError) and "without its tag" in str(shapeless)


def test_each_relation_is_checked_once_against_its_model_and_a_source_table_is_not() -> None:
    columns = (
        DbtColumn("email", None, "TEXT", "dimension"),
        DbtColumn("phone", None, "TEXT", "dimension", governance_tags=("SEC.GOV.PHONE",)),
        DbtColumn("hidden", None, "TEXT", "dimension", excluded=True),
    )
    view = tagged_models(*columns)
    with_source = SemanticView(
        "DB.S.OTHER", (Table("ORDERS", "DB.S.ORDERS"), Table("RAW", "DB.RAW.EVENTS")), table_models=view.table_models
    )
    tags = {
        "email": (QualifiedName.parse("SEC.GOV.EMAIL"), QualifiedName.parse("OTHER.S.T")),
        "phone": (QualifiedName.parse("SEC.GOV.PHONE"), QualifiedName.parse("SEC.GOV.MOBILE")),
        "hidden": (QualifiedName.parse("SEC.GOV.SSN"),),
        "ssn": (QualifiedName.parse("SEC.GOV.SSN"),),
    }
    answers: dict[str, TagAnswer] = {
        "DB.S.ORDERS": MappingProxyType(tags),
        "DB.RAW.EVENTS": SnowflakePortError("never read"),
    }
    found = tag_findings((view, with_source), answers, PATTERNS)
    assert [(item.context["column"], item.context["name"]) for item in found] == [
        ("email", "SEC.GOV.EMAIL"),
        ("phone", "SEC.GOV.MOBILE"),
        ("ssn", "SEC.GOV.SSN"),
    ]
    assert found[2].subject == "dbt_column:orders.ssn"


def test_a_relation_whose_tags_cannot_be_read_is_reported_as_skipped() -> None:
    view = tagged_models(DbtColumn("email", None, "TEXT", "dimension"))
    [skipped] = tag_findings((view,), {"DB.S.ORDERS": SnowflakePortError("\nnot authorized")}, PATTERNS)
    assert (skipped.code, skipped.context["rule_id"]) == ("SST-VAL020", "SST-VAL338")
    assert skipped.message == "SST-VAL338 skipped: the column tags of DB.S.ORDERS were not read: not authorized"
    [empty] = tag_findings((view,), {"DB.S.ORDERS": SnowflakePortError("")}, PATTERNS)
    assert empty.message.endswith("were not read: SnowflakePortError")


def test_connected_validation_reads_tags_only_when_governance_reads_them() -> None:
    rows = {"DB.S.ORDERS": (("EMAIL", "SEC.GOV.EMAIL", "SCHEMA", "INHERITED"),)}
    view = tagged_models(DbtColumn("email", None, "TEXT", "dimension"))
    for governance in (None, governance_config({"governance": {"sensitive_tags": ["SEC.GOV.*"]}})):
        quiet = TaggedWarehouse(rows)
        ValidateArtifacts(quiet, governance=governance).run(_compiled(view), strict=False, connected=True)
        assert quiet.tag_reads == []
    warehouse = TaggedWarehouse(rows, refused={"DB.S.CUSTOMERS": "does not exist or not authorized"})
    other = SemanticView(
        "DB.S.MENU",
        (Table("ORDERS", "DB.S.ORDERS"), Table("CUSTOMERS", "DB.S.CUSTOMERS")),
        columns=view.columns,
        table_models=MappingProxyType(
            {**view.table_models, "CUSTOMERS": DbtModel("model.p.customers", "customers", "DB.S.CUSTOMERS", (), (), ())}
        ),
    )
    result = ValidateArtifacts(warehouse, governance=GOVERNANCE).run(
        _compiled(view, other), strict=False, connected=True
    )
    assert sorted(warehouse.tag_reads) == ["DB.S.CUSTOMERS", "DB.S.ORDERS"]
    assert [(item.code, item.subject) for item in result.diagnostics] == [
        ("SST-VAL020", None),
        ("SST-VAL338", "dbt_column:orders.email"),
    ]
