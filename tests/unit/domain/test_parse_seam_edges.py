"""The edges of the parse and dbt-seam helpers that no single code's pair reaches."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtModel
from snowflake_semantic_tools.domain.parse.fields import edit_distance, nearest_field
from snowflake_semantic_tools.domain.parse.names import identifier_problem
from snowflake_semantic_tools.domain.validate.dbt_seam import literal_relation_diagnostic

CATALOG = DbtCatalog(
    "v12",
    None,
    None,
    (
        DbtModel("model.x.prices", "prices", "DB.SCH.PRICE_LIST", (), (), ()),
        DbtModel("model.x.legacy", "legacy", "LEGACY_TABLE", (), (), ()),
    ),
)


def test_a_name_invalid_even_quoted_or_already_quoted_keeps_its_own_problem() -> None:
    too_long = "x " * 200
    assert [item.code for item in (identifier_problem(too_long, artifact="a", subject="s"),) if item] == ["SST-PRS005"]
    unbalanced = identifier_problem('"open', artifact="a", subject="s")
    assert unbalanced is not None and unbalanced.code == "SST-PRS005"
    assert identifier_problem("fine_name", artifact="a", subject="s") is None


def test_only_a_three_part_relation_is_compared_with_the_models() -> None:
    assert literal_relation_diagnostic(CATALOG, "SCH.PRICES", subject="t") is None
    assert literal_relation_diagnostic(CATALOG, "DB.SCH.LEGACY", subject="t") is None
    assert literal_relation_diagnostic(CATALOG, "DB.OTHER.PRICES", subject="t") is None
    found = literal_relation_diagnostic(CATALOG, '"DB"."SCH"."PRICES"', subject="t")
    assert found is not None and found.code == "SST-DBT006"


def test_a_transposition_is_one_edit_and_the_nearest_field_wins() -> None:
    assert edit_distance("tabels", "tables") == 1
    assert nearest_field("expt", ("expr", "exp")) == "exp"
    assert nearest_field("name", ("name",)) is None
