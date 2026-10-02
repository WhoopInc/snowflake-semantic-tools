from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.validate.expression import call_arguments, is_aggregate_expression

COLUMN = "{{ ref('orders', 'amount') }}"


@pytest.mark.parametrize(
    "expression",
    [
        f"SUM({COLUMN})",
        f"SUM(COALESCE({COLUMN}, 0))",
        f"SUM({COLUMN}) / 60",
        f"SUM({COLUMN}) / NULLIF(SUM({{{{ ref('orders', 'count') }}}}), 0)",
        f"ROUND(AVG({COLUMN}), 2)",
        f"AVG(CASE\n  WHEN {{{{ ref('scores', 'group_type') }}}} = 'overall'\n  THEN {COLUMN} END)",
        f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {COLUMN})",
        f"CASE WHEN SUM({COLUMN}) > 0 THEN COUNT_IF({COLUMN} > 10) END",
        "COUNT(*)",
        "DIV0({{ metric('revenue') }}, 60)",
    ],
)
def test_composed_aggregates_are_aggregates(expression: str) -> None:
    assert is_aggregate_expression(expression)


@pytest.mark.parametrize(
    "expression",
    [
        COLUMN,
        f"{COLUMN} * 100",
        f"SUM({COLUMN}) + {COLUMN}",
        f"SUM({COLUMN}) OVER (PARTITION BY 1)",
        f"UPPER({COLUMN})",
        "42",
    ],
)
def test_columns_outside_every_aggregate_and_windows_are_not(expression: str) -> None:
    assert not is_aggregate_expression(expression)


def test_an_unclosed_call_is_left_to_the_syntax_check() -> None:
    # The span runs to the end of the text; Snowflake's compile reports the imbalance.
    assert is_aggregate_expression(f"SUM({COLUMN}")


@pytest.mark.parametrize(
    ("expression", "arguments"),
    [
        ("SUM({{ metric('revenue') }})", ("{{ metric('revenue') }}",)),
        (f"LAG({COLUMN}, 1)", (COLUMN, "1")),
        (f"(AVG(SUM({COLUMN})))", (f"SUM({COLUMN})",)),
        ("CONCAT('a,b', \"c,d\", F(x, y))", ("'a,b'", '"c,d"', "F(x, y)")),
        ("COUNT(*)", ("*",)),
        ("F()", ("",)),
        (f"SUM({COLUMN}) / 2", None),
        (COLUMN, None),
    ],
)
def test_call_arguments_are_the_root_calls_top_level_arguments(
    expression: str, arguments: tuple[str, ...] | None
) -> None:
    assert call_arguments(expression) == arguments
