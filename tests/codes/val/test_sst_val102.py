"""SST-VAL102: a derived metric's expression calls a window function.

A `prop` code: besides the fires/silent pair, generated derived metrics show the rule holds for
every window function and stays quiet on every window-free combination of metrics.
"""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity
from tests.helpers.semantic_members import metric, metric_findings
from tests.helpers.semantic_strategies import BASES, CRITICAL, WINDOW_FUNCTIONS, base_metrics, combination

TOTAL = "{{ ref('orders', 'total') }}"


def test_sst_val102_fires() -> None:
    base = metric("total", f"SUM({TOTAL})")
    lagged = metric("lagged", "LAG({{ metric('total') }}) OVER (ORDER BY 1)", derived=True)
    [found] = metric_findings(base, lagged, code="SST-VAL102")
    assert (found.severity, ERROR_REGISTRY[found.code].demotable) == (Severity.ERROR, False)
    assert found.message == "window function LAG in derived metric 'lagged'"
    assert found.subject == "metric:lagged"


def test_sst_val102_silent() -> None:
    base = metric("total", f"SUM({TOTAL})")
    assert metric_findings(base, metric("doubled", "{{ metric('total') }} * 2", derived=True), code="SST-VAL102") == []


@given(function=st.sampled_from(WINDOW_FUNCTIONS), names=st.lists(st.sampled_from(BASES), min_size=1, max_size=3))
def test_sst_val102_fires_for_every_window_function(function: str, names: list[str]) -> None:
    references = ", ".join(f"{{{{ metric('{name}') }}}}" for name in names)
    windowed = metric("windowed", f"{function}({references}) OVER (ORDER BY 1)", derived=True)
    assert [item.subject for item in metric_findings(*base_metrics(), windowed, code="SST-VAL102")] == [
        "metric:windowed"
    ]


@given(expression=combination())
def test_no_restriction_fires_on_a_window_free_combination_of_metrics(expression: str) -> None:
    derived = metric("combined", expression, derived=True)
    for code in CRITICAL:
        assert metric_findings(*base_metrics(), derived, code=code) == [], code
