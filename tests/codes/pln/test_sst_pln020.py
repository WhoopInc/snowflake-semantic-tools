"""SST-PLN020: no smoke query can be built for a public metric."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.compile.project import CompileProject
from snowflake_semantic_tools.app.smoke import unprobed_metrics
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.project import SemanticViewProject
from snowflake_semantic_tools.domain.model.semantic_view import Metric, Window
from tests.helpers.compile_builders import view
from tests.helpers.project_inputs import InMemoryProjectInputs
from tests.helpers.sql_values import authored


def _unprobed(excluded: str) -> tuple[Diagnostic, ...]:
    # A window metric's probe must request each dimension the window excludes.
    window = Window(partition_excluding=(authored(excluded),))
    metric = Metric("RUNNING", authored("SUM(T.C)"), table="T", window=window)
    sales = replace(view("SALES"), metrics=(metric,))
    return unprobed_metrics(CompileProject(InMemoryProjectInputs(views=SemanticViewProject((sales,)))).run())


def test_sst_pln020_fires() -> None:
    [diagnostic] = _unprobed("DATE_TRUNC('month', T.C)")
    assert (diagnostic.code, diagnostic.severity) == ("SST-PLN020", Severity.ERROR)
    assert diagnostic.message == "semantic_view:sales: no smoke query can be built for metric 'T.RUNNING'"
    assert diagnostic.subject == "metric:t.running"


def test_sst_pln020_silent() -> None:
    assert _unprobed("T.C") == ()
