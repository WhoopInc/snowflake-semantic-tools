"""SST-VAL010: the compiled artifacts depend on one another in a cycle."""

from __future__ import annotations

from snowflake_semantic_tools.app.validate import _cycle_diagnostics
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.artifact_builders import rendered


def test_sst_val010_fires() -> None:
    a = rendered("A", depends_on=("semantic_view:b",))
    b = rendered("B", depends_on=("semantic_view:a",))
    [found] = _cycle_diagnostics((a, b))
    assert found.severity is Severity.ERROR
    assert found.message == "semantic_view reference cycle: semantic_view:a -> semantic_view:b -> semantic_view:a"
    assert found.subject == "semantic_view:a"


def test_sst_val010_silent() -> None:
    assert _cycle_diagnostics((rendered("A", depends_on=("semantic_view:b",)), rendered("B"))) == ()
