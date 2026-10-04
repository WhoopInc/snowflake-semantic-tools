"""SST-MEM105: two views share a table and attach custom instructions that each say something different."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.resolve_builders import member, membership

SQL = frozenset(("ai_sql_generation",))
CHANNELS = {"conventions": SQL, "menu_rules": SQL, "sales_rules": SQL}
INSTRUCTIONS = tuple(member("custom_instruction", name) for name in CHANNELS)


def _conflicts(menu: set[str], sales: set[str]) -> list[Diagnostic]:
    """The SST-MEM105 findings when the two views, which share `orders`, attach these instructions."""
    named = {"semantic_view:menu": frozenset(menu), "semantic_view:sales": frozenset(sales)}
    result = membership(*INSTRUCTIONS, view_named_members=named, instruction_channels=CHANNELS)
    return coded(result.diagnostics, "SST-MEM105")


def test_sst_mem105_fires() -> None:
    [diagnostic] = _conflicts({"menu_rules"}, {"sales_rules"})
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "semantic_view:menu and semantic_view:sales share table 'orders' with conflicting instructions"
    )
    assert diagnostic.subject == "semantic_view:menu"


def test_sst_mem105_silent() -> None:
    # One view repeating the other's guidance and adding to it is not a conflict.
    assert _conflicts({"conventions", "menu_rules"}, {"conventions"}) == []
