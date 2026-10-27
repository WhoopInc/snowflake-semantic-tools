"""SST-MEM104: a view whose tables are all dbt models holds no dimension, fact or metric."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.render.semantic_view import render
from tests.helpers.diagnostic_filters import coded
from tests.helpers.reference_project import MENU_VIEW, edited, load, reported
from tests.helpers.resolve_builders import member, membership


def test_sst_mem104_fires() -> None:
    [diagnostic, *_] = coded(membership(member("metric", "m", ("customers",))).diagnostics, "SST-MEM104")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "semantic_view:menu resolves no members"
    assert diagnostic.subject == "semantic_view:menu"


def test_sst_mem104_silent() -> None:
    assert coded(membership(member("metric", "m", ("orders",))).diagnostics, "SST-MEM104") == []


def _menu_view(root: Path, name: str) -> Path:
    """A reference project copy under `root` whose `jaffle_menu` view is renamed `name`."""
    root.mkdir()
    return edited(root, MENU_VIEW, "name: jaffle_menu", f"name: {name}")


def test_sst_mem104_silent_for_a_view_name_with_capitals(tmp_path: Path) -> None:
    # A view's name is case-insensitive: `Sales2` attaches what `sales2` does and publishes as SALES2.
    upper, lower = _menu_view(tmp_path / "upper", "Sales2"), _menu_view(tmp_path / "lower", "sales2")
    assert reported(upper, "SST-MEM104") == []
    [mixed] = [view for view in load(upper).views if view.fqn.endswith(".SALES2")]
    [plain] = [view for view in load(lower).views if view.fqn.endswith(".SALES2")]
    assert render(mixed).text == render(plain).text
    assert mixed.metrics and mixed.columns
