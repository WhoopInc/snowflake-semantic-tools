"""SST-REF044: a view's `tables:` entry is not one `{{ ref('<model>') }}` call."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.resolve_builders import coded, edited_fixture

FILE = "semantic_models/semantic_views/core/semantic_views.yml"
BEFORE = "- \"{{ ref('products') }}\""


def test_sst_ref044_fires(tmp_path: Path) -> None:
    project = edited_fixture(tmp_path, FILE, BEFORE, '- "products"')
    [diagnostic] = coded(project.diagnostics, "SST-REF044")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "semantic_view:jaffle_menu: table entry 'products' is not a { ref('<model>') } call"
    assert diagnostic.subject == "semantic_view:jaffle_menu"


def test_sst_ref044_silent(tmp_path: Path) -> None:
    project = edited_fixture(tmp_path, FILE, BEFORE, "- \"{{ ref( 'products' ) }}\"")
    assert coded(project.diagnostics, "SST-REF044") == []
