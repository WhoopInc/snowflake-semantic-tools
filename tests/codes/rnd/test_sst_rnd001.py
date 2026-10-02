"""SST-RND001: a semantic view has no tables, which `CREATE SEMANTIC VIEW` requires."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.render.semantic_view import render_checked
from tests.helpers.compile_builders import view


def test_sst_rnd001_fires() -> None:
    ddl, [diagnostic] = render_checked(replace(view("V"), tables=()))
    assert ddl is None
    assert (diagnostic.code, diagnostic.severity, diagnostic.subject) == (
        "SST-RND001",
        Severity.ERROR,
        "semantic_view:DB.SCH.V",
    )
    assert diagnostic.message == "DB.SCH.V: the semantic view dialect requires 'tables'"


def test_sst_rnd001_silent() -> None:
    ddl, found = render_checked(view("V"))
    assert ddl is not None and found == ()
