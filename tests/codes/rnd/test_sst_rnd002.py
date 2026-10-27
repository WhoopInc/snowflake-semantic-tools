"""SST-RND002: a name in a semantic view holds a character no quoting can carry."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.semantic_view import Table
from snowflake_semantic_tools.domain.render.semantic_view import render_checked
from tests.helpers.compile_builders import view


def test_sst_rnd002_fires() -> None:
    broken = replace(view("V"), tables=(Table(logical_name='"T\x00"', fqn="DB.SCH.T"),))
    ddl, [diagnostic] = render_checked(broken)
    assert ddl is None
    assert (diagnostic.code, diagnostic.severity) == ("SST-RND002", Severity.ERROR)
    assert diagnostic.message == "DB.SCH.V: '\"T\\x00\"' cannot be safely quoted"


def test_sst_rnd002_silent() -> None:
    quoted = replace(view("V"), tables=(Table(logical_name='"Odd Name"', fqn="DB.SCH.T"),))
    ddl, found = render_checked(quoted)
    assert ddl is not None and found == ()
