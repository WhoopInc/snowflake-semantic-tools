"""SST-RND003: a rendered statement is larger than the size SST assumes Snowflake accepts."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.render.invariants import STATEMENT_SIZE_GUESS
from snowflake_semantic_tools.domain.render.semantic_view import render_checked
from tests.helpers.compile_builders import view


def test_sst_rnd003_fires() -> None:
    ddl, [diagnostic] = render_checked(replace(view("V"), comment="x" * STATEMENT_SIZE_GUESS))
    assert ddl is not None
    assert (diagnostic.code, diagnostic.severity) == ("SST-RND003", Severity.WARNING)
    size = len(ddl.text.encode("utf-8"))
    assert diagnostic.message == f"DB.SCH.V: rendered DDL is {size} bytes, over the {STATEMENT_SIZE_GUESS} guess"


def test_sst_rnd003_silent() -> None:
    assert render_checked(replace(view("V"), comment="x" * 1000))[1] == ()
