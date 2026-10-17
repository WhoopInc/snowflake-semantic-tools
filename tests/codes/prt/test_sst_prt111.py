"""SST-PRT111: two artifacts of one type would write the same `--emit-ddl` file.

An artifact's key is its name casefolded, so a compile that succeeds cannot hold two such
artifacts; the refusal stands in case one ever does, rather than letting one file overwrite the
other.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.cli.commands.compile import _payload_files
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.compiled_things import Thing


def test_sst_prt111_fires() -> None:
    files, [diagnostic] = _payload_files(CompileResult((Thing("Menu"), Thing("menu"))), agents_only=False)
    assert (diagnostic.code, diagnostic.severity) == ("SST-PRT111", Severity.ERROR)
    assert diagnostic.message == (
        "semantic_view/menu.sql would hold the payload of both semantic_view:Menu and semantic_view:menu"
    )
    assert diagnostic.subject == "semantic_view:menu"
    assert list(files) == ["semantic_view/menu.sql"]


def test_sst_prt111_silent() -> None:
    files, clashes = _payload_files(CompileResult((Thing("menu"), Thing("menu", "tool"))), agents_only=False)
    assert (list(files), clashes) == (["semantic_view/menu.sql", "tool/menu.sql"], ())
