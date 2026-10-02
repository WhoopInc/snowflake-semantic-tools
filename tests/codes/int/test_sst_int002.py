"""SST-INT002: a renderer in the pure ring reached a file, a socket, or a process."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.app.compile import CompileResult, compile_each
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag, Severity
from tests.helpers.compiled_things import Thing


def compiled(render_file: Path | None) -> CompileResult:
    def render(label: str) -> Thing:
        if render_file is not None:
            render_file.read_text(encoding="utf-8")
        return Thing(label)

    return compile_each(("v",), lambda label: f"semantic_view:{label}", render, DiagnosticBag())


def test_sst_int002_fires(tmp_path: Path) -> None:
    (tmp_path / "x.txt").write_text("x", encoding="utf-8")
    result = compiled(tmp_path / "x.txt")
    [diagnostic] = result.diagnostics
    assert (diagnostic.code, diagnostic.severity, diagnostic.subject) == (
        "SST-INT002",
        Severity.ERROR,
        "semantic_view:v",
    )
    assert diagnostic.message == "rendering semantic_view:v attempted I/O from a pure ring"
    assert result.compiled == ()


def test_sst_int002_silent(tmp_path: Path) -> None:
    (tmp_path / "x.txt").write_text("x", encoding="utf-8")
    assert compiled(None).diagnostics == ()
    # Outside a pure phase the same read is ordinary adapter work.
    assert (tmp_path / "x.txt").read_text(encoding="utf-8") == "x"
