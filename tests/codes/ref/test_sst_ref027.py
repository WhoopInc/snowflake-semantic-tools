"""SST-REF027: an agent's `{{ file() }}` instruction resolves outside the project root."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.ref_codes import loaded_instructions


def test_sst_ref027_fires(tmp_path: Path) -> None:
    [diagnostic] = loaded_instructions(tmp_path, "{{ file('../../../route.md') }}")
    assert (diagnostic.code, diagnostic.severity) == ("SST-REF027", Severity.ERROR)
    assert diagnostic.message == "{ file('../../../route.md') } resolves outside the project root"
    assert diagnostic.origin is not None and diagnostic.origin.file == "agents/router/agent.yml"


def test_sst_ref027_silent(tmp_path: Path) -> None:
    assert loaded_instructions(tmp_path, "{{ file('route.md') }}", ("route.md", "Route sales questions.")) == ()
