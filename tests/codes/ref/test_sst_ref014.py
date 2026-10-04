"""SST-REF014: an agent's `{{ file() }}` instruction names a sidecar file that does not exist."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.ref_codes import loaded_instructions


def test_sst_ref014_fires(tmp_path: Path) -> None:
    [diagnostic] = loaded_instructions(tmp_path, "{{ file('route.md') }}")
    assert (diagnostic.code, diagnostic.severity) == ("SST-REF014", Severity.ERROR)
    assert diagnostic.message == "{ file('route.md') } does not resolve to a file"
    assert diagnostic.origin is not None and diagnostic.origin.file == "agents/router/agent.yml"


def test_sst_ref014_silent(tmp_path: Path) -> None:
    assert loaded_instructions(tmp_path, "{{ file('route.md') }}", ("route.md", "Route sales questions.")) == ()
