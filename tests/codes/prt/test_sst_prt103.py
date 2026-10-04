"""SST-PRT103: a `state:` selector is given without `--state` to compare with."""

from __future__ import annotations

from pathlib import Path

from tests.helpers.cli_json import invoke_json
from tests.helpers.cli_projects import common
from tests.helpers.reference_project import project_copy


def test_sst_prt103_fires(tmp_path: Path) -> None:
    exit_code, [diagnostic] = invoke_json(["validate", *common(project_copy(tmp_path)), "--select", "state:modified"])
    assert (exit_code, diagnostic["code"], diagnostic["severity"]) == (3, "SST-PRT103", "error")
    assert diagnostic["message"] == "selector 'state:modified' requires --state"


def test_sst_prt103_silent(tmp_path: Path) -> None:
    from snowflake_semantic_tools.domain.plan.selectors import Selectable, Selection, resolve_selectors

    universe = (
        Selectable("semantic_view:a", "semantic_view", "a", "f1"),
        Selectable("semantic_view:b", "semantic_view", "b", "f2"),
    )
    previous = {"semantic_view:a": "f1", "semantic_view:b": "old", "semantic_view:gone": "f3"}

    def keys(state: str) -> frozenset[str] | None:
        resolved = resolve_selectors(
            (f"state:{state}",), artifact_types=("semantic_view",), universe=universe, previous=previous
        )
        assert isinstance(resolved, Selection), resolved
        return resolved.keys

    assert keys("unmodified") == frozenset(("semantic_view:a",))
    assert keys("modified") == frozenset(("semantic_view:b",))
    assert keys("orphaned") == frozenset(("semantic_view:gone",))
    assert keys("new") == frozenset()
