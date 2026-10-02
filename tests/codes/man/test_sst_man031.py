"""SST-MAN031: the dbt manifest was rewritten while SST compiled from it."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.cli.wiring import compile as compiling
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.cli_projects import DBT_MANIFEST, project_copy


class RewritesManifest:
    """A compile during which a concurrent `dbt compile` drops a model from the manifest."""

    def __init__(self, inputs: object, manifest: Path) -> None:
        self._manifest = manifest

    def run(self) -> CompileResult:
        document = json.loads(self._manifest.read_text(encoding="utf-8"))
        dropped = next(key for key, node in document["nodes"].items() if node.get("resource_type") == "model")
        del document["nodes"][dropped]
        self._manifest.write_text(json.dumps(document), encoding="utf-8")
        return CompileResult(())


def test_sst_man031_fires(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = project_copy(tmp_path)
    manifest = tmp_path / "manifest.json"
    shutil.copy(DBT_MANIFEST, manifest)
    monkeypatch.setattr(compiling, "CompileProject", lambda inputs: RewritesManifest(inputs, manifest))
    [diagnostic] = compiling.compile_result(project, None, manifest).diagnostics
    assert (diagnostic.code, diagnostic.severity) == ("SST-MAN031", Severity.ERROR)
    assert diagnostic.message == "dbt manifest digest changed during the run"


def test_sst_man031_silent(tmp_path: Path) -> None:
    project = project_copy(tmp_path)
    result = compiling.compile_result(project, None, DBT_MANIFEST)
    assert "SST-MAN031" not in [item.code for item in result.diagnostics]
