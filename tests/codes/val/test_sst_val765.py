"""SST-VAL765: two eval files of one kind in an agent's `evals/` folder share a name."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.evals import load_eval_catalog
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.eval_projects import EVALS, eval_project


def _findings(tmp_path: Path, name: str) -> list[Diagnostic]:
    agents, diagnostics = eval_project(tmp_path)
    dataset = tmp_path / EVALS / "dataset.yml"
    (tmp_path / EVALS / "dataset_v2.yml").write_text(dataset.read_text().replace("name: questions", f"name: {name}"))
    catalog = load_eval_catalog(tmp_path, agents, initial_diagnostics=diagnostics)
    return coded(catalog.diagnostics, "SST-VAL765")


def test_sst_val765_fires(tmp_path: Path) -> None:
    [diagnostic] = _findings(tmp_path, "Questions")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        f"{EVALS}: eval dataset files {EVALS}/dataset.yml and {EVALS}/dataset_v2.yml are both named 'Questions'"
    )
    assert diagnostic.subject == "eval:sales"


def test_sst_val765_silent(tmp_path: Path) -> None:
    # A second version under its own name sits beside the published one.
    assert _findings(tmp_path, "questions_v2") == []
