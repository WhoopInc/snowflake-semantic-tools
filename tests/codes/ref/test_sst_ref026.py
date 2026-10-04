"""SST-REF026: an eval config's `eval_metric()` names no metric in the eval_metrics/ tree."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.agents import load_agents
from snowflake_semantic_tools.adapters.yaml.evals.catalog import load_eval_catalog
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.reference_project import project_copy

CONFIG = "agents/jaffle_analytics/evals/config.yml"


def _references(tmp_path: Path, metric: str) -> list[Diagnostic]:
    """The SST-REF026 findings when the reference project's eval config names `metric`."""
    project = project_copy(tmp_path)
    config = project / CONFIG
    text = config.read_text(encoding="utf-8")
    config.write_text(text.replace("eval_metric('answer_grounding')", f"eval_metric('{metric}')"), encoding="utf-8")
    agents, diagnostics = load_agents(project)
    catalog = load_eval_catalog(project, agents, initial_diagnostics=diagnostics)
    return [item for item in catalog.diagnostics if item.code == "SST-REF026"]


def test_sst_ref026_fires(tmp_path: Path) -> None:
    [diagnostic] = _references(tmp_path, "answer_groundng")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "{ eval_metric('answer_groundng') } does not resolve"
    assert diagnostic.subject == "eval:jaffle_analytics_agent"


def test_sst_ref026_silent(tmp_path: Path) -> None:
    assert _references(tmp_path, "answer_grounding") == []
