"""SST-REF048: an eval reference names no file of its kind in the agent's `evals/` folder."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.agents import load_agents
from snowflake_semantic_tools.adapters.yaml.evals import load_eval_catalog
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.eval import EvalCatalog
from tests.helpers.diagnostic_filters import coded
from tests.helpers.eval_projects import EVALS, eval_project, write


def _catalog(tmp_path: Path, config: str = "run") -> EvalCatalog:
    """Load the eval project with the agent naming the config `config`."""
    eval_project(tmp_path)
    agent = tmp_path / "agents" / "sales" / "agent.yml"
    agent.write_text(agent.read_text().replace("eval_config('run')", f"eval_config('{config}')"))
    agents, diagnostics = load_agents(tmp_path)
    return load_eval_catalog(tmp_path, agents, initial_diagnostics=diagnostics)


def test_sst_ref048_fires(tmp_path: Path) -> None:
    catalog = _catalog(tmp_path, "nightly")
    [diagnostic] = coded(catalog.diagnostics, "SST-REF048")
    assert diagnostic.severity is Severity.ERROR
    assert (
        diagnostic.message
        == f"agents/sales/agent.yml: {{{{ eval_config('nightly') }}}} names no eval config in {EVALS}"
    )
    assert diagnostic.subject == "eval:sales"
    # Nothing publishes without the named pair.
    assert catalog.evals == ()


def test_sst_ref048_does_not_search_another_agents_folder(tmp_path: Path) -> None:
    write(tmp_path / "agents" / "other" / "evals" / "nightly.yml", "name: nightly\nmetrics: {}\n")
    assert len(coded(_catalog(tmp_path, "nightly").diagnostics, "SST-REF048")) == 1


def test_sst_ref048_silent(tmp_path: Path) -> None:
    assert coded(_catalog(tmp_path).diagnostics, "SST-REF048") == []
