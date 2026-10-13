"""SST-REF047: an agent's `evals.dataset` or `evals.config` is not one reference call."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.agents import load_agents
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.eval_projects import write


def _findings(tmp_path: Path, config: str) -> list[Diagnostic]:
    write(
        tmp_path / "agents" / "sales" / "agent.yml",
        f"""
name: sales
evals:
  dataset: "{{{{ eval_dataset('questions') }}}}"
  config: {config}
""",
    )
    return coded(load_agents(tmp_path)[1], "SST-REF047")


def test_sst_ref047_fires(tmp_path: Path) -> None:
    [diagnostic] = _findings(tmp_path, "evals/config.yml")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "agents/sales/agent.yml: evals.config is 'evals/config.yml'; it takes one {{ eval_config('<name>') }} call"
    )


def test_sst_ref047_fires_for_the_other_kind_of_reference(tmp_path: Path) -> None:
    # A dataset reference in the config slot names the wrong kind of file.
    assert len(_findings(tmp_path, "\"{{ eval_dataset('run') }}\"")) == 1


def test_sst_ref047_silent(tmp_path: Path) -> None:
    assert _findings(tmp_path, "\"{{ eval_config('run') }}\"") == []
