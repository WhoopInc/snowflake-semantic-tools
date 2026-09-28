from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.agents import load_agents


def test_agent_loader_reads_complete_documents_and_sidecars(tmp_path: Path) -> None:
    root = tmp_path / "agents" / "sales"
    root.mkdir(parents=True)
    (root / "instructions.md").write_text("Route sales questions to Sales.\n", encoding="utf-8")
    (root / "agent.yml").write_text(
        """
name: sales_agent
profile:
  display_name: Sales
spec:
  models:
    orchestration: model
  instructions:
    orchestration: "{{ file('instructions.md') }}"
    sample_questions:
      - question: What is revenue?
  tools:
    - type: cortex_analyst_text_to_sql
      semantic_view: "{{ semantic_view('sales') }}"
      description: Use for sales. Do not use for support.
alias: promoted
evals:
  dataset: evals/dataset.yml
  config: evals/config.yml
""".strip(),
        encoding="utf-8",
    )
    agents, diagnostics = load_agents(tmp_path)
    assert diagnostics == ()
    assert len(agents) == 1
    agent = agents[0]
    assert agent.name == "sales_agent"
    assert agent.orchestration_instructions == "Route sales questions to Sales."
    assert agent.source_files == (
        "agents/sales/agent.yml",
        "agents/sales/instructions.md",
    )
    assert agent.tools[0].semantic_view == "sales"
    assert agent.evals is not None
    assert agent.evals.dataset == "agents/sales/evals/dataset.yml"
    assert agent.evals.config == "agents/sales/evals/config.yml"


def test_agent_loader_reports_eval_pointer_escape(tmp_path: Path) -> None:
    root = tmp_path / "agents" / "bad"
    root.mkdir(parents=True)
    (root / "agent.yml").write_text(
        """
name: bad
evals:
  dataset: ../../../outside.yml
  config: evals/config.yml
""".strip(),
        encoding="utf-8",
    )
    agents, diagnostics = load_agents(tmp_path)
    assert len(agents) == 1
    assert agents[0].evals is not None
    assert agents[0].evals.dataset is None
    assert [diagnostic.code for diagnostic in diagnostics] == ["SST-REF027"]


def test_agent_loader_reports_missing_sidecar_and_bad_sample_shape(tmp_path: Path) -> None:
    root = tmp_path / "agents" / "bad"
    root.mkdir(parents=True)
    (root / "agent.yml").write_text(
        """
name: bad
spec:
  instructions:
    orchestration: "{{ file('missing.md') }}"
    sample_questions:
      - bare string
""".strip(),
        encoding="utf-8",
    )
    _, diagnostics = load_agents(tmp_path)
    assert {diagnostic.code for diagnostic in diagnostics} == {"SST-LOD018", "SST-PRS118"}
