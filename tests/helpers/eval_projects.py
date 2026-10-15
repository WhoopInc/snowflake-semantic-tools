"""A small project with one agent, `sales`, and its eval files, for the eval loader tests."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.agents import load_agents
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.model.agent import AgentModel

EVALS = "agents/sales/evals"


def write(path: Path, text: str) -> None:
    """Write `text`, stripped, creating its folders."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip(), encoding="utf-8")


def eval_project(tmp_path: Path, *, metric_ref: str = "judge") -> tuple[tuple[AgentModel, ...], DiagnosticBag]:
    """Write the project under `tmp_path` and load its agents: `sales` names the dataset `questions` and
    the config `run`, whose custom metric is `metric_ref`.
    """
    write(
        tmp_path / "agents" / "sales" / "agent.yml",
        """
name: sales
evals:
  dataset: "{{ eval_dataset('questions') }}"
  config: "{{ eval_config('run') }}"
""",
    )
    write(
        tmp_path / "agents" / "sales" / "evals" / "dataset.yml",
        """
name: questions
questions:
  - question: Decline this request.
    ground_truth:
      ground_truth_invocations: []
      ground_truth_output: Decline cleanly.
      custom_flag: true
""",
    )
    write(
        tmp_path / "agents" / "sales" / "evals" / "config.yml",
        f"""
name: run
config:
  agent_version: committed
  eval_tier: blocking
  retry: 1
  concurrency: 2
  baseline_runs: 5
dataset:
  mint: auto
  name_template: "EVAL_{{{{ agent | upper }}}}_{{{{ sha7 }}}}"
  source_table_template: "EVAL_SRC_{{{{ agent | upper }}}}_{{{{ sha7 }}}}"
  column_mapping:
    query_text: input_query
    ground_truth: ground_truth
metrics:
  system:
    - name: tool_selection_accuracy
      version: v3
      gate: true
      threshold: {{min: 0.8}}
  custom:
    - "{{{{ eval_metric('{metric_ref}') }}}}"
run:
  name_template: "EVAL_{{{{ agent | upper }}}}_{{{{ sha7 }}}}_{{{{ variant }}}}_{{{{ ts }}}}"
  variant: ci
  accept_statuses: [COMPLETED]
""",
    )
    write(
        tmp_path / "eval_metrics" / "judge.yml",
        """
name: judge
model: claude-sonnet-4-6
score_ranges:
  min_score: [0, 1]
  median_score: [2, 3]
  max_score: [4, 5]
prompt: |-
  Score {{input}} against {{ground_truth}} from 0 to 5.
  Return only the numeric score. If evidence is insufficient, score 0.
gate_default: true
threshold_default: {min: 4}
enabled: true
meta: {owner: data}
""",
    )
    return load_agents(tmp_path)
