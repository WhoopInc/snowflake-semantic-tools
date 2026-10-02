from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.agents import load_agents
from snowflake_semantic_tools.adapters.yaml.evals import load_eval_catalog, parse_eval_defaults
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.model.agent import AgentModel
from snowflake_semantic_tools.domain.model.eval import EvalDefaults


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip(), encoding="utf-8")


def _project(tmp_path: Path, *, metric_ref: str = "judge") -> tuple[tuple[AgentModel, ...], DiagnosticBag]:
    _write(
        tmp_path / "agents" / "sales" / "agent.yml",
        """
name: sales
evals:
  dataset: evals/dataset.yml
  config: evals/config.yml
""",
    )
    _write(
        tmp_path / "agents" / "sales" / "evals" / "dataset.yml",
        """
agent: sales
questions:
  - question: Decline this request.
    ground_truth:
      ground_truth_invocations: []
      ground_truth_output: Decline cleanly.
      custom_flag: true
""",
    )
    _write(
        tmp_path / "agents" / "sales" / "evals" / "config.yml",
        f"""
agent: sales
agent_version: committed
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
  tier: blocking
  retry: 1
  concurrency: 2
  baseline_runs: 5
  accept_statuses: [COMPLETED]
""",
    )
    _write(
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


def test_eval_loader_reads_dataset_config_metric_and_preserves_empty_invocations(tmp_path: Path) -> None:
    agents, agent_diagnostics = _project(tmp_path)
    catalog = load_eval_catalog(tmp_path, agents, initial_diagnostics=agent_diagnostics)
    assert not catalog.diagnostics.has_errors
    assert len(catalog.evals) == 1
    value = catalog.evals[0]
    assert value.dataset.questions[0].ground_truth is not None
    assert value.dataset.questions[0].ground_truth.invocations == ()
    assert value.dataset.questions[0].ground_truth.extra == {"custom_flag": True}
    assert value.config.system_metrics[0].threshold is not None
    assert value.config.system_metrics[0].threshold.min == 0.8
    assert value.custom_metrics[0].model == "claude-sonnet-4-6"
    assert value.source_files == (
        "agents/sales/evals/dataset.yml",
        "agents/sales/evals/config.yml",
        "eval_metrics/judge.yml",
    )


def test_eval_loader_reports_missing_custom_metric_without_dropping_eval(tmp_path: Path) -> None:
    agents, agent_diagnostics = _project(tmp_path, metric_ref="missing")
    catalog = load_eval_catalog(tmp_path, agents, initial_diagnostics=agent_diagnostics)
    assert len(catalog.evals) == 1
    assert catalog.evals[0].custom_metrics == ()
    assert "SST-REF026" in [diagnostic.code for diagnostic in catalog.diagnostics]


def test_eval_loader_recovers_after_malformed_metric_and_reports_wrong_shapes(tmp_path: Path) -> None:
    agents, agent_diagnostics = _project(tmp_path)
    _write(tmp_path / "eval_metrics" / "bad.yml", "name: [")
    _write(
        tmp_path / "eval_metrics" / "wrong.yml",
        """
name: wrong
score_ranges: []
prompt: 3
threshold_default: 4
""",
    )
    catalog = load_eval_catalog(tmp_path, agents, initial_diagnostics=agent_diagnostics)
    codes = {diagnostic.code for diagnostic in catalog.diagnostics}
    assert {"SST-LOD001", "SST-PRS003"}.issubset(codes)
    assert catalog.metric("judge") is not None


def test_eval_defaults_are_parsed_without_being_merged_into_authored_config() -> None:
    defaults, diagnostics = parse_eval_defaults(
        {
            "+eval_tier": "blocking",
            "+metrics": ["tool_selection_accuracy"],
            "+metric_version": "v3",
            "+min_dataset_rows": 10,
        }
    )
    assert diagnostics == ()
    assert defaults.eval_tier == "blocking"
    assert defaults.metrics == ("tool_selection_accuracy",)
    assert defaults.min_dataset_rows == 10

    _, bad_defaults = parse_eval_defaults({"+eval_tier": "sometimes"})
    assert bad_defaults[0].code == "SST-PRS013"


def test_eval_loader_runs_static_validation_with_compiler_projections(tmp_path: Path) -> None:
    agents, agent_diagnostics = _project(tmp_path)
    catalog = load_eval_catalog(
        tmp_path,
        agents,
        initial_diagnostics=agent_diagnostics,
        agent_tool_names={"sales": ("web_search",)},
        allowed_models=("claude-sonnet-4-6",),
    )
    codes = [diagnostic.code for diagnostic in catalog.diagnostics]
    assert "SST-VAL711" in codes
    assert "SST-VAL712" in codes
    assert "SST-VAL725" in codes
    assert not catalog.diagnostics.has_errors


def test_eval_loader_uses_inherited_agent_version_for_validation(tmp_path: Path) -> None:
    agents, agent_diagnostics = _project(tmp_path)
    config = tmp_path / "agents" / "sales" / "evals" / "config.yml"
    _write(
        config,
        config.read_text(encoding="utf-8").replace("agent_version: committed\n", ""),
    )
    catalog = load_eval_catalog(
        tmp_path,
        agents,
        defaults=EvalDefaults(agent_version="committed"),
        initial_diagnostics=agent_diagnostics,
    )
    assert "SST-VAL719" not in [diagnostic.code for diagnostic in catalog.diagnostics]


def test_eval_loader_rejects_unknown_accept_status(tmp_path: Path) -> None:
    agents, agent_diagnostics = _project(tmp_path)
    config = tmp_path / "agents" / "sales" / "evals" / "config.yml"
    _write(config, config.read_text(encoding="utf-8").replace("[COMPLETED]", "[NOT_A_STATUS]"))
    catalog = load_eval_catalog(tmp_path, agents, initial_diagnostics=agent_diagnostics)
    assert "SST-PRS013" in [diagnostic.code for diagnostic in catalog.diagnostics]


def test_run_block_problems_are_reported_in_read_order(tmp_path: Path) -> None:
    agents, agent_diagnostics = _project(tmp_path)
    config = tmp_path / "agents" / "sales" / "evals" / "config.yml"
    text = config.read_text(encoding="utf-8")
    run = "run:\n  retry: x\n  tier: sometimes\n  accept_statuses: [NOPE]\n  label: 3\n  retention: [a]\n"
    _write(config, text[: text.index("run:\n")] + run)
    catalog = load_eval_catalog(tmp_path, agents, initial_diagnostics=agent_diagnostics)
    found = [
        (item.code, item.context.get("field"))
        for item in catalog.diagnostics
        if item.context.get("artifact") == "agents/sales/evals/config.yml"
    ]
    # Retention, accepted statuses and tier come first, then the fields in EvalRunConfig's order.
    assert found[:5] == [
        ("SST-PRS003", "run.retention"),
        ("SST-PRS013", "run.accept_statuses"),
        ("SST-PRS013", "run.tier"),
        ("SST-PRS003", "label"),
        ("SST-PRS003", "retry"),
    ]


def test_eval_loader_flags_forbidden_custom_metric_version_field(tmp_path: Path) -> None:
    agents, agent_diagnostics = _project(tmp_path)
    metric = tmp_path / "eval_metrics" / "judge.yml"
    _write(metric, metric.read_text(encoding="utf-8") + "\nversion: v2")
    catalog = load_eval_catalog(tmp_path, agents, initial_diagnostics=agent_diagnostics)
    assert "SST-PRS004" in [diagnostic.code for diagnostic in catalog.diagnostics]


def test_an_over_long_dataset_name_is_reported_once(tmp_path: Path) -> None:
    agents, agent_diagnostics = _project(tmp_path)
    config = tmp_path / "agents" / "sales" / "evals" / "config.yml"
    short = 'name_template: "EVAL_{{ agent | upper }}_{{ sha7 }}"'
    long = 'name_template: "EVAL_{{ agent | upper }}_' + "X" * 130 + '"'
    _write(config, config.read_text(encoding="utf-8").replace(short, long))
    catalog = load_eval_catalog(tmp_path, agents, initial_diagnostics=agent_diagnostics)
    length = [(item.code, item.subject) for item in catalog.diagnostics if item.code in ("SST-VAL702", "SST-PRS010")]
    assert length == [("SST-VAL702", "eval:sales")]
