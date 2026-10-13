from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.evals import load_eval_catalog, parse_eval_defaults
from snowflake_semantic_tools.app.compile.evals import CompiledEval, CompileEvals
from snowflake_semantic_tools.app.evals.gate import capture_baseline, evaluate_gate, recorded_judges
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.model.eval import EvalCatalog, EvalDefaults
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.eval_builders import attempt, run_result
from tests.helpers.eval_projects import EVALS
from tests.helpers.eval_projects import eval_project as _project
from tests.helpers.eval_projects import write as _write


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


def test_an_unknown_mint_policy_and_any_sweep_block_are_refused(tmp_path: Path) -> None:
    agents, agent_diagnostics = _project(tmp_path)
    config = tmp_path / "agents" / "sales" / "evals" / "config.yml"
    text = config.read_text(encoding="utf-8").replace("mint: auto", "mint: sometimes")
    _write(config, text + "\nsweep:\n  enabled: true\n  models: [claude-sonnet-4-6]\n")
    catalog = load_eval_catalog(tmp_path, agents, initial_diagnostics=agent_diagnostics)
    found = [(item.code, item.context.get("field") or item.context.get("key")) for item in catalog.diagnostics]
    assert ("SST-PRS013", "dataset.mint") in found
    assert ("SST-CFG044", "sweep") in found
    [config_value] = [item.config for item in catalog.evals]
    assert config_value.dataset is not None and config_value.dataset.mint == "sometimes"


SYSTEM_BLOCK = """  system:
    - name: tool_selection_accuracy
      version: v3
      gate: true
      threshold: {min: 0.8}
"""


def _edit(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    assert old in text
    _write(path, text.replace(old, new))


def _compiled(catalog: EvalCatalog) -> CompiledEval:
    result = CompileEvals(catalog, agent_targets={"sales": QualifiedName.parse("DB.S.SALES")}).run_result()
    [compiled] = [item for item in result.compiled if isinstance(item, CompiledEval)]
    return compiled


def test_a_config_without_metrics_system_enables_the_project_metrics_ungated(tmp_path: Path) -> None:
    agents, agent_diagnostics = _project(tmp_path)
    config = tmp_path / "agents" / "sales" / "evals" / "config.yml"
    _edit(config, SYSTEM_BLOCK, "")
    defaults = EvalDefaults(metrics=("tool_selection_accuracy", "answer_correctness"), metric_version="v3")
    catalog = load_eval_catalog(tmp_path, agents, defaults=defaults, initial_diagnostics=agent_diagnostics)
    assert not catalog.diagnostics.has_errors
    [value] = catalog.evals
    metrics = value.config.system_metrics
    assert [(metric.name, metric.version, metric.gate, metric.threshold) for metric in metrics] == [
        ("tool_selection_accuracy", "v3", None, None),
        ("answer_correctness", "v3", None, None),
    ]
    assert metrics[0].origin.file == "agents/sales/evals/config.yml"
    rendered = _compiled(catalog).rendered.config_yaml
    assert '- name: "tool_selection_accuracy"\n    version: "v3"' in rendered
    assert '- name: "answer_correctness"\n    version: "v3"' in rendered


def test_a_config_listing_system_metrics_or_none_does_not_inherit_the_project_metrics(tmp_path: Path) -> None:
    agents, agent_diagnostics = _project(tmp_path)
    defaults = EvalDefaults(metrics=("answer_correctness",), metric_version="v3")
    listed = load_eval_catalog(tmp_path, agents, defaults=defaults, initial_diagnostics=agent_diagnostics)
    assert [metric.name for metric in listed.evals[0].config.system_metrics] == ["tool_selection_accuracy"]

    config = tmp_path / "agents" / "sales" / "evals" / "config.yml"
    _edit(config, SYSTEM_BLOCK, "  system: []\n")
    empty = load_eval_catalog(tmp_path, agents, defaults=defaults, initial_diagnostics=agent_diagnostics)
    assert empty.evals[0].config.system_metrics == ()


def test_an_unknown_project_metric_is_refused_and_left_out() -> None:
    defaults, diagnostics = parse_eval_defaults(
        {"+metrics": ["answer_correctness", "answer_relevance", "answer_correctness"]}, file="sst_config.yml"
    )
    assert defaults.metrics == ("answer_correctness",)
    assert [(item.code, item.context.get("field"), item.context.get("found")) for item in diagnostics] == [
        ("SST-PRS013", "evals.+metrics", "answer_relevance")
    ]
    assert diagnostics[0].origin is not None and diagnostics[0].origin.file == "sst_config.yml"


def test_an_inherited_metric_version_is_the_one_render_baseline_and_gate_read(tmp_path: Path) -> None:
    agents, agent_diagnostics = _project(tmp_path)
    config = tmp_path / "agents" / "sales" / "evals" / "config.yml"
    pinned = _compiled(load_eval_catalog(tmp_path, agents, initial_diagnostics=agent_diagnostics))
    _edit(config, "      version: v3\n", "")
    inherited = _compiled(
        load_eval_catalog(
            tmp_path, agents, defaults=EvalDefaults(metric_version="v3"), initial_diagnostics=agent_diagnostics
        )
    )
    assert inherited.resolved.config.system_metrics[0].version == "v3"
    assert inherited.rendered.config_yaml == pinned.rendered.config_yaml
    assert [item.context.get("value") for item in recorded_judges(inherited)] == ["v3"]
    flags = (("q", "tool_selection_accuracy", True), ("q", "judge", True))
    attempts = tuple(attempt(f"run-{index}", *flags) for index in range(1, 6))
    baseline = capture_baseline(pinned, run_result(*attempts), reason="initial", captured_at="2026-09-01T00:00:00Z")
    assert dict(baseline.metric_versions)["tool_selection_accuracy"] == "v3"
    recaptured = capture_baseline(
        inherited, run_result(*attempts), reason="initial", captured_at="2026-09-01T00:00:00Z"
    )
    assert recaptured.metric_versions == baseline.metric_versions
    verdict, diagnostics = evaluate_gate(inherited, run_result(attempts[0]), baseline, now="2026-09-02T00:00:00Z")
    assert verdict.reason is None, [item.code for item in diagnostics]


def test_a_custom_metric_without_a_model_takes_the_project_judge_model(tmp_path: Path) -> None:
    agents, agent_diagnostics = _project(tmp_path)
    _edit(tmp_path / "eval_metrics" / "judge.yml", "model: claude-sonnet-4-6\n", "")
    catalog = load_eval_catalog(
        tmp_path,
        agents,
        defaults=EvalDefaults(judge_model="claude-sonnet-4-6"),
        initial_diagnostics=agent_diagnostics,
        allowed_models=("claude-sonnet-4-6",),
    )
    assert not catalog.diagnostics.has_errors
    assert catalog.evals[0].custom_metrics[0].model == "claude-sonnet-4-6"
    assert 'model: "claude-sonnet-4-6"' in _compiled(catalog).rendered.config_yaml


def test_a_metric_model_wins_and_the_project_judge_model_must_still_be_explicit(tmp_path: Path) -> None:
    agents, agent_diagnostics = _project(tmp_path)
    own = load_eval_catalog(
        tmp_path, agents, defaults=EvalDefaults(judge_model="other-model"), initial_diagnostics=agent_diagnostics
    )
    assert own.evals[0].custom_metrics[0].model == "claude-sonnet-4-6"

    _edit(tmp_path / "eval_metrics" / "judge.yml", "model: claude-sonnet-4-6\n", "")
    auto = load_eval_catalog(
        tmp_path, agents, defaults=EvalDefaults(judge_model="auto"), initial_diagnostics=agent_diagnostics
    )
    unset = load_eval_catalog(tmp_path, agents, initial_diagnostics=agent_diagnostics)
    assert [item.message for item in auto.diagnostics if item.code == "SST-VAL738"] == [
        "eval metric 'judge' names no judge model (model: auto)"
    ]
    assert [item.message for item in unset.diagnostics if item.code == "SST-VAL738"] == [
        "eval metric 'judge' names no judge model (model: unset)"
    ]


def test_only_the_named_pair_publishes_and_every_other_version_is_checked(tmp_path: Path) -> None:
    alone = load_eval_catalog(tmp_path / "alone", *_project(tmp_path / "alone")[:1])
    published_files = {f"{EVALS}/dataset.yml", f"{EVALS}/config.yml"}
    agents, agent_diagnostics = _project(tmp_path)
    evals = tmp_path / EVALS
    # An older dataset, named apart, whose only question carries a relative date.
    older = (evals / "dataset.yml").read_text().replace("name: questions", "name: questions_v1")
    (evals / "dataset_v1.yml").write_text(older.replace("Decline this request.", "What sold yesterday?"))
    # An older config, named apart, that pins a system metric to no version.
    config = (evals / "config.yml").read_text().replace("name: run", "name: run_v1")
    (evals / "config_v1.yml").write_text(config.replace("version: v3", "version: v1"))
    catalog = load_eval_catalog(tmp_path, agents, initial_diagnostics=agent_diagnostics)

    [published] = catalog.evals
    assert (published.dataset.name, published.config.name) == ("questions", "run")
    assert published.source_files[:2] == (f"{EVALS}/dataset.yml", f"{EVALS}/config.yml")
    # Each unpublished file is checked, and its findings name only that file.
    found = {(item.code, item.origin.file) for item in catalog.diagnostics if item.origin is not None}
    assert ("SST-VAL706", f"{EVALS}/dataset_v1.yml") in found
    assert ("SST-VAL764", f"{EVALS}/config_v1.yml") in found

    # What the published pair reports is what it reports with no other version beside it.
    def on_published(bag: DiagnosticBag) -> list[str]:
        return sorted(item.message for item in bag if item.origin is not None and item.origin.file in published_files)

    assert on_published(catalog.diagnostics) == on_published(alone.diagnostics)


def test_an_eval_file_without_a_name_is_reported_and_cannot_be_named(tmp_path: Path) -> None:
    agents, agent_diagnostics = _project(tmp_path)
    dataset = tmp_path / EVALS / "dataset.yml"
    dataset.write_text(dataset.read_text().replace("name: questions\n", ""))
    catalog = load_eval_catalog(tmp_path, agents, initial_diagnostics=agent_diagnostics)
    codes = [(item.code, item.context.get("field")) for item in catalog.diagnostics if item.code.startswith("SST-")]
    assert ("SST-PRS002", "name") in codes
    assert ("SST-REF048", None) in codes
    assert catalog.evals == ()
