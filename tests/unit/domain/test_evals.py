from __future__ import annotations

from collections.abc import Mapping
from dataclasses import FrozenInstanceError, replace

import pytest

from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.agent import AgentModel, AgentTool, ResolvedAgent, ResolvedAgentTool
from snowflake_semantic_tools.domain.model.eval import (
    CustomEvalMetric,
    EvalCatalog,
    EvalConfig,
    EvalDataset,
    EvalDatasetConfig,
    EvalDefaults,
    EvalGateVerdict,
    EvalGroundTruth,
    EvalInvocation,
    EvalQuestion,
    EvalRegression,
    EvalRunConfig,
    EvalScoreRanges,
    EvalSystemMetric,
    ResolvedEval,
    ThresholdRange,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.resolve.eval_name import render_eval_name_template
from snowflake_semantic_tools.domain.validate.eval import eval_placement, validate_eval_catalog


def test_resolved_eval_identity_sources_and_dependency_are_deterministic() -> None:
    origin = Origin("agent.yml")
    agent = AgentModel("sales", origin, ("agent.yml",))
    dataset = EvalDataset(origin, "dataset.yml", "sales", None, (EvalQuestion(origin, "q", EvalGroundTruth(origin)),))
    config = EvalConfig(origin, "config.yml", "sales", "committed", None, (), ("judge",), None)
    judge = CustomEvalMetric(
        origin,
        "judge.yml",
        "judge",
        None,
        "model",
        EvalScoreRanges((0, 1), (2, 3), (4, 5)),
        "Return only a score.",
    )
    value = ResolvedEval(agent, dataset, config, (judge,))
    assert value.key == "eval:sales"
    assert value.depends_on == ("agent:sales",)
    assert value.source_files == ("dataset.yml", "config.yml", "judge.yml")
    with pytest.raises(FrozenInstanceError):
        value.dataset.agent = "other"  # type: ignore[misc]
    resolved_agent = ResolvedAgent(
        AgentModel("a", origin, ("a.yml",)),
        (
            ResolvedAgentTool("generic", "one", "one", depends_on=("tool:x", "semantic_view:v")),
            ResolvedAgentTool("generic", "two", "two", depends_on=("tool:x",)),
        ),
    )
    assert resolved_agent.depends_on == ("tool:x", "semantic_view:v")


def test_eval_catalog_lookup_and_name_template_resolution() -> None:
    origin = Origin("judge.yml")
    judge = CustomEvalMetric(origin, "judge.yml", "Grounded", None, "model", None, "prompt")
    catalog = EvalCatalog((), (judge,))
    assert catalog.metric("grounded") is judge
    assert (
        render_eval_name_template(
            "EVAL_{{ agent | upper }}_{{ sha7 }}_{{ variant }}_{{ ts }}",
            agent="sales",
            sha7="1234567",
            variant="ci",
            ts="20260927T120000Z",
        )
        == "EVAL_SALES_1234567_ci_20260927T120000Z"
    )
    with pytest.raises(ValueError, match="unsupported expression"):
        render_eval_name_template("{{ target.database }}", agent="sales", sha7="1234567")
    with pytest.raises(ValueError, match="has no value"):
        render_eval_name_template("{{ variant }}", agent="sales", sha7="1234567")
    assert catalog.metric("missing") is None


def _valid_metric(name: str = "judge", **changes: object) -> CustomEvalMetric:
    values: dict[str, object] = {
        "origin": Origin(f"{name}.yml"),
        "source_file": f"{name}.yml",
        "name": name,
        "description": None,
        "model": "claude-sonnet-4-6",
        "score_ranges": EvalScoreRanges((0, 1), (2, 3), (4, 5)),
        "prompt": (
            "Score from 0 to 5. If information is insufficient, score 0. "
            "Return only the numeric score. {{input}} {{ground_truth}}"
        ),
        "gate_default": True,
        "threshold_default": ThresholdRange(min=3),
    }
    values.update(changes)
    return CustomEvalMetric(**values)  # type: ignore[arg-type, unused-ignore]


def _resolved_eval(
    *,
    dataset: EvalDataset | None = None,
    config: EvalConfig | None = None,
    metric: CustomEvalMetric | None = None,
) -> ResolvedEval:
    agent_origin = Origin("agents/sales/agent.yml")
    agent = AgentModel(
        "sales",
        agent_origin,
        ("agents/sales/agent.yml",),
        sample_questions=("Show revenue by region.",),
        tools=(
            AgentTool("cortex_analyst_text_to_sql", agent_origin, semantic_view="sales_view"),
            AgentTool("web_search", agent_origin, name="web_search", description="Search the web."),
        ),
    )
    judge = metric or _valid_metric()
    eval_dataset = dataset or EvalDataset(
        Origin("agents/sales/evals/dataset.yml"),
        "agents/sales/evals/dataset.yml",
        "sales",
        None,
        (
            EvalQuestion(
                Origin("agents/sales/evals/dataset.yml", 4),
                "Show revenue in calendar year 2025.",
                EvalGroundTruth(
                    Origin("agents/sales/evals/dataset.yml", 6),
                    (EvalInvocation(Origin("agents/sales/evals/dataset.yml", 7), "SALES_VIEW"),),
                    "State the direction and cite the source.",
                ),
            ),
        ),
    )
    eval_config = config or EvalConfig(
        Origin("agents/sales/evals/config.yml"),
        "agents/sales/evals/config.yml",
        "sales",
        "committed",
        EvalDatasetConfig("auto", "EVAL_{{ agent | upper }}_{{ sha7 }}", "SRC_{{ agent }}_{{ sha7 }}"),
        (
            EvalSystemMetric(
                Origin("agents/sales/evals/config.yml", 10),
                "tool_selection_accuracy",
                "v3",
                True,
                ThresholdRange(min=0.8),
            ),
        ),
        (judge.name,),
        EvalRunConfig(
            "EVAL_{{ agent | upper }}_{{ sha7 }}_{{ variant }}_{{ ts }}",
            variant="ci",
            retry=1,
            concurrency=2,
            baseline_runs=5,
            accept_statuses=("COMPLETED",),
        ),
    )
    return ResolvedEval(agent, eval_dataset, eval_config, (judge,))


def _codes(
    catalog: EvalCatalog,
    *,
    agent_tool_names: Mapping[str, tuple[str, ...] | frozenset[str]] | None = None,
    allowed_models: tuple[str, ...] = (),
) -> list[str]:
    return [
        diagnostic.code
        for diagnostic in validate_eval_catalog(
            catalog,
            agent_tool_names=agent_tool_names,
            allowed_models=allowed_models,
        )
    ]


def test_valid_eval_uses_exact_compiler_tool_projection_and_v3_pins() -> None:
    value = _resolved_eval()
    projection = ResolvedAgent(
        value.agent,
        (
            ResolvedAgentTool("cortex_analyst_text_to_sql", "SALES_VIEW", "Sales"),
            ResolvedAgentTool("web_search", "web_search", "Search"),
        ),
    ).agent_facing_tool_names
    diagnostics = validate_eval_catalog(
        EvalCatalog((value,), value.custom_metrics),
        agent_tool_names={"sales": projection},
        allowed_models=("claude-sonnet-4-6",),
    )
    assert not diagnostics.has_errors
    assert "SST-VAL711" in [diagnostic.code for diagnostic in diagnostics]
    assert all(diagnostic.code != "SST-VAL744" for diagnostic in diagnostics)


def test_dataset_validation_covers_agreement_placement_rows_dates_overlap_and_tools() -> None:
    origin = Origin("agents/sales/evals/dataset.yml", 4)
    dataset = EvalDataset(
        origin,
        origin.file,
        "other",
        None,
        (
            EvalQuestion(origin, None, EvalGroundTruth(origin)),
            EvalQuestion(
                origin,
                "Show revenue by region.",
                EvalGroundTruth(
                    origin,
                    (EvalInvocation(origin, "web_search_tool"), EvalInvocation(origin, "missing")),
                    "Compare this quarter and report 42.",
                ),
            ),
        ),
        ("database", "schema", "enabled"),
    )
    value = _resolved_eval(dataset=dataset)
    codes = _codes(
        EvalCatalog((value,), value.custom_metrics, EvalDefaults(min_dataset_rows=3)),
        agent_tool_names={"sales": ("SALES_VIEW", "web_search")},
        allowed_models=("claude-sonnet-4-6",),
    )
    assert {"SST-REF012", "SST-VAL703", "SST-VAL705", "SST-VAL706", "SST-VAL707"}.issubset(codes)
    assert {"SST-VAL708", "SST-VAL709", "SST-VAL710", "SST-PRS015"}.issubset(codes)


def test_agent_absent_from_tool_projection_skips_tool_checks() -> None:
    origin = Origin("agents/sales/evals/dataset.yml", 4)
    dataset = EvalDataset(
        origin,
        origin.file,
        None,
        None,
        (
            EvalQuestion(
                origin,
                "Show revenue by region.",
                EvalGroundTruth(origin, (EvalInvocation(origin, "SALES_VIEW"),), "Revenue by region."),
            ),
        ),
    )
    value = _resolved_eval(dataset=dataset)
    # The agent failed to compile, so the compiler projected no tools for it.
    codes = _codes(EvalCatalog((value,), value.custom_metrics), agent_tool_names={"other": ("SALES_VIEW",)})
    assert not {"SST-VAL708", "SST-VAL709", "SST-VAL711"}.intersection(codes)


def test_dataset_name_template_bounds_and_collisions_are_checked_when_renderable() -> None:
    first = _resolved_eval()
    second_agent = AgentModel("other", Origin("agents/other/agent.yml"), ("agents/other/agent.yml",))
    shared_template = "X" * 129
    config = EvalConfig(
        Origin("agents/other/evals/config.yml"),
        "agents/other/evals/config.yml",
        "other",
        "committed",
        EvalDatasetConfig("auto", shared_template, "SRC_{{ agent }}_{{ sha7 }}"),
        (EvalSystemMetric(Origin("config.yml"), "answer_correctness", "v3"),),
        (),
        EvalRunConfig("RUN_{{ sha7 }}_{{ ts }}", variant="ci", accept_statuses=("COMPLETED",)),
    )
    dataset = EvalDataset(
        Origin("agents/other/evals/dataset.yml"),
        "agents/other/evals/dataset.yml",
        "other",
        None,
        first.dataset.questions,
    )
    second = ResolvedEval(second_agent, dataset, config, ())
    first_config = EvalConfig(
        first.config.origin,
        first.config.source_file,
        first.config.agent,
        first.config.agent_version,
        EvalDatasetConfig("auto", shared_template, "SRC_{{ agent }}_{{ sha7 }}"),
        first.config.system_metrics,
        first.config.custom_metric_names,
        first.config.run,
    )
    first = ResolvedEval(first.agent, first.dataset, first_config, first.custom_metrics)
    codes = _codes(EvalCatalog((first, second), first.custom_metrics), allowed_models=("claude-sonnet-4-6",))
    assert {"SST-VAL701", "SST-VAL702", "SST-PRS002"}.issubset(codes)
    # An over-long dataset name is VAL702 alone, never PRS010 as well.
    assert "SST-PRS010" not in codes

    assert first_config.dataset is not None
    long_source = replace(first_config, dataset=replace(first_config.dataset, source_table_template="SRC_" + "X" * 130))
    source_codes = _codes(
        EvalCatalog((ResolvedEval(first.agent, first.dataset, long_source, first.custom_metrics),), ()),
        allowed_models=("claude-sonnet-4-6",),
    )
    assert "SST-PRS010" in source_codes


def test_rendered_run_names_must_be_unique_per_agent() -> None:
    first = _resolved_eval()
    second_config = EvalConfig(
        first.config.origin,
        first.config.source_file,
        first.config.agent,
        first.config.agent_version,
        first.config.dataset,
        first.config.system_metrics,
        first.config.custom_metric_names,
        first.config.run,
    )
    second = ResolvedEval(first.agent, first.dataset, second_config, first.custom_metrics)
    codes = _codes(EvalCatalog((first, second), first.custom_metrics), allowed_models=("claude-sonnet-4-6",))
    assert "SST-VAL717" in codes


def test_eval_config_validation_covers_system_metrics_thresholds_statuses_and_bounds() -> None:
    value = _resolved_eval()
    config = EvalConfig(
        value.config.origin,
        value.config.source_file,
        "sales",
        "LIVE",
        value.config.dataset,
        (
            EvalSystemMetric(value.config.origin, "unknown", "v3"),
            EvalSystemMetric(value.config.origin, "answer_correctness", None, judge_model="model"),
            EvalSystemMetric(
                value.config.origin,
                "logical_consistency",
                "v2",
                True,
                ThresholdRange(min=0.9, max=0.2),
            ),
        ),
        ("missing",),
        EvalRunConfig(
            "RUN_{{ ts }}",
            variant="ci",
            retry=-1,
            concurrency=9,
            baseline_runs=0,
            accept_statuses=("PARTIALLY_COMPLETED", "CANCELLED"),
        ),
    )
    value = _resolved_eval(config=config)
    codes = _codes(
        EvalCatalog((value,), value.custom_metrics, EvalDefaults(concurrency=8)),
        allowed_models=("claude-sonnet-4-6",),
    )
    assert {"SST-VAL719", "SST-VAL721", "SST-VAL722", "SST-VAL724", "SST-VAL726"}.issubset(codes)
    assert {"SST-VAL723", "SST-VAL731", "SST-VAL733", "SST-VAL735", "SST-VAL718"}.issubset(codes)
    assert {"SST-PRS016", "SST-PRS013"}.issubset(codes)


def test_custom_metric_validation_covers_duplicates_models_scale_placeholders_and_rubric() -> None:
    bad = _valid_metric(
        "answer_correctness",
        model="auto",
        score_ranges=EvalScoreRanges((0, 2), (2, 3), (5, 4)),
        prompt="Explain the answer from 0 to 9 using {{unknown}}.",
        threshold_default=ThresholdRange(min=6, max=2),
    )
    duplicate = _valid_metric("answer_correctness", model="not-allowed")
    codes = _codes(
        EvalCatalog((), (bad, duplicate)),
        allowed_models=("claude-sonnet-4-6",),
    )
    assert {"SST-VAL001", "SST-VAL737", "SST-VAL738", "SST-VAL739"}.issubset(codes)
    assert {"SST-VAL741", "SST-VAL742", "SST-VAL743", "SST-PRS114"}.issubset(codes)
    assert {"SST-VAL747", "SST-PRS115", "SST-PRS116"}.issubset(codes)


def test_custom_invariant_metric_warns_when_scale_exceeds_three_values() -> None:
    metric = _valid_metric(description="Checks an invariant")
    codes = _codes(EvalCatalog((), (metric,)), allowed_models=("claude-sonnet-4-6",))
    assert "SST-VAL748" in codes


def test_custom_threshold_is_validated_against_entire_declared_scale() -> None:
    metric = _valid_metric(threshold_default=ThresholdRange(min=3))
    diagnostics = validate_eval_catalog(
        EvalCatalog((), (metric,)),
        allowed_models=("claude-sonnet-4-6",),
    )
    assert all(diagnostic.code != "SST-PRS115" for diagnostic in diagnostics)
    assert all(diagnostic.severity is not Severity.ERROR for diagnostic in diagnostics)


def test_custom_metric_version_field_is_not_required() -> None:
    metric = _valid_metric()
    diagnostics = validate_eval_catalog(
        EvalCatalog((), (metric,)),
        allowed_models=("claude-sonnet-4-6",),
    )
    assert all(diagnostic.code != "SST-VAL744" for diagnostic in diagnostics)


def test_missing_dataset_templates_fail_validation_before_compile() -> None:
    value = _resolved_eval()
    assert value.config.dataset is not None
    missing = replace(value, config=replace(value.config, dataset=replace(value.config.dataset, name_template=None)))
    diagnostics = validate_eval_catalog(EvalCatalog((missing,), missing.custom_metrics))
    found = [item for item in diagnostics if item.code == "SST-VAL762"]
    assert [item.context["field"] for item in found] == ["name_template"]
    assert found[0].subject == missing.key and found[0].origin == missing.config.origin
    absent = replace(value, config=replace(value.config, dataset=None))
    fields = [
        item.context["field"]
        for item in validate_eval_catalog(EvalCatalog((absent,), absent.custom_metrics))
        if item.code == "SST-VAL762"
    ]
    assert fields == ["name_template", "source_table_template"]


def test_eval_validation_covers_omitted_configs_templates_invocations_and_defaults() -> None:
    value = _resolved_eval()
    dataset = EvalDataset(
        value.dataset.origin,
        value.dataset.source_file,
        "sales",
        None,
        (
            EvalQuestion(
                value.dataset.origin,
                "Absolute 2025 question",
                EvalGroundTruth(
                    value.dataset.origin,
                    (EvalInvocation(value.dataset.origin, None, "input", "output"),),
                    None,
                    (),
                    False,
                    "reason requires immutable",
                ),
            ),
            EvalQuestion(
                value.dataset.origin,
                "Another question",
                EvalGroundTruth(value.dataset.origin, None, "stable text", (), True, None),
            ),
        ),
    )
    config = EvalConfig(
        value.config.origin,
        value.config.source_file,
        "sales",
        "VERSION$2",
        None,
        (),
        (),
        None,
    )
    resolved = ResolvedEval(
        AgentModel(
            "sales",
            value.agent.origin,
            value.agent.source_files,
            tools=(AgentTool("mcp", value.agent.origin), AgentTool("agent", value.agent.origin)),
        ),
        dataset,
        config,
        (),
    )
    codes = _codes(EvalCatalog((resolved,), ()), agent_tool_names={"sales": ()})
    assert {"SST-PRS015", "SST-PRS101"}.issubset(codes)

    run_config = replace(
        config,
        dataset=EvalDatasetConfig(
            "auto",
            "{{ unknown }}",
            "SRC_{{ unknown }}",
        ),
        system_metrics=(
            EvalSystemMetric(value.config.origin, "answer_correctness", "v3", False, ThresholdRange(min=0.5)),
        ),
        run=EvalRunConfig("{{ unknown }}", concurrency=0),
    )
    codes = _codes(EvalCatalog((ResolvedEval(resolved.agent, dataset, run_config, ()),), ()))
    assert {"SST-VAL733", "SST-PRS016", "SST-VAL732"}.issubset(codes)

    forbidden = replace(config, forbidden_keys=("database",))
    codes = _codes(EvalCatalog((ResolvedEval(resolved.agent, dataset, forbidden, ()),), ()))
    assert "SST-VAL703" in codes

    missing_source_template = replace(config, dataset=EvalDatasetConfig("auto", "{{ agent }}", None))
    codes = _codes(EvalCatalog((ResolvedEval(resolved.agent, dataset, missing_source_template, ()),), ()))
    assert "SST-PRS101" in codes

    concrete_run = replace(
        config,
        system_metrics=(EvalSystemMetric(value.config.origin, "answer_correctness", "v3"),),
        run=EvalRunConfig("RUN_{{ sha7 }}", concurrency=1),
    )
    codes = _codes(EvalCatalog((ResolvedEval(resolved.agent, dataset, concrete_run, ()),), ()))
    assert "SST-PRS016" not in codes

    nameless_run = replace(
        concrete_run,
        run=EvalRunConfig(None, concurrency=1),
    )
    codes = _codes(EvalCatalog((ResolvedEval(resolved.agent, dataset, nameless_run, ()),), ()))
    assert "SST-VAL717" not in codes


def test_custom_metric_validation_covers_ungated_threshold_intent_and_band_anchors() -> None:
    intent = _valid_metric(
        "custom",
        gate_default=False,
        threshold_default=ThresholdRange(min=3),
        score_ranges=None,
        prompt="Evaluate answer correctness. Return a score from 0 to 5. If tied, choose lower.",
    )
    anchored = _valid_metric(
        "anchored",
        prompt="0 to 1 is poor. 2 to 3 is acceptable. 4 to 5 is strong. If tied, choose lower.",
        threshold_default=None,
    )
    codes = _codes(EvalCatalog((), (intent, anchored)), allowed_models=("claude-sonnet-4-6",))
    assert "SST-VAL733" in codes
    assert "SST-VAL740" in codes
    assert "SST-VAL741" not in codes


def test_eval_helpers_cover_threshold_scale_anchor_and_regression_count_edges() -> None:
    from snowflake_semantic_tools.domain.validate.eval_config import _usable_threshold
    from snowflake_semantic_tools.domain.validate.eval_metric import _prompt_anchors_declared_bands, _prompt_scale

    assert not _usable_threshold(ThresholdRange())
    assert _usable_threshold(ThresholdRange(max=1))
    assert _prompt_scale("no numeric range") is None
    assert _prompt_scale("Score from 5 to 0") == (0.0, 5.0)
    assert not _prompt_anchors_declared_bands("0 to 5", None)
    verdict = EvalGateVerdict("blocking", (EvalRegression("q", "metric"),), False)
    assert verdict.regression_count == 1


def test_eval_validation_covers_absolute_text_immutable_numbers_and_internal_scale() -> None:
    value = _resolved_eval()
    ground_truth = EvalGroundTruth(
        value.dataset.origin,
        None,
        "Use 42 exactly.",
        (),
        True,
        "frozen fixture",
    )
    dataset = replace(
        value.dataset,
        questions=(EvalQuestion(value.dataset.origin, "Question for 2025", ground_truth),),
    )
    diagnostics = validate_eval_catalog(
        EvalCatalog((replace(value, dataset=dataset),), value.custom_metrics),
        allowed_models=("claude-sonnet-4-6",),
    )
    assert all(diagnostic.code not in {"SST-PRS015", "SST-VAL706"} for diagnostic in diagnostics)

    internal_scale = _valid_metric(
        "internal",
        score_ranges=EvalScoreRanges((0, 1), (2, 3), (4, 5)),
        prompt="Return a score from 1 to 4. If tied, choose lower.",
        threshold_default=None,
    )
    diagnostics = validate_eval_catalog(
        EvalCatalog((), (internal_scale,)),
        allowed_models=("claude-sonnet-4-6",),
    )
    assert all(diagnostic.code != "SST-VAL747" for diagnostic in diagnostics)


def test_eval_validation_covers_ground_truth_free_rows_valid_unique_runs_and_no_ranges() -> None:
    value = _resolved_eval()
    dataset = replace(
        value.dataset,
        questions=(EvalQuestion(value.dataset.origin, "Question", None),),
    )
    config = replace(
        value.config,
        custom_metric_names=(),
        run=EvalRunConfig("RUN_{{ sha7 }}", concurrency=1, baseline_runs=1),
    )
    diagnostics = validate_eval_catalog(EvalCatalog((replace(value, dataset=dataset, config=config),), ()))
    assert "SST-VAL705" in {diagnostic.code for diagnostic in diagnostics}
    assert "SST-VAL717" not in {diagnostic.code for diagnostic in diagnostics}

    no_ranges = _valid_metric(
        "no_ranges",
        score_ranges=None,
        threshold_default=None,
        prompt="Return a score from 0 to 5. If tied, choose lower.",
    )
    diagnostics = validate_eval_catalog(
        EvalCatalog((), (no_ranges,)),
        allowed_models=("claude-sonnet-4-6",),
    )
    assert "SST-VAL741" not in {diagnostic.code for diagnostic in diagnostics}

    unscaled_ranges = _valid_metric(
        "unscaled",
        prompt="Return the numeric score. If tied, choose lower.",
        threshold_default=ThresholdRange(min=3),
    )
    diagnostics = validate_eval_catalog(
        EvalCatalog((), (unscaled_ranges,)),
        allowed_models=("claude-sonnet-4-6",),
    )
    assert "SST-VAL741" in {diagnostic.code for diagnostic in diagnostics}


def test_eval_placement_accepts_bare_and_same_schema_names_and_reports_any_other_schema() -> None:
    target = QualifiedName.parse("DB.S.SALES")
    value = _resolved_eval()
    assert value.config.dataset is not None

    def placed(name: str | None, source: str | None) -> list[str]:
        dataset = replace(value.config.dataset, name_template=name, source_table_template=source)  # type: ignore[arg-type]
        resolved = replace(value, config=replace(value.config, dataset=dataset))
        return [item.context["found"] for item in eval_placement(resolved, target)]

    assert placed("EVAL_{{ agent }}", "SRC_{{ agent }}") == []
    assert placed("db.s.EVAL_{{ agent }}", "S.SRC_{{ agent }}") == []
    assert placed("OTHER.S.EVAL_{{ agent }}", "X.SRC_{{ agent }}") == ["OTHER.S", "DB.X"]
    assert placed(None, "{{ unknown }}") == []
    assert placed("A.B.C.EVAL_{{ agent }}", None) == []
    assert eval_placement(replace(value, config=replace(value.config, dataset=None)), target) == ()
