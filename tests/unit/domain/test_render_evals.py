from __future__ import annotations

import json
from dataclasses import replace
from types import MappingProxyType

import pytest

from snowflake_semantic_tools.domain.model.diagnostic import Origin
from snowflake_semantic_tools.domain.model.eval import (
    CustomEvalMetric,
    EvalColumnMapping,
    EvalConfig,
    EvalDataset,
    EvalDatasetConfig,
    EvalGroundTruth,
    EvalInvocation,
    EvalQuestion,
    EvalRunConfig,
    EvalScoreRanges,
    EvalSystemMetric,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.render.eval import (
    _emit_yaml,
    render_create_dataset_sql,
    render_dataset_payload,
    render_eval,
    render_eval_config,
    render_eval_config_with_metrics,
    render_source_table_sql,
)

ORIGIN = Origin("eval.yml", 1, 1)


def dataset() -> EvalDataset:
    return EvalDataset(
        ORIGIN,
        "agents/a/evals/dataset.yml",
        "a",
        "review only",
        (
            EvalQuestion(
                ORIGIN,
                "What's new?",
                EvalGroundTruth(
                    ORIGIN,
                    (EvalInvocation(ORIGIN, "TOOL", "input", "output"),),
                    "Return O'Reilly's answer.",
                    ("required_filter",),
                    True,
                    "fixture value",
                    MappingProxyType({"extension": {"stable": True}}),
                ),
            ),
            EvalQuestion(ORIGIN, "Decline this", EvalGroundTruth(ORIGIN, (), "Decline")),
        ),
    )


def config() -> EvalConfig:
    return EvalConfig(
        ORIGIN,
        "agents/a/evals/config.yml",
        "a",
        "committed",
        EvalDatasetConfig(
            "auto",
            "EVAL_{{ agent | upper }}_{{ sha7 }}",
            "EVAL_SRC_{{ agent | upper }}_{{ sha7 }}",
            EvalColumnMapping("input_query", "ground_truth"),
        ),
        (EvalSystemMetric(ORIGIN, "answer_correctness", "v3", True),),
        ("grounding",),
        EvalRunConfig(label="ci", description="Line one\nLine two"),
    )


def metric() -> CustomEvalMetric:
    return CustomEvalMetric(
        ORIGIN,
        "eval_metrics/grounding.yml",
        "grounding",
        "Grounded answer",
        "claude-sonnet-4-6",
        EvalScoreRanges((0, 1), (2, 3), (4, 5)),
        "Score {{output}} from 0 to 5. If tied, choose the lower score.",
        True,
        None,
        True,
    )


def test_dataset_payload_is_canonical_and_keeps_only_snowflake_ground_truth() -> None:
    first = render_dataset_payload(dataset())
    second = render_dataset_payload(dataset())
    assert first == second
    value = json.loads(first)
    assert value[0]["input_query"] == "What's new?"
    assert value[0]["ground_truth"]["ground_truth_invocations"][0]["tool_name"] == "TOOL"
    assert value[1]["ground_truth"]["ground_truth_invocations"] == []
    assert "immutable" not in first
    assert "description" not in first


def test_source_table_sql_uses_json_parse_json_and_variant() -> None:
    sql = render_source_table_sql(render_dataset_payload(dataset()), QualifiedName.parse("DB.S.EVAL_SRC"))
    assert "GROUND_TRUTH VARIANT NOT NULL" in sql
    assert sql.count("PARSE_JSON(") == 2
    assert "O''Reilly" in sql
    assert "OBJECT_CONSTRUCT" not in sql


def test_create_dataset_sql_uses_expected_tools_mapping() -> None:
    sql = render_create_dataset_sql(
        config(),
        QualifiedName.parse("DB.S.EVAL_SRC"),
        QualifiedName.parse("DB.S.EVAL_DATASET"),
    )
    assert "SYSTEM$CREATE_EVALUATION_DATASET" in sql
    assert "'query_text', 'INPUT_QUERY'" in sql
    assert "'expected_tools', 'GROUND_TRUTH'" in sql
    assert "'ground_truth'" not in sql

    invalid = replace(config(), dataset=EvalDatasetConfig(column_mapping=EvalColumnMapping("query", "truth")))
    with pytest.raises(ValueError, match="input_query and ground_truth"):
        render_create_dataset_sql(
            invalid,
            QualifiedName.parse("DB.S.EVAL_SRC"),
            QualifiedName.parse("DB.S.EVAL_DATASET"),
        )


def test_repeat_run_yaml_has_closed_schema_and_inlines_custom_metric() -> None:
    rendered = render_eval_config_with_metrics(
        config(),
        (metric(),),
        agent_target=QualifiedName.parse("DB.S.A"),
        dataset_target=QualifiedName.parse("DB.S.EVAL_DATASET"),
    )
    top_level = tuple(line[:-1] for line in rendered.splitlines() if line and not line.startswith(" "))
    assert top_level == ("evaluation", "metrics")
    assert 'agent_version: "LAST"' in rendered
    assert 'version: "v3"' in rendered
    assert 'model: "claude-sonnet-4-6"' in rendered
    assert 'name: "grounding"' in rendered
    for forbidden in ("dataset:", "gate:", "threshold:", "retry:", "baseline_runs:", "retention:", "meta:"):
        assert forbidden not in rendered


def test_alias_and_explicit_version_are_preserved_in_repeat_run_yaml() -> None:
    alias = render_eval_config_with_metrics(
        EvalConfig(
            ORIGIN,
            "config.yml",
            "a",
            "alias:promoted",
            None,
            (),
            (),
            None,
        ),
        (),
        agent_target=QualifiedName.parse("DB.S.A"),
        dataset_target=QualifiedName.parse("DB.S.D"),
    )
    explicit = alias.replace('agent_version: "promoted"', 'agent_version: "VERSION$4"')
    assert 'agent_version: "promoted"' in alias
    assert 'agent_version: "VERSION$4"' in explicit


def test_render_eval_returns_all_publication_outputs_and_fingerprints() -> None:
    rendered = render_eval(
        dataset(),
        config(),
        agent_target=QualifiedName.parse("DB.S.A"),
        source_table=QualifiedName.parse("DB.S.EVAL_SRC"),
        dataset_target=QualifiedName.parse("DB.S.EVAL_DATASET"),
    )
    assert rendered.dataset_payload == render_dataset_payload(dataset())
    assert rendered.config_yaml == render_eval_config(
        config(),
        agent_target=QualifiedName.parse("DB.S.A"),
        dataset_target=QualifiedName.parse("DB.S.EVAL_DATASET"),
    )
    assert "CREATE TABLE DB.S.EVAL_SRC" in rendered.source_table_sql
    assert "SYSTEM$CREATE_EVALUATION_DATASET" in rendered.create_dataset_sql
    assert len(rendered.dataset_fingerprint) == len(rendered.config_fingerprint) == 64


def test_eval_rendering_covers_empty_dataset_ground_truth_and_optional_metric_values() -> None:
    empty = EvalDataset(ORIGIN, "dataset.yml", "a", None, (EvalQuestion(ORIGIN, None, None),))
    assert json.loads(render_dataset_payload(empty)) == [{"ground_truth": {}, "input_query": ""}]
    assert "INSERT INTO" not in render_source_table_sql("[]\n", QualifiedName.parse("DB.S.EMPTY"))

    partial_truth = EvalDataset(
        ORIGIN,
        "dataset.yml",
        "a",
        None,
        (
            EvalQuestion(
                ORIGIN,
                "q",
                EvalGroundTruth(
                    ORIGIN,
                    (EvalInvocation(ORIGIN, None, "input", None),),
                    None,
                    (),
                ),
            ),
        ),
    )
    truth = json.loads(render_dataset_payload(partial_truth))[0]["ground_truth"]
    assert truth == {"ground_truth_invocations": [{"tool_input": "input"}]}

    output_only = EvalDataset(
        ORIGIN,
        "dataset.yml",
        "a",
        None,
        (EvalQuestion(ORIGIN, "q", EvalGroundTruth(ORIGIN, None, "expected")),),
    )
    assert json.loads(render_dataset_payload(output_only))[0]["ground_truth"] == {"ground_truth_output": "expected"}

    custom = CustomEvalMetric(
        ORIGIN,
        "metric.yml",
        "minimal",
        None,
        None,
        None,
        None,
        enabled=True,
    )
    disabled = replace(metric(), name="disabled", enabled=False)
    no_run = replace(config(), run=EvalRunConfig(), system_metrics=(EvalSystemMetric(ORIGIN, None),))
    rendered = render_eval_config_with_metrics(
        no_run,
        (custom, disabled),
        agent_target=QualifiedName.parse("DB.S.A"),
        dataset_target=QualifiedName.parse("DB.S.D"),
    )
    assert "run_params:" not in rendered
    assert 'name: ""' in rendered
    assert 'name: "minimal"' in rendered
    assert "disabled" not in rendered

    with pytest.raises(ValueError, match="immutable agent version"):
        render_eval_config_with_metrics(
            replace(config(), agent_version=None),
            (),
            agent_target=QualifiedName.parse("DB.S.A"),
            dataset_target=QualifiedName.parse("DB.S.D"),
        )

    explicit = render_eval_config_with_metrics(
        replace(config(), agent_version="VERSION$4"),
        (),
        agent_target=QualifiedName.parse("DB.S.A"),
        dataset_target=QualifiedName.parse("DB.S.D"),
    )
    assert 'agent_version: "VERSION$4"' in explicit


def test_yaml_emitter_handles_nested_sequences_empty_mappings_scalars_and_errors() -> None:
    rendered = _emit_yaml(
        {
            "items": (
                {},
                {"nested": {"flag": True}},
                {"block": "line one\nline two\n"},
                None,
                False,
                1.5,
                ("a", 2),
            )
        }
    )
    assert "  - {}" in rendered
    assert "flag: true" in rendered
    assert "block: |-" in rendered
    assert "  - null" in rendered
    assert "  - false" in rendered
    assert "  - 1.5" in rendered
    assert '  - ["a", 2]' in rendered
    with pytest.raises(TypeError, match="unsupported YAML value set"):
        _emit_yaml({"bad": {1}})
