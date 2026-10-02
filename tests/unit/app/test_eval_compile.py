from __future__ import annotations

from dataclasses import replace

import pytest

from snowflake_semantic_tools.app.compile.evals import CompiledEval, CompileEvals
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag
from snowflake_semantic_tools.domain.model.eval import (
    EvalCatalog,
    EvalDefaults,
    EvalGroundTruth,
    EvalQuestion,
    EvalRunConfig,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.compile_builders import compiled_as
from tests.helpers.eval_builders import ORIGIN, compile_eval, compiled_eval_of, resolved_eval
from tests.helpers.sql_values import texts


def test_compile_eval_projects_composite_metadata_and_manifest_impact() -> None:
    result = compile_eval()
    compiled = result.compiled[0]
    rendered = compiled.rendered_artifact
    assert rendered.object_type == ""
    assert rendered.render_dialect == "eval_yaml"
    assert rendered.depends_on == ("agent:sales_agent",)
    assert not rendered.generic_apply_safe
    assert rendered.statements == ()
    # Each custom metric is recorded under its name, so a later plan can tell it was edited in place.
    assert tuple(name for name, _ in rendered.component_fingerprints) == (
        "dataset",
        "config",
        "dataset_version",
        "metric:grounding",
    )
    # The version follows the questions, so it never moves between commits.
    assert dict(rendered.component_fingerprints)["dataset_version"] == (
        f"SST_{compiled.rendered_artifact.component_fingerprints[0][1][:12].upper()}"
    )
    assert (
        dict(rendered.component_fingerprints)["metric:grounding"] == resolved_eval().custom_metrics[0].definition_digest
    )
    assert tuple(object_type for object_type, _ in rendered.physical_resources) == ("TABLE", "DATASET")
    assert compiled.source_files == (
        "agents/sales/evals/dataset.yml",
        "agents/sales/evals/config.yml",
        "eval_metrics/grounding.yml",
    )
    manifest = build_manifest(result)
    entry = manifest.artifacts["eval:sales_agent"]
    assert entry.component_fingerprints == rendered.component_fingerprints
    assert tuple(object_type for object_type, _ in entry.physical_resources) == ("TABLE", "DATASET")
    assert manifest.impact.by_file["eval_metrics/grounding.yml"] == ("eval:sales_agent",)


def test_compile_eval_applies_inherited_agent_version() -> None:
    value = resolved_eval()
    value = replace(value, config=replace(value.config, agent_version=None))
    result = CompileEvals(
        EvalCatalog(
            (value,),
            value.custom_metrics,
            EvalDefaults(agent_version="committed"),
        ),
        agent_targets={"sales_agent": QualifiedName.parse("DB.S.SALES_AGENT")},
    ).run_result()

    assert not result.diagnostics.has_errors
    assert 'agent_version: "LAST"' in compiled_as(result, CompiledEval).rendered.config_yaml


def test_rendered_for_publish_emits_source_table_and_dataset_statements() -> None:
    compiled = compiled_eval_of()

    published = compiled.rendered_for_publish("unused-manifest-id")

    version = compiled.dataset_version
    assert texts(published.create_statements) == (
        compiled.rendered.source_table_sql.split(";\n\n", 1)[0],
        compiled.rendered.source_table_sql.split(";\n\n", 1)[1].removesuffix(";\n"),
        str(compiled.rendered.create_dataset_statement),
        f"ALTER DATASET {compiled.dataset_target.sql} ADD VERSION '{version}'\n"
        f"  FROM (SELECT INPUT_QUERY, GROUND_TRUTH FROM {compiled.source_table.sql})\n"
        f"  COMMENT = 'SST eval questions {version.removeprefix('SST_').lower()}'\n"
        f"  METADATA = '{compiled.version_metadata}'",
    )
    assert dict(published.component_fingerprints)["version_metadata"] == compiled.version_metadata
    # The commit reaches only what publishes, never the artifact the fingerprint covers.
    assert published.fingerprint == compiled.rendered_artifact.fingerprint


def test_dataset_identity_is_independent_of_config_and_git_sha() -> None:
    first = compiled_eval_of()
    changed_config = replace(
        resolved_eval(),
        config=replace(resolved_eval().config, run=EvalRunConfig(label="changed")),
    )
    second = compiled_eval_of(changed_config)
    assert first.rendered.dataset_fingerprint == second.rendered.dataset_fingerprint
    assert first.dataset_target == second.dataset_target
    assert first.rendered.config_fingerprint != second.rendered.config_fingerprint


def test_dataset_identity_changes_with_question_payload() -> None:
    value = resolved_eval()
    changed = replace(
        value,
        dataset=replace(
            value.dataset,
            questions=(EvalQuestion(ORIGIN, "Different question", EvalGroundTruth(ORIGIN, (), "Answer")),),
        ),
    )
    first = compiled_eval_of(value)
    second = compiled_eval_of(changed)
    assert first.rendered.dataset_fingerprint != second.rendered.dataset_fingerprint
    assert first.dataset_target != second.dataset_target


def test_compile_skips_only_eval_with_subject_error() -> None:
    value = resolved_eval()
    catalog = EvalCatalog(
        (value,),
        value.custom_metrics,
        diagnostics=DiagnosticBag((D("SST-PRS002", artifact="config.yml", field="agent", subject=value.key),)),
    )

    result = CompileEvals(
        catalog,
        agent_targets={"sales_agent": QualifiedName.parse("DB.S.SALES_AGENT")},
    ).run_result()

    assert result.compiled == ()
    assert result.diagnostics == catalog.diagnostics


def test_compile_turns_missing_agent_target_into_invariant_diagnostic() -> None:
    value = resolved_eval()

    result = CompileEvals(EvalCatalog((value,), value.custom_metrics), agent_targets={}).run_result()

    assert result.compiled == ()
    assert result.diagnostics[0].code == "SST-INT902"
    assert result.diagnostics[0].subject == value.key
    assert result.diagnostics[0].origin == value.config.origin


def test_compile_turns_missing_dataset_templates_into_invariant_diagnostic() -> None:
    value = resolved_eval()
    assert value.config.dataset is not None
    invalid = replace(value, config=replace(value.config, dataset=replace(value.config.dataset, name_template=None)))

    result = compile_eval(invalid)

    assert result.compiled == ()
    assert result.diagnostics[0].code == "SST-INT902"
    assert "requires name_template and source_table_template" in result.diagnostics[0].message


def test_compile_turns_render_type_error_into_invariant_diagnostic(monkeypatch: pytest.MonkeyPatch) -> None:
    def raise_type_error(*_args: object, **_kwargs: object) -> None:
        raise TypeError("bad eval payload")

    monkeypatch.setattr(
        "snowflake_semantic_tools.app.compile.evals._render",
        raise_type_error,
    )

    result = compile_eval()

    assert result.compiled == ()
    assert result.diagnostics[0].code == "SST-INT902"
    assert "bad eval payload" in result.diagnostics[0].message
