from __future__ import annotations

from dataclasses import replace

import pytest

from snowflake_semantic_tools.app.compile.evals import CompileEvals
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.domain.model.diagnostic import D, DiagnosticBag
from snowflake_semantic_tools.domain.model.eval import (
    EvalCatalog,
    EvalDefaults,
    EvalGroundTruth,
    EvalQuestion,
    EvalRunConfig,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.eval_builders import ORIGIN, compile_eval, resolved_eval


def test_compile_eval_projects_composite_metadata_and_manifest_impact() -> None:
    result = compile_eval()
    compiled = result.compiled[0]
    rendered = compiled.rendered_artifact
    assert rendered.object_type == ""
    assert rendered.render_dialect == "eval_yaml"
    assert rendered.depends_on == ("agent:sales_agent",)
    assert not rendered.generic_apply_safe
    assert rendered.statements == ()
    assert tuple(name for name, _ in rendered.component_fingerprints) == ("dataset", "config")
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
    assert 'agent_version: "LAST"' in result.compiled[0].rendered.config_yaml


def test_rendered_for_publish_emits_source_table_and_dataset_statements() -> None:
    compiled = compile_eval().compiled[0]

    published = compiled.rendered_for_publish("unused-manifest-id")

    assert published.create_statements == (
        compiled.rendered.source_table_sql.split(";\n\n", 1)[0],
        compiled.rendered.source_table_sql.split(";\n\n", 1)[1].removesuffix(";\n"),
        compiled.rendered.create_dataset_sql.strip(),
    )


def test_dataset_identity_is_independent_of_config_and_git_sha() -> None:
    first = compile_eval().compiled[0]
    changed_config = replace(
        resolved_eval(),
        config=replace(resolved_eval().config, run=EvalRunConfig(label="changed")),
    )
    second = compile_eval(changed_config).compiled[0]
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
    first = compile_eval(value).compiled[0]
    second = compile_eval(changed).compiled[0]
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
