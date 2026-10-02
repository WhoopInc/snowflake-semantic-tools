"""The diagnostic registry is immutable, total, and owns severity."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.diagnostics import (
    ERROR_REGISTRY,
    D,
    DiagnosticBag,
    Origin,
    Severity,
    render_diagnostic,
    resolve_severities,
)


def test_registered_code_owns_severity_and_formats_context() -> None:
    diagnostic = D("SST-REF001", model="missing", origin=Origin("views.yml", 3, 7))
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "{{ ref('missing') }} is not a model in the dbt manifest"
    assert diagnostic.origin == Origin("views.yml", 3, 7)


def test_unregistered_code_becomes_an_internal_diagnostic() -> None:
    diagnostic = D("SST-NOPE999")
    assert diagnostic.code == "SST-INT900"
    assert diagnostic.message == "unregistered code SST-NOPE999"


def test_0_3_codes_are_not_registered_and_never_emitted() -> None:
    import snowflake_semantic_tools.domain.diagnostics as module

    assert not hasattr(module, "LEGACY_ALIASES") and not hasattr(module, "resolve_code")
    assert D("SST-V021").code == "SST-INT900"
    assert not any(code.split("-")[1].startswith("V0") for code in ERROR_REGISTRY)


def test_hard_deprecated_input_is_an_error_and_retired_codes_are_gone() -> None:
    for code in ("SST-DBT032", "SST-CFG044", "SST-REF045"):
        assert ERROR_REGISTRY[code].severity is Severity.ERROR, code
    for code in ("SST-PRS121", "SST-VAL122", "SST-CFG045"):
        assert code not in ERROR_REGISTRY, code


def test_enrich_codes_are_registered_and_the_collection_refusal_cannot_be_demoted() -> None:
    expected = {
        "SST-CFG038": Severity.ERROR,
        "SST-VAL325": Severity.WARNING,
        "SST-VAL327": Severity.WARNING,
        "SST-VAL328": Severity.WARNING,
        "SST-DBT031": Severity.WARNING,
        "SST-PRS125": Severity.WARNING,
        "SST-SNO030": Severity.ERROR,
        "SST-SNO031": Severity.ERROR,
    }
    assert {code: ERROR_REGISTRY[code].severity for code in expected} == expected
    assert not ERROR_REGISTRY["SST-CFG038"].demotable
    # The checks on what enrich writes point at the command that writes it.
    for code in ("SST-VAL308", "SST-VAL309", "SST-VAL315", "SST-VAL316"):
        assert "sst enrich" in str(ERROR_REGISTRY[code].suggestion), code


def test_static_eval_diagnostics_are_registered_with_catalog_severities() -> None:
    expected = {
        "SST-VAL701": Severity.ERROR,
        "SST-VAL702": Severity.ERROR,
        "SST-VAL703": Severity.ERROR,
        "SST-VAL705": Severity.ERROR,
        "SST-VAL706": Severity.ERROR,
        "SST-VAL707": Severity.WARNING,
        "SST-VAL708": Severity.ERROR,
        "SST-VAL709": Severity.ERROR,
        "SST-VAL710": Severity.WARNING,
        "SST-VAL711": Severity.INFO,
        "SST-VAL712": Severity.INFO,
        "SST-VAL717": Severity.ERROR,
        "SST-VAL718": Severity.ERROR,
        "SST-VAL719": Severity.ERROR,
        "SST-VAL721": Severity.ERROR,
        "SST-VAL722": Severity.ERROR,
        "SST-VAL724": Severity.ERROR,
        "SST-VAL725": Severity.INFO,
        "SST-VAL726": Severity.WARNING,
        "SST-VAL731": Severity.WARNING,
        "SST-VAL732": Severity.INFO,
        "SST-VAL733": Severity.ERROR,
        "SST-VAL735": Severity.WARNING,
        "SST-VAL737": Severity.ERROR,
        "SST-VAL738": Severity.ERROR,
        "SST-VAL739": Severity.ERROR,
        "SST-VAL740": Severity.WARNING,
        "SST-VAL741": Severity.ERROR,
        "SST-VAL742": Severity.WARNING,
        "SST-VAL743": Severity.WARNING,
        "SST-VAL747": Severity.ERROR,
        "SST-VAL748": Severity.WARNING,
    }
    assert {code: ERROR_REGISTRY[code].severity for code in expected} == expected


def test_missing_template_context_becomes_an_internal_diagnostic() -> None:
    diagnostic = D("SST-DBT017", found="v13")
    assert diagnostic.code == "SST-INT901"
    assert "expected" in diagnostic.message


def test_registry_and_context_are_immutable() -> None:
    with pytest.raises(TypeError):
        ERROR_REGISTRY["SST-X001"] = ERROR_REGISTRY["SST-REF001"]  # type: ignore[index]
    diagnostic = D("SST-REF001", model="missing")
    with pytest.raises(TypeError):
        diagnostic.context["model"] = "other"  # type: ignore[index]


def test_diagnostic_bag_counts_severity_and_reports_errors() -> None:
    bag = DiagnosticBag((D("SST-REF001", model="missing"), D("SST-LOD003", file="empty.yml")))
    assert bag.count(Severity.ERROR) == 1
    assert bag.count(Severity.WARNING) == 1
    assert bag.has_errors


def test_strict_promotes_warnings_once() -> None:
    bag = DiagnosticBag((D("SST-VAL316", artifact="m", member="c", field="sample_values", value="nan"),))
    resolved, promoted = resolve_severities(bag, strict=True)
    assert resolved[0].severity is Severity.ERROR
    assert promoted == 1


def test_diagnostic_identity_and_human_render_are_stable() -> None:
    diagnostic = D(
        "SST-REF001",
        model="missing",
        subject="semantic_view:test",
        origin=Origin("semantic_models/views.yml", 4, 7),
    )
    assert diagnostic.phase == "ref"
    assert len(diagnostic.fingerprint) == 64
    rendered = render_diagnostic(diagnostic)
    assert rendered.startswith("semantic_models/views.yml:4:7: error[SST-REF001]")
    assert "help:" in rendered
    assert diagnostic.help_url in rendered

    file_only = D("SST-LOD003", file="empty.yml", origin=Origin("empty.yml"))
    line_only = D("SST-LOD003", file="empty.yml", origin=Origin("empty.yml", 2))
    assert render_diagnostic(file_only).startswith("empty.yml: warning")
    assert render_diagnostic(line_only).startswith("empty.yml:2: warning")
    # A LOD code given no origin points at the file its context names.
    assert render_diagnostic(D("SST-LOD003", file="empty.yml")).startswith("empty.yml: warning")
    no_origin = D("SST-INT902", detail="x")
    assert render_diagnostic(no_origin).startswith("error[SST-INT902]")


def test_human_render_omits_help_when_registry_has_no_suggestion() -> None:
    from dataclasses import replace

    from snowflake_semantic_tools.domain import diagnostics as module

    original = module.ERROR_REGISTRY
    module.ERROR_REGISTRY = {**original, "SST-LOD003": replace(original["SST-LOD003"], suggestion=None)}
    try:
        assert "help:" not in render_diagnostic(D("SST-LOD003", file="empty.yml"))
    finally:
        module.ERROR_REGISTRY = original


def test_registry_integrity_checks_invalid_codes_and_duplicates() -> None:
    from snowflake_semantic_tools.domain.diagnostics import ErrorSpec, RegistryIntegrityError, build_registry

    first = ErrorSpec("BAD-X001", Severity.ERROR, "x", "{value}", None, "X00", "x00", "https://x")
    with pytest.raises(RegistryIntegrityError, match="SST-REG012"):
        build_registry((first,))
    valid = ERROR_REGISTRY["SST-REF001"]
    with pytest.raises(RegistryIntegrityError, match="SST-REG002"):
        build_registry((valid, valid))


def test_non_strict_resolution_returns_the_same_bag() -> None:
    bag = DiagnosticBag((D("SST-LOD003", file="empty.yml"),))
    resolved, promoted = resolve_severities(bag, strict=False)
    assert resolved is bag
    assert promoted == 0
