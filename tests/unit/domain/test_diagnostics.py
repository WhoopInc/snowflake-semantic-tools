"""The diagnostic registry is immutable, total, and owns severity."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.model.diagnostic import (
    ERROR_REGISTRY,
    D,
    DiagnosticBag,
    Origin,
    Severity,
    dedupe_diagnostics,
    render_diagnostic,
    resolve_code,
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


def test_legacy_validation_codes_resolve_as_aliases_but_are_never_emitted() -> None:
    assert resolve_code("SST-V021") == ("SST-VAL308",)
    assert resolve_code("SST-V005") == (
        "SST-PRS003",
        "SST-PRS018",
        "SST-PRS019",
        "SST-PRS029",
        "SST-PRS113",
        "SST-VAL113",
        "SST-VAL418",
    )
    assert resolve_code("SST-V050") == ("SST-VAL403",)
    assert resolve_code("SST-REF001") == ("SST-REF001",)
    assert D("SST-V021").code == "SST-INT900"


def test_every_legacy_alias_resolves_to_registered_successors() -> None:
    from snowflake_semantic_tools.domain.model.diagnostic import LEGACY_ALIASES

    assert all(successor in ERROR_REGISTRY for successors in LEGACY_ALIASES.values() for successor in successors)


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
    diagnostic = D("SST-PRT007", found="v13")
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
    no_origin = D("SST-LOD003", file="empty.yml")
    assert render_diagnostic(no_origin).startswith("warning[SST-LOD003]")


def test_human_render_omits_help_when_registry_has_no_suggestion() -> None:
    from dataclasses import replace

    from snowflake_semantic_tools.domain.model import diagnostic as module

    original = module.ERROR_REGISTRY
    module.ERROR_REGISTRY = {**original, "SST-LOD003": replace(original["SST-LOD003"], suggestion=None)}
    try:
        assert "help:" not in render_diagnostic(D("SST-LOD003", file="empty.yml"))
    finally:
        module.ERROR_REGISTRY = original


def test_diagnostics_dedupe_by_identity() -> None:
    diagnostic = D("SST-REF001", model="missing", subject="semantic_view:test")
    assert dedupe_diagnostics(DiagnosticBag((diagnostic, diagnostic))) == (diagnostic,)


def test_registry_integrity_checks_invalid_codes_and_duplicates() -> None:
    from snowflake_semantic_tools.domain.model.diagnostic import ErrorSpec, RegistryIntegrityError, _build_registry

    first = ErrorSpec("BAD-X001", Severity.ERROR, "x", "{value}", None, "X00", "x00", "https://x")
    with pytest.raises(RegistryIntegrityError, match="invalid error code"):
        _build_registry((first,))
    valid = ERROR_REGISTRY["SST-REF001"]
    with pytest.raises(RegistryIntegrityError, match="duplicate error code"):
        _build_registry((valid, valid))


def test_non_strict_resolution_returns_the_same_bag() -> None:
    bag = DiagnosticBag((D("SST-LOD003", file="empty.yml"),))
    resolved, promoted = resolve_severities(bag, strict=False)
    assert resolved is bag
    assert promoted == 0
