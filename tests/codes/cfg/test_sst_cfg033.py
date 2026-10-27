"""SST-CFG033: a severity override breaks the demotion floor."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY, Severity
from snowflake_semantic_tools.domain.validate.config import validate_config


def test_sst_cfg033_fires() -> None:
    [diagnostic] = [
        item
        for item in validate_config({"diagnostics": {"severity_overrides": {"SST-REF001": "info"}}})
        if item.code == "SST-CFG033"
    ]
    assert diagnostic.severity is Severity.ERROR
    assert (
        diagnostic.message
        == "severity_overrides SST-REF001: info is not permitted (an error is demoted no lower than warning)"
    )
    assert diagnostic.subject == "config:diagnostics.severity_overrides.SST-REF001"
    assert diagnostic.origin is not None and not ERROR_REGISTRY["SST-CFG033"].demotable
    refused = validate_config({"diagnostics": {"severity_overrides": {"SST-VAL102": "warning", "SST-VAL020": "error"}}})
    assert [str(item.context["reason"]) for item in refused if item.code == "SST-CFG033"] == [
        "SST-VAL102 is non-demotable",
        "an info code is promoted no higher than warning",
    ]


def test_sst_cfg033_silent() -> None:
    assert "SST-CFG033" not in [
        item.code
        for item in validate_config(
            {"diagnostics": {"severity_overrides": {"SST-REF001": "warning", "SST-VAL003": "error"}}}
        )
    ]
