"""`diagnostics.severity_overrides` end to end: each direction changes what a code reports and what blocks.

An override is policy, applied before strict promotion and recorded on the diagnostic, so the
emission audit accepts it: no run here reports SST-INT007.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from tests.helpers.cli_projects import validate_json
from tests.helpers.reference_project import project_copy


def _project(tmp_path: Path, overrides: dict[str, str], *, duplicate: bool = False) -> Path:
    """Copy the reference project, with a duplicated filter (SST-PRS106) when asked, and `overrides`."""
    project = project_copy(tmp_path)
    if duplicate:
        filters = project / "semantic_models" / "filters" / "filters.yml"
        text = filters.read_text(encoding="utf-8")
        start = text.index("  - name: is_completed_order")
        filters.write_text(text + "\n" + text[start : text.index("  - name:", start + 1)], encoding="utf-8")
    if overrides:
        lines = "".join(f"    {code}: {severity}\n" for code, severity in overrides.items())
        config = project / "sst_config.yml"
        config.write_text(
            config.read_text(encoding="utf-8") + f"\ndiagnostics:\n  severity_overrides:\n{lines}", encoding="utf-8"
        )
    return project


def _of(diagnostics: list[dict[str, Any]], code: str) -> set[tuple[str, str | None, str | None]]:
    return {
        (item["severity"], item["promoted_from"], item["demoted_from"]) for item in diagnostics if item["code"] == code
    }


def _codes(diagnostics: list[dict[str, Any]]) -> set[str]:
    return {item["code"] for item in diagnostics}


def test_promoting_a_warning_to_an_error_blocks_the_run(tmp_path: Path) -> None:
    exit_code, diagnostics = validate_json(_project(tmp_path, {"SST-CFG018": "error"}), "--no-strict")
    assert exit_code == 1
    assert _of(diagnostics, "SST-CFG018") == {("error", "warning", None)}
    assert "SST-INT007" not in _codes(diagnostics)


def test_promoting_an_info_code_to_a_warning_reports_it_and_passes(tmp_path: Path) -> None:
    exit_code, diagnostics = validate_json(_project(tmp_path, {"SST-VAL020": "warning"}), "--no-strict")
    assert exit_code == 0
    assert _of(diagnostics, "SST-VAL020") == {("warning", "info", None)}
    assert "SST-INT007" not in _codes(diagnostics)


def test_demoting_an_error_to_a_warning_lets_the_run_pass(tmp_path: Path) -> None:
    blocked, before = validate_json(_project(tmp_path / "a", {}, duplicate=True), "--no-strict")
    assert blocked == 1 and _of(before, "SST-PRS106") == {("error", None, None)}
    exit_code, diagnostics = validate_json(
        _project(tmp_path / "b", {"SST-PRS106": "warning"}, duplicate=True), "--no-strict"
    )
    assert exit_code == 0
    assert _of(diagnostics, "SST-PRS106") == {("warning", None, "error")}
    assert "SST-INT007" not in _codes(diagnostics)


def test_demoting_a_warning_to_info_reports_it_as_info(tmp_path: Path) -> None:
    exit_code, diagnostics = validate_json(_project(tmp_path, {"SST-CFG018": "info"}), "--no-strict")
    assert exit_code == 0
    assert _of(diagnostics, "SST-CFG018") == {("info", None, "warning")}
    assert "SST-INT007" not in _codes(diagnostics)


def test_strict_promotes_a_demoted_error_again_since_strict_applies_after_the_override(tmp_path: Path) -> None:
    exit_code, diagnostics = validate_json(_project(tmp_path, {"SST-PRS106": "warning"}, duplicate=True), "--strict")
    assert exit_code == 1
    assert _of(diagnostics, "SST-PRS106") == {("error", None, None)}
    assert "SST-INT007" not in _codes(diagnostics)


def test_demoting_a_non_demotable_code_is_refused_with_its_catalog_code(tmp_path: Path) -> None:
    project = _project(tmp_path, {"SST-VAL102": "warning", "SST-PRS106": "info"}, duplicate=True)
    exit_code, diagnostics = validate_json(project, "--no-strict")
    assert exit_code == 1
    refusals = {item["message"] for item in diagnostics if item["code"] == "SST-CFG033"}
    assert refusals == {
        "severity_overrides SST-VAL102: warning is not permitted (SST-VAL102 is non-demotable)",
        "severity_overrides SST-PRS106: info is not permitted (an error is demoted no lower than warning)",
    }
    # A refused override is never applied: the duplicate still reports at its declared severity.
    assert _of(diagnostics, "SST-PRS106") == {("error", None, None)}
    assert "SST-INT007" not in _codes(diagnostics)
