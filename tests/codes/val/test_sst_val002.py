"""SST-VAL002: a skill or plugin the project publishes takes the name of an extension it consumes."""

from __future__ import annotations

from pathlib import Path

from tests.helpers.semantic_projects import CONFIG, compiled_diagnostics, edited

CONSUMED = "    partner-glossary:\n"


def test_sst_val002_fires(tmp_path: Path) -> None:
    project = edited(tmp_path, CONFIG, CONSUMED, CONSUMED + "    jaffle-semantics:\n")
    [found] = compiled_diagnostics(project, "SST-VAL002")
    assert found["severity"] == "error"
    assert found["message"] == (
        "skill 'jaffle-semantics' collides with extension 'SST_REF_DEV.PARTNER.JAFFLE_SEMANTICS' in another namespace"
    )


def test_sst_val002_silent(tmp_path: Path) -> None:
    assert compiled_diagnostics(edited(tmp_path, CONFIG, CONSUMED, CONSUMED), "SST-VAL002") == []
