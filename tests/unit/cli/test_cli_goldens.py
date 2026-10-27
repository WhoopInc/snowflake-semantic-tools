"""What `sst` prints for a few runs of the reference project matches its goldens, on every terminal.

`python -m tests.helpers.cli_goldens` rewrites a golden when the change to the output is meant.
"""

from __future__ import annotations

import dataclasses
import os
from pathlib import Path

import pytest

from tests.helpers import cli_goldens
from tests.helpers.cli_goldens import CASES, Case, golden_path, render_case

PARAMS = [pytest.param(case, variant, id=f"{case.name}-{variant}") for case in CASES for variant in case.variants]


@pytest.mark.parametrize(("case", "variant"), PARAMS)
def test_the_output_matches_its_golden(tmp_path: Path, case: Case, variant: str) -> None:
    expected = golden_path(case, variant)
    assert expected.is_file(), f"no golden at {expected}; run python -m tests.helpers.cli_goldens"
    assert render_case(case, variant, tmp_path) == expected.read_text(encoding="utf-8")


def test_the_ascii_and_plain_variants_hold_no_escape_and_no_unicode_mark() -> None:
    for case in CASES:
        for variant in set(case.variants) & {"plain", "ascii"}:
            text = golden_path(case, variant).read_text(encoding="utf-8")
            assert "\x1b[" not in text, case.name
            if variant == "ascii":
                assert not set("✓✗·→…⠋") & set(text), case.name


def test_the_color_variant_is_colored() -> None:
    text = golden_path(CASES[0], "color").read_text(encoding="utf-8")
    assert "\x1b[31m" in text and "\x1b[1m" in text


@pytest.mark.parametrize("marker", ["COCO_AGENT", "CLAUDECODE", "SST_OUTPUT"])
def test_the_writer_renders_as_the_tests_do_whatever_the_shell_sets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], marker: str
) -> None:
    # Run from a coding agent's shell, the writer once rewrote every golden in the agent format.
    [case] = [item for item in CASES if item.name == "list"]
    variant = case.variants[0]
    committed = golden_path(case, variant)
    (tmp_path / committed.name).write_text(committed.read_text(encoding="utf-8"), encoding="utf-8")
    monkeypatch.setattr(cli_goldens, "CASES", (dataclasses.replace(case, variants=(variant,)),))
    monkeypatch.setattr(cli_goldens, "GOLDEN_DIR", tmp_path)
    monkeypatch.setenv(marker, "json" if marker == "SST_OUTPUT" else "1")

    assert cli_goldens.main() == 0
    assert "0 CLI golden file(s) written" in capsys.readouterr().out
    assert (tmp_path / committed.name).read_text(encoding="utf-8") == committed.read_text(encoding="utf-8")
    assert os.environ[marker]
