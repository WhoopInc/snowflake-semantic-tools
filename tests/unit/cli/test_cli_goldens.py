"""What `sst` prints for a few runs of the reference project matches its goldens, on every terminal.

`python -m tests.helpers.cli_goldens` rewrites a golden when the change to the output is meant.
"""

from __future__ import annotations

from pathlib import Path

import pytest

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


def test_the_colour_variant_is_coloured() -> None:
    text = golden_path(CASES[0], "color").read_text(encoding="utf-8")
    assert "\x1b[31m" in text and "\x1b[1m" in text
