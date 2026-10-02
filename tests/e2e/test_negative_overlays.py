"""The negative corpus: each overlay, laid alone over the reference project, adds exactly its codes.

Every case runs `sst validate --strict` offline over a fresh copy, so cases never mask each other,
and compares with the clean project's own run: the overlay must add exactly the diagnostics its
`expected.json` entry lists -- code, declared severity and count, not a substring -- must not
remove any, and must not reach a file other than its own. A case that poisons the rest of the
project has found a cascade.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import pytest

from tests.helpers.cli_projects import DBT_MANIFEST
from tests.helpers.e2e_cli import Key, expected_cases, overlaid, overlay_files, reference_copy, strict_validate

pytestmark = pytest.mark.e2e


@pytest.fixture(scope="module")
def clean(tmp_path_factory: pytest.TempPathFactory) -> Counter[Key]:
    run = strict_validate(reference_copy(tmp_path_factory.mktemp("clean")), DBT_MANIFEST)
    assert run.stderr == ""
    return run.blocking()


def test_every_overlay_has_an_expectation_and_every_expectation_an_overlay() -> None:
    assert overlay_files() == sorted(expected_cases())
    assert len(overlay_files()) == 11


@pytest.mark.parametrize("overlay", overlay_files())
def test_an_overlay_adds_exactly_its_codes_and_touches_no_other_file(
    overlay: str, clean: Counter[Key], tmp_path: Path
) -> None:
    case = expected_cases()[overlay]
    project, manifest = overlaid(tmp_path, overlay, case["into"])
    run = strict_validate(project, manifest)

    assert run.exit_code == 1
    assert run.stderr == ""
    found = run.blocking()
    added, removed = found - clean, clean - found
    assert removed == Counter()
    assert Counter({(code, severity): count for (code, severity, _), count in added.items()}) == Counter(
        {(item["code"], item["severity"]): item["count"] for item in case["adds"]}
    )
    own_file = None if case["into"] == "manifest" else f"{case['into']}/{overlay}"
    assert {path for _, _, path in added} <= {own_file, None}
    promoted = {item["code"] for item in run.envelope["diagnostics"] if item["promoted_from"] == "warning"}
    assert {item["code"] for item in case["adds"] if item["severity"] == "warning"} <= promoted
