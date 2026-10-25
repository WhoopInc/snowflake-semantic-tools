"""The negative corpus: each overlay, laid alone over the reference project, adds exactly its codes.

Every case runs `sst validate --strict` offline over a fresh copy, so cases never mask each other,
and compares with the clean project's own run: the overlay must add exactly the diagnostics its
`expected.json` entry lists -- code, declared severity and count, not a substring -- must not
remove any, and must not reach a file other than the ones it writes. A case that poisons the rest
of the project has found a cascade; the only ones allowed are listed under the case's `reaches`,
where an error in one artifact is meant to be reported on the artifacts that carry it.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import pytest

from tests.helpers.e2e_cli import Key, expected_cases, overlaid, overlay_files, overlay_paths, strict_validate
from tests.helpers.reference_project import DBT_MANIFEST, project_copy

# Each copies and compiles the reference project; deselected locally, run in CI.
pytestmark = [pytest.mark.e2e, pytest.mark.slow]


@pytest.fixture(scope="module")
def clean(tmp_path_factory: pytest.TempPathFactory) -> Counter[Key]:
    run = strict_validate(project_copy(tmp_path_factory.mktemp("clean"), offline=False), DBT_MANIFEST)
    assert run.stderr == ""
    return run.blocking()


def test_every_overlay_has_an_expectation_and_every_expectation_an_overlay() -> None:
    assert overlay_files() == sorted(expected_cases())
    assert len(overlay_files()) == 38


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
    # Summed, not keyed: one code may land on several files, as a cascade does.
    by_code: Counter[tuple[str, str]] = Counter()
    for (code, severity, _), count in added.items():
        by_code[code, severity] += count
    assert by_code == Counter({(item["code"], item["severity"]): item["count"] for item in case["adds"]})
    own_files = overlay_paths(overlay, case["into"]) | set(case.get("reaches", ()))
    assert {path for _, _, path in added} <= own_files | {None}
    promoted = {item["code"] for item in run.envelope["diagnostics"] if item["promoted_from"] == "warning"}
    assert {item["code"] for item in case["adds"] if item["severity"] == "warning"} <= promoted
