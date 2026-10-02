"""What the loader would do to an authored `verified_by`, which the renderer must publish as written."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.adapters.yaml.semantic.checks.fidelity import _verifier_change


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("data-platform", None),
        (" data-platform", "trimmed"),
        ("   ", "dropped"),
        (42, "converted to text"),
        (["a"], "converted to text"),
        (0, "dropped"),
        (False, "dropped"),
    ],
)
def test_a_verified_by_reaches_the_ddl_only_as_text_without_surrounding_whitespace(
    value: object, expected: str | None
) -> None:
    assert _verifier_change({"verified_by": value}) == expected
