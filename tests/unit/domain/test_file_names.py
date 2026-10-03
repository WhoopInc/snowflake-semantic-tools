"""A file name derived from an artifact name is one safe segment, and distinct for a distinct name."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.file_names import MAX_FILE_NAME, file_name


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("jaffle_sales", "jaffle_sales"),
        ("jaffle-toolkit.v2", "jaffle-toolkit.v2"),
        ('"x/../../pwned"', "%22x%2F..%2F..%2Fpwned%22"),
        ("..", "%2E."),
        (".hidden", "%2Ehidden"),
        ("a b", "a%20b"),
        ("100%", "100%25"),
        ("caf\u00e9", "caf%C3%A9"),
        ("back\\slash", "back%5Cslash"),
        ("", "%"),
    ],
)
def test_a_name_keeps_safe_characters_and_encodes_the_rest(name: str, expected: str) -> None:
    assert file_name(name) == expected


def test_a_long_name_is_cut_and_ends_in_a_hash_of_the_whole_name() -> None:
    first, second = file_name("v" * 300), file_name("v" * 299 + "w")
    assert len(first) == len(second) == MAX_FILE_NAME
    assert first != second and first.startswith("v" * 100)
    assert file_name("v" * MAX_FILE_NAME) == "v" * MAX_FILE_NAME
