"""The shared persisted-document shapes: fingerprint pairs, physical resources, optional objects."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.state.codec import (
    optional_object,
    pairs_from_json,
    pairs_to_json,
    resources_from_json,
    resources_to_json,
)


def test_fingerprint_pairs_sort_and_round_trip() -> None:
    pairs = pairs_from_json({"b": 2, "a": "1"})
    assert pairs == (("a", "1"), ("b", "2"))
    assert pairs_from_json(pairs_to_json(pairs)) == pairs


def test_resources_round_trip_in_order_and_refuse_a_non_object() -> None:
    pairs = (("STAGE", "DB.S.B"), ("TABLE", "DB.S.A"))
    assert resources_from_json(resources_to_json(pairs), "bad") == pairs
    with pytest.raises(ValueError, match="^bad$"):
        resources_from_json([{"object_type": "T", "qualified_name": "N"}, "x"], "bad")
    with pytest.raises(KeyError):
        resources_from_json([{"object_type": "T"}], "bad")


def test_an_optional_object_reads_absent_as_empty_and_keys_as_text() -> None:
    assert optional_object(None, "bad") == {}
    assert optional_object({1: "one"}, "bad") == {"1": "one"}
    with pytest.raises(ValueError, match="^bad$"):
        optional_object([], "bad")
