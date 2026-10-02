"""The typed field readers shared by the YAML artifact parsers."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.yaml.fields import (
    checked_list,
    checked_mapping,
    checked_strings,
    checked_text,
    mapping,
    optional_int,
    optional_string,
    project_relative,
    report_unknown_keys,
    strings,
    unknown_keys,
)
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin

ORIGIN = Origin("profiles/a/profile.yml", 1)


@pytest.mark.parametrize(
    ("value", "expected"),
    (
        (" padded ", "padded"),
        ("x", "x"),
        ("", None),
        ("   ", None),
        (None, None),
        (7, None),
        (True, None),
        (["x"], None),
    ),
)
def test_optional_string_strips_text_and_reads_anything_else_as_none(value: object, expected: str | None) -> None:
    assert optional_string(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    ((0, 0), (12, 12), (-3, -3), (True, None), (False, None), (1.0, None), ("1", None), (None, None)),
)
def test_optional_int_takes_integers_but_not_booleans(value: object, expected: int | None) -> None:
    assert optional_int(value) == expected


def test_strings_stringifies_every_list_item_without_checking_it() -> None:
    assert strings(["a", 1, 2.5, True, None]) == ("a", "1", "2.5", "True", "None")
    assert strings([]) == ()


@pytest.mark.parametrize("value", (None, "a", ("a",), {"a": 1}, 3), ids=("none", "str", "tuple", "dict", "int"))
def test_strings_reads_anything_but_a_list_as_empty(value: object) -> None:
    assert strings(value) == ()


def test_mapping_copies_a_dict_with_string_keys_and_reads_anything_else_as_empty() -> None:
    source = {1: "one", None: [1], "k": {2: "nested"}}
    copied = mapping(source)
    assert copied == {"1": "one", "None": [1], "k": {2: "nested"}}
    copied["extra"] = 1
    assert "extra" not in source
    assert mapping(None) == mapping([("a", 1)]) == mapping("a") == {}


def test_checked_text_reads_a_string_like_optional_string_without_reporting() -> None:
    sink: list[Diagnostic] = []
    values = (" x ", "  ", None)
    assert [checked_text(value, sink, field="f", artifact="a", origin=ORIGIN) for value in values] == [
        optional_string(value) for value in values
    ]
    assert sink == []


def test_checked_text_reports_a_value_that_is_not_a_string() -> None:
    sink: list[Diagnostic] = []
    assert checked_text(3, sink, field="owner_team", artifact="plugin:p", origin=ORIGIN, subject="plugin:p") is None
    assert sink == [
        D(
            "SST-PRS003",
            origin=ORIGIN,
            subject="plugin:p",
            artifact="plugin:p",
            field="owner_team",
            expected="a string",
            found="int",
        )
    ]
    assert list(sink[0].context) == ["artifact", "field", "expected", "found"]
    assert sink[0].message == "plugin:p: 'owner_team' expects a string, found int"


def test_checked_strings_returns_a_list_of_strings_and_reads_absence_as_empty() -> None:
    sink: list[Diagnostic] = []
    assert checked_strings(["a", "b"], sink, field="skills", artifact="a", origin=ORIGIN) == ("a", "b")
    assert checked_strings([], sink, field="skills", artifact="a", origin=ORIGIN) == ()
    assert checked_strings(None, sink, field="skills", artifact="a", origin=ORIGIN) == ()
    assert sink == []


@pytest.mark.parametrize(
    ("value", "found"),
    ((["a", 1], "list"), ("a", "str"), ({"a": "b"}, "dict"), ([True], "list")),
    ids=("mixed-list", "scalar", "mapping", "boolean-item"),
)
def test_checked_strings_reports_anything_else_where_strings_would_stringify(value: object, found: str) -> None:
    sink: list[Diagnostic] = []
    assert checked_strings(value, sink, field="+metrics", artifact="sst_config.yml", origin=ORIGIN) == ()
    assert sink == [
        D(
            "SST-PRS003",
            origin=ORIGIN,
            artifact="sst_config.yml",
            field="+metrics",
            expected="list of strings",
            found=found,
        )
    ]
    assert sink[0].subject is None
    assert list(sink[0].context) == ["artifact", "field", "expected", "found"]


@pytest.mark.parametrize("expected", ("a list of names", "a list of skill names"))
def test_checked_strings_names_the_expected_type_as_the_caller_words_it(expected: str) -> None:
    sink: list[Diagnostic] = [D("SST-LOD003", file="earlier.yml")]
    checked_strings(
        "x", sink, field="skills", artifact="profile:a", origin=ORIGIN, subject="profile:a", expected=expected
    )
    assert sink[1] == D(
        "SST-PRS003",
        origin=ORIGIN,
        subject="profile:a",
        artifact="profile:a",
        field="skills",
        expected=expected,
        found="str",
    )
    assert sink[0] == D("SST-LOD003", file="earlier.yml")


def test_checked_list_returns_a_list_as_written_and_reads_absence_or_emptiness_as_empty() -> None:
    sink: list[Diagnostic] = []
    items = [{"type": "generic"}, "text"]
    assert checked_list(items, sink, field="spec.tools", artifact="a", origin=ORIGIN) is items
    assert checked_list(None, sink, field="spec.tools", artifact="a", origin=ORIGIN) == []
    assert checked_list("", sink, field="spec.tools", artifact="a", origin=ORIGIN) == []
    assert sink == []


@pytest.mark.parametrize(("value", "found"), ((5, "int"), ("text", "str"), ({"a": 1}, "dict")))
def test_checked_list_reports_anything_else_against_its_subject(value: object, found: str) -> None:
    sink: list[Diagnostic] = []
    read = checked_list(value, sink, field="spec.tools", artifact="a.yml", origin=ORIGIN, subject="agent:a")
    assert read == []
    assert sink == [
        D(
            "SST-PRS003",
            origin=ORIGIN,
            subject="agent:a",
            artifact="a.yml",
            field="spec.tools",
            expected="a list",
            found=found,
        )
    ]


def test_checked_mapping_copies_a_mapping_keeping_its_keys_and_reads_absence_as_empty() -> None:
    sink: list[Diagnostic] = []
    written = {1: "one", "b": [2]}
    read = checked_mapping(written, sink, field="meta", artifact="a", origin=ORIGIN)
    assert read == {1: "one", "b": [2]} and read is not written
    assert checked_mapping(None, sink, field="meta", artifact="a", origin=ORIGIN) == {}
    assert checked_mapping([], sink, field="meta", artifact="a", origin=ORIGIN) == {}
    assert sink == []


@pytest.mark.parametrize(("value", "found"), (("text", "str"), (3, "int"), ([["a", 1]], "list")))
def test_checked_mapping_reports_anything_else_instead_of_raising(value: object, found: str) -> None:
    sink: list[Diagnostic] = []
    assert checked_mapping(value, sink, field="meta", artifact="a.yml", origin=ORIGIN) == {}
    assert sink == [D("SST-PRS003", origin=ORIGIN, artifact="a.yml", field="meta", expected="a mapping", found=found)]


def test_unknown_keys_sort_by_their_text_whatever_type_yaml_gave_them() -> None:
    assert unknown_keys({"zeta": 1, "alpha": 2, "name": 3}, frozenset(("name",))) == ["alpha", "zeta"]
    assert unknown_keys({"b": 1, 10: 2, 2: 3, None: 4}, frozenset()) == [10, 2, None, "b"]
    assert unknown_keys({"name": 1}, frozenset(("name",))) == []


def test_report_unknown_keys_points_every_report_at_one_origin() -> None:
    sink: list[Diagnostic] = []
    report_unknown_keys(
        {"name": 1, "zz": 2, "aa": 3}, frozenset(("name",)), sink, artifact="eval_metrics/m.yml", origin=ORIGIN
    )
    assert sink == [
        D("SST-PRS004", origin=ORIGIN, artifact="eval_metrics/m.yml", field="aa"),
        D("SST-PRS004", origin=ORIGIN, artifact="eval_metrics/m.yml", field="zz"),
    ]
    assert all(item.subject is None for item in sink)
    assert list(sink[0].context) == ["artifact", "field"]
    assert sink[0].message == "eval_metrics/m.yml: unknown field 'aa'"


def test_report_unknown_keys_can_locate_each_key_on_its_own_and_tag_a_subject() -> None:
    sink: list[Diagnostic] = []
    lines = {"extra": 3, "odd": 5}

    def located(key: str) -> Origin:
        return Origin("plugins/p/plugin.yml", lines[key], 1)

    report_unknown_keys(
        {"odd": 1, "extra": 2, "name": 3},
        frozenset(("name",)),
        sink,
        artifact="plugin:p",
        origin=located,
        subject="plugin:p",
    )
    assert sink == [
        D("SST-PRS004", origin=located("extra"), subject="plugin:p", artifact="plugin:p", field="extra"),
        D("SST-PRS004", origin=located("odd"), subject="plugin:p", artifact="plugin:p", field="odd"),
    ]


def test_report_unknown_keys_keeps_a_key_yaml_did_not_type_as_a_string() -> None:
    sink: list[Diagnostic] = []
    report_unknown_keys({1: "x", "hidden": True}, frozenset(("hidden",)), sink, artifact="command:c", origin=ORIGIN)
    assert [item.context["field"] for item in sink] == [1]
    assert sink[0].message == "command:c: unknown field '1'"


def test_project_relative_is_the_posix_path_under_the_project(tmp_path: Path) -> None:
    assert project_relative(tmp_path, tmp_path / "profiles" / "a" / "profile.yml") == "profiles/a/profile.yml"
    with pytest.raises(ValueError):
        project_relative(tmp_path / "project", tmp_path / "elsewhere.yml")
