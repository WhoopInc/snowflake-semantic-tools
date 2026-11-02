"""A session reuses a view only when every argument it is built from is identical to last time.

`identical` is stricter than `==` where `==` would let a different value pass for the same one,
`ViewBuilds` forgets what a load does not build, and `LoadCache` keeps one parse of each file.
"""

from __future__ import annotations

import dataclasses
import inspect
from datetime import date
from pathlib import Path
from types import MappingProxyType

import pytest

from snowflake_semantic_tools.adapters.yaml.documents import LoadCache, ParsedYaml
from snowflake_semantic_tools.adapters.yaml.semantic.build import _build_view
from snowflake_semantic_tools.adapters.yaml.semantic.reuse import ViewBuilds, identical
from snowflake_semantic_tools.domain.model.semantic_view import SemanticView


@dataclasses.dataclass(frozen=True)
class _Holder:
    shown: int
    hidden: object = dataclasses.field(default=None, compare=False)


@pytest.mark.parametrize(
    ("left", "right"),
    [
        (1, True),
        (1, 1.0),
        (0.0, -0.0),
        ("1", 1),
        ((1, 2), [1, 2]),
        ({"a": 1, "b": 2}, {"b": 2, "a": 1}),
        (frozenset({1}), frozenset({True})),
        (_Holder(1, "x"), _Holder(1, "y")),
        (date(2026, 1, 1), "2026-01-01"),
        (object(), object()),
    ],
    ids=[
        "int-is-not-bool",
        "int-is-not-float",
        "zero-keeps-its-sign",
        "str-is-not-int",
        "tuple-is-not-list",
        "mapping-order-counts",
        "set-members-keep-their-type",
        "a-field-left-out-of-equality-still-counts",
        "date-is-not-its-text",
        "an-unknown-type-is-only-itself",
    ],
)
def test_values_equal_under_eq_but_different_are_not_identical(left: object, right: object) -> None:
    assert not identical(left, right)


@pytest.mark.parametrize(
    "value",
    [
        {"name": "orders", "tables": [{"name": "o", "dims": (1, 2.5, None, True)}]},
        MappingProxyType({"a": frozenset({"x", "y"})}),
        _Holder(3, hidden=[date(2026, 1, 1)]),
        float("nan"),
        Path("models/orders.yml"),
    ],
)
def test_a_copy_of_a_value_is_identical_to_it(value: object) -> None:
    copied = _copy(value)

    assert copied is not value or isinstance(value, float)
    assert identical(value, copied)


def _copy(value: object) -> object:
    """A copy of `value` sharing no container with it, so only a field-by-field comparison matches."""
    if isinstance(value, MappingProxyType):
        return MappingProxyType({key: _copy(item) for key, item in value.items()})
    if isinstance(value, dict):
        return {key: _copy(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value)(_copy(item) for item in value)
    if isinstance(value, frozenset):
        return frozenset(set(value))
    if isinstance(value, _Holder):
        return _Holder(value.shown, _copy(value.hidden))
    if isinstance(value, Path):
        return Path(str(value))
    return value


def _view(name: str) -> SemanticView:
    return SemanticView(fqn=f"DB.S.{name}", tables=())


def test_a_view_built_from_identical_arguments_is_reused_and_one_built_from_others_is_not() -> None:
    builds = ViewBuilds()
    built = _view("ORDERS")
    builds.begin(("shared",))
    assert builds.get("semantic_view:orders", ({"name": "orders"},)) is None
    builds.put("semantic_view:orders", ({"name": "orders"},), built)
    builds.end()

    builds.begin(("shared",))
    assert builds.get("semantic_view:orders", ({"name": "orders"},)) is built
    assert builds.get("semantic_view:orders", ({"name": "orders", "description": "x"},)) is None


def test_a_change_to_what_every_view_reads_forgets_every_view() -> None:
    builds = ViewBuilds()
    builds.begin(({"vars": {"a": 1}},))
    builds.put("semantic_view:orders", (), _view("ORDERS"))
    builds.end()

    builds.begin(({"vars": {"a": True}},))

    assert builds.get("semantic_view:orders", ()) is None


def test_a_view_the_load_did_not_reach_is_forgotten() -> None:
    builds = ViewBuilds()
    builds.begin(())
    builds.put("semantic_view:orders", (), _view("ORDERS"))
    builds.put("semantic_view:menu", (), _view("MENU"))
    builds.end()

    builds.begin(())
    assert builds.get("semantic_view:orders", ()) is not None
    builds.end()

    builds.begin(())
    assert builds.get("semantic_view:menu", ()) is None


def test_the_load_cache_keeps_one_parse_of_each_file(tmp_path: Path) -> None:
    cache = LoadCache()
    path = tmp_path / "metric.yml"
    first, second = ParsedYaml({"a": 1}, {}, {}), ParsedYaml({"a": 2}, {}, {})

    cache.put(path, "one", first)
    cache.put(path, "two", second)

    assert cache.get(path, "two") is second
    assert cache.get(path, "one") is None
    assert len(cache._parsed) == 1


def test_every_argument_a_view_is_built_from_is_part_of_what_reuse_compares() -> None:
    """A new `_build_view` argument must join the reuse key in `_build_views`, or a reload reuses
    a view built without it. Update both, and this list, together."""
    compared = {
        # Shared by every view: `ViewBuilds.begin`.
        "project_dir",
        "models",
        "config",
        "sources",
        # Each view's own: the arguments `ViewBuilds.get` compares; `members` and `attachment`
        # as the members attached to the view, which is all `_build_view` reads of them.
        "node",
        "path",
        "target",
        "members",
        "attachment",
        "settings",
        "variable_origins",
        # The view file's folder below the semantic-model directory: `path` below what `config` names.
        "folder",
    }

    assert set(inspect.signature(_build_view).parameters) == compared
