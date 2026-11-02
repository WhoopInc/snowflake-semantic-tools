"""Every key of every authored file and of `sst_config.yml` is presented, and nothing else is.

An editor reads the presentation tables to name, group and lay out keys, so a key without an
entry would fall back to its spelling, and an entry for no key would hide a typo. These tests walk
every kind, variant, nested node and configuration key, and allow no exception.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from snowflake_semantic_tools.domain.model.authored_schema import FILE_KINDS, Field, FieldStatus, Node
from snowflake_semantic_tools.domain.model.config_schema import CONFIG_SCHEMA, KeyStatus
from snowflake_semantic_tools.domain.model.presentation import (
    CONFIG_SECTIONS,
    CONFIG_UI,
    FIELD_UI,
    KIND_UI,
    LIST_UI,
    Ui,
)


def _fields(node: Node, path: str) -> Iterator[tuple[str, Field]]:
    """Every field at and below `node`, read or not, by its place."""
    for item in node.all_fields():
        place = f"{path}.{item.name}"
        yield place, item
        if item.nested is not None:
            yield from _fields(item.nested, place)


_AUTHORED = {place: item for name, kind in FILE_KINDS.items() for place, item in _fields(kind.node, name)}


def _node_at(place: str) -> Node | None:
    """The node a list or mapping at `place` holds, or None."""
    item = _AUTHORED.get(place)
    return item.nested if item is not None else None


def _resolves(node: Node, dotted: str) -> bool:
    """Whether `dotted` names a key of `node`, through nested mappings, or a free-form mapping's key."""
    head, _, rest = dotted.partition(".")
    found = next((item for item in node.all_fields() if item.name == head), None)
    if found is None:
        return False
    if not rest:
        return True
    return found.nested is None or _resolves(found.nested, rest)


def test_every_authored_key_has_an_entry_and_every_entry_a_key() -> None:
    assert sorted(set(_AUTHORED) - set(FIELD_UI)) == []
    assert sorted(set(FIELD_UI) - set(_AUTHORED)) == []


def test_every_list_of_mappings_says_how_its_items_are_named_and_laid_out() -> None:
    lists = {place for place, item in _AUTHORED.items() if isinstance(item.items, Node) and item.read}
    assert sorted(lists - set(LIST_UI)) == []
    assert sorted(set(LIST_UI) - lists) == []
    for place, ui in LIST_UI.items():
        node = _node_at(place)
        assert node is not None
        named = (ui.title, *((ui.subtitle,) if ui.subtitle else ()), *ui.columns)
        assert [key for key in named if not _resolves(node, key)] == [], place


def test_every_kind_is_presented_with_sections_its_keys_fill() -> None:
    assert sorted(FILE_KINDS) == sorted(KIND_UI)
    for name, kind in FILE_KINDS.items():
        ui = KIND_UI[name]
        ids = [group.id for group in ui.groups]
        assert len(ids) == len(set(ids)), name
        shown = [
            place
            for place, item in _AUTHORED.items()
            if item.status is FieldStatus.CURRENT and _section_level(name, place, ui.flatten)
        ]
        assert [place for place in shown if FIELD_UI[place].group not in ids] == [], name
        used = {FIELD_UI[place].group for place in shown}
        assert [group for group in ids if group not in used] == [], name
        for key in (ui.description, ui.owner, *ui.key_settings):
            assert key is None or _resolves(kind.node, key), (name, key)
        assert all(_resolves(kind.node, key) for key in ui.flatten), name


def _section_level(kind: str, place: str, flatten: tuple[str, ...]) -> bool:
    """Whether `place` is shown at its kind's top level: its own key, or a flattened mapping's."""
    relative = place.removeprefix(f"{kind}.")
    if relative == place or relative in flatten:
        return False
    parent = relative.rpartition(".")[0]
    return parent == "" or parent in flatten


def test_a_key_below_the_top_level_names_no_section() -> None:
    nested = [
        place
        for place in FIELD_UI
        if FIELD_UI[place].group is not None
        and not _section_level(place.split(".")[0], place, KIND_UI[place.split(".")[0]].flatten)
    ]
    assert nested == []


def test_every_config_key_has_an_entry_and_every_top_level_key_a_section() -> None:
    paths = {key.path for key in CONFIG_SCHEMA}
    assert sorted(paths - set(CONFIG_UI)) == []
    assert sorted(set(CONFIG_UI) - paths) == []
    sections = {section.id for section in CONFIG_SECTIONS}
    top = [key for key in CONFIG_SCHEMA if "." not in key.path and key.status is KeyStatus.CURRENT]
    assert [key.path for key in top if CONFIG_UI[key.path].group not in sections] == []
    used = {CONFIG_UI[key.path].group for key in CONFIG_SCHEMA if "." not in key.path}
    assert sorted(sections - used) == []
    assert [path for path in CONFIG_UI if "." in path and CONFIG_UI[path].group is not None] == []


@pytest.mark.parametrize("table", [FIELD_UI, CONFIG_UI], ids=["authored", "config"])
def test_no_label_is_a_key_spelled_out(table: dict[str, Ui]) -> None:
    """A label is what a person calls the key: never its snake_case or `+` spelling."""
    spelled = [place for place, ui in table.items() if "_" in ui.label or ui.label.startswith("+") or not ui.label]
    assert spelled == []
