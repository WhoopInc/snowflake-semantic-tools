"""One precedence chain for every artifact's settings, and the checks on its `config:` block."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.config_schema import (
    KeyKind,
    Setting,
    block_settings,
    resolve_settings,
    setting_value,
    with_defaults,
)
from snowflake_semantic_tools.domain.model.registry import ARTIFACT_REGISTRY
from snowflake_semantic_tools.domain.state.manifest import ArtifactEntry
from snowflake_semantic_tools.domain.validate.config import validate_config
from snowflake_semantic_tools.domain.validate.semantic.authored_keys import view_config_diagnostics
from snowflake_semantic_tools.domain.validate.semantic.nodes import node_root
from snowflake_semantic_tools.domain.validate.settings import item_config_diagnostics
from tests.helpers.authored_documents import document, documents

BLOCK = {
    "+schema": "ROOT",
    "+database": "DB",
    "+budget_seconds": 120,
    "+warehouse": None,
    "finance": {"+schema": "FIN", "+tags": [{"name": "t", "value": "v"}], "close": {"+schema": "CLOSE"}},
    "flat": "not a route",
}
ORIGIN = Origin("agents/a/agent.yml", 2, 3)


def _resolved(folder: tuple[str, ...], config: object = None) -> dict[str, tuple[object, str]]:
    return {key: (item.value, item.source) for key, item in resolve_settings(BLOCK, folder, config).items()}


def test_each_key_resolves_closest_first_and_an_absent_key_inherits() -> None:
    assert _resolved(()) == {"schema": ("ROOT", "root"), "database": ("DB", "root"), "budget_seconds": (120, "root")}
    assert _resolved(("finance", "close", "deeper")) == {
        "schema": ("CLOSE", "route:finance/close"),
        "database": ("DB", "root"),
        "budget_seconds": (120, "root"),
        "tags": ([{"name": "t", "value": "v"}], "route:finance"),
    }
    # The walk stops at the first directory with no route, and at a key that is not one.
    assert _resolved(("other", "finance"))["schema"] == ("ROOT", "root")
    assert _resolved(("flat",))["schema"] == ("ROOT", "root")


def test_an_artifacts_own_value_wins_even_when_it_is_falsy_and_null_reads_as_absent() -> None:
    own = {"budget_seconds": 0, "tags": [], "schema": None, "+database": "IGNORED"}
    resolved = _resolved(("finance",), own)
    assert resolved["budget_seconds"] == (0, "config")
    assert resolved["tags"] == ([], "config")
    assert resolved["schema"] == ("FIN", "route:finance")
    assert resolved["database"] == ("DB", "root")
    assert resolve_settings(None, ("x",), "not a block") == {}


def test_a_resolved_value_is_read_and_the_defaults_fill_only_what_nothing_set() -> None:
    settings = resolve_settings(BLOCK, ())
    assert setting_value(settings, "schema") == "ROOT"
    assert setting_value(settings, "alias") is None
    filled = with_defaults(settings, {"schema": "TARGET", "enabled": True, "warehouse": None})
    assert [(item.key, item.value, item.source) for item in filled] == [
        ("budget_seconds", 120, "root"),
        ("database", "DB", "root"),
        ("enabled", True, "default"),
        ("schema", "ROOT", "root"),
    ]
    assert Setting("schema", "S", "config").as_dict() == {"value": "S", "source": "config"}


def test_a_blocks_settings_are_its_current_plus_keys_without_the_plus() -> None:
    names = tuple(block_settings("semantic_views"))
    assert names == ("database", "schema", "enabled", "tags", "max_staleness")
    assert "meta" not in names
    assert block_settings("nowhere") == {}


def _item(config: object, block: str = "agents") -> list[tuple[str, str]]:
    return [
        (item.code, item.message)
        for item in item_config_diagnostics(block, config, subject="agent:a", origin_of=lambda key: ORIGIN)
    ]


def test_a_config_block_is_checked_key_by_key_with_the_plus_keys_rules() -> None:
    assert _item(None) == []
    assert _item({}) == []
    assert _item(["not", "a", "block"]) == [("SST-CFG004", "config key 'config' expects block, found list")]
    assert _item({"secure": "yes", "tool_not_accessible": "maybe", "budget": 1, "alias": None}) == [
        ("SST-CFG004", "config key 'config.secure' expects boolean, found str"),
        ("SST-CFG008", "config key 'config.tool_not_accessible' value 'maybe' is outside accept, reject, legacy"),
        ("SST-CFG053", "'config.budget' is not a setting of agents"),
        ("SST-CFG054", "'config.alias' is set to null"),
    ]
    assert _item({"max_staleness": 60}, "semantic_views") == [
        ("SST-CFG023", "config.max_staleness is 60; the minimum is 120")
    ]
    assert _item({"max_staleness": 600, "enabled": False, "tags": []}, "semantic_views") == []


def test_a_null_plus_key_is_an_error_wherever_it_is_written() -> None:
    found = validate_config(
        {"semantic_views": {"+enabled": None, "core": {"+schema": None}}, "state": {"+table": None}}
    )
    assert [(item.code, item.subject) for item in found] == [
        ("SST-CFG054", "config:semantic_views.+enabled"),
        ("SST-CFG054", "config:semantic_views.core.+schema"),
        ("SST-CFG054", "config:state.+table"),
    ]


def test_a_views_config_keys_are_placed_where_they_are_written_else_at_the_view() -> None:
    views = node_root("semantic_view")
    tree = {
        views: [
            {"name": "placed", "config": {"bogus": 1, "max_staleness": 60}},
            {"name": "unplaced", "config": "not a block"},
            {"name": "plain", "tables": []},
            "not a node",
            {"config": {"enabled": None}},
        ]
    }
    file = document(
        "views.yml",
        tree,
        ((views, 0), 2),
        ((views, 0, "config", "bogus"), 4),
        ((views, 1, "config"), 9),
    )
    found = view_config_diagnostics(documents(file, document("other.yml", {views: "not a list"})))
    assert [(item.code, item.subject, item.origin) for item in found] == [
        ("SST-CFG053", "semantic_view:placed", Origin("views.yml", 4, 1)),
        ("SST-CFG023", "semantic_view:placed", Origin("views.yml", 2, 1)),
        ("SST-CFG004", "semantic_view:unplaced", Origin("views.yml", 9, 1)),
        ("SST-CFG054", "semantic_view:4", Origin("views.yml", None, None)),
    ]


def test_the_manifest_records_an_artifacts_settings_and_refuses_a_malformed_block() -> None:
    entry = ArtifactEntry("agent", "a", "f" * 64, (), (), (), "DB.S.A", 1, settings=(Setting("schema", "S", "root"),))
    stored = entry.as_dict()
    assert stored["settings"] == {"schema": {"value": "S", "source": "root"}}
    assert ArtifactEntry.from_dict(stored) == entry
    with pytest.raises(ValueError, match="artifact settings"):
        ArtifactEntry.from_dict({**stored, "settings": {"schema": "S"}})
    legacy = {key: value for key, value in stored.items() if key != "settings"}
    assert ArtifactEntry.from_dict(legacy).settings == ()


ROUTED_BLOCKS = tuple(
    artifact.routed_block for artifact in ARTIFACT_REGISTRY.artifacts.values() if artifact.routed_block
)
# One value of each kind a `+` key takes; four distinct ones, so each step of the chain is visible.
_SAMPLES: dict[KeyKind, tuple[object, object, object, object]] = {
    KeyKind.STRING: ("root", "outer", "inner", "own"),
    KeyKind.BOOLEAN: (True, False, True, False),
    KeyKind.INTEGER: (1, 2, 3, 4),
    KeyKind.LIST: (["root"], ["outer"], ["inner"], []),
    KeyKind.ENUM: ("root", "outer", "inner", "own"),
    KeyKind.IDENTIFIER: ("ROOT", "OUTER", "INNER", "OWN"),
    KeyKind.OBJECT_NAME: ("ROOT", "OUTER", "INNER", "OWN"),
    KeyKind.SCHEMA_NAME: ("R.S", "O.S", "I.S", "W.S"),
}


def test_the_chain_is_checked_for_every_routed_type_and_every_kind_of_key() -> None:
    assert ROUTED_BLOCKS == (
        "semantic_views",
        "tools",
        "skills",
        "plugins",
        "coco_profiles",
        "agents",
        "cowork_profiles",
        "evals",
    )
    kinds = {spec.kind for block in ROUTED_BLOCKS for spec in block_settings(block).values()}
    assert kinds <= set(_SAMPLES)
    assert all(block_settings(block) for block in ROUTED_BLOCKS)


@pytest.mark.parametrize(
    ("block", "key"),
    [(block, key) for block in ROUTED_BLOCKS for key in block_settings(block)],
)
def test_every_routed_key_resolves_item_then_closest_route_then_root_then_built_in(block: str, key: str) -> None:
    root, outer, inner, own = _SAMPLES[block_settings(block)[key].kind]
    tree = {f"+{key}": root, "outer": {f"+{key}": outer, "inner": {f"+{key}": inner, "leaf": {}}, "bare": {}}}

    def found(folder: tuple[str, ...], config: object = None) -> tuple[object, str] | None:
        setting = resolve_settings(tree, folder, config).get(key)
        return None if setting is None else (setting.value, setting.source)

    assert found(("outer", "inner", "leaf"), {key: own}) == (own, "config")
    assert found(("outer", "inner", "leaf")) == (inner, "route:outer/inner")
    assert found(("outer", "bare")) == (outer, "route:outer")
    assert found(("elsewhere",)) == (root, "root")
    # Nothing set: the key is absent, and the consumer's built-in default fills it, marked as one.
    assert resolve_settings({}, ("outer",)).get(key) is None
    [filled] = with_defaults(resolve_settings({}, ("outer",)), {key: "built-in"})
    assert (filled.value, filled.source) == ("built-in", "default")
    [kept] = with_defaults(resolve_settings(tree, ()), {key: "built-in"})
    assert (kept.value, kept.source) == (root, "root")
    # Null is never a value: it is SST-CFG054 wherever it is written, and reads as absent.
    assert found(("outer",), {key: None}) == (outer, "route:outer")
    assert [item.code for item in item_config_diagnostics(block, {key: None}, subject="x", origin_of=_at)] == [
        "SST-CFG054"
    ]
    assert [item.code for item in validate_config({block: {f"+{key}": None}})] == ["SST-CFG054"]
    assert [item.code for item in validate_config({block: {"outer": {f"+{key}": None}}})] == ["SST-CFG054"]


def _at(key: str) -> Origin:
    del key
    return ORIGIN
