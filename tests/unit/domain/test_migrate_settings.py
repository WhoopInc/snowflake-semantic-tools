"""Moving an item's old settings under `config:`: placement, comments, nesting, and refusals."""

from __future__ import annotations

from snowflake_semantic_tools.domain.migrate.changes import Changes
from snowflake_semantic_tools.domain.migrate.settings import Move, delete_key, move_settings
from tests.helpers.migrate_outline import applied, lines, outline

AGENT = """name: a
profile:
  color: blue

spec:
  models:
    # The model.
    orchestration: m
  orchestration:
    # The budget.
    budget:
      seconds: 10
  instructions: x

# Alias.
alias: promoted
enabled: true
"""
MOVES = (
    Move(("alias",), "alias"),
    Move(("enabled",), "enabled"),
    Move(("spec", "models", "orchestration"), "orchestration_model"),
    Move(("spec", "orchestration", "budget", "seconds"), "budget_seconds"),
)


def moved(text: str, moves: tuple[Move, ...], *, block: str = "config", item: str | None = None) -> tuple[str, Changes]:
    root = outline(text)
    node = root.get(item) if item else root
    assert node is not None
    changes = Changes()
    move_settings(lines(text), node, moves, changes, block=block)
    return applied(text, changes), changes


def test_nested_settings_go_before_their_holder_and_emptied_parents_go() -> None:
    text, changes = moved(AGENT, MOVES)
    assert text == (
        "name: a\nprofile:\n  color: blue\n\n"
        "config:\n"
        "  # The model.\n  orchestration_model: m\n"
        "  # The budget.\n  budget_seconds: 10\n"
        "  # Alias.\n  alias: promoted\n  enabled: true\n\n"
        "spec:\n  instructions: x\n\n"
    )
    assert sorted(item.after for item in changes.rewrites) == [
        "config.alias",
        "config.budget_seconds",
        "config.enabled",
        "config.orchestration_model",
    ]


def test_a_top_level_setting_opens_the_block_where_it_was() -> None:
    source = "views:\n  - name: v\n    enabled: true\n    tables: []\n    max_staleness: 300\n"
    text, _ = moved(source, (Move(("enabled",), "enabled"), Move(("max_staleness",), "max_staleness")), item=None)
    assert text == source  # the outline root holds no such key
    root = outline(source)
    view = root.get("views").items[0]  # type: ignore[union-attr]
    changes = Changes()
    move_settings(
        lines(source),
        view,
        (Move(("enabled",), "enabled"), Move(("max_staleness",), "max_staleness")),
        changes,
        block="config",
    )
    assert applied(source, changes) == (
        "views:\n  - name: v\n    config:\n      enabled: true\n      max_staleness: 300\n    tables: []\n"
    )


def test_settings_join_an_existing_block_and_unset_ones_go() -> None:
    source = "warehouse: null\ntarget_lag: 1 day\nconfig:\n    schema: S\nother: 1\n"
    text, changes = moved(source, (Move(("warehouse",), "warehouse"), Move(("target_lag",), "target_lag")))
    assert text == "config:\n    schema: S\n    target_lag: 1 day\nother: 1\n"
    assert {(item.before, item.kind) for item in changes.rewrites} == {
        ("warehouse", "removed"),
        ("target_lag", "setting"),
    }


def test_only_unset_settings_write_no_block() -> None:
    text, _ = moved("a: 1\n\nwarehouse:\n\nb: 2\n", (Move(("warehouse",), "warehouse"),))
    assert text == "a: 1\n\nb: 2\n"


def test_a_dropped_move_and_a_rewritten_value() -> None:
    source = "run:\n  retention:\n    audit: [ci]\n  variant: ci\n  tier: x\n"
    text, changes = moved(
        source, (Move(("run", "retention"), "retention", "audit"), Move(("run", "tier"), "eval_tier", drop=True))
    )
    assert text == "config:\n  retention: audit\nrun:\n  variant: ci\n"
    assert {item.kind for item in changes.rewrites} == {"setting", "removed"}


def test_refusals_leave_the_setting_and_report_it() -> None:
    _, flow = moved("config: {a: 1}\nenabled: true\n", (Move(("enabled",), "enabled"),))
    assert [item.reason for item in flow.untouched] == ["config: is not a block mapping"]
    _, taken = moved("enabled: true\nconfig:\n  enabled: false\n", (Move(("enabled",), "enabled"),))
    assert taken.untouched[0].reason == "config.enabled is already set" and not taken.edits
    _, inline = moved("spec: {models: {orchestration: m}}\n", (Move(("spec", "models", "orchestration"), "x"),))
    assert inline.untouched[0].reason.startswith("the setting is not a plain block-style key")
    _, quoted = moved("'enabled': true\n", (Move(("enabled",), "enabled"),))
    assert quoted.untouched
    source = "items:\n  - enabled: true\n    name: x\n"
    root = outline(source)
    changes = Changes()
    move_settings(lines(source), root.get("items").items[0], (Move(("enabled",), "enabled"),), changes, block="config")  # type: ignore[union-attr]
    assert changes.untouched and not changes.edits
    _, missing = moved("a: 1\n", (Move(("enabled",), "enabled"), Move(("a", "b"), "b")))
    assert not missing.edits and not missing.untouched


def test_a_setting_under_a_key_on_the_dash_line_is_left_alone() -> None:
    source = "items:\n  - spec:\n      models:\n        orchestration: m\n    name: x\nnext: 1\n"
    items = outline(source).get("items")
    assert items is not None
    changes = Changes()
    move_settings(
        lines(source), items.items[0], (Move(("spec", "models", "orchestration"), "o"),), changes, block="config"
    )
    assert changes.untouched  # the holder is on the dash line, so it cannot be told apart


def test_the_indent_unit_follows_the_item() -> None:
    source = "a:\n    b: 1\nlist:\n    - x\nenabled: true\n"
    text, _ = moved(source, (Move(("enabled",), "enabled"),))
    assert text.endswith("config:\n    enabled: true\n")
    text, _ = moved("flow: {a: 1}\nseq:\n- x\nenabled: true\n", (Move(("enabled",), "enabled"),))
    assert text.endswith("config:\n  enabled: true\n")
    text, _ = moved("enabled: true\n", (Move(("enabled",), "enabled"),))
    assert text == "config:\n  enabled: true\n"


def test_a_parent_emptied_with_nothing_above_it_carries_no_comment() -> None:
    source = "spec:\n  models:\n    orchestration: m\n  orchestration:\n    tool_not_accessible: reject\nx: 1\n"
    text, _ = moved(
        source,
        (
            Move(("spec", "models", "orchestration"), "orchestration_model"),
            Move(("spec", "orchestration", "tool_not_accessible"), "tool_not_accessible"),
        ),
    )
    assert text == "config:\n  orchestration_model: m\n  tool_not_accessible: reject\nx: 1\n"


def test_delete_key_takes_its_comments_and_a_spare_blank_line() -> None:
    source = "a: 1\n\n# Says whose.\nagent: x\n\nb: 2\n"
    root = outline(source)
    changes = Changes()
    entry = root.entry("agent")
    assert entry is not None
    delete_key(lines(source), entry, changes)
    assert applied(source, changes) == "a: 1\n\nb: 2\n"
    first = Changes()
    top = outline("agent: x\n\nb: 2\n").entry("agent")
    assert top is not None
    delete_key(lines("agent: x\n\nb: 2\n"), top, first)
    assert applied("agent: x\n\nb: 2\n", first) == "b: 2\n"
    last = Changes()
    tail = outline("b: 2\nagent: x\n").entry("agent")
    assert tail is not None
    delete_key(lines("b: 2\nagent: x\n"), tail, last)
    assert applied("b: 2\nagent: x\n", last) == "b: 2\n"
