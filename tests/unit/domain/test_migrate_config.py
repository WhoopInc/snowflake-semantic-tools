"""`sst migrate split` for `sst_config.yml`: the blocks that left `skills:`, unset keys, tool routes."""

from __future__ import annotations

from snowflake_semantic_tools.domain.migrate.config_file import migrate_config
from tests.helpers.migrate_outline import applied, lines, outline

CONFIG = """agents:
  +warehouse: null
  ops:
    +schema: OPS
    +query_timeout:
skills:
  +certified: false
  +version_prefix: "SST_"
  # Consumed extensions.
  extensions:
    default_prefix: DB.PARTNER
  # The catalog.
  catalog:
    +database: DB
    +schema: S
    # Must be true.
    +flatten: true
    +bundle_stage: SRC
    +prune_deleted: true
  stage:
    +database: DB
    +stage: BUNDLES
    +registry_table: REG
    +layout: by_type
    +auto_compress: false
apply:
  fail_fast: false
"""


def test_skills_blocks_move_out_and_unset_keys_go() -> None:
    changes = migrate_config(lines(CONFIG), outline(CONFIG), (), has_plugins=True)
    assert applied(CONFIG, changes) == (
        "agents:\n  ops:\n    +schema: OPS\n"
        'skills:\n  +certified: false\n  +version_prefix: "SST_"\n'
        "  # The catalog.\n  +database: DB\n  +schema: S\n  +bundle_stage: SRC\n"
        "\n# The skills catalog published plugins too; they keep its location.\n"
        "plugins:\n  +database: DB\n  +schema: S\n  +bundle_stage: SRC\n"
        '  +certified: false\n  +version_prefix: "SST_"\n'
        "\nprofiles:\n  +database: DB\n  +stage: BUNDLES\n  +registry_table: REG\n"
        "\n# Consumed extensions.\nextensions:\n  default_prefix: DB.PARTNER\n"
        "apply:\n  fail_fast: false\n"
    )
    assert not changes.untouched


def test_a_skills_block_left_empty_is_replaced_by_the_blocks_it_held() -> None:
    text = "# Skills.\nskills:\n  stage:\n    +stage: B\nnext: 1\n"
    assert (
        applied(text, migrate_config(lines(text), outline(text), (), has_plugins=False))
        == "profiles:\n  +stage: B\nnext: 1\n"
    )


def test_moves_that_would_collide_are_reported() -> None:
    text = (
        "skills:\n  +database: X\n  catalog:\n    +database: DB\n  stage:\n    +stage: B\n"
        "  extensions: {a: 1}\nprofiles:\n  +stage: C\n"
    )
    changes = migrate_config(lines(text), outline(text), (), has_plugins=True)
    assert sorted(item.text for item in changes.untouched) == ["skills.catalog", "skills.extensions", "skills.stage"]
    assert not changes.edits
    flow = "skills:\n  catalog: {+database: DB}\n"
    assert migrate_config(lines(flow), outline(flow), (), has_plugins=False).untouched[0].text == "skills.catalog"
    assert migrate_config(lines("skills: {a: 1}\n"), outline("skills: {a: 1}\n"), (), has_plugins=False).edits == []
    plugins = "skills:\n  catalog:\n    +bundle_stage: S\nplugins:\n  +bundle_stage: P\n"
    assert "published plugins too" not in applied(
        plugins, migrate_config(lines(plugins), outline(plugins), (), has_plugins=True)
    )


def test_routes_join_the_tools_block() -> None:
    def routed(text: str, *routes: tuple[str, ...]) -> str:
        return applied(text, migrate_config(lines(text), outline(text), routes, has_plugins=False))

    assert routed("a: 1\n", ("p",)) == "a: 1\n\ntools:\n  p:\n    +immutable: true\n"
    text = "tools:\n  +schema: S\n  p:\n    +schema: P\n  q:\n    +immutable: true\nx: 1\n"
    assert routed(text, ("p",), ("q",), ("r", "s")) == (
        "tools:\n  +schema: S\n  p:\n    +schema: P\n    +immutable: true\n  q:\n    +immutable: true\n"
        "  r:\n    s:\n      +immutable: true\nx: 1\n"
    )
    changes = migrate_config(lines("tools: {}\n"), outline("tools: {}\n"), (("p",),), has_plugins=False)
    assert changes.untouched[0].text == "tools"
    changes = migrate_config(lines("tools:\n  p:\n"), outline("tools:\n  p:\n"), (("p",),), has_plugins=False)
    assert changes.untouched[0].text == "tools.p"
