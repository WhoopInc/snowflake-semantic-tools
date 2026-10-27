"""SST-CFG059: a variable is a list where text is needed, or not a list of targets where one is."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.config import render_config

VARS = {"production_targets": ["prod", "ci_prod"], "suffix": "_PROD"}


def test_sst_cfg059_fires() -> None:
    tree = {"vars": VARS, "state": {"+database": "{{ 'SST' if target.name in var('suffix') else '' }}"}}
    resolved, [diagnostic] = render_config(tree, target_name="prod", file="sst_config.yml")
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG059", Severity.ERROR)
    assert diagnostic.message == "var('suffix') holds '_PROD'"
    assert diagnostic.subject == "config:state.+database"
    assert resolved["state"] == {}
    # A list used as text, which would substitute its Python spelling.
    _, [as_text] = render_config({"vars": VARS, "a": "{{ var('production_targets') }}"}, target_name=None, file="f")
    assert (as_text.code, as_text.message) == ("SST-CFG059", "var('production_targets') holds a list")
    for held, found in (([], "an empty list"), (["prod", 3], "a list with an entry that is not a target name")):
        tree = {"vars": {"t": held}, "a": "{{ 'A' if target.name in var('t') else 'B' }}"}
        _, [shaped] = render_config(tree, target_name="prod", file="f")
        assert shaped.message == f"var('t') holds {found}"
    _, [mapping] = render_config(
        {"vars": {"t": {"a": "b"}}, "a": "{{ 'A' if target.name in var('t') }}"}, target_name="prod", file="f"
    )
    assert mapping.message == "var('t') holds a mapping"


def test_sst_cfg059_silent() -> None:
    tree = {
        "vars": VARS,
        "state": {"+database": "{{ 'SST' if target.name in var('production_targets') else '' }}"},
        "agents": {"+schema": "{{ 'DEV' if target.name not in var( 'production_targets' ) else 'AGENTS' }}"},
        "x": "DB{{ var('suffix') }}",
    }
    for target, database, schema in (("ci_prod", "SST", "AGENTS"), ("dev", "", "DEV")):
        resolved, diagnostics = render_config(tree, target_name=target, file="f")
        assert diagnostics == ()
        assert (resolved["state"]["+database"], resolved["agents"]["+schema"], resolved["x"]) == (
            database,
            schema,
            "DB_PROD",
        )
