"""SST-CFG060: a target conditional is part of a longer value, so it would never resolve."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.config import render_config

DB = "{{ 'SST' if target.name == 'prod' else 'SST_DEV' }}"


def test_sst_cfg060_fires() -> None:
    for value, tree_vars in (
        ("{{ var('db') }}.STATE.AGENT_SPECS", {"db": DB}),
        ("X_{{ 'A' if target.name == 'prod' else 'B' }}", {}),
        ("{{ 'A' if target.name == 'prod' else 'B' }} and {{ 'C' if target.name == 'prod' else 'D' }}", {}),
    ):
        tree = {"vars": tree_vars, "apply": {"agent_spec_stage": value}}
        for target in ("prod", None):
            resolved, [diagnostic] = render_config(tree, target_name=target, file="sst_config.yml")
            assert (diagnostic.code, diagnostic.severity) == ("SST-CFG060", Severity.ERROR)
            assert diagnostic.message == (
                "'agent_spec_stage' has a target conditional inside a longer value, which never resolves"
            )
            assert diagnostic.subject == "config:apply.agent_spec_stage"
            assert resolved["apply"] == {}


def test_sst_cfg060_silent() -> None:
    tree = {
        "vars": {"stage": "{{ 'SST.STATE.AGENT_SPECS' if target.name == 'prod' else 'AGENT_SPECS' }}"},
        "apply": {"agent_spec_stage": "{{ var('stage') }}"},
        "state": {"+database": "{{ target.database }}", "+table": "STATE_{{ target.name }}"},
    }
    resolved, diagnostics = render_config(tree, target_name="prod", file="f")
    assert diagnostics == ()
    assert resolved["apply"] == {"agent_spec_stage": "SST.STATE.AGENT_SPECS"}
    assert resolved["state"] == {"+database": "{{ target.database }}", "+table": "STATE_{{ target.name }}"}
