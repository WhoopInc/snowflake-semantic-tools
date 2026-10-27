"""SST-CFG058: a target conditional is not one the configuration's grammar covers."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.config import render_config

# The first arm matches `prod`; the second is outside the grammar, and still reported.
UNREADABLE = {"state": {"+database": "{{ 'SST' if target.name == 'prod' else 'X' if target.name.startswith('ci') }}"}}


def test_sst_cfg058_fires() -> None:
    for target in ("prod", "dev"):
        tree, [diagnostic] = render_config(UNREADABLE, target_name=target, file="sst_config.yml")
        assert (diagnostic.code, diagnostic.severity) == ("SST-CFG058", Severity.ERROR)
        assert diagnostic.message == (
            "'+database' is a target conditional SST cannot read, from `'X' if target.name.startswith('ci')`"
        )
        assert diagnostic.subject == "config:state.+database"
        # Removed, so its reader sees the key unset rather than the target's own location.
        assert tree == {"state": {}}


def test_sst_cfg058_silent() -> None:
    tree = {
        "state": {"+database": "{{ 'SST' if target.name in ('prod', 'ci_prod') else '' }}"},
        "agents": {"+schema": "{{ 'DEV' if target.name not in ['prod'] else 'PROD' }}"},
    }
    resolved, diagnostics = render_config(tree, target_name="ci_prod", file="sst_config.yml")
    assert (resolved, diagnostics) == ({"state": {"+database": "SST"}, "agents": {"+schema": "DEV"}}, ())
