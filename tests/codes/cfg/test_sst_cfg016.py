"""SST-CFG016: a target conditional has no arm for the current target, so the value does not resolve."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.config import has_target_conditional, render_config

PROD_ONLY = {"semantic_views": {"+database": "{{ 'PROD_DB' if target.name == 'prod' }}"}}


def test_sst_cfg016_fires() -> None:
    _, [diagnostic] = render_config(PROD_ONLY, target_name="dev", file="sst_config.yml")
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG016", Severity.ERROR)
    assert diagnostic.message == "+database does not resolve for target 'dev'"
    assert diagnostic.subject == "config:semantic_views.+database"


def test_sst_cfg016_silent() -> None:
    tree, diagnostics = render_config(PROD_ONLY, target_name="prod", file="sst_config.yml")
    assert (tree["semantic_views"]["+database"], diagnostics) == ("PROD_DB", ())
    chained = {
        "agents": {
            "+schema": "{{ 'P' if target.name == 'prod' else 'S' if target.name != 'ci' else 'C' }}",
            "+database": "{{ target.database }}",
        }
    }
    assert has_target_conditional(chained) and not has_target_conditional({"vars": {"x": "{{ 'a' if target.name }}"}})
    assert render_config(chained, target_name="dev", file="f")[0]["agents"]["+schema"] == "S"
    assert render_config(chained, target_name="ci", file="f")[0]["agents"] == {
        "+schema": "C",
        "+database": "{{ target.database }}",
    }
