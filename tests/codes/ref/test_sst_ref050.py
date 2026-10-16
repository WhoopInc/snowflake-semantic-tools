"""SST-REF050: a field read only as a template call holds a bare value."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.agents import load_agents
from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.resolve.tool_column import ColumnSite, tool_column

SITE = ColumnSite("docs_search", "search_column", "tool:docs_search", "tool:docs_search", Origin("tools/t.yml", 4, 3))


def test_sst_ref050_fires() -> None:
    column, diagnostic = tool_column("body", SITE, referenced=False, on_model="docs")
    assert column is None and diagnostic is not None
    assert (diagnostic.code, diagnostic.severity, diagnostic.subject) == ("SST-REF050", Severity.ERROR, SITE.subject)
    assert diagnostic.message == "tool:docs_search: search_column is 'body'; it takes a {{ ref(...) }} call"


def test_sst_ref050_silent() -> None:
    assert tool_column("{{ ref('docs', 'body') }}", SITE, referenced=False, on_model="docs") == ("body", None)
    # A referenced member's columns are the object's own, written bare.
    assert tool_column("body", SITE, referenced=True, on_model=None) == ("body", None)


def test_sst_ref050_names_the_skill_call_a_plugin_entry_takes(tmp_path: Path) -> None:
    # A plugin entry's member is named by a skill() call; a bare member name says so.
    agent = tmp_path / "agents/zz_pmo/agent.yml"
    agent.parent.mkdir(parents=True)
    agent.write_text(
        "name: zz_pmo\nspec:\n  skills:\n    - name: jaffle-analytics\n"
        "      source: {type: CORTEX_EXTENSION, path: \"{{ plugin('jaffle-toolkit') }}\"}\n",
        encoding="utf-8",
    )
    [diagnostic] = [item for item in load_agents(tmp_path)[1] if item.code == "SST-REF050"]
    assert diagnostic.message == (
        "agent:zz_pmo: spec.skills[0].name is 'jaffle-analytics'; it takes a {{ skill(...) }} call"
    )
    assert diagnostic.suggestion == "write the value as a skill() call; a bare value is not read"
