"""SST-CFG017: a `{{ tool(...) }}` in a configuration value names no declared tool group and member."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.config import config_tool_references
from tests.helpers.agent_context import docs_catalog


def test_sst_cfg017_fires() -> None:
    tree = {"agents": {"+alias": "{{ tool('platform', 'missing') }}"}}
    [diagnostic] = config_tool_references(tree, docs_catalog(), file="sst_config.yml")
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG017", Severity.ERROR)
    assert diagnostic.message == "{ tool('platform','missing') } does not resolve"
    assert diagnostic.subject == "config:agents.+alias"


def test_sst_cfg017_silent() -> None:
    assert config_tool_references({"agents": {"+alias": "{{ tool('Platform', 'docs') }}"}}, docs_catalog()) == ()
