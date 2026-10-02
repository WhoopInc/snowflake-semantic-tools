"""SST-CFG029: a `var()` in a configuration value names a variable `vars:` does not declare."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.config import render_config


def test_sst_cfg029_fires() -> None:
    tree, [diagnostic] = render_config(
        {"tags": {"default_prefix": "{{ var('gov') }}.TAGS"}}, target_name=None, file="sst_config.yml"
    )
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG029", Severity.ERROR)
    assert diagnostic.message == "{ var('gov') } is not declared in config"
    assert diagnostic.subject == "config:tags.default_prefix"
    assert tree["tags"]["default_prefix"] == "{{ var('gov') }}.TAGS"


def test_sst_cfg029_silent() -> None:
    tree, diagnostics = render_config(
        {"vars": {"gov": "GOVERNANCE"}, "tags": {"default_prefix": "{{ var('gov') }}.TAGS"}, "list": ["a", 1]},
        target_name=None,
        file="sst_config.yml",
    )
    assert (tree["tags"]["default_prefix"], tree["vars"], diagnostics) == ("GOVERNANCE.TAGS", {"gov": "GOVERNANCE"}, ())
