"""SST-CFG029: a `var()` call, in a member or a configuration value, names no variable `vars:` declares."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.config import render_config
from snowflake_semantic_tools.domain.resolve.template import METRIC_EXPR
from tests.helpers.resolve_builders import resolved


def test_sst_cfg029_fires() -> None:
    value, (diagnostic,) = resolved("{{ var('missing') }}", METRIC_EXPR)
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG029", Severity.ERROR)
    assert diagnostic.message == "{ var('missing') } is not declared in config"
    assert diagnostic.subject == "metric:m"
    assert value.poisoned


def test_sst_cfg029_fires_in_a_configuration_value() -> None:
    tree, [diagnostic] = render_config(
        {"tags": {"default_prefix": "{{ var('gov') }}.TAGS"}}, target_name=None, file="sst_config.yml"
    )
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG029", Severity.ERROR)
    assert diagnostic.message == "{ var('gov') } is not declared in config"
    assert diagnostic.subject == "config:tags.default_prefix"
    assert tree["tags"]["default_prefix"] == "{{ var('gov') }}.TAGS"


def test_sst_cfg029_silent() -> None:
    value, diagnostics = resolved("{{ var('state') }}", METRIC_EXPR, variables={"state": "completed"})
    assert diagnostics == ()
    assert not value.poisoned
    tree, found = render_config(
        {"vars": {"gov": "GOVERNANCE"}, "tags": {"default_prefix": "{{ var('gov') }}.TAGS"}, "list": ["a", 1]},
        target_name=None,
        file="sst_config.yml",
    )
    assert (tree["tags"]["default_prefix"], tree["vars"], found) == ("GOVERNANCE.TAGS", {"gov": "GOVERNANCE"}, ())
