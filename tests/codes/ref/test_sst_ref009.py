"""SST-REF009: a call resolves, and to an empty string."""

from __future__ import annotations

from pathlib import Path

import pytest

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.resolve.template import METRIC_EXPR
from tests.helpers.cli_projects import validate_json
from tests.helpers.reference_project import AGENTS_SCHEMA, VARS_END, config_edited
from tests.helpers.resolve_builders import resolved

# One configuration key per block whose fallback an empty variable would silently take.
AGENTS = (AGENTS_SCHEMA, AGENTS_SCHEMA.replace("JAFFLE", "\"{{ var('v') }}\""))
VIEWS_DATABASE = (
    'semantic_views:\n  +database: "{{ target.database }}"',
    "semantic_views:\n  +database: \"{{ var('v') }}\"",
)
TOOLS_LAG = ('  +target_lag: "1 hour"', "  +target_lag: \"{{ var('v') }}\"")


def test_sst_ref009_fires() -> None:
    value, (diagnostic,) = resolved("{{ var('blank') }}", METRIC_EXPR, variables={"blank": ""})
    assert (diagnostic.code, diagnostic.severity) == ("SST-REF009", Severity.ERROR)
    assert diagnostic.message == "{{ var('blank') }} resolved to an empty string"
    assert diagnostic.subject == "metric:m"
    assert value.poisoned


def test_sst_ref009_fires_for_a_null_variable() -> None:
    value, (diagnostic,) = resolved("{{ var('blank') }}", METRIC_EXPR, variables={"blank": None})
    assert diagnostic.code == "SST-REF009"
    assert value.poisoned


def _with_var(tmp_path: Path, edit: tuple[str, str], value: str) -> Path:
    return config_edited(tmp_path, edit, (VARS_END, f"{VARS_END}\n  v: {value}"))


@pytest.mark.parametrize(
    ("edit", "key"),
    [(AGENTS, "agents.+schema"), (VIEWS_DATABASE, "semantic_views.+database"), (TOOLS_LAG, "tools.+target_lag")],
)
@pytest.mark.parametrize("value", ['""', ""])
def test_sst_ref009_fires_for_an_empty_configuration_variable(
    tmp_path: Path, edit: tuple[str, str], key: str, value: str
) -> None:
    code, diagnostics = validate_json(_with_var(tmp_path, edit, value))
    [diagnostic] = [item for item in diagnostics if item["code"] == "SST-REF009"]
    assert code == 1
    assert diagnostic["member"] == {"type": "config", "name": key}
    assert diagnostic["location"]["file"] == "sst_config.yml" and diagnostic["location"]["line"]
    # The key is unset for its reader, which reports nothing of its own.
    assert "SST-RND042" not in [item["code"] for item in diagnostics]


def test_sst_ref009_silent(tmp_path: Path) -> None:
    value, diagnostics = resolved("{{ var('blank') }}", METRIC_EXPR, variables={"blank": "x"})
    assert diagnostics == ()
    assert not value.poisoned
    code, found = validate_json(_with_var(tmp_path, AGENTS, "JAFFLE"))
    assert code == 0 and "SST-REF009" not in [item["code"] for item in found]
