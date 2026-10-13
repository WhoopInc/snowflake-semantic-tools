"""SST-REF049: a plugin or profile member is not one call of its key's reference function."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.profiles import load_profile_catalog
from snowflake_semantic_tools.adapters.yaml.skills import load_skill_catalog
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.diagnostic_filters import coded
from tests.helpers.file_trees import write_tree

PLUGIN = "name: kit\ndescription: Kit.\nowner_team: Data\nskills: [{members}]\n"
PROFILE = "name: analyst\ndescription: Analyst.\nowner_team: Data\nhooks: [{members}]\n"


def _plugin(tmp_path: Path, members: str) -> tuple[tuple[str, ...], list[Diagnostic]]:
    """Load a plugin listing `members` under `skills:`, as its members and its REF049 findings."""
    write_tree(tmp_path, {"plugins/kit/plugin.yml": PLUGIN.format(members=members)})
    catalog = load_skill_catalog(tmp_path, skills_dir="skills", plugins_dir="plugins")
    return catalog.plugins[0].members, coded(catalog.diagnostics, "SST-REF049")


def test_sst_ref049_fires(tmp_path: Path) -> None:
    members, [diagnostic] = _plugin(tmp_path, "month-close")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == ("plugin:kit: skills[0] is 'month-close'; it takes one {{ skill('<name>') }} call")
    assert diagnostic.subject == "plugin:kit"
    assert members == ()


def test_sst_ref049_fires_for_another_keys_function(tmp_path: Path) -> None:
    members, [diagnostic] = _plugin(tmp_path, "\"{{ skill('a') }}\", \"{{ plugin('b') }}\"")
    assert diagnostic.context["field"] == "skills[1]"
    assert members == ("a",)


def test_sst_ref049_fires_for_a_profile_member(tmp_path: Path) -> None:
    write_tree(tmp_path, {"profiles/analyst/profile.yml": PROFILE.format(members="guard")})
    catalog = load_profile_catalog(tmp_path, profiles_dir="profiles", hooks_dir="hooks", mcp_servers_dir="mcp-servers")
    [diagnostic] = coded(catalog.diagnostics, "SST-REF049")
    assert diagnostic.message.endswith("hooks[0] is 'guard'; it takes one {{ hook('<name>') }} call")


def test_sst_ref049_silent(tmp_path: Path) -> None:
    members, found = _plugin(tmp_path, "\"{{ skill('month-close') }}\"")
    assert (members, found) == (("month-close",), [])
