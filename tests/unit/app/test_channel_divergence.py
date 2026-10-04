"""When the live channel comparison has nothing to compare, and what it compares when it does."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.lifecycle.channels import _skill_pointers, channel_divergence
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.publications import (
    both_channels_published,
)
from tests.helpers.snowflake_fake import FakeSnowflake


def test_a_profile_with_no_registry_row_or_a_skill_with_no_extension_compares_nothing() -> None:
    port, result = both_channels_published()
    profile = result.compiled[1]
    assert channel_divergence(FakeSnowflake(existing=()), result) == ()
    # A skill the profile ships that this run compiled no extension for is not compared.
    assert channel_divergence(port, CompileResult((profile,), DiagnosticBag())) == ()
    port.extensions.clear()
    assert channel_divergence(port, result) == ()


def test_an_extension_without_a_default_version_or_an_unreadable_channel_compares_nothing() -> None:
    port, result = both_channels_published()
    for extension in port.extensions.values():
        for version in extension["versions"]:  # type: ignore[attr-defined]
            version["is_default"] = False
    assert channel_divergence(port, result) == ()

    class Unreadable(FakeSnowflake):
        def list_location(self, location: str) -> tuple[str, ...]:
            raise SnowflakePortError("LIST refused")

    unreadable = Unreadable(existing=())
    unreadable.__dict__.update(port.__dict__)
    assert channel_divergence(unreadable, result) == ()


def test_only_stage_pointers_in_skill_repos_are_followed_each_as_a_folder() -> None:
    value = [{"snowflake_stage": "@DB.S.P/skills/a/H"}, {"snowflake_stage": "@DB.S.P/skills/b/H/"}, {"x": 1}, "s"]
    assert list(_skill_pointers(value)) == ["@DB.S.P/skills/a/H/", "@DB.S.P/skills/b/H/"]
    assert list(_skill_pointers({"snowflake_stage": "@DB.S.P/"})) == []


def test_a_file_at_a_tree_root_belongs_to_no_skill() -> None:
    port, result = both_channels_published()
    [stage_skill] = [path for path in port.stage_files if path.endswith("/month-close/SKILL.md") and "PROFILES" in path]
    port.stage_files.add(stage_skill.removesuffix("month-close/SKILL.md") + "README")
    assert channel_divergence(port, result) == ()
