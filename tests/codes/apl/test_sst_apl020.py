"""SST-APL020: a skill's version name was minted twice by parallel deploys.

The property check shows why a retry is safe: a version is named for its content, so two
bundles share a name exactly when their files are identical.
"""

from __future__ import annotations

from collections.abc import Sequence

from hypothesis import given, settings
from hypothesis import strategies as st

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, ExecutionError
from snowflake_semantic_tools.domain.sql import Sql
from tests.helpers.publications import compiled_skill, publish_skill
from tests.helpers.recorded_snowflake import RecordedSnowflake


class MintedElsewhere(RecordedSnowflake):
    """Another deploy added the same version name a moment before this one."""

    def execute_script(self, statements: Sequence[Sql]) -> ExecResult:
        if any(" ADD VERSION " in str(statement) for statement in statements):
            self.scripts.append(tuple(str(statement) for statement in statements))
            return ExecResult(False, error=ExecutionError("Version alias already exists.", "42710"))
        return super().execute_script(statements)


def test_sst_apl020_fires() -> None:
    compiled = compiled_skill()
    _, result, _ = publish_skill(MintedElsewhere(existing=()), compiled)
    [diagnostic] = [item for item in result.diagnostics if item.code == "SST-APL020"]
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == f"skill:month-close: version '{compiled.release.alias}' was minted twice"


def test_sst_apl020_silent() -> None:
    _, result, _ = publish_skill(RecordedSnowflake(existing=()), compiled_skill())
    assert "SST-APL020" not in [item.code for item in result.diagnostics]


@settings(max_examples=40, deadline=None)
@given(st.binary(min_size=1, max_size=64), st.binary(min_size=1, max_size=64))
def test_sst_apl020_version_names_collide_only_for_identical_content(first: bytes, second: bytes) -> None:
    same_name = compiled_skill(steps=first).release.alias == compiled_skill(steps=second).release.alias
    assert same_name == (first == second)
