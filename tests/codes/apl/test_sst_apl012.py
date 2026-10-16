"""SST-APL012: the object changed between plan and apply."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.tools import CompiledTool, CompileTools, ToolPlacement
from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action, Change, ChangeReason, ExecResult, ExecutionError
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolMember, ToolOwnership
from tests.helpers.apply_runs import apply_plan
from tests.helpers.artifact_builders import change, changeset, rendered
from tests.helpers.compile_builders import compiled_as
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.snowflake_fake import FakeSnowflake


def test_sst_apl012_fires() -> None:
    artifact = rendered()
    port = FakeSnowflake()
    port.existing = {artifact.target.sql, *(relation.sql for relation in artifact.required_relations)}
    diagnostic = only(apply_plan(changeset(change(artifact)), port).diagnostics, "SST-APL012")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "semantic_view:v: semantic_view:v changed since the plan"
    assert port.scripts == []


def test_sst_apl012_silent() -> None:
    assert "SST-APL012" not in codes(apply_plan(changeset(change(rendered()))).diagnostics)


def test_sst_apl012_silent_when_snowflake_refuses_a_routines_comment() -> None:
    # The routine was created and its comment statement then failed: Snowflake's error is the
    # cause, so the change fails with it rather than as an object that changed since the plan.
    member = ToolMember(
        "owner_count",
        "procedure",
        ToolOwnership.DEFINE,
        Origin("tools.yml"),
        "tools.yml",
        description="Counts orders.",
        body="BEGIN RETURN 1; END;",
        returns="NUMBER",
        language="sql",
        warehouse="WH",
    )
    compiled = CompileTools(
        ToolCatalog((member,), "dev", frozenset(("dev",))),
        default=ToolPlacement("DB", "S", None, None, None, None),
        dbt_relations={},
    ).run_result()
    published = compiled_as(compiled, CompiledTool).rendered_for_publish("a" * 64)
    assert "ALTER PROCEDURE DB.S.OWNER_COUNT() SET COMMENT" in str(published.statements[-1])
    port = FakeSnowflake()
    port.execute_results = [
        ExecResult(False, ("q1",), ExecutionError("SQL compilation error: syntax error unexpected 'SET'."))
    ]
    tool = Change(published.key, "tool", Action.CREATE, ChangeReason.NOT_PRESENT, published, None, (), 50)
    found = codes(apply_plan(changeset(tool), port).diagnostics)
    assert "SST-APL012" not in found and "SST-APL001" in found
