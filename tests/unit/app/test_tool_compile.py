from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.tools import CompiledTool, CompileTools
from snowflake_semantic_tools.domain.model.diagnostic import D, Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolGroup, ToolMember, ToolOwnership, ToolParameter
from tests.helpers.artifact_builders import rendered
from tests.helpers.compile_builders import compiled_as


def test_tool_compiler_applies_config_defaults_and_emits_only_managed_members() -> None:
    managed = ToolMember(
        "platform",
        "search",
        "cortex_search_service",
        ToolOwnership.DEFINE,
        Origin("tools.yml"),
        "tools.yml",
        on_model="docs",
        search_column="body",
    )
    reference = ToolMember(
        "platform",
        "lookup",
        "procedure",
        ToolOwnership.REFERENCE,
        Origin("tools.yml"),
        "tools.yml",
        relations=MappingProxyType({"dev": "DB.S.LOOKUP"}),
    )
    catalog = ToolCatalog(
        (ToolGroup("platform", Origin("tools.yml"), "tools.yml", members=(managed, reference)),),
        "dev",
        frozenset(("dev",)),
    )
    result = CompileTools(
        catalog,
        database="DB",
        schema="S",
        warehouse="WH",
        target_lag="1 hour",
        embedding_model="model",
        execute_as="caller",
        dbt_relations={"docs": "DB.S.DOCS"},
    ).run_result()
    assert [item.artifact_key for item in result.compiled] == ["tool:search"]
    rendered = result.rendered[0]
    assert rendered.target.sql == "DB.S.SEARCH"
    assert "WAREHOUSE = WH" in rendered.ddl
    assert "TARGET_LAG = '1 hour'" in rendered.ddl
    assert rendered.required_relations[0].sql == "DB.S.DOCS"
    published = compiled_as(result, CompiledTool).rendered_for_publish("a" * 64)
    assert f"[sst:{'a' * 64}:{rendered.fingerprint}]" in published.statements[0]


def test_compiled_tool_projections_cover_sidecars_relations_and_routine_markers() -> None:
    managed = ToolMember(
        "platform",
        "routine",
        "function",
        ToolOwnership.DEFINE,
        Origin("tools.yml"),
        "tools.yml",
        language="sql",
        body_file="routine.sql",
        body="RETURN 1",
        returns="NUMBER",
        signature=(ToolParameter("value", "NUMBER", True),),
    )
    catalog = ToolCatalog(
        (ToolGroup("platform", Origin("tools.yml"), "tools.yml", members=(managed,)),),
        "dev",
        frozenset(("dev",)),
    )
    result = CompileTools(
        catalog,
        database="DB",
        schema="S",
        warehouse=None,
        target_lag=None,
        embedding_model=None,
        execute_as=None,
        dbt_relations={},
    ).run_result()
    compiled = compiled_as(result, CompiledTool)
    assert compiled.source_files == ("tools.yml", "routine.sql")
    assert compiled.member_keys == ()
    assert compiled.referenced_models == ()
    assert compiled.dbt_relations == ()
    assert compiled.name == "routine"
    assert compiled.artifact_type == "tool"
    published = compiled.rendered_for_publish("b" * 64)
    assert published.statements[-1].startswith("ALTER FUNCTION DB.S.ROUTINE(NUMBER) SET COMMENT")


def test_tool_compiler_skips_poisoned_and_reports_render_invariant() -> None:
    poisoned = ToolMember(
        "platform",
        "poisoned",
        "stage",
        ToolOwnership.DEFINE,
        Origin("tools.yml"),
        "tools.yml",
    )
    invalid = ToolMember(
        "platform",
        "invalid",
        "procedure",
        ToolOwnership.DEFINE,
        Origin("tools.yml"),
        "tools.yml",
        body_file="body.sql",
        body="RETURN 1",
    )
    from snowflake_semantic_tools.domain.model.diagnostic import D, DiagnosticBag

    catalog = ToolCatalog(
        (ToolGroup("platform", Origin("tools.yml"), "tools.yml", members=(poisoned, invalid)),),
        "dev",
        frozenset(("dev",)),
        DiagnosticBag((D("SST-VAL606", a="platform", subject="tool:poisoned"),)),
    )
    result = CompileTools(
        catalog,
        database="DB",
        schema="S",
        warehouse=None,
        target_lag=None,
        embedding_model=None,
        execute_as=None,
        dbt_relations={},
    ).run_result()
    assert result.compiled == ()
    assert {diagnostic.code for diagnostic in result.diagnostics} == {"SST-VAL606", "SST-INT902"}


def test_tool_defaults_cover_explicit_values_and_procedure_inheritance() -> None:
    procedure = ToolMember(
        "platform",
        "procedure",
        "procedure",
        ToolOwnership.DEFINE,
        Origin("tools.yml"),
        "tools.yml",
        warehouse="LOCAL_WH",
        execute_as="owner",
        language="sql",
        body_file="body.sql",
        body="RETURN 1",
        returns="NUMBER",
    )
    catalog = ToolCatalog(
        (ToolGroup("platform", Origin("tools.yml"), "tools.yml", members=(procedure,)),),
        "dev",
        frozenset(("dev",)),
    )
    result = CompileTools(
        catalog,
        database="DB",
        schema="S",
        warehouse="DEFAULT_WH",
        target_lag="1 hour",
        embedding_model="model",
        execute_as="caller",
        dbt_relations={},
    ).run_result()
    assert "EXECUTE AS OWNER" in result.rendered[0].ddl


def test_compiled_tool_publication_replaces_existing_search_comment() -> None:
    managed = ToolMember(
        "platform",
        "search",
        "cortex_search_service",
        ToolOwnership.DEFINE,
        Origin("tools.yml"),
        "tools.yml",
        description="Owner's search.",
        on_model="docs",
        search_column="body",
    )
    catalog = ToolCatalog(
        (ToolGroup("platform", Origin("tools.yml"), "tools.yml", members=(managed,)),),
        "dev",
        frozenset(("dev",)),
    )
    result = CompileTools(
        catalog,
        database="DB",
        schema="S",
        warehouse="WH",
        target_lag="1 hour",
        embedding_model="model",
        execute_as=None,
        dbt_relations={"docs": "DB.S.DOCS"},
    ).run_result()
    compiled = compiled_as(result, CompiledTool)
    published = compiled.rendered_for_publish("a" * 64)
    assert published.statements[0].count("COMMENT =") == 1
    assert "Owner''s search" in published.statements[0]


def test_compiled_tool_publication_covers_stage_and_empty_projections() -> None:
    stage = ToolMember(
        "platform",
        "stage",
        "stage",
        ToolOwnership.DEFINE,
        Origin("tools.yml"),
        "tools.yml",
        description="Owner's stage.",
    )
    catalog = ToolCatalog(
        (ToolGroup("platform", Origin("tools.yml"), "tools.yml", members=(stage,)),),
        "dev",
        frozenset(("dev",)),
    )
    compiled = compiled_as(
        CompileTools(
            catalog,
            database="DB",
            schema="S",
            warehouse=None,
            target_lag=None,
            embedding_model=None,
            execute_as=None,
            dbt_relations={},
        ).run_result(),
        CompiledTool,
    )
    assert compiled.source_files == ("tools.yml",)
    assert compiled.referenced_models == ()
    assert compiled.dbt_relations == ()
    published = compiled.rendered_for_publish("b" * 64)
    assert published.statements[-1] == (
        f"ALTER STAGE DB.S.STAGE SET COMMENT = '[sst:{'b' * 64}:{compiled.rendered.fingerprint}] Owner''s stage.'"
    )


def test_compiled_tool_omits_dbt_relation_without_required_relation() -> None:
    stage = ToolMember(
        "platform",
        "stage",
        "stage",
        ToolOwnership.DEFINE,
        Origin("tools.yml"),
        "tools.yml",
        on_model="docs",
    )
    catalog = ToolCatalog(
        (ToolGroup("platform", Origin("tools.yml"), "tools.yml", members=(stage,)),),
        "dev",
        frozenset(("dev",)),
    )
    compiled = compiled_as(
        CompileTools(
            catalog,
            database="DB",
            schema="S",
            warehouse=None,
            target_lag=None,
            embedding_model=None,
            execute_as=None,
            dbt_relations={},
        ).run_result(),
        CompiledTool,
    )
    assert compiled.referenced_models == ("docs",)
    assert compiled.dbt_relations == ()


def test_compiled_tool_publication_adds_search_comment_when_missing() -> None:
    managed = ToolMember(
        "platform",
        "search",
        "cortex_search_service",
        ToolOwnership.DEFINE,
        Origin("tools.yml"),
        "tools.yml",
        on_model="docs",
        search_column="body",
    )
    catalog = ToolCatalog(
        (ToolGroup("platform", Origin("tools.yml"), "tools.yml", members=(managed,)),),
        "dev",
        frozenset(("dev",)),
    )
    compiled = compiled_as(
        CompileTools(
            catalog,
            database="DB",
            schema="S",
            warehouse="WH",
            target_lag="1 hour",
            embedding_model="model",
            execute_as=None,
            dbt_relations={"docs": "DB.S.DOCS"},
        ).run_result(),
        CompiledTool,
    )
    without_comment = replace(
        compiled, rendered=replace(compiled.rendered, ddl=compiled.rendered.ddl.replace("\n  COMMENT = ''", ""))
    )
    published = without_comment.rendered_for_publish("c" * 64)
    assert published.statements[0].count("COMMENT =") == 1
    assert f"[sst:{'c' * 64}:" in published.statements[0]


def test_compiled_tool_publication_covers_routine_without_signature() -> None:
    procedure = ToolMember(
        "platform",
        "procedure",
        "procedure",
        ToolOwnership.DEFINE,
        Origin("tools.yml"),
        "tools.yml",
        body_file="body.sql",
        body="RETURN 1",
        returns="NUMBER",
        language="sql",
        warehouse="WH",
    )
    catalog = ToolCatalog(
        (ToolGroup("platform", Origin("tools.yml"), "tools.yml", members=(procedure,)),),
        "dev",
        frozenset(("dev",)),
    )
    result = CompileTools(
        catalog,
        database="DB",
        schema="S",
        warehouse=None,
        target_lag=None,
        embedding_model=None,
        execute_as=None,
        dbt_relations={},
    ).run_result()
    assert result.compiled
    compiled = compiled_as(result, CompiledTool)
    published = compiled.rendered_for_publish("d" * 64)
    assert published.statements[-1].startswith("ALTER PROCEDURE DB.S.PROCEDURE SET COMMENT")


def compile_stage(*diagnostics: Diagnostic) -> CompileResult:
    stage = ToolMember(
        "platform", "landing", "stage", ToolOwnership.DEFINE, Origin("tools.yml"), "tools.yml", description="Landing."
    )
    catalog = ToolCatalog(
        (ToolGroup("platform", Origin("tools.yml"), "tools.yml", members=(stage,)),),
        "dev",
        frozenset(("dev",)),
        DiagnosticBag(diagnostics),
    )
    return CompileTools(
        catalog,
        database="DB",
        schema="S",
        warehouse=None,
        target_lag=None,
        embedding_model=None,
        execute_as=None,
        dbt_relations={},
    ).run_result()


def test_a_tool_over_no_dbt_model_reads_no_relation() -> None:
    compiled = compiled_as(compile_stage(), CompiledTool)
    assert (compiled.member_keys, compiled.referenced_models, compiled.dbt_relations) == ((), (), ())


def test_a_search_service_reports_the_dbt_model_it_reads_with_its_relation() -> None:
    search = ToolMember(
        "platform",
        "search",
        "cortex_search_service",
        ToolOwnership.DEFINE,
        Origin("tools.yml"),
        "tools.yml",
        on_model="docs",
        search_column="body",
    )
    catalog = ToolCatalog(
        (ToolGroup("platform", Origin("tools.yml"), "tools.yml", members=(search,)),), "dev", frozenset(("dev",))
    )
    compiled = compiled_as(
        CompileTools(
            catalog,
            database="DB",
            schema="S",
            warehouse="WH",
            target_lag="1 hour",
            embedding_model=None,
            execute_as=None,
            dbt_relations={"docs": "DB.S.DOCS"},
        ).run_result(),
        CompiledTool,
    )
    assert (compiled.referenced_models, compiled.dbt_relations) == (("docs",), (("docs", "DB.S.DOCS"),))


def test_an_object_type_without_a_comment_keeps_its_statements_and_still_expects_the_marker() -> None:
    compiled = compiled_as(compile_stage(), CompiledTool)
    other = replace(compiled, rendered=rendered("OTHER"))
    published = other.rendered_for_publish("e" * 64)
    assert published.statements == other.rendered.statements
    assert published.expected_marker is not None and published.expected_marker.manifest_id == "e" * 64


def test_any_catalog_diagnostic_that_names_a_member_keeps_it_back_even_a_warning() -> None:
    warning = D("SST-REF023", ref_function="tool", name="landing", value="DB.S.LANDING", subject="tool:landing")
    result = compile_stage(warning)
    assert result.compiled == ()
    assert list(result.diagnostics) == [warning]
