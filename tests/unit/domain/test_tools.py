from __future__ import annotations

from types import MappingProxyType

import pytest

from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtColumn, DbtModel
from snowflake_semantic_tools.domain.model.setting import Setting
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolMember, ToolOwnership, ToolParameter
from snowflake_semantic_tools.domain.validate.tool import validate_tool_catalog

_IMMUTABLE = MappingProxyType({"immutable": Setting("immutable", True, "route:vendor")})


def test_tool_catalog_reference_resolution_requires_current_target() -> None:
    member = ToolMember(
        "lookup",
        "procedure",
        ToolOwnership.REFERENCE,
        Origin("tools.yml"),
        "tools.yml",
        relations=MappingProxyType({"prod": "DB.S.LOOKUP"}),
    )
    catalog = ToolCatalog(
        (member,),
        "dev",
        frozenset(("dev", "prod")),
    )
    relation, diagnostics = catalog.relation(member)
    assert relation is None
    assert diagnostics[0].code == "SST-REF018"


def test_tool_member_keys_and_resolution_are_case_insensitive_and_the_first_declaration_answers() -> None:
    origin = Origin("tools.yml")
    managed = ToolMember("Lookup", "stage", ToolOwnership.DEFINE, origin, "tools.yml")
    referenced = ToolMember(
        "Lookup",
        "procedure",
        ToolOwnership.REFERENCE,
        origin,
        "tools.yml",
        relations=MappingProxyType({"dev": "DB.S.LOOKUP"}),
    )
    catalog = ToolCatalog((managed, referenced), "dev", frozenset(("dev",)))
    assert managed.artifact_key == "tool:lookup"
    assert referenced.artifact_key is None
    resolved, diagnostics = catalog.resolve("LOOKUP")
    assert resolved is managed and diagnostics == catalog.diagnostics


def test_a_member_is_immutable_only_when_its_settings_resolve_immutable_true() -> None:
    origin = Origin("tools.yml")
    member = ToolMember("lookup", "stage", ToolOwnership.DEFINE, origin, "tools.yml")
    assert not member.immutable
    assert ToolMember("lookup", "stage", ToolOwnership.DEFINE, origin, "tools.yml", settings=_IMMUTABLE).immutable
    mutable = MappingProxyType({"immutable": Setting("immutable", False, "config")})
    assert not ToolMember("lookup", "stage", ToolOwnership.DEFINE, origin, "tools.yml", settings=mutable).immutable


def test_a_member_warehouse_an_agent_tool_runs_on_is_its_own_config_warehouse() -> None:
    origin = Origin("tools.yml")
    configured = MappingProxyType({"warehouse": "TOOL_WH"})
    assert ToolMember("p", "procedure", ToolOwnership.DEFINE, origin, "t", config=configured).configured_warehouse == (
        "TOOL_WH"
    )
    assert ToolMember("p", "procedure", ToolOwnership.DEFINE, origin, "t").configured_warehouse is None


def test_tool_validation_covers_reference_creation_keys_targets_and_repeated_names() -> None:
    origin = Origin("tools.yml")
    first = ToolMember(
        "shared",
        "procedure",
        ToolOwnership.REFERENCE,
        origin,
        "tools.yml",
        relations=MappingProxyType({"other": "bad"}),
        creation_keys=("handler",),
    )
    second = ToolMember(
        "shared",
        "procedure",
        ToolOwnership.REFERENCE,
        origin,
        "tools.yml",
        relations=MappingProxyType({"dev": "DB.S.SHARED"}),
    )
    diagnostics = validate_tool_catalog(
        ToolCatalog((first, second), "dev", frozenset(("dev",))),
        DbtCatalog("v12", None, None, ()),
    )
    codes = {diagnostic.code for diagnostic in diagnostics}
    assert {"SST-VAL001", "SST-VAL605", "SST-REF018", "SST-REF019"}.issubset(codes)
    [repeated] = [diagnostic for diagnostic in diagnostics if diagnostic.code == "SST-VAL001"]
    assert repeated.message == "tool member 'shared' is declared more than once"
    assert repeated.subject == "tool:shared"


def test_search_validation_accepts_existing_search_and_attribute_columns() -> None:
    model = DbtModel(
        "model.docs",
        "docs",
        "DB.S.DOCS",
        (),
        (),
        (DbtColumn("body", None, None, None), DbtColumn("title", None, None, None)),
    )
    member = ToolMember(
        "search",
        "cortex_search_service",
        ToolOwnership.DEFINE,
        Origin("tools.yml"),
        "tools.yml",
        on_model="docs",
        search_column="body",
        attribute_columns=("title",),
    )
    diagnostics = validate_tool_catalog(
        ToolCatalog((member,), "dev", frozenset(("dev",))),
        DbtCatalog("v12", None, None, (model,)),
    )
    assert "SST-VAL609" not in {diagnostic.code for diagnostic in diagnostics}


def test_tool_catalog_properties_and_resolution_cover_an_unknown_name_and_invalid_fqn() -> None:
    managed = ToolMember(
        "search",
        "stage",
        ToolOwnership.DEFINE,
        Origin("one.yml"),
        "one.yml",
    )
    first = ToolMember(
        "same",
        "procedure",
        ToolOwnership.REFERENCE,
        Origin("one.yml"),
        "one.yml",
        relations=MappingProxyType({"dev": "bad"}),
    )
    second = ToolMember(
        "same",
        "procedure",
        ToolOwnership.REFERENCE,
        Origin("two.yml"),
        "two.yml",
        relations=MappingProxyType({"dev": "DB.S.SAME"}),
    )
    catalog = ToolCatalog((managed, first, second), "dev", frozenset(("dev",)))
    assert catalog.managed == (managed,)
    assert catalog.references == (first, second)
    unresolved, diagnostics = catalog.resolve("missing")
    assert unresolved is None and diagnostics[0].code == "SST-REF010"
    assert diagnostics[0].message == "{{ tool('missing') }} does not resolve"
    relation, diagnostics = catalog.relation(managed)
    assert relation is None and diagnostics == ()
    relation, diagnostics = catalog.relation(first)
    assert relation is None and diagnostics[0].code == "SST-REF019"


def test_tool_validation_covers_duplicate_unknown_category_signature_and_columns() -> None:
    dbt = DbtCatalog(
        "v12",
        None,
        None,
        (
            DbtModel(
                "model.docs",
                "docs",
                "DB.S.DOCS",
                (),
                (),
                (DbtColumn("body", None, None, None),),
            ),
        ),
    )
    origin = Origin("tools.yml")
    broken = ToolMember(
        "broken",
        "unknown",
        ToolOwnership.DEFINE,
        origin,
        "tools.yml",
        relations=MappingProxyType({"dev": "DB.S.BROKEN"}),
        signature=(ToolParameter("payload", "object", True),),
    )
    bad_search = ToolMember(
        "search",
        "cortex_search_service",
        ToolOwnership.DEFINE,
        origin,
        "tools.yml",
        on_model="docs",
        search_column="missing",
        settings=_IMMUTABLE,
    )
    diagnostics = validate_tool_catalog(ToolCatalog((broken, broken, bad_search), "dev", frozenset(("dev",))), dbt)
    assert {
        "SST-VAL001",
        "SST-VAL603",
        "SST-VAL604",
        "SST-VAL606",
        "SST-VAL609",
        "SST-PRS032",
    }.issubset({diagnostic.code for diagnostic in diagnostics})


def test_tool_validation_covers_reference_without_relations_and_shared_target_warning() -> None:
    origin = Origin("tools.yml")
    missing_relations = ToolMember(
        "missing",
        "procedure",
        ToolOwnership.REFERENCE,
        origin,
        "tools.yml",
    )
    shared = ToolMember(
        "shared",
        "procedure",
        ToolOwnership.REFERENCE,
        origin,
        "tools.yml",
        relations=MappingProxyType({"dev": "DB.S.SHARED", "prod": "DB.S.SHARED"}),
    )
    pinned = ToolMember(
        "pinned",
        "procedure",
        ToolOwnership.REFERENCE,
        origin,
        "tools.yml",
        relations=MappingProxyType({"dev": "DB.S.PINNED", "prod": "DB.S.PINNED"}),
        settings=_IMMUTABLE,
    )
    catalog = ToolCatalog((missing_relations, shared, pinned), "dev", frozenset(("dev", "prod")))
    diagnostics = validate_tool_catalog(catalog, DbtCatalog("v12", None, None, ()))
    assert {"SST-PRS002", "SST-REF018", "SST-REF023"}.issubset({diagnostic.code for diagnostic in diagnostics})
    # An immutable member may name one object for every target.
    assert [item.subject for item in diagnostics if item.code == "SST-REF023"] == ["tool:shared"]


def test_tool_validation_reports_unknown_search_model() -> None:
    member = ToolMember(
        "search",
        "cortex_search_service",
        ToolOwnership.DEFINE,
        Origin("tools.yml"),
        "tools.yml",
        on_model="missing",
        search_column="body",
    )
    diagnostics = validate_tool_catalog(
        ToolCatalog((member,), "dev", frozenset(("dev",))),
        DbtCatalog("v12", None, None, ()),
    )
    assert any(diagnostic.code == "SST-VAL608" for diagnostic in diagnostics)


def test_a_relation_with_an_undeclared_target_is_reported_before_the_missing_current_target() -> None:
    origin = Origin("tools.yml")
    member = ToolMember(
        "lookup",
        "procedure",
        ToolOwnership.REFERENCE,
        origin,
        "tools.yml",
        relations=MappingProxyType({"staging": "DB.S.LOOKUP"}),
    )
    diagnostics = validate_tool_catalog(
        ToolCatalog((member,), "dev", frozenset(("dev",))),
        DbtCatalog("v12", None, None, ()),
    )
    assert [item.message for item in diagnostics if item.code == "SST-REF018"] == [
        "{{ tool target('staging') }} has no entry for target 'profiles.yml'",
        "{{ tool('lookup') }} has no entry for target 'dev'",
    ]


def _validated(member: ToolMember) -> list[tuple[str, str]]:
    catalog = ToolCatalog((member,), "dev", frozenset(("dev",)))
    return [
        (diagnostic.code, diagnostic.message)
        for diagnostic in validate_tool_catalog(catalog, DbtCatalog("v12", None, None, ()))
        if diagnostic.code in ("SST-PRS003", "SST-PRS005", "SST-PRS013", "SST-VAL418")
    ]


def test_every_value_a_defined_tool_writes_unquoted_is_checked_before_it_renders() -> None:
    unsafe = ToolMember(
        "lookup",
        "procedure",
        ToolOwnership.DEFINE,
        Origin("tools.yml"),
        "tools.yml",
        body_file="lookup.sql",
        body="RETURN 1",
        language="sql; DROP",
        execute_as="everyone",
        returns="NUMBER); DROP TABLE x; --",
        warehouse="WH X",
        signature=(ToolParameter("id", "NUMBER) --", True),),
        secrets=MappingProxyType({"token": "SECRET"}),
        where="1 = 1; DROP TABLE x",
    )
    assert sorted(code for code, _ in _validated(unsafe)) == [
        "SST-PRS003",
        "SST-PRS003",
        "SST-PRS005",
        "SST-PRS005",
        "SST-PRS013",
        "SST-PRS013",
        "SST-VAL418",
    ]
    assert ("SST-PRS005", "tool:lookup: 'WH X' is not a valid identifier") in _validated(unsafe)


def test_a_tool_whose_values_render_or_wait_for_a_template_reports_none() -> None:
    safe = ToolMember(
        "lookup",
        "function",
        ToolOwnership.DEFINE,
        Origin("tools.yml"),
        "tools.yml",
        body_file="lookup.sql",
        body="SELECT 1",
        language="Python",
        execute_as="restricted   caller",
        returns="TABLE (id NUMBER)",
        warehouse="{{ env_var('WH') }}",
        signature=(ToolParameter("id", "NUMBER(38, 0)", True),),
        secrets=MappingProxyType({"token": "DB.S.SECRET"}),
        where="{{ var('predicate') }}",
    )
    assert _validated(safe) == []
    assert _validated(ToolMember("r", "procedure", ToolOwnership.REFERENCE, Origin("t.yml"), "t.yml", where=";")) == []
    assert _validated(ToolMember("s", "stage", ToolOwnership.DEFINE, Origin("t.yml"), "t.yml", where="X")) == []


def _required(kind: str, **fields: object) -> list[str]:
    """The fields SST-PRS002 reports a defined member of `kind` with `fields` lacks."""
    member = ToolMember("lookup", kind, ToolOwnership.DEFINE, Origin("t.yml"), "t.yml", **fields)  # type: ignore[arg-type]
    catalog = ToolCatalog((member,), "dev", frozenset(("dev",)))
    found = validate_tool_catalog(catalog, DbtCatalog("v12", None, None, ()))
    return [str(item.context["field"]) for item in found if item.code == "SST-PRS002"]


_ROUTINE = {"body_file": "lookup.sql", "body": "RETURN 1", "language": "python", "returns": "NUMBER"}
_ROUTINE_FIELDS = {**_ROUTINE, "runtime_version": "3.11", "handler": "run"}


@pytest.mark.parametrize("field", ["language", "returns", "runtime_version", "handler"])
@pytest.mark.parametrize("kind", ["procedure", "function"])
def test_a_defined_routine_lacking_a_field_its_ddl_needs_reports_it_by_name(kind: str, field: str) -> None:
    fields = {key: value for key, value in _ROUTINE_FIELDS.items() if key != field}
    # A routine with no language is not known to need a handler, so only the language is missing.
    assert _required(kind, **fields) == [field]


def test_a_defined_search_service_lacking_a_search_column_reports_it() -> None:
    assert _required("cortex_search_service", on_model="docs") == ["search_column"]
    assert _required("cortex_search_service", on_model="docs", search_column="BODY") == []


def test_a_sql_routine_needs_no_runtime_version_or_handler_and_a_stage_needs_nothing() -> None:
    assert _required("procedure", **{**_ROUTINE, "language": "sql"}) == []
    assert _required("function", **_ROUTINE_FIELDS) == []
    assert _required("stage") == []


@pytest.mark.parametrize("kind", ["procedure", "function"])
def test_a_javascript_routine_needs_no_runtime_version_or_handler_as_its_grammar_has_neither(kind: str) -> None:
    assert _required(kind, **{**_ROUTINE, "language": "JavaScript"}) == []


@pytest.mark.parametrize("language", ["java", "python", "scala"])
def test_a_java_python_or_scala_routine_still_needs_runtime_version_and_handler(language: str) -> None:
    assert _required("function", **{**_ROUTINE, "language": language}) == ["runtime_version", "handler"]


def _handler_keys(kind: str, fields: dict[str, object], ownership: ToolOwnership = ToolOwnership.DEFINE) -> list[str]:
    """The keys SST-VAL621 reports on one member of `kind` with `fields`."""
    member = ToolMember("lookup", kind, ownership, Origin("t.yml"), "t.yml", **fields)  # type: ignore[arg-type]
    catalog = ToolCatalog((member,), "dev", frozenset(("dev",)))
    found = validate_tool_catalog(catalog, DbtCatalog("v12", None, None, ()))
    return [str(item.context["key"]) for item in found if item.code == "SST-VAL621"]


def test_a_javascript_or_sql_routine_reports_each_key_only_handler_languages_take() -> None:
    keys: dict[str, object] = {"packages": ("p",), "imports": ("@s/x.py",)}
    assert _handler_keys("function", {**_ROUTINE_FIELDS, **keys, "language": "JavaScript"}) == [
        "runtime_version",
        "handler",
        "packages",
        "imports",
    ]
    assert _handler_keys("procedure", {**_ROUTINE, **keys, "language": "sql"}) == ["packages", "imports"]
    # A Python routine takes all four; a reference creates nothing; a search service is no routine.
    assert _handler_keys("function", {**_ROUTINE_FIELDS, **keys}) == []
    assert _handler_keys("procedure", {"language": "sql", "packages": ("p",)}, ToolOwnership.REFERENCE) == []
    assert (
        _handler_keys("cortex_search_service", {"on_model": "docs", "search_column": "BODY", "packages": ("p",)}) == []
    )
