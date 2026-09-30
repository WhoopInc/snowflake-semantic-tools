from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtColumn, DbtModel
from snowflake_semantic_tools.domain.model.diagnostic import Origin
from snowflake_semantic_tools.domain.model.tool import (
    ToolCatalog,
    ToolGroup,
    ToolMember,
    ToolOwnership,
    ToolParameter,
    validate_tool_catalog,
)


def test_tool_catalog_reference_resolution_requires_current_target() -> None:
    member = ToolMember(
        "platform",
        "lookup",
        "procedure",
        ToolOwnership.REFERENCE,
        Origin("tools.yml"),
        "tools.yml",
        relations=MappingProxyType({"prod": "DB.S.LOOKUP"}),
    )
    catalog = ToolCatalog(
        (ToolGroup("platform", Origin("tools.yml"), "tools.yml", members=(member,)),),
        "dev",
        frozenset(("dev", "prod")),
    )
    relation, diagnostics = catalog.relation(member)
    assert relation is None
    assert diagnostics[0].code == "SST-REF018"


def test_tool_member_keys_and_two_argument_resolution_are_case_insensitive() -> None:
    origin = Origin("tools.yml")
    managed = ToolMember("Platform", "Lookup", "stage", ToolOwnership.DEFINE, origin, "tools.yml")
    referenced = ToolMember(
        "Other",
        "Lookup",
        "procedure",
        ToolOwnership.REFERENCE,
        origin,
        "tools.yml",
        relations=MappingProxyType({"dev": "DB.S.LOOKUP"}),
    )
    catalog = ToolCatalog(
        (
            ToolGroup("Platform", origin, "tools.yml", members=(managed,)),
            ToolGroup("Other", origin, "tools.yml", members=(referenced,)),
        ),
        "dev",
        frozenset(("dev",)),
    )
    assert managed.declaration_key == "platform:lookup"
    assert managed.artifact_key == "tool:lookup"
    assert referenced.artifact_key is None
    resolved, diagnostics = catalog.resolve("platform", "lookup")
    assert resolved is managed and diagnostics == catalog.diagnostics


def test_tool_validation_covers_reference_creation_keys_targets_and_cross_group_names() -> None:
    origin = Origin("tools.yml")
    first = ToolMember(
        "one",
        "shared",
        "procedure",
        ToolOwnership.REFERENCE,
        origin,
        "tools.yml",
        relations=MappingProxyType({"other": "bad"}),
        creation_keys=("handler",),
    )
    second = ToolMember(
        "two",
        "shared",
        "procedure",
        ToolOwnership.REFERENCE,
        origin,
        "tools.yml",
        relations=MappingProxyType({"dev": "DB.S.SHARED"}),
    )
    diagnostics = validate_tool_catalog(
        ToolCatalog(
            (
                ToolGroup("one", origin, "tools.yml", immutable=True, members=(first,)),
                ToolGroup("two", origin, "tools.yml", immutable=True, members=(second,)),
            ),
            "dev",
            frozenset(("dev",)),
        ),
        DbtCatalog("v12", None, None, ()),
    )
    codes = {diagnostic.code for diagnostic in diagnostics}
    assert {"SST-VAL602", "SST-VAL605", "SST-REF018", "SST-REF019"}.issubset(codes)


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
        "group",
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
        ToolCatalog(
            (ToolGroup("group", Origin("tools.yml"), "tools.yml", members=(member,)),),
            "dev",
            frozenset(("dev",)),
        ),
        DbtCatalog("v12", None, None, (model,)),
    )
    assert "SST-VAL609" not in {diagnostic.code for diagnostic in diagnostics}


def test_tool_catalog_properties_and_resolution_cover_ambiguity_and_invalid_fqn() -> None:
    managed = ToolMember(
        "one",
        "search",
        "stage",
        ToolOwnership.DEFINE,
        Origin("one.yml"),
        "one.yml",
    )
    first = ToolMember(
        "one",
        "same",
        "procedure",
        ToolOwnership.REFERENCE,
        Origin("one.yml"),
        "one.yml",
        relations=MappingProxyType({"dev": "bad"}),
    )
    second = ToolMember(
        "two",
        "same",
        "procedure",
        ToolOwnership.REFERENCE,
        Origin("two.yml"),
        "two.yml",
        relations=MappingProxyType({"dev": "DB.S.SAME"}),
    )
    catalog = ToolCatalog(
        (
            ToolGroup("one", Origin("one.yml"), "one.yml", members=(managed, first)),
            ToolGroup("two", Origin("two.yml"), "two.yml", members=(second,)),
        ),
        "dev",
        frozenset(("dev",)),
    )
    assert catalog.members == (managed, first, second)
    assert catalog.managed == (managed,)
    assert catalog.references == (first, second)
    unresolved, diagnostics = catalog.resolve("same")
    assert unresolved is None and diagnostics[0].code == "SST-REF010"
    unresolved, diagnostics = catalog.resolve()
    assert unresolved is None and diagnostics[0].code == "SST-REF010"
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
        "group",
        "broken",
        "unknown",
        ToolOwnership.DEFINE,
        origin,
        "tools.yml",
        relations=MappingProxyType({"dev": "DB.S.BROKEN"}),
        signature=(ToolParameter("payload", "object", True),),
    )
    bad_search = ToolMember(
        "group",
        "search",
        "cortex_search_service",
        ToolOwnership.DEFINE,
        origin,
        "tools.yml",
        on_model="docs",
        search_column="missing",
    )
    group = ToolGroup("group", origin, "tools.yml", immutable=True, members=(broken, broken, bad_search))
    duplicate_group = ToolGroup("group", Origin("other.yml"), "other.yml")
    diagnostics = validate_tool_catalog(
        ToolCatalog((group, duplicate_group), "dev", frozenset(("dev",))),
        dbt,
    )
    assert {
        "SST-VAL001",
        "SST-VAL601",
        "SST-VAL603",
        "SST-VAL604",
        "SST-VAL605",
        "SST-VAL606",
        "SST-VAL609",
        "SST-PRS032",
    }.issubset({diagnostic.code for diagnostic in diagnostics})


def test_tool_validation_covers_reference_without_relations_shared_target_warning_and_sidecars() -> None:
    origin = Origin("tools.yml")
    missing_relations = ToolMember(
        "group",
        "missing",
        "procedure",
        ToolOwnership.REFERENCE,
        origin,
        "tools.yml",
    )
    shared = ToolMember(
        "group",
        "shared",
        "procedure",
        ToolOwnership.REFERENCE,
        origin,
        "tools.yml",
        relations=MappingProxyType({"dev": "DB.S.SHARED", "prod": "DB.S.SHARED"}),
    )
    missing_sidecar = ToolMember(
        "group",
        "missing_body",
        "procedure",
        ToolOwnership.DEFINE,
        origin,
        "tools.yml",
        body_file="missing.sql",
        body=None,
    )
    empty_sidecar = ToolMember(
        "group",
        "empty_body",
        "function",
        ToolOwnership.DEFINE,
        origin,
        "tools.yml",
        body_file="empty.sql",
        body="",
    )
    healthy_sidecar = ToolMember(
        "group",
        "healthy_body",
        "function",
        ToolOwnership.DEFINE,
        origin,
        "tools.yml",
        body_file="healthy.sql",
        body="RETURN 1",
    )
    catalog = ToolCatalog(
        (
            ToolGroup(
                "group",
                origin,
                "tools.yml",
                members=(missing_relations, shared, missing_sidecar, empty_sidecar, healthy_sidecar),
            ),
        ),
        "dev",
        frozenset(("dev", "prod")),
    )
    diagnostics = validate_tool_catalog(catalog, DbtCatalog("v12", None, None, ()))
    assert {
        "SST-PRS002",
        "SST-REF018",
        "SST-REF023",
        "SST-LOD018",
        "SST-LOD019",
    }.issubset({diagnostic.code for diagnostic in diagnostics})


def test_tool_validation_reports_unknown_search_model() -> None:
    member = ToolMember(
        "group",
        "search",
        "cortex_search_service",
        ToolOwnership.DEFINE,
        Origin("tools.yml"),
        "tools.yml",
        on_model="missing",
        search_column="body",
    )
    diagnostics = validate_tool_catalog(
        ToolCatalog(
            (ToolGroup("group", Origin("tools.yml"), "tools.yml", members=(member,)),),
            "dev",
            frozenset(("dev",)),
        ),
        DbtCatalog("v12", None, None, ()),
    )
    assert any(diagnostic.code == "SST-VAL608" for diagnostic in diagnostics)


def test_a_relation_with_an_undeclared_target_is_reported_before_the_missing_current_target() -> None:
    origin = Origin("tools.yml")
    member = ToolMember(
        "group",
        "lookup",
        "procedure",
        ToolOwnership.REFERENCE,
        origin,
        "tools.yml",
        relations=MappingProxyType({"staging": "DB.S.LOOKUP"}),
    )
    diagnostics = validate_tool_catalog(
        ToolCatalog((ToolGroup("group", origin, "tools.yml", members=(member,)),), "dev", frozenset(("dev",))),
        DbtCatalog("v12", None, None, ()),
    )
    assert [item.message for item in diagnostics if item.code == "SST-REF018"] == [
        "{{ tool target('staging') }} has no entry for target 'profiles.yml'",
        "{{ tool('group', 'lookup') }} has no entry for target 'dev'",
    ]
