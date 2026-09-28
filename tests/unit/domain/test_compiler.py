"""Pure compiler records, reference resolution, and attachment."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.compiler import (
    CUSTOM_INSTRUCTION_ITEM,
    DESCRIPTION,
    METRIC_EXPR,
    TAG_NAME,
    ResolveContext,
    resolve_scalar,
)
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtColumn, DbtModel
from snowflake_semantic_tools.domain.model.diagnostic import Origin
from snowflake_semantic_tools.domain.model.project import ParsedMember
from snowflake_semantic_tools.domain.model.reference import scan_template_calls
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.resolve.members import attach_members, attach_view_members, effective_tables


def catalog() -> DbtCatalog:
    return DbtCatalog(
        "v12",
        "1.11",
        "fixture",
        (
            DbtModel(
                "model.fixture.orders",
                "orders",
                "DB.SCH.ORDERS",
                ("id",),
                (),
                (DbtColumn("id", None, "NUMBER", "dimension"),),
            ),
        ),
    )


def test_resolver_retains_origins_and_poisoned_state() -> None:
    resolved, diagnostics = resolve_scalar(
        "COUNT({{ ref('orders', 'id') }})",
        METRIC_EXPR,
        Origin("metrics.yml", 3, 11),
        ResolveContext(catalog(), frozenset(), frozenset()),
        field="expression",
    )
    assert resolved.text == "COUNT(ORDERS.ID)"
    assert resolved.origins[0].model_name == "orders"
    assert not resolved.poisoned
    assert diagnostics == ()

    failed, diagnostics = resolve_scalar(
        "{{ ref('missing') }}",
        METRIC_EXPR,
        Origin("metrics.yml", 4, 5),
        ResolveContext(catalog()),
        field="expression",
    )
    assert failed.poisoned
    assert [diagnostic.code for diagnostic in diagnostics] == ["SST-REF001"]


def test_attachment_is_deterministic_and_uses_subset_membership() -> None:
    member = ParsedMember(
        "metric",
        "orders",
        Origin("metrics.yml", 2, 3),
        object(),
        ("orders",),
        scan_template_calls("COUNT({{ ref('orders', 'id') }})"),
    )
    views = {
        "semantic_view:z": frozenset(("orders", "customers")),
        "semantic_view:a": frozenset(("orders",)),
    }
    first = attach_members(views, (member,), SEMANTIC_REGISTRY)
    second = attach_members(views, (member,), SEMANTIC_REGISTRY)
    assert first == second
    assert first[member.key] == ("semantic_view:a", "semantic_view:z")
    assert effective_tables(member) == frozenset(("orders",))


def test_resolver_handles_metric_instruction_var_and_tag_calls() -> None:
    context = ResolveContext(
        catalog(),
        metric_names=frozenset(("order_count",)),
        instruction_names=frozenset(("sql_rules",)),
        variables={"state": "completed"},
        tags={"domain": "DB.SCH.DOMAIN"},
    )
    metric, diagnostics = resolve_scalar(
        "{{ metric('order_count') }} = '{{ var('state') }}'",
        METRIC_EXPR,
        Origin("metrics.yml"),
        context,
        field="expression",
    )
    assert metric.text == "ORDER_COUNT = 'completed'"
    assert [origin.kind.name for origin in metric.origins] == ["METRIC", "VAR"]
    assert diagnostics == ()

    instruction, diagnostics = resolve_scalar(
        "{{ custom_instructions('sql_rules') }}",
        CUSTOM_INSTRUCTION_ITEM,
        Origin("views.yml"),
        context,
        field="custom_instructions",
    )
    assert instruction.text == "SQL_RULES"
    assert diagnostics == ()

    tag, diagnostics = resolve_scalar(
        "{{ tag('domain') }}",
        TAG_NAME,
        Origin("views.yml"),
        context,
        field="tag",
    )
    assert tag.text == "DB.SCH.DOMAIN"
    assert diagnostics == ()


def test_resolver_reports_policy_arity_and_unknown_targets() -> None:
    context = ResolveContext(catalog())
    description, diagnostics = resolve_scalar(
        "{{ ref('orders') }}",
        DESCRIPTION,
        Origin("views.yml"),
        context,
        field="description",
    )
    assert description.poisoned
    assert diagnostics[0].code == "SST-INT902"

    bad_arity, diagnostics = resolve_scalar(
        "{{ ref() }}",
        METRIC_EXPR,
        Origin("metrics.yml"),
        context,
        field="expression",
    )
    assert bad_arity.poisoned
    assert diagnostics[0].code == "SST-INT902"

    unknown_metric, diagnostics = resolve_scalar(
        "{{ metric('missing') }}",
        METRIC_EXPR,
        Origin("metrics.yml"),
        context,
        field="expression",
    )
    assert unknown_metric.poisoned
    assert diagnostics[0].code == "SST-INT902"


def test_resolver_rejects_legacy_calls_and_required_missing_refs() -> None:
    context = ResolveContext(catalog())
    legacy, diagnostics = resolve_scalar(
        "{{ table('orders') }} {{ column('orders', 'id') }}",
        METRIC_EXPR,
        Origin("metrics.yml"),
        context,
        field="expression",
    )
    assert legacy.poisoned
    assert {diagnostic.code for diagnostic in diagnostics} == {"SST-REF034", "SST-REF035"}

    missing, diagnostics = resolve_scalar(
        "plain text",
        TAG_NAME,
        Origin("views.yml"),
        context,
        field="tag",
    )
    assert missing.poisoned
    assert diagnostics[0].code == "SST-INT902"


def test_resolver_reports_unknown_columns_and_malformed_templates() -> None:
    context = ResolveContext(catalog())
    unknown, diagnostics = resolve_scalar(
        "{{ ref('orders', 'missing') }}",
        METRIC_EXPR,
        Origin("metrics.yml"),
        context,
        field="expression",
    )
    assert unknown.poisoned
    assert diagnostics[0].code == "SST-REF002"

    malformed, diagnostics = resolve_scalar(
        "{{ ref('orders')",
        METRIC_EXPR,
        Origin("metrics.yml"),
        context,
        field="expression",
    )
    assert malformed.poisoned
    assert diagnostics[0].code == "SST-LOD004"


def test_resolver_covers_remaining_error_and_value_branches() -> None:
    context = ResolveContext(
        catalog(),
        metric_names=frozenset(("m",)),
        metric_values={"m": "ORDERS.M"},
        instruction_names=frozenset(("rules",)),
        variables={"empty": ""},
        tags={"domain": "DB.SCH.DOMAIN"},
    )
    resolved, diagnostics = resolve_scalar(
        "{{ metric('m') }}",
        METRIC_EXPR,
        Origin("metrics.yml"),
        context,
        field="expression",
    )
    assert resolved.text == "ORDERS.M"
    assert diagnostics == ()

    for text, policy in (
        ("{{ custom_instructions() }}", CUSTOM_INSTRUCTION_ITEM),
        ("{{ var('missing') }}", METRIC_EXPR),
        ("{{ tag('missing') }}", TAG_NAME),
        ("{{ tag() }}", TAG_NAME),
    ):
        resolved, diagnostics = resolve_scalar(
            text,
            policy,
            Origin("bad.yml"),
            context,
            field="value",
        )
        assert resolved.poisoned
        assert diagnostics

    empty, diagnostics = resolve_scalar(
        "{{ var('empty') }}",
        METRIC_EXPR,
        Origin("bad.yml"),
        context,
        field="value",
    )
    assert empty.text == ""
    assert not empty.poisoned
    assert diagnostics == ()

    resolved, diagnostics = resolve_scalar(
        "{{ ref('orders') }} + {{ ref('orders') }}",
        TAG_NAME,
        Origin("bad.yml"),
        context,
        field="tag",
    )
    assert resolved.poisoned
    assert len(diagnostics) >= 2

    relation, diagnostics = resolve_scalar(
        "{{ ref('orders') }}",
        METRIC_EXPR,
        Origin("metrics.yml"),
        ResolveContext(catalog()),
        field="expression",
    )
    assert relation.text == "DB.SCH.ORDERS"
    assert diagnostics == ()

    column_kind, diagnostics = resolve_scalar(
        "{{ column('orders', 'id') }}",
        METRIC_EXPR,
        Origin("legacy.yml"),
        context,
        field="expression",
    )
    assert column_kind.poisoned
    assert diagnostics[0].code == "SST-REF035"

    direct_column_kind, diagnostics = resolve_scalar(
        "{{ column('orders', 'id') }}",
        METRIC_EXPR,
        Origin("legacy.yml"),
        context,
        field="expression",
    )
    assert direct_column_kind.poisoned

    instruction, diagnostics = resolve_scalar(
        "{{ custom_instructions('rules') }}",
        CUSTOM_INSTRUCTION_ITEM,
        Origin("views.yml"),
        context,
        field="custom_instructions",
    )
    assert instruction.text == "RULES"
    assert diagnostics == ()

    custom_kind, diagnostics = resolve_scalar(
        "{{ custom_instructions('rules') }}",
        CUSTOM_INSTRUCTION_ITEM,
        Origin("views.yml"),
        ResolveContext(catalog(), instruction_names=frozenset(("rules",))),
        field="custom_instructions",
    )
    assert custom_kind.origins[0].kind.name == "CUSTOM_INSTRUCTION"
    assert diagnostics == ()

    table_kind, diagnostics = resolve_scalar(
        "{{ ref('orders') }}",
        METRIC_EXPR,
        Origin("metrics.yml"),
        ResolveContext(catalog()),
        field="expression",
    )
    assert table_kind.origins[0].kind.name == "TABLE"
    assert diagnostics == ()

    empty_context = ResolveContext(catalog(), metric_names=frozenset(("m",)), metric_values={"m": ""})
    empty_metric, diagnostics = resolve_scalar(
        "{{ metric('m') }}",
        METRIC_EXPR,
        Origin("metrics.yml"),
        empty_context,
        field="expression",
    )
    assert empty_metric.text == ""
    assert not empty_metric.poisoned
    assert diagnostics == ()


def test_parsed_project_groups_members_and_resolved_project_defaults() -> None:
    from snowflake_semantic_tools.domain.model.project import ParsedProject, ResolvedProject

    member = ParsedMember("metric", "m", Origin("metrics.yml"), object(), ("orders",))
    parsed = ParsedProject((), (member,))
    assert parsed.members_by_type == {"metric": (member,)}
    resolved = ResolvedProject((), {})
    assert resolved.custom_instruction_names == {}


def test_attachment_handles_inference_poisoning_view_names_and_metric_dependencies() -> None:
    metric = ParsedMember(
        "metric",
        "base",
        Origin("metrics.yml"),
        object(),
        None,
        scan_template_calls("COUNT({{ ref('orders', 'id') }})"),
    )
    derived = ParsedMember("metric", "derived", Origin("metrics.yml"), object(), None)
    instruction = ParsedMember("custom_instruction", "rules", Origin("instructions.yml"), object(), None)
    poisoned = ParsedMember("filter", "bad", Origin("filters.yml"), object(), ("orders",), poisoned=True)
    views = {"semantic_view:sales": frozenset(("orders",))}
    result = attach_view_members(
        views,
        (metric, derived, instruction, poisoned),
        SEMANTIC_REGISTRY,
        view_named_members={"semantic_view:sales": frozenset(("rules",))},
        metric_dependencies={derived.key: (metric.key,)},
    )
    assert result[metric.key] == ("semantic_view:sales",)
    assert result[derived.key] == ("semantic_view:sales",)
    assert result[instruction.key] == ("semantic_view:sales",)
    assert result[poisoned.key] == ()
    assert effective_tables(metric) == frozenset(("orders",))


def test_metric_dependencies_do_not_widen_declared_table_attachment() -> None:
    base = ParsedMember("metric", "base", Origin("metrics.yml"), object(), ("orders",))
    constrained = ParsedMember("metric", "constrained", Origin("metrics.yml"), object(), ("customers",))
    views = {
        "semantic_view:orders": frozenset(("orders",)),
        "semantic_view:sales": frozenset(("orders", "customers")),
    }
    result = attach_view_members(
        views,
        (base, constrained),
        SEMANTIC_REGISTRY,
        metric_dependencies={constrained.key: (base.key,)},
    )
    assert result[constrained.key] == ("semantic_view:sales",)

    impossible = ParsedMember("metric", "impossible", Origin("metrics.yml"), object(), ("missing",))
    result = attach_view_members(
        views,
        (base, impossible),
        SEMANTIC_REGISTRY,
        metric_dependencies={impossible.key: (base.key,)},
    )
    assert result[impossible.key] == ()
