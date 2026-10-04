"""Inputs for resolver and member-resolution tests: one dbt catalog, members, and a membership request."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Origin
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtColumn, DbtModel
from snowflake_semantic_tools.domain.model.project import ParsedMember
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.parse.template import scan_template_calls
from snowflake_semantic_tools.domain.resolve.membership import MembershipResult, resolve_membership
from snowflake_semantic_tools.domain.resolve.membership_model import MemberFacts, MembershipRequest
from snowflake_semantic_tools.domain.resolve.template import (
    METRIC_EXPR,
    RefPolicy,
    ResolveContext,
    Resolved,
    resolve_scalar,
)

ORIGIN = Origin("semantic_models/metrics/metrics.yml", 3, 5)

CATALOG = DbtCatalog(
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


def resolved(text: str, policy: RefPolicy = METRIC_EXPR, **context: Any) -> tuple[Resolved, tuple[Diagnostic, ...]]:
    """Resolve `text` as metric `m`'s expression against `CATALOG` and the context fields given."""
    value, diagnostics = resolve_scalar(
        text,
        policy,
        ORIGIN,
        ResolveContext(CATALOG, **context),
        field="metric.expression",
        subject="metric:m",
    )
    return value, tuple(diagnostics)


def member(
    type_name: str,
    name: str,
    tables: tuple[str, ...] | None = None,
    expr: str = "",
    *,
    poisoned: bool = False,
) -> ParsedMember:
    """A parsed member of `type_name` with the declared tables and expression given."""
    return ParsedMember(
        type_name,
        name,
        Origin(f"semantic_models/{type_name}.yml", 1, 3),
        object(),
        tables,
        scan_template_calls(expr),
        poisoned,
    )


VIEWS: Mapping[str, frozenset[str]] = {
    "semantic_view:menu": frozenset(("orders", "products")),
    "semantic_view:sales": frozenset(("orders", "customers")),
}


def request(
    *members: ParsedMember,
    views: Mapping[str, frozenset[str]] = VIEWS,
    facts: Mapping[str, MemberFacts] | None = None,
    **fields: Any,
) -> MembershipRequest:
    """A membership request over `members` and `views`, reporting every view, with the fields given."""
    defaults: dict[str, Any] = {
        "reported_views": frozenset(views),
        "known_models": frozenset(("orders", "products", "customers", "suppliers")),
    }
    return MembershipRequest(
        tuple(members),
        views,
        SEMANTIC_REGISTRY,
        facts=facts or {},
        **{**defaults, **fields},
    )


def membership(*members: ParsedMember, **fields: Any) -> MembershipResult:
    """Resolve the membership of `members`; see `request` for the fields."""
    return resolve_membership(request(*members, **fields))
