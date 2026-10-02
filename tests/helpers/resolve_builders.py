"""Inputs for resolver and member-resolution tests: one dbt catalog, members, and a membership request."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtColumn, DbtModel
from snowflake_semantic_tools.domain.model.project import ParsedMember, SemanticViewProject
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
from tests.helpers.cli_projects import MANIFEST, project_copy
from tests.helpers.projects import load_project

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


def coded(diagnostics: DiagnosticBag | tuple[Diagnostic, ...], code: str) -> list[Diagnostic]:
    """The diagnostics of one code, in order."""
    return [item for item in diagnostics if item.code == code]


def edited_fixture(tmp_path: Path, file: str, before: str, after: str) -> SemanticViewProject:
    """Load the reference project with one edit to one of its files: the first `before` becomes `after`."""
    project = project_copy(tmp_path)
    path = project / file
    text = path.read_text(encoding="utf-8")
    assert before in text, before
    path.write_text(text.replace(before, after, 1), encoding="utf-8")
    return load_project(project, manifest_path=MANIFEST)


def fixture_with(tmp_path: Path, file: str, text: str) -> SemanticViewProject:
    """Load the reference project with one more file in it."""
    project = project_copy(tmp_path)
    (project / file).write_text(text, encoding="utf-8")
    return load_project(project, manifest_path=MANIFEST)


def reference_fixture(tmp_path: Path) -> SemanticViewProject:
    """Load the reference project as committed."""
    return load_project(project_copy(tmp_path), manifest_path=MANIFEST)
