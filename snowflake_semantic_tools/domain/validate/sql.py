"""Turn the SQL guards' refusals into diagnostics, for loaders and validators to report.

A loader guards every authored expression and query, and checks every name it will render,
before it builds the model a renderer reads; these helpers give each refusal its diagnostic.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.sql import (
    AuthoredExpression,
    AuthoredQuery,
    UnsafeSqlError,
    guard_expression,
    guard_query,
    ident,
    qname,
)


def checked_expression(
    text: str, *, kind: str, name: str, subject: str, origin: Origin | None = None
) -> AuthoredExpression | Diagnostic:
    """Guard one authored expression, or return the diagnostic for the guard's refusal.

    Args:
        kind: The member type the diagnostic names, such as `metric` or `filter`.

    Diagnostics:
        SST-VAL418: the expression is not one expression SST can splice into a statement.
    """
    try:
        return guard_expression(text)
    except UnsafeSqlError as exc:
        return _refused(exc, kind=kind, name=name, subject=subject, origin=origin)


def checked_query(
    text: str, *, kind: str, name: str, subject: str, origin: Origin | None = None
) -> AuthoredQuery | Diagnostic:
    """Guard one authored query, or return the diagnostic for the guard's refusal.

    Diagnostics:
        SST-VAL418: the SQL is not one SELECT or WITH query.
    """
    try:
        return guard_query(text)
    except UnsafeSqlError as exc:
        return _refused(exc, kind=kind, name=name, subject=subject, origin=origin)


def _refused(exc: UnsafeSqlError, *, kind: str, name: str, subject: str, origin: Origin | None) -> Diagnostic:
    """The diagnostic for a refusal, quoting the checked text from where the guard stopped."""
    return D(
        "SST-VAL418",
        type=kind,
        name=name,
        detail=f"{exc.reason} at {exc.near!r}, so SST will not send it to Snowflake",
        subject=subject,
        origin=origin,
    )


def name_problem(value: str, *, artifact: str, subject: str, origin: Origin | None = None) -> Diagnostic | None:
    """Report a name that does not render as one Snowflake identifier; None when it does.

    The name is parsed as written: unquoted it must be a letter or underscore followed by
    letters, digits, `_`, or `$`; quoted, any text but a NUL, up to 255 characters.

    Diagnostics:
        SST-PRS005: the name is not a valid identifier.
    """
    try:
        ident(Identifier.parse(value))
    except (TypeError, ValueError):
        return D("SST-PRS005", artifact=artifact, value=value, subject=subject, origin=origin)
    return None


def qualified_name_problem(
    value: str, *, artifact: str, subject: str, origin: Origin | None = None
) -> Diagnostic | None:
    """Report a name that does not render as a three-part name of identifiers; None when it does.

    Diagnostics:
        SST-PRS005: the name is not three valid identifiers joined by dots.
    """
    try:
        qname(QualifiedName.parse(value))
    except (TypeError, ValueError):
        return D("SST-PRS005", artifact=artifact, value=value, subject=subject, origin=origin)
    return None
