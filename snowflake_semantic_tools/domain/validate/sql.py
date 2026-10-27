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
    stage_path,
)

# Any stage, to ask `stage_path` whether a name is a segment it writes unquoted.
_ANY_STAGE = QualifiedName.from_parts("DB", "SCHEMA", "STAGE")


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


def quoted_name_problem(value: str, *, artifact: str, subject: str, origin: Origin | None = None) -> Diagnostic | None:
    """Report a published object's name written quoted; None for one written unquoted or unparsed.

    SST keys a tool or agent by its name casefolded, and an object it observes by the name SHOW
    prints, casefolded too unless Snowflake shows it quoted. The two agree for every unquoted
    name, which Snowflake stores upper-cased, and for no quoted one, which keeps its quotes and
    case: a plan would never find the object it published. A name that does not parse is
    `name_problem`'s to report.

    Diagnostics:
        SST-PRS005: the name is written quoted.
    """
    try:
        quoted = Identifier.parse(value).quoted
    except ValueError:
        return None
    return D("SST-PRS005", artifact=artifact, value=value, subject=subject, origin=origin) if quoted else None


def stage_segment_problem(
    value: str, *, artifact: str, subject: str, origin: Origin | None = None
) -> Diagnostic | None:
    """Report a name `stage_path` would refuse as one segment of a stage path; None when it writes it.

    An agent's specification is staged below a folder named by the agent, so a name no stage
    path can hold must be refused before plan renders the path.

    Diagnostics:
        SST-PRS005: the name is not a segment a stage path writes unquoted.
    """
    try:
        stage_path(_ANY_STAGE, f"{value}/")
    except ValueError:
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
