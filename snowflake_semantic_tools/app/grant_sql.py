"""The GRANT that gives an object's explicit grant back, spelled from what SHOW GRANTS printed.

Apply replays these after replacing an object whose replace drops its grants, and a captured
prior appends them, so running the prior keeps the grants as apply does.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import GrantRow
from snowflake_semantic_tools.domain.sql import Sql, ident, join, keyword, privilege, qname, sql


def grant_statement(object_type: str, target: QualifiedName, grant: GrantRow) -> Sql:
    """Render the GRANT that restores one explicit grant, spelling a database role as Snowflake does.

    Raises:
        ValueError: the privilege, object type, or grantee is not one SQL can spell, as the
            constructors of `domain.sql` refuse.
    """
    grantee_kind = "DATABASE ROLE" if grant.granted_to.upper() == "DATABASE_ROLE" else grant.granted_to
    return sql(
        "GRANT {privilege} ON {kind} {target} TO {grantee_kind} {grantee}{grant_option}",
        privilege=privilege(grant.privilege),
        kind=keyword(object_type),
        target=qname(target),
        grantee_kind=keyword(grantee_kind),
        grantee=_grantee_identifier(grant.grantee_name),
        grant_option=sql(" WITH GRANT OPTION") if grant.grant_option else sql(""),
    )


def _grantee_identifier(value: str) -> Sql:
    """Render a grantee as SHOW GRANTS printed it; a name of two or three dotted parts part by part."""
    parts = value.split(".")
    if len(parts) in (2, 3):
        return join(".", (_simple_identifier(part) for part in parts))
    return _simple_identifier(value)


def _simple_identifier(value: str) -> Sql:
    """Render one name part as SHOW GRANTS printed it, naming the same object.

    A part Snowflake would store as printed, upper case and valid unquoted, is written bare;
    any other, one with a lowercase letter, a space, or a quote, is quoted exactly with each
    inner quote doubled, so no name can close the identifier early.

    Raises:
        ValueError: the part is empty or is a name no identifier can spell, as `ident` refuses.
    """
    try:
        return ident(Identifier.shown(value))
    except ValueError:
        return ident(Identifier(value, quoted=True))
