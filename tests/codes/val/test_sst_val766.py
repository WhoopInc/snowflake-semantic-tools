"""SST-VAL766: an eval's agent lives in a personal database, where its objects cannot be created.

Fires for an agent in `USER$<user>.PUBLIC`; the same eval of an agent in an ordinary
database's schema stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.validate.eval import personal_database_placement
from tests.helpers.eval_builders import resolved_eval


def test_sst_val766_fires() -> None:
    [diagnostic] = personal_database_placement(
        resolved_eval(), QualifiedName.parse("USER$JDOE.PUBLIC.SALES_AGENT"), mints=True
    )
    assert (diagnostic.code, diagnostic.severity) == ("SST-VAL766", Severity.ERROR)
    assert diagnostic.message == (
        "eval 'sales_agent' would create its source table, dataset and config stage in USER$JDOE.PUBLIC, "
        "a personal database, which holds no table, dataset or stage"
    )
    assert diagnostic.subject == "eval:sales_agent"


def test_sst_val766_silent() -> None:
    target = QualifiedName.parse("DEV_DB.JDOE.SALES_AGENT")
    assert personal_database_placement(resolved_eval(), target, mints=True) == ()
