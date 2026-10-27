"""SST-VAL208: an equality join reaches a table whose grain is finer than one row per joined key."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.semantic_view import Relationship
from snowflake_semantic_tools.domain.validate.semantic.joins import key_diagnostic
from tests.helpers.semantic_members import BALANCES


def test_sst_val208_fires() -> None:
    partial = Relationship("ACCOUNTS_TO_BALANCES", "ACCOUNTS", ("ACCOUNT_ID",), "BALANCES", ("ACCOUNT_ID",))
    found = key_diagnostic(partial, BALANCES, subject="relationship:accounts_to_balances", origin=None)
    assert found is not None and found.code == "SST-VAL208"
    assert found.severity is Severity.WARNING
    assert found.message == (
        "relationship 'accounts_to_balances' joins 'accounts' to 'balances', whose grain is finer than one row per key"
    )
    assert found.subject == "relationship:accounts_to_balances"


def test_sst_val208_silent() -> None:
    whole = Relationship("X_TO_BALANCES", "X", ("ACCOUNT_ID", "AS_OF"), "BALANCES", ("ACCOUNT_ID", "AS_OF"))
    assert key_diagnostic(whole, BALANCES, subject="relationship:x_to_balances", origin=None) is None
