"""The Snowflake signature table: most specific match first, fragile rows listed, diagnostics filled."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.diagnostics.signatures import (
    SIGNATURES,
    UNRECOGNISED,
    SessionFailure,
    detail_of,
    fragile_signatures,
    match_signature,
    session_failure,
    signature_codes,
    snowflake_diagnostic,
)


def test_a_number_beats_a_sqlstate_and_a_sqlstate_beats_the_wording() -> None:
    assert match_signature("Schema 'A.B' does not exist", errno=2002).code == "SST-SNO002"
    assert match_signature("Schema 'A.B' does not exist", sqlstate="42710").code == "SST-SNO002"
    assert match_signature("Schema 'A.B' does not exist").code == "SST-SNO005"
    assert match_signature("x", errno=123456, sqlstate="ZZZZZ") is UNRECOGNISED


def test_a_fragile_row_has_neither_a_number_nor_a_sqlstate() -> None:
    fragile = fragile_signatures()
    assert fragile and all(row.errno is None and row.sqlstate is None for row in fragile)
    assert {"SST-SNO006", "SST-SNO007", "SST-SNO008"} <= {row.code for row in fragile}
    assert not any(row.fragile for row in SIGNATURES if row.errno is not None)


def test_the_detail_drops_the_number_and_the_heading() -> None:
    assert detail_of("001003 (42000): SQL compilation error:\nsyntax error") == "syntax error"
    assert detail_of("plain") == "plain"


def test_a_diagnostic_names_the_statement_target_when_the_message_quotes_nothing() -> None:
    diagnostic = snowflake_diagnostic("SST-SNO002", "already exists", value="DB.S.V", subject="semantic_view:v")
    assert diagnostic.message == "DB.S.V already exists" and diagnostic.subject == "semantic_view:v"
    assert dict(diagnostic.context) == {"value": "DB.S.V"}
    assert snowflake_diagnostic("SST-SNO008", "No active warehouse", value="x").message == (
        "no active warehouse in the session"
    )


def test_only_a_signature_code_reports_as_a_snowflake_refusal() -> None:
    assert "SST-SNO001" in signature_codes() and "SST-APL001" not in signature_codes()
    with pytest.raises(KeyError):
        snowflake_diagnostic("SST-APL001", "x", value="y")


def test_a_shared_number_matches_only_with_its_wording() -> None:
    login = "250001 (08001): Failed to connect to DB: Incorrect username or password was specified."
    assert match_signature(login, errno=250001, sqlstate="08001").code == "SST-SNO013"
    network = "250001 (08001): Failed to connect to DB: Could not connect to Snowflake backend"
    assert match_signature(network, errno=250001, sqlstate="08001").code == "SST-SNO014"
    assert match_signature("JWT token is invalid", errno=390144).code == "SST-SNO013"


@pytest.mark.parametrize(
    ("message", "errno", "sqlstate", "failure"),
    [
        ("Incorrect username or password was specified.", 390100, None, SessionFailure.AUTHENTICATION),
        ("Object 'DB.S.T' does not exist or not authorized.", 2003, "02000", SessionFailure.NOT_VISIBLE),
        ("Schema 'DB.S' does not exist or not authorized.", None, None, SessionFailure.NOT_VISIBLE),
        ("Database 'DB' does not exist or not authorized.", None, None, SessionFailure.NOT_VISIBLE),
        ("Warehouse 'WH' does not exist or not authorized.", None, None, None),
        ("Statement reached its statement or warehouse timeout of 10 seconds", None, None, SessionFailure.DEADLINE),
        ("Connection is closed", 250002, "08003", SessionFailure.DEADLINE),
        ("network drop", None, "08006", SessionFailure.DEADLINE),
        ("Insufficient privileges to operate on schema 'S'", None, "42501", SessionFailure.PRIVILEGE),
        ("SQL compilation error", None, "42000", None),
    ],
)
def test_the_session_reads_a_failure_from_the_table(
    message: str, errno: int | None, sqlstate: str | None, failure: SessionFailure | None
) -> None:
    assert session_failure(message, errno=errno, sqlstate=sqlstate) is failure
