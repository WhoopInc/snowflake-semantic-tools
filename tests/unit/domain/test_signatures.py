"""The Snowflake signature table: most specific match first, fragile rows listed, diagnostics filled."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.diagnostics.signatures import (
    SIGNATURES,
    UNRECOGNISED,
    detail_of,
    fragile_signatures,
    match_signature,
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
