"""SST-SNO001, as a property: an error no signature matches is SST-SNO001, carrying its text, and nothing else."""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from snowflake_semantic_tools.domain.diagnostics.signatures import (
    SIGNATURES,
    UNRECOGNISED,
    match_signature,
    snowflake_diagnostic,
)

_NUMBERS = {row.errno for row in SIGNATURES if row.errno is not None}
_STATES = {row.sqlstate for row in SIGNATURES if row.sqlstate is not None}

# Text no signature's wording can match: digits, spaces, and punctuation hold no word a pattern needs.
unmatched_text = st.text(alphabet="0123456789 .,;:-_()[]#", min_size=1, max_size=60).filter(str.strip)
unknown_number = st.one_of(st.none(), st.integers(min_value=-1, max_value=999_999).filter(lambda n: n not in _NUMBERS))
unknown_state = st.one_of(st.none(), st.from_regex(r"\A[0-9A-Z]{5}\Z").filter(lambda s: s not in _STATES))


@given(unmatched_text, unknown_number, unknown_state)
def test_an_unmatched_error_is_sno001_with_its_text(message: str, errno: int | None, sqlstate: str | None) -> None:
    signature = match_signature(message, errno=errno, sqlstate=sqlstate)
    assert signature is UNRECOGNISED
    diagnostic = snowflake_diagnostic(signature.code, message, value="DB.S.V")
    assert diagnostic.code == "SST-SNO001"
    assert diagnostic.message.startswith("Snowflake refused: ")
    assert "SST-" not in diagnostic.message


@given(st.text(max_size=80), st.one_of(st.none(), st.integers()), st.one_of(st.none(), st.text(max_size=6)))
def test_every_error_maps_to_exactly_one_registered_signature(
    message: str, errno: int | None, sqlstate: str | None
) -> None:
    signature = match_signature(message, errno=errno, sqlstate=sqlstate)
    assert signature in SIGNATURES or signature is UNRECOGNISED
    assert match_signature(message, errno=errno, sqlstate=sqlstate) is signature
