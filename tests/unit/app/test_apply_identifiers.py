"""Grant replay names each grantee exactly as SHOW GRANTS printed it, whatever the name holds."""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from snowflake_semantic_tools.app.apply.one import _grantee_identifier, _simple_identifier
from snowflake_semantic_tools.domain.model.identifier import Identifier

# Names as SHOW prints them: anything at all, with the characters that could end or split an
# identifier, and the shapes of names that are valid unquoted, drawn often.
NAMES = st.one_of(
    st.text(),
    st.text(alphabet=st.sampled_from("\"'.;- aZ_$09\n"), min_size=1),
    st.from_regex(r"[A-Z_][A-Z0-9_$]{0,10}", fullmatch=True),
)


@given(NAMES)
def test_any_name_round_trips_to_the_object_show_printed(name: str) -> None:
    rendered = _simple_identifier(name)
    assert Identifier.parse(rendered).folded == name
    if rendered.startswith('"'):
        # Every quote inside is doubled, so none can close the identifier early.
        assert '"' not in rendered[1:-1].replace('""', "")
    else:
        assert rendered == name


def test_a_lowercase_or_mixed_case_name_is_quoted_so_it_is_not_upper_cased() -> None:
    assert _simple_identifier("analyst") == '"analyst"'
    assert _simple_identifier("ANALYST") == "ANALYST"
    assert _simple_identifier('a"; DROP ROLE X; --') == '"a""; DROP ROLE X; --"'
    assert _simple_identifier("") == '""'
    assert _grantee_identifier("DB.reader") == 'DB."reader"'
