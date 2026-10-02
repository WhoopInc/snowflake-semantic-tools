"""Grant replay names each grantee exactly as SHOW GRANTS printed it, whatever the name holds."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from snowflake_semantic_tools.app.apply.one import _grantee_identifier, _simple_identifier
from snowflake_semantic_tools.domain.model.identifier import Identifier

# Names as SHOW prints them: any text an identifier can hold, with the characters that could
# end or split an identifier, and the shapes of names that are valid unquoted, drawn often.
_SPELLABLE = st.text(
    alphabet=st.characters(blacklist_categories=["Cs"], blacklist_characters="\x00"),
    min_size=1,
    max_size=255,
)
NAMES = st.one_of(
    _SPELLABLE,
    st.text(alphabet=st.sampled_from("\"'.;- aZ_$09\n"), min_size=1),
    st.from_regex(r"[A-Z_][A-Z0-9_$]{0,10}", fullmatch=True),
)


@given(NAMES)
def test_any_name_round_trips_to_the_object_show_printed(name: str) -> None:
    rendered = str(_simple_identifier(name))
    assert Identifier.parse(rendered).folded == name
    if rendered.startswith('"'):
        # Every quote inside is doubled, so none can close the identifier early.
        assert '"' not in rendered[1:-1].replace('""', "")
    else:
        assert rendered == name


def test_a_lowercase_or_mixed_case_name_is_quoted_so_it_is_not_upper_cased() -> None:
    assert str(_simple_identifier("analyst")) == '"analyst"'
    assert str(_simple_identifier("ANALYST")) == "ANALYST"
    assert str(_simple_identifier('a"; DROP ROLE X; --')) == '"a""; DROP ROLE X; --"'
    assert str(_grantee_identifier("DB.reader")) == 'DB."reader"'


@pytest.mark.parametrize("name", ["", "a\x00b", "x" * 256])
def test_a_name_no_identifier_can_spell_is_refused_rather_than_rendered(name: str) -> None:
    with pytest.raises(ValueError):
        _simple_identifier(name)
