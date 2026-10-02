"""`explain`: a registered code, an SST 0.3 alias, a retired number, and text that is none of them."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.explain import explain
from snowflake_semantic_tools.domain.diagnostics.specs.aliases import ALIASES, TOMBSTONES
from snowflake_semantic_tools.domain.diagnostics.specs.retired import RETIRED_CODES


def test_a_registered_code_reports_its_entry_in_any_case() -> None:
    explanation = explain(" sst-cfg033 ")
    assert explanation is not None
    assert (explanation.code, explanation.kind, explanation.severity) == ("SST-CFG033", "code", "error")
    assert explanation.non_demotable and not explanation.retired
    assert explanation.phase == "cfg"
    assert explanation.help_url.endswith("#sst-cfg033")


def test_a_registered_code_lists_the_0_3_codes_that_resolve_to_it() -> None:
    explanation = explain("SST-REF001")
    assert explanation is not None
    assert "SST-V002" in explanation.origin
    assert [row.code for row in explanation.aliases] == list(explanation.origin)


def test_an_alias_names_what_it_became() -> None:
    explanation = explain("sst-v002")
    assert explanation is not None
    assert (explanation.kind, explanation.title) == ("alias", "Unknown table reference")
    [row] = explanation.aliases
    assert row.targets == ("SST-DBT002", "SST-REF001", "SST-MEM003")
    assert explanation.help_url.endswith("#sst-dbt002")


def test_an_alias_retired_with_no_successor_points_at_itself() -> None:
    explanation = explain("SST-V081")
    assert explanation is not None
    assert explanation.aliases[0].targets == ()
    assert explanation.help_url.endswith("#sst-v081")


def test_a_retired_number_answers_from_its_tombstone() -> None:
    explanation = explain("SST-PRT007")
    assert explanation is not None
    assert explanation.retired
    assert (explanation.superseded_by, explanation.retired_in) == ("SST-DBT017", "1.0.0")
    assert explanation.severity is None


def test_unknown_text_is_not_explained() -> None:
    assert explain("SST-XYZ999") is None
    assert explain("") is None


def test_every_retired_number_has_a_tombstone_and_every_alias_a_live_successor() -> None:
    assert set(TOMBSTONES) == set(RETIRED_CODES)
    for alias in ALIASES.values():
        for target in alias.targets:
            explained = explain(target)
            assert explained is not None and explained.kind == "code", (alias.code, target)
