"""`sst enrich`'s flag resolution and its derivation rules."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.enrich import (
    COMPONENT_NAMES,
    DEFAULT_COMPONENTS,
    Component,
    EnrichOptions,
    SampleDecision,
    clean_synonyms,
    collection_refusal,
    decide_samples,
    derive_column_type,
    is_sampled_type,
    parse_components,
    resolve_options,
    same_data_type,
    semantic_data_type,
    taken_names,
    usable_sample,
)

C = Component


def test_flag_values_expand_comma_lists_and_groups_and_report_unknown_names_once() -> None:
    components, unknown = parse_components(["Column-Types, data-types", " synonyms ,", "bogus", "BOGUS,,"])
    assert components == {C.COLUMN_TYPES, C.DATA_TYPES, C.COLUMN_SYNONYMS, C.TABLE_SYNONYMS}
    assert unknown == ("bogus",)
    assert parse_components(["all"])[0] == frozenset(Component)
    assert COMPONENT_NAMES[-2:] == ("synonyms", "all")


def test_options_default_and_forcing_includes_and_enums_include_sample_values() -> None:
    assert resolve_options(frozenset(), frozenset()) == EnrichOptions(DEFAULT_COMPONENTS)
    options = resolve_options(frozenset({C.ENUMS}), frozenset({C.COLUMN_TYPES}))
    assert options.components == {C.ENUMS, C.SAMPLE_VALUES, C.COLUMN_TYPES}
    assert options.forced == {C.COLUMN_TYPES}
    assert options.includes(C.SAMPLE_VALUES) and not options.includes(C.DATA_TYPES)
    assert options.forces(C.COLUMN_TYPES) and not options.forces(C.ENUMS)
    assert options.ordered == (C.COLUMN_TYPES, C.SAMPLE_VALUES, C.ENUMS)
    assert options.reads_data and not resolve_options(frozenset(), frozenset()).reads_data


def test_collection_is_refused_only_when_the_run_reads_data_and_the_project_forbids_it() -> None:
    reading = resolve_options(frozenset({C.ENUMS}), frozenset())
    assert collection_refusal(reading, allowed=True) is None
    assert collection_refusal(EnrichOptions(DEFAULT_COMPONENTS), allowed=False) is None
    refusal = collection_refusal(reading, allowed=False)
    assert refusal is not None and refusal.code == "SST-CFG038"
    assert refusal.message.startswith("--include sample-values enums reads row data")
    assert refusal.subject == "config:enrichment.allow_sample_value_collection"


def test_data_types_fold_into_their_information_schema_family() -> None:
    assert [semantic_data_type(t) for t in ("varchar(16)", "bigint", "double", "bool", "datetime", "variant")] == [
        "TEXT",
        "NUMBER",
        "FLOAT",
        "BOOLEAN",
        "TIMESTAMP_NTZ",
        "VARIANT",
    ]
    assert same_data_type("string", "TEXT") and same_data_type("number(38,0)", "INTEGER")
    assert not same_data_type("TEXT", "NUMBER")


def test_a_numeric_column_is_a_fact_unless_it_is_a_key() -> None:
    assert derive_column_type("NUMBER", key=False) == "fact"
    assert derive_column_type("NUMBER(38,0)", key=True) == "dimension"
    assert derive_column_type("TIMESTAMP_NTZ", key=False) == "dimension"
    assert is_sampled_type("TIMESTAMP_TZ") and is_sampled_type("varchar") and not is_sampled_type("VARIANT")


def test_only_short_single_line_values_that_are_not_placeholders_are_usable() -> None:
    refused = ("", "  ", " nan ", "NULL", "x" * 501, "{{ ref('x') }}", "a{%b", "{# c", "two\nlines", "bell\x07")
    assert not any(usable_sample(value) for value in refused)
    assert all(usable_sample(value) for value in ("returned", "tab\tseparated", "x" * 500, "café"))


def test_samples_are_complete_only_when_every_distinct_value_was_read_and_is_usable() -> None:
    assert decide_samples(["b", "a", "b"], distinct_limit=3, display_limit=1) == SampleDecision(("b", "a"), True)
    over = decide_samples(["d", "c", "b", "a"], distinct_limit=3, display_limit=2)
    assert over == SampleDecision(("d", "c"), False)
    refused = decide_samples(["a", "nan"], distinct_limit=3, display_limit=2)
    assert refused == SampleDecision(("a",), False)
    assert decide_samples([], distinct_limit=3, display_limit=2) == SampleDecision((), True)


def test_synonyms_are_cleaned_capped_and_kept_apart_from_names_already_used() -> None:
    proposals = [
        "Order  Total",
        "order total",
        7,
        "",
        "x" * 101,
        "client" + chr(39) + "s total",
        "{{ total }}",
        "total_amount",
        "total amount",
        "revenue",
        "gross",
        "net",
    ]
    taken = taken_names(["revenue"], ["Gross"])
    assert taken == {"revenue", "gross"}
    kept = clean_synonyms(proposals, name="total_amount", taken=taken, limit=2)
    assert kept == ("Order Total", "net")
    assert clean_synonyms(["a", "b"], name="c", taken=(), limit=1) == ("a",)
    assert taken_names(["order_items"], []) == {"order_items", "order items"}
