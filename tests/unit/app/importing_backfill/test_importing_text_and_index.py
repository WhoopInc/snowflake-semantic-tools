"""What an import reads of SHOW and DESCRIBE text, and how it indexes the project it maps into."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.base import CompileResult
from snowflake_semantic_tools.app.importing_index import folder_below, project_index
from snowflake_semantic_tools.app.importing_text import (
    argument_types,
    parse_manifest,
    parse_profile,
    parse_secure,
    parse_signature,
)
from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtSource
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolMember, ToolOwnership
from tests.helpers.import_fakes import dbt_model


def test_a_routines_argument_types_are_read_from_shows_arguments_upper_cased() -> None:
    assert argument_types("F(varchar, number) RETURN VARCHAR") == ("VARCHAR", "NUMBER")
    assert argument_types("F(VARCHAR, NUMBER) RETURN VARCHAR") == ("VARCHAR", "NUMBER")
    assert argument_types("F() RETURN VARCHAR") == ()


def test_a_signature_pairs_each_argument_name_with_its_type_when_every_type_is_named() -> None:
    assert parse_signature("(ORDER_ID varchar, N NUMBER)", ("VARCHAR", "NUMBER")) == (
        ("ORDER_ID", "VARCHAR"),
        ("N", "NUMBER"),
    )
    assert parse_signature("ORDER_ID VARCHAR", ("VARCHAR",)) == (("ORDER_ID", "VARCHAR"),)


def test_a_signature_that_does_not_name_every_argument_reads_as_none() -> None:
    assert parse_signature("(ORDER_ID, N NUMBER)", ("VARCHAR", "NUMBER")) == ()
    assert parse_signature("()", ()) == ()


def test_a_plugin_manifest_reads_only_when_it_is_a_json_object() -> None:
    assert parse_manifest(b'{"name": "ops"}') == {"name": "ops"}
    assert parse_manifest(None) == {}
    assert parse_manifest(b"") == {}
    assert parse_manifest(b"not json") == {}
    assert parse_manifest(b"\xff\xfe") == {}
    assert parse_manifest(b"[1, 2]") == {}


def test_an_agents_profile_reads_only_when_it_is_a_json_object() -> None:
    assert parse_profile('{"display_name": "Sales"}') == {"display_name": "Sales"}
    assert parse_profile(None) == {}
    assert parse_profile("") == {}
    assert parse_profile("{oops") == {}
    assert parse_profile('"text"') == {}


def test_is_secure_reads_true_yes_and_y_as_secure_and_blank_as_unseen() -> None:
    assert parse_secure(" TRUE ") is True
    assert parse_secure("Y") is True
    assert parse_secure("false") is False
    assert parse_secure(None) is None
    assert parse_secure("  ") is None


def test_a_source_outside_the_types_directory_keeps_its_whole_folder() -> None:
    assert folder_below("elsewhere/team/sales.yml", "semantic_models", "sales", own=False) == ("elsewhere", "team")
    assert folder_below("agents/team/bot/agent.yml", "agents", "bot", own=True) == ("team",)


def test_the_index_maps_source_relations_and_skips_names_that_do_not_parse() -> None:
    dbt = DbtCatalog(
        "v12",
        None,
        "p",
        (dbt_model("orders", "DB.MARTS.ORDERS"), dbt_model("broken", "not a relation")),
        sources=(
            DbtSource("source.p.raw.events", "raw", "events", "DB.RAW.EVENTS"),
            DbtSource("source.p.raw.unbound", "raw", "unbound", None),
        ),
    )
    origin = Origin("tools/t.yml")
    tools = ToolCatalog(
        (
            ToolMember(
                "faq",
                "cortex_search_service",
                ToolOwnership.REFERENCE,
                origin,
                "tools/t.yml",
                relations={"dev": "DB.T.FAQ"},
            ),
            ToolMember(
                "odd", "cortex_search_service", ToolOwnership.REFERENCE, origin, "tools/t.yml", relations={"dev": "x"}
            ),
            ToolMember("own", "procedure", ToolOwnership.DEFINE, origin, "tools/t.yml"),
        ),
        "dev",
        frozenset({"dev"}),
    )
    index = project_index(CompileResult(()), dbt, tools, "dev", {})
    assert dict(index.sources) == {("DB", "RAW", "EVENTS"): ("raw", "events")}
    assert dict(index.models) == {("DB", "MARTS", "ORDERS"): "orders"}
    assert [(item.name, item.relation.sql) for item in index.tools.values()] == [("faq", "DB.T.FAQ")]
    assert set(index.dbt_models) == {"orders", "broken"}
