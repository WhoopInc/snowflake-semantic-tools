"""Pure config readers: deferred relations, the `dbt:` block, and typed target conditionals."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.config_schema import DbtSettings, dbt_settings
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtModel, DbtSource
from snowflake_semantic_tools.domain.resolve.config import render_config
from snowflake_semantic_tools.domain.resolve.defer import defer_relations


def _model(unique_id: str, relation: str) -> DbtModel:
    return DbtModel(unique_id, unique_id.rsplit(".", 1)[-1], relation, (), (), (), raw_relation_name=relation.lower())


def test_defer_relations_takes_the_deferred_relation_of_each_node_both_hold() -> None:
    current = DbtCatalog(
        "v12",
        None,
        "p",
        (_model("model.p.orders", "DEV.S.ORDERS"), _model("model.p.new", "DEV.S.NEW")),
        sources=(
            DbtSource("source.p.raw.a", "raw", "a", "DEV.RAW.A"),
            DbtSource("source.p.raw.b", "raw", "b", "DEV.RAW.B"),
        ),
    )
    deferred = DbtCatalog(
        "v12",
        None,
        "p",
        (_model("model.p.orders", "PROD.S.ORDERS"),),
        sources=(DbtSource("source.p.raw.a", "raw", "a", "PROD.RAW.A"),),
    )
    result = defer_relations(current, deferred)
    assert [(model.relation_name, model.raw_relation_name) for model in result.models] == [
        ("PROD.S.ORDERS", "prod.s.orders"),
        ("DEV.S.NEW", "dev.s.new"),
    ]
    assert [source.relation_name for source in result.sources] == ["PROD.RAW.A", "DEV.RAW.B"]


def test_the_dbt_block_reads_its_defaults_and_its_values() -> None:
    assert dbt_settings({}) == DbtSettings()
    block = {"dbt": {"invoke": False, "command": "compile", "manifest_schema_versions": [11, 12, True, "13"]}}
    assert dbt_settings(block) == DbtSettings(False, "compile", frozenset((11, 12)))
    assert dbt_settings({"dbt": {"command": 3, "manifest_schema_versions": []}}) == DbtSettings()
    assert dbt_settings({"dbt": {"command": "run", "manifest_schema_versions": "12"}}) == DbtSettings()


def _chosen(value: str) -> str:
    return f"{{{{ '{value}' if target.name == 'prod' else 'other' }}}}"


def test_a_conditional_literal_is_read_as_its_key_type() -> None:
    tree = {
        "validation": {"strict": _chosen("TRUE"), "snowflake_syntax_check": _chosen("maybe")},
        "enrichment": {"distinct_limit": _chosen("30"), "synonym_max_count": _chosen("3.5")},
        "semantic_views": {"core": {"+enabled": _chosen("false")}},
        "diagnostics": {"severity_overrides": {"SST-CFG018": _chosen("error")}},
        "project": {"semantic_models_dir": _chosen("models")},
        "unknown": {"key": _chosen("true")},
        "plain": "true",
    }
    rendered, diagnostics = render_config(tree, target_name="prod", file="sst_config.yml")
    assert diagnostics == ()
    assert rendered["validation"] == {"strict": True, "snowflake_syntax_check": "maybe"}
    assert rendered["enrichment"] == {"distinct_limit": 30, "synonym_max_count": "3.5"}
    assert rendered["semantic_views"] == {"core": {"+enabled": False}}
    assert rendered["diagnostics"] == {"severity_overrides": {"SST-CFG018": "error"}}
    assert rendered["project"] == {"semantic_models_dir": "models"}
    assert (rendered["unknown"], rendered["plain"]) == ({"key": "true"}, "true")
