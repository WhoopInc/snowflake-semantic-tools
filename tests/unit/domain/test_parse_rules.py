"""The parse-level rules, called directly: unknown fields, risky names, passthrough keys, root keys, records."""

from __future__ import annotations

from dataclasses import dataclass

from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.parse.fields import unknown_field
from snowflake_semantic_tools.domain.parse.names import identifier_problem, name_warnings
from snowflake_semantic_tools.domain.parse.passthrough import AGENT_SPEC_KEYS, passthrough_diagnostics
from snowflake_semantic_tools.domain.parse.records import (
    is_frozen_record,
    mutable_records,
    registered_root_keys,
    root_key_diagnostics,
)

ORIGIN = Origin("agents/a/agent.yml")


@dataclass(frozen=True)
class _Frozen:
    value: int


@dataclass
class _Mutable:
    value: int


def test_an_unknown_field_close_to_a_known_one_names_it_with_its_prefix() -> None:
    typo = unknown_field("colum", ("column", "direction"), label="window.order_by[0].colum", artifact="metric:m")
    assert (typo.code, typo.context["expected"]) == ("SST-PRS022", "window.order_by[0].column")
    other = unknown_field("colum", ("column",), label="renamed", artifact="metric:m")
    assert other.context["expected"] == "column"
    unknown = unknown_field("zzz", ("column",), artifact="metric:m")
    assert (unknown.code, unknown.context["field"]) == ("SST-PRS004", "zzz")


def test_a_risky_name_is_reported_for_each_risk_in_order() -> None:
    codes = [item.code for item in name_warnings("Snowflake_ünits", subject="metric:m")]
    assert codes == ["SST-PRS012", "SST-PRS100"]
    assert [item.code for item in name_warnings("select", subject="metric:m")] == ["SST-PRS031"]
    assert [item.code for item in name_warnings("SNOWFLAKE", subject="metric:m")] == ["SST-PRS100"]
    assert name_warnings("revenue", subject="metric:m") == ()


def test_a_name_valid_only_once_quoted_is_its_own_code() -> None:
    assert identifier_problem("revenue", artifact="m", subject="metric:m") is None
    quoted_only = identifier_problem("total revenue", artifact="m", subject="metric:m")
    assert quoted_only is not None and quoted_only.code == "SST-PRS011"
    already = identifier_problem('"total revenue', artifact="m", subject="metric:m")
    assert already is not None and already.code == "SST-PRS005"


def test_a_passthrough_block_names_each_key_sst_renders_then_counts_the_rest() -> None:
    found = passthrough_diagnostics(
        "agent:a", {"models": {}, "extra": 1, "other": 2}, AGENT_SPEC_KEYS, subject="agent:a", origin=ORIGIN
    )
    assert [(item.code, item.context.get("key"), item.context.get("count")) for item in found] == [
        ("SST-PRS023", "models", None),
        ("SST-PRS024", None, 2),
    ]
    assert (
        passthrough_diagnostics("agent:a", {"tools": []}, AGENT_SPEC_KEYS, subject="agent:a", origin=ORIGIN)[0].code
        == "SST-PRS023"
    )


def test_root_keys_no_type_owns_are_reported_one_by_one_or_as_the_whole_document() -> None:
    known = registered_root_keys(SEMANTIC_REGISTRY)
    assert "semantic_views" in known and "snowflake_metrics" in known
    whole = root_key_diagnostics("x.yml", ("dashboards",), known, lambda key: 3)
    assert [item.code for item in whole] == ["SST-LOD021"]
    partly = root_key_diagnostics("x.yml", ("semantic_views", "dashboards"), known, lambda key: 7)
    assert [(item.code, item.origin) for item in partly] == [("SST-PRS001", Origin("x.yml", 7))]
    assert root_key_diagnostics("x.yml", (), known, lambda key: None) == ()


def test_a_parsed_record_must_be_a_frozen_dataclass_or_a_tuple_of_them() -> None:
    assert is_frozen_record(_Frozen(1)) and is_frozen_record((_Frozen(1), (_Frozen(2),)))
    assert not is_frozen_record(_Mutable(1)) and not is_frozen_record(_Frozen) and not is_frozen_record({})
    found = mutable_records("metric", (_Frozen(1), _Mutable(1), _Mutable(2), {"a": 1}))
    assert [item.context["cls"] for item in found] == ["_Mutable", "dict"]
