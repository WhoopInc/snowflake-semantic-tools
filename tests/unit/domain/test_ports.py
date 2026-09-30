"""Port protocols contain no hidden implementation."""

from __future__ import annotations

from snowflake_semantic_tools.domain.ports.eval_state import EvalStateStore
from snowflake_semantic_tools.domain.ports.lifecycle import CompositeLifecycleHandler
from snowflake_semantic_tools.domain.ports.semantic_view_source import SemanticViewSource
from snowflake_semantic_tools.domain.ports.snowflake import (
    ClockPort,
    SnowflakePort,
    SnowflakePortError,
    StateStore,
)


def test_semantic_view_source_protocol_methods_are_declarations_only() -> None:
    source = object()
    assert SemanticViewSource.load_project(source) is None  # type: ignore[arg-type]


def test_lifecycle_port_protocol_methods_are_declarations_only() -> None:
    source = object()
    assert SnowflakePort.show_objects(source, "x", object()) is None  # type: ignore[arg-type]
    assert SnowflakePort.show_grants(source, "x", object()) is None  # type: ignore[arg-type]
    assert SnowflakePort.describe_marker(source, object()) is None  # type: ignore[arg-type]
    assert SnowflakePort.query(source, "x") is None  # type: ignore[arg-type]
    assert SnowflakePort.execute_script(source, ()) is None  # type: ignore[arg-type]
    assert SnowflakePort.try_execute(source, "x") is None  # type: ignore[arg-type]
    assert SnowflakePort.current_role(source) is None  # type: ignore[arg-type]
    assert SnowflakePort.current_account_locator(source) is None  # type: ignore[arg-type]
    assert SnowflakePort.object_exists(source, "x", object()) is None  # type: ignore[arg-type]
    assert SnowflakePort.read_state(source, object(), "x") is None  # type: ignore[arg-type]
    assert SnowflakePort.write_state(source, object(), "x", "m", {}) is None  # type: ignore[arg-type]
    assert SnowflakePort.ensure_state_table(source, object()) is None  # type: ignore[arg-type]
    assert SnowflakePort.delete_state(source, object(), "x", "k") is None  # type: ignore[arg-type]
    assert SnowflakePort.upsert_state(source, object(), "x", "k", object()) is None  # type: ignore[arg-type]
    assert ClockPort.now_iso(source) is None  # type: ignore[arg-type]
    assert ClockPort.monotonic_ms(source) is None  # type: ignore[arg-type]
    assert ClockPort.sleep(source, 1) is None  # type: ignore[arg-type]
    assert ClockPort.new_run_id(source) is None  # type: ignore[arg-type]
    assert StateStore.config_path.fget(source) is None  # type: ignore[arg-type,union-attr]
    assert StateStore.read_local(source) is None  # type: ignore[arg-type]
    assert StateStore.write_local(source, object()) is None  # type: ignore[arg-type]
    assert StateStore.acquire_lock(source, "x", break_stale=False) is None  # type: ignore[arg-type]
    assert StateStore.release_lock(source, "x") is None  # type: ignore[arg-type]
    error = SnowflakePortError("x", sqlstate="42", errno=1)
    assert (str(error), error.sqlstate, error.errno) == ("x", "42", 1)


def test_eval_and_composite_lifecycle_protocol_methods_are_declarations_only() -> None:
    source = object()
    assert EvalStateStore.read_baseline(source, "target", "eval:a") is None  # type: ignore[arg-type]
    assert EvalStateStore.write_baseline(source, "target", object()) is None  # type: ignore[arg-type]
    assert EvalStateStore.write_baselines(source, "target", ()) is None  # type: ignore[arg-type]
    assert EvalStateStore.read_gate(source, "target", "eval:a") is None  # type: ignore[arg-type]
    assert EvalStateStore.write_gate(source, "target", object()) is None  # type: ignore[arg-type]
    assert CompositeLifecycleHandler.plan(source, object(), None, object()) is None  # type: ignore[arg-type]
    assert CompositeLifecycleHandler.apply(source, object(), object()) is None  # type: ignore[arg-type]
    assert CompositeLifecycleHandler.merge_physical_resources(source, (), None) is None  # type: ignore[arg-type]
    assert CompositeLifecycleHandler.report_prune(source, "eval:a", object()) is None  # type: ignore[arg-type]
