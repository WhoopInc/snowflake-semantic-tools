"""Port protocols contain no hidden implementation."""

from __future__ import annotations

from snowflake_semantic_tools.domain.ports.eval_state import EvalStateStore
from snowflake_semantic_tools.domain.ports.lifecycle import CompositeLifecycleHandler
from snowflake_semantic_tools.domain.ports.semantic_view_source import SemanticViewSource
from snowflake_semantic_tools.domain.ports.snowflake import (
    CatalogPort,
    ClockPort,
    ExecutionPort,
    ProfileRegistryPort,
    SnowflakePort,
    SnowflakePortError,
    StagePort,
    StatePort,
    StateStore,
)

ROLES = (CatalogPort, ExecutionPort, StagePort, ProfileRegistryPort, StatePort)


def test_semantic_view_source_protocol_methods_are_declarations_only() -> None:
    source = object()
    assert SemanticViewSource.load_project(source) is None  # type: ignore[arg-type]


def _declared(protocol: type) -> set[str]:
    return {name for name, value in vars(protocol).items() if callable(value) and not name.startswith("_")}


def test_snowflake_port_is_the_union_of_disjoint_roles() -> None:
    assert SnowflakePort.__bases__[: len(ROLES)] == ROLES
    assert _declared(SnowflakePort) == set()
    declared = [_declared(role) for role in ROLES]
    assert sum(len(names) for names in declared) == len(set().union(*declared))


def test_catalog_port_methods_are_declarations_only() -> None:
    source = object()
    assert CatalogPort.show_objects(source, "x", object()) is None  # type: ignore[arg-type]
    assert CatalogPort.show_grants(source, "x", object()) is None  # type: ignore[arg-type]
    assert CatalogPort.describe_marker(source, object()) is None  # type: ignore[arg-type]
    assert CatalogPort.object_exists(source, "x", object()) is None  # type: ignore[arg-type]
    assert CatalogPort.dataset_exists(source, object()) is None  # type: ignore[arg-type]
    assert CatalogPort.table_columns(source, object()) is None  # type: ignore[arg-type]
    assert CatalogPort.observe_stage(source, object()) is None  # type: ignore[arg-type]
    assert CatalogPort.describe_stage_file_format(source, object()) is None  # type: ignore[arg-type]
    assert CatalogPort.stage_type(source, object()) is None  # type: ignore[arg-type]
    assert CatalogPort.observe_extension(source, object()) is None  # type: ignore[arg-type]
    assert CatalogPort.extension_versions(source, object()) is None  # type: ignore[arg-type]
    assert CatalogPort.agent_has_live_version(source, object()) is None  # type: ignore[arg-type]
    assert CatalogPort.resolve_agent_version(source, object(), "committed") is None  # type: ignore[arg-type]
    assert CatalogPort.current_role(source) is None  # type: ignore[arg-type]
    assert CatalogPort.current_account_locator(source) is None  # type: ignore[arg-type]


def test_execution_and_stage_port_methods_are_declarations_only() -> None:
    source = object()
    assert ExecutionPort.query(source, "x") is None  # type: ignore[arg-type]
    assert ExecutionPort.query_in_context(source, object(), "x") is None  # type: ignore[arg-type]
    assert ExecutionPort.execute_script(source, ()) is None  # type: ignore[arg-type]
    assert ExecutionPort.try_execute(source, "x") is None  # type: ignore[arg-type]
    assert StagePort.observe_staged_file(source, "@x") is None  # type: ignore[arg-type]
    assert StagePort.stage_file_exists(source, "@x") is None  # type: ignore[arg-type]
    assert StagePort.read_staged_file(source, "@x") is None  # type: ignore[arg-type]
    assert StagePort.upload(source, "@x", b"") is None  # type: ignore[arg-type]
    assert StagePort.list_location(source, "@x/") is None  # type: ignore[arg-type]


def test_profile_registry_and_state_port_methods_are_declarations_only() -> None:
    source = object()
    assert ProfileRegistryPort.ensure_profile_registry(source, object()) is None  # type: ignore[arg-type]
    assert ProfileRegistryPort.read_profile_row(source, object(), "x") is None  # type: ignore[arg-type]
    assert (
        ProfileRegistryPort.merge_profile_row(source, object(), {}, expected_version=None) is None  # type: ignore[arg-type]
    )
    assert (
        ProfileRegistryPort.deactivate_profile_row(source, object(), "x", expected_version="1") is None  # type: ignore[arg-type]
    )
    assert ProfileRegistryPort.desktop_profile_rows(source, object()) is None  # type: ignore[arg-type]
    assert StatePort.read_state(source, object(), "x") is None  # type: ignore[arg-type]
    assert StatePort.write_state(source, object(), "x", "m", {}) is None  # type: ignore[arg-type]
    assert StatePort.ensure_state_table(source, object()) is None  # type: ignore[arg-type]
    assert StatePort.delete_state(source, object(), "x", "k") is None  # type: ignore[arg-type]
    assert StatePort.upsert_state(source, object(), "x", "k", object()) is None  # type: ignore[arg-type]


def test_clock_and_state_store_protocol_methods_are_declarations_only() -> None:
    source = object()
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
