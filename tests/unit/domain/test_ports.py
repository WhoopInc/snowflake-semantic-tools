"""Port protocols contain no hidden implementation."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.ports.clock import ClockPort
from snowflake_semantic_tools.domain.ports.eval_state import EvalStateStore
from snowflake_semantic_tools.domain.ports.lifecycle import CompositeLifecycleHandler
from snowflake_semantic_tools.domain.ports.semantic_view_source import SemanticViewSource
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePort
from snowflake_semantic_tools.domain.ports.snowflake.catalog import AgentVersion, AgentVersions, CatalogPort
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.execution import ExecutionPort, WriteGuardPort
from snowflake_semantic_tools.domain.ports.snowflake.profile_registry import ProfileRegistryPort
from snowflake_semantic_tools.domain.ports.snowflake.stage import StagePort
from snowflake_semantic_tools.domain.ports.snowflake.state import StatePort
from snowflake_semantic_tools.domain.ports.state import StateStore
from snowflake_semantic_tools.domain.ports.timing import NO_PHASES, PhaseTimer

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
    assert CatalogPort.describe_marker(source, object(), "x") is None  # type: ignore[arg-type]
    assert CatalogPort.object_exists(source, "x", object()) is None  # type: ignore[arg-type]
    assert CatalogPort.dataset_exists(source, object()) is None  # type: ignore[arg-type]
    assert CatalogPort.table_columns(source, object()) is None  # type: ignore[arg-type]
    assert CatalogPort.describe_stage_file_format(source, object()) is None  # type: ignore[arg-type]
    assert CatalogPort.stage_type(source, object()) is None  # type: ignore[arg-type]
    assert CatalogPort.observe_extension(source, object()) is None  # type: ignore[arg-type]
    assert CatalogPort.extension_versions(source, object()) is None  # type: ignore[arg-type]
    assert CatalogPort.observe_agent_versions(source, object()) is None  # type: ignore[arg-type]
    assert CatalogPort.get_ddl(source, "x", object()) is None  # type: ignore[arg-type]
    assert CatalogPort.resolve_agent_version(source, object(), "committed") is None  # type: ignore[arg-type]
    assert CatalogPort.current_role(source) is None  # type: ignore[arg-type]
    assert CatalogPort.current_account_locator(source) is None  # type: ignore[arg-type]


def test_agent_versions_derive_the_live_version_and_the_aliases_from_the_rows() -> None:
    assert (AgentVersions().has_live_version, AgentVersions().aliases) == (False, ())
    versions = AgentVersions(
        (AgentVersion(None, "CANDIDATE"), AgentVersion("VERSION$2", None), AgentVersion("VERSION$1", "PROMOTED"))
    )
    assert (versions.has_live_version, versions.aliases) == (True, ("CANDIDATE", "PROMOTED"))


def test_execution_and_stage_port_methods_are_declarations_only() -> None:
    source = object()
    assert ExecutionPort.query(source, "x") is None  # type: ignore[arg-type]
    assert ExecutionPort.query_in_context(source, object(), "x") is None  # type: ignore[arg-type]
    assert ExecutionPort.execute_script(source, ()) is None  # type: ignore[arg-type]
    assert ExecutionPort.try_execute(source, "x") is None  # type: ignore[arg-type]
    WriteGuardPort.guard_writes(source, None)  # type: ignore[arg-type]
    assert StagePort.observe_staged_file(source, "@x") is None  # type: ignore[arg-type]
    assert StagePort.read_staged_file(source, "@x") is None  # type: ignore[arg-type]
    StagePort.upload(source, "@x", b"")  # type: ignore[arg-type]
    assert StagePort.list_location(source, "@x/") is None  # type: ignore[arg-type]


def test_profile_registry_and_state_port_methods_are_declarations_only() -> None:
    source = object()
    ProfileRegistryPort.ensure_profile_registry(source, object())  # type: ignore[arg-type]
    assert ProfileRegistryPort.read_profile_row(source, object(), "x") is None  # type: ignore[arg-type]
    assert (
        ProfileRegistryPort.merge_profile_row(source, object(), {}, expected_version=None) is None  # type: ignore[arg-type]
    )
    assert (
        ProfileRegistryPort.deactivate_profile_row(source, object(), "x", expected_version="1") is None  # type: ignore[arg-type]
    )
    assert ProfileRegistryPort.desktop_profile_rows(source, object()) is None  # type: ignore[arg-type]
    assert StatePort.read_state(source, object(), "x") is None  # type: ignore[arg-type]
    assert StatePort.read_state_manifest(source, object(), "x") is None  # type: ignore[arg-type]
    assert StatePort.write_state(source, object(), "x", "m", {}, object()) is None  # type: ignore[arg-type]
    StatePort.ensure_state_table(source, object())  # type: ignore[arg-type]
    assert (
        StatePort.acquire_run_lock(source, object(), "x", object(), break_stale=False) is None  # type: ignore[arg-type]
    )
    assert StatePort.extend_run_lock(source, object(), "x", object(), 1) is None  # type: ignore[arg-type]
    StatePort.release_run_lock(source, object(), "x", object())  # type: ignore[arg-type]
    assert StatePort.verify_run_lock(source, object(), "x", object()) is None  # type: ignore[arg-type]


def test_clock_and_state_store_protocol_methods_are_declarations_only() -> None:
    source = object()
    assert ClockPort.now_iso(source) is None  # type: ignore[arg-type]
    assert ClockPort.monotonic_ms(source) is None  # type: ignore[arg-type]
    ClockPort.sleep(source, 1)  # type: ignore[arg-type]
    assert ClockPort.new_run_id(source) is None  # type: ignore[arg-type]
    assert vars(StateStore)["config_path"].fget(source) is None
    assert StateStore.read_local(source) is None  # type: ignore[arg-type]
    StateStore.write_local(source, object())  # type: ignore[arg-type]
    assert StateStore.acquire_lock(source, "x", break_stale=False) is None  # type: ignore[arg-type]
    StateStore.release_lock(source, "x")  # type: ignore[arg-type]
    assert StateStore.describe_lock(source, "x") is None  # type: ignore[arg-type]
    error = SnowflakePortError("x", sqlstate="42", errno=1)
    assert (str(error), error.sqlstate, error.errno) == ("x", "42", 1)


def test_eval_and_composite_lifecycle_protocol_methods_are_declarations_only() -> None:
    source = object()
    assert EvalStateStore.read_baseline(source, "target", "eval:a") is None  # type: ignore[arg-type]
    EvalStateStore.write_baselines(source, "target", ())  # type: ignore[arg-type]
    EvalStateStore.write_gate(source, "target", object())  # type: ignore[arg-type]
    assert CompositeLifecycleHandler.plan(source, object(), None, object()) is None  # type: ignore[arg-type]
    assert CompositeLifecycleHandler.apply(source, object(), object()) is None  # type: ignore[arg-type]
    assert CompositeLifecycleHandler.merge_physical_resources(source, (), None) is None  # type: ignore[arg-type]
    assert CompositeLifecycleHandler.report_prune(source, "eval:a", object()) is None  # type: ignore[arg-type]


def test_the_phase_timer_is_a_declaration_and_no_phases_runs_the_block_untimed() -> None:
    assert PhaseTimer.phase(object(), "observe") is None  # type: ignore[arg-type]
    ran = []
    with NO_PHASES.phase("observe"):
        ran.append("observe")
    assert ran == ["observe"]
    with pytest.raises(ValueError, match="kept"), NO_PHASES.phase("observe"):
        raise ValueError("kept")
