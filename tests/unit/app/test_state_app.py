from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

from snowflake_semantic_tools.app.state import read_state
from snowflake_semantic_tools.domain.state import AppliedEntry, AppliedResource, ResourceStatus, State
from tests.helpers.app_ports import InMemorySnowflake, InMemoryStateStore
from tests.helpers.artifact_builders import rendered, target


def test_state_reconciliation_uses_remote_and_warns_on_cache_drift() -> None:
    artifact = rendered()
    local = State.empty(target())
    store = InMemoryStateStore(local)
    port = InMemorySnowflake()
    entry = AppliedEntry("a" * 64, artifact.target.sql, "now", "r", "applied", "a" * 64, "m")
    port.remote_state = MappingProxyType({artifact.key: entry})
    result, diagnostics = read_state(
        store,
        port,
        state_table=artifact.target,
        target=target(),
    )
    assert result.applied[artifact.key] == entry
    assert diagnostics[0].code == "SST-MAN027"
    assert store.state == result


def test_state_reconciliation_handles_offline_and_unreadable_remote() -> None:
    artifact = rendered()
    store = InMemoryStateStore()
    offline, diagnostics = read_state(store, None, state_table=artifact.target, target=target())
    assert offline.applied == {} and diagnostics[0].code == "SST-MAN020"
    cached = State.empty(target())
    store.state = cached
    port = InMemorySnowflake()
    port.remote_state = None
    fallback, remote_diagnostics = read_state(store, port, state_table=artifact.target, target=target())
    assert fallback.applied == {} and remote_diagnostics[0].code == "SST-PLN001"
    same, no_diagnostics = read_state(store, None, state_table=artifact.target, target=target())
    assert same is cached and no_diagnostics == ()


def test_state_reconciliation_compares_complete_applied_entries() -> None:
    artifact = rendered()
    remote = AppliedEntry(
        "a" * 64,
        artifact.target.sql,
        "now",
        "r",
        "applied",
        "a" * 64,
        "m",
        component_fingerprints=(("config", "c" * 64),),
        physical_resources=(AppliedResource("DATASET", artifact.target.sql, ResourceStatus.VERIFIED),),
    )
    variants = (
        replace(remote, applied_at="later"),
        replace(remote, run_id="other-run"),
        replace(remote, ddl_sha256="b" * 64),
        replace(remote, git_sha="other-git"),
        replace(remote, component_fingerprints=(("config", "d" * 64),)),
        replace(
            remote,
            physical_resources=(
                AppliedResource("DATASET", artifact.target.sql, ResourceStatus.UNVERIFIED_AFTER_WRITE),
            ),
        ),
    )

    for cached_entry in variants:
        store = InMemoryStateStore(
            replace(State.empty(target()), applied=MappingProxyType({artifact.key: cached_entry}))
        )
        port = InMemorySnowflake()
        port.remote_state = MappingProxyType({artifact.key: remote})

        result, diagnostics = read_state(
            store,
            port,
            state_table=artifact.target,
            target=target(),
        )

        assert diagnostics[0].code == "SST-MAN027"
        assert result.applied[artifact.key] == remote
        assert store.state == result


def test_state_reconciliation_does_not_rewrite_equal_complete_entries() -> None:
    artifact = rendered()
    entry = AppliedEntry(
        "a" * 64,
        artifact.target.sql,
        "now",
        "r",
        "applied",
        "a" * 64,
        "m",
        physical_resources=(AppliedResource("TABLE", artifact.target.sql, ResourceStatus.RETAINED),),
    )
    cached = replace(State.empty(target()), applied=MappingProxyType({artifact.key: entry}))
    store = InMemoryStateStore(cached)
    port = InMemorySnowflake()
    port.remote_state = MappingProxyType({artifact.key: entry})

    result, diagnostics = read_state(store, port, state_table=artifact.target, target=target())

    assert diagnostics == ()
    assert result.applied == cached.applied
    assert store.writes == []
