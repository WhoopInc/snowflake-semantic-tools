"""The selector grammar: every form `--select` takes, and every refusal, resolved against compiled artifacts."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.plan.selectors import Selectable, Selection, resolve_selectors

TYPES = ("semantic_view", "agent", "tool")
UNIVERSE = (
    Selectable("semantic_view:orders", "semantic_view", "orders", "f1", ("semantic_models/semantic_views/core.yml",)),
    Selectable("agent:orders", "agent", "orders", "f2", ("agents/orders/agent.yml",)),
    Selectable("tool:docs", "tool", "docs", "f3", ("tools/p.yml",)),
)


def _resolve(*values: str, previous: dict[str, str] | None = None) -> Selection | Diagnostic:
    return resolve_selectors(values, artifact_types=TYPES, universe=UNIVERSE, previous=previous)


def test_names_globs_types_paths_and_keys_resolve_against_the_compiled_artifacts() -> None:
    assert _resolve() == Selection(None, None)
    assert _resolve("ORDERS") == Selection(None, frozenset(("semantic_view:orders", "agent:orders")))
    assert _resolve("d*") == Selection(None, frozenset(("tool:docs",)))
    assert _resolve("nothing*") == Selection(None, None)
    # A plain name nothing compiled is still a view's key, so a deleted view can be pruned.
    assert _resolve("deleted") == Selection(None, frozenset(("semantic_view:deleted",)))
    assert _resolve("type:Agent", "tool:Docs") == Selection(frozenset(("agent",)), frozenset(("tool:docs",)))
    assert _resolve("path:agents/*") == Selection(None, frozenset(("agent:orders",)))


@pytest.mark.parametrize(
    ("value", "code", "message"),
    [
        (
            "+orders",
            "SST-PRT100",
            "selector '+orders': graph operators (+name, name+) are not supported in this release",
        ),
        (
            "orders+",
            "SST-PRT100",
            "selector 'orders+': graph operators (+name, name+) are not supported in this release",
        ),
        ("agent:", "SST-PRT100", "selector 'agent:': agent: names no artifact"),
        (
            "type:nope",
            "SST-PRT100",
            "selector 'type:nope': unknown artifact type 'nope'; one of agent, semantic_view, tool",
        ),
        ("tag:core", "SST-PRT100", "selector 'tag:core': tag: selectors are not supported by this command"),
        ("model:orders", "SST-PRT100", "selector 'model:orders': model: selectors are not supported by this command"),
        (
            "state:stale",
            "SST-PRT100",
            "selector 'state:stale': unknown state 'stale'; one of new, modified, unmodified, orphaned",
        ),
        ("source:*", "SST-PRT102", "selector 'source:*' names an unknown kind"),
        ("a,b", "SST-PRT101", "selector 'a,b' contains a comma"),
    ],
)
def test_each_refusal_names_the_selector(value: str, code: str, message: str) -> None:
    refused = _resolve(value, previous={})
    assert isinstance(refused, Diagnostic) and (refused.code, refused.message) == (code, message)


def test_new_is_what_the_previous_run_did_not_have() -> None:
    assert _resolve("state:new", previous={"semantic_view:orders": "f1"}) == Selection(
        None, frozenset(("agent:orders", "tool:docs"))
    )


def test_each_state_compares_the_compiled_artifacts_with_the_previous_run() -> None:
    previous = {"semantic_view:orders": "f1", "agent:orders": "changed", "tool:gone": "f9"}
    assert _resolve("state:orphaned", previous=previous) == Selection(None, frozenset(("tool:gone",)))
    assert _resolve("state:unmodified", previous=previous) == Selection(None, frozenset(("semantic_view:orders",)))
    assert _resolve("state:modified", previous=previous) == Selection(None, frozenset(("agent:orders",)))


def test_a_state_selector_without_state_and_a_typed_glob_are_refused() -> None:
    without = _resolve("state:modified")
    assert isinstance(without, Diagnostic) and without.message == "selector 'state:modified' requires --state"
    glob = _resolve("tool:d*")
    assert isinstance(glob, Diagnostic) and glob.code == "SST-PRT102"
