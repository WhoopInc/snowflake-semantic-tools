"""The selector grammar: every form `--select` takes, and every refusal, resolved against compiled artifacts."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.lineage.graph import LineageGraph, LineageNode
from snowflake_semantic_tools.domain.plan.selectors import Selectable, Selection, SelectionScope, resolve_selectors

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
    assert _resolve("nothing*") == Selection(None, frozenset())
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
            "selector '+orders': graph operators need the lineage the compiled manifest records",
        ),
        (
            "or+ders",
            "SST-PRT100",
            "selector 'or+ders': a graph operator goes before or after one selector: +x, x+, 2+x or x+3",
        ),
        ("agent:", "SST-PRT100", "selector 'agent:': agent: names no artifact"),
        (
            "type:nope",
            "SST-PRT100",
            "selector 'type:nope': unknown artifact type 'nope'; one of agent, semantic_view, tool",
        ),
        ("tag:core", "SST-PRT100", "selector 'tag:core': tag: selectors are not supported by this command"),
        (
            "model:orders",
            "SST-PRT100",
            "selector 'model:orders': model: names a dbt node, not an artifact; write model:orders+ for what reads it",
        ),
        (
            "metric:revenue",
            "SST-PRT100",
            "selector 'metric:revenue': metric: names a member of a view, not an artifact; "
            "write metric:revenue+ for what reads it",
        ),
        (
            "state:stale",
            "SST-PRT100",
            "selector 'state:stale': unknown state 'stale'; one of new, modified, unmodified, orphaned",
        ),
        ("nope:*", "SST-PRT102", "selector 'nope:*' names an unknown kind"),
        ("a,b", "SST-PRT101", "selector 'a,b' contains a comma"),
        (
            " ",
            "SST-PRT100",
            "selector ' ': is empty; name an artifact, or write type:<type> or path:<glob>",
        ),
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
    assert isinstance(glob, Diagnostic) and glob.code == "SST-PRT113"
    assert (
        glob.suggestion == "use an exact value after 'tool:', or the bare glob 'd*', which matches a name of every type"
    )
    typed = _resolve("type:ag*")
    assert isinstance(typed, Diagnostic) and typed.code == "SST-PRT113"


def test_an_unknown_kind_is_refused_with_the_kinds_there_are() -> None:
    unknown = _resolve("view:orders")
    assert isinstance(unknown, Diagnostic) and unknown.message == "selector 'view:orders' names an unknown kind"
    assert unknown.suggestion == "use one of type, path, tag, state, model, source"


def test_a_selection_names_by_type_or_key_and_expands_its_types() -> None:
    by_type = Selection(frozenset(("agent",)), frozenset(("tool:docs",)))
    assert by_type.names("agent", "agent:orders") and by_type.names("tool", "tool:docs")
    assert not by_type.names("semantic_view", "semantic_view:orders")
    assert by_type.keys_in(UNIVERSE) == frozenset(("agent:orders", "tool:docs"))
    assert Selection(None, None).keys_in(UNIVERSE) == frozenset()


def test_one_scope_covers_selected_less_excluded_and_nothing_named_selects_nothing() -> None:
    assert SelectionScope().covers("tool", "tool:docs")
    excluding = SelectionScope(excluded=Selection(frozenset(("tool",)), None))
    assert not excluding.covers("tool", "tool:docs") and excluding.covers("agent", "agent:orders")
    named_nothing = _resolve("nothing*")
    assert isinstance(named_nothing, Selection)
    assert not SelectionScope(named_nothing).covers("tool", "tool:docs")


def test_a_scope_reaches_what_it_chooses_and_every_subject_that_names_no_artifact() -> None:
    views = SelectionScope(Selection(frozenset(("semantic_view",)), None))
    assert views.reaches("semantic_view:orders", artifact_types=TYPES)
    assert not views.reaches("agent:orders", artifact_types=TYPES)
    assert not views.reaches("tool:docs", artifact_types=TYPES)
    for project_wide in (None, "", "cli", "config:validation.strict", "tool_ref:partner", "metric:orders.m"):
        assert views.reaches(project_wide, artifact_types=TYPES)
    excluding = SelectionScope(excluded=Selection(None, frozenset(("agent:orders",))))
    assert not excluding.reaches("agent:orders", artifact_types=TYPES)
    assert excluding.reaches("agent:other", artifact_types=TYPES)
    assert SelectionScope().reaches("agent:orders", artifact_types=TYPES)


def _graph() -> LineageGraph:
    nodes = {
        "model:stg": LineageNode("model:stg", "model", "stg"),
        "model:orders": LineageNode("model:orders", "model", "orders"),
        "metric:revenue": LineageNode("metric:revenue", "metric", "revenue"),
        "semantic_view:orders": LineageNode("semantic_view:orders", "semantic_view", "orders"),
        "tool:docs": LineageNode("tool:docs", "tool", "docs"),
        "agent:orders": LineageNode("agent:orders", "agent", "orders"),
    }
    edges = (
        ("model:stg", "model:orders"),
        ("model:orders", "metric:revenue"),
        ("model:orders", "tool:docs"),
        ("metric:revenue", "semantic_view:orders"),
        ("semantic_view:orders", "agent:orders"),
        ("tool:docs", "agent:orders"),
    )
    parents: dict[str, list[str]] = {}
    children: dict[str, list[str]] = {}
    for upstream, downstream in edges:
        parents.setdefault(downstream, []).append(upstream)
        children.setdefault(upstream, []).append(downstream)
    return LineageGraph(
        nodes,
        {key: tuple(sorted(value)) for key, value in parents.items()},
        {key: tuple(sorted(value)) for key, value in children.items()},
    )


def test_graph_operators_select_the_artifacts_lineage_reaches_and_build_the_graph_once() -> None:
    built: list[LineageGraph] = []

    def graph() -> LineageGraph:
        built.append(_graph())
        return built[-1]

    def walked(*values: str) -> Selection | Diagnostic:
        return resolve_selectors(values, artifact_types=TYPES, universe=UNIVERSE, graph=graph)

    assert walked("model:orders+") == Selection(None, frozenset(("semantic_view:orders", "tool:docs", "agent:orders")))
    assert walked("1+model:orders+1", "model:st*+1") == Selection(None, frozenset(("tool:docs",)))
    assert walked("+agent:orders") == Selection(None, frozenset(("semantic_view:orders", "tool:docs", "agent:orders")))
    assert walked("metric:rev*+") == Selection(None, frozenset(("semantic_view:orders", "agent:orders")))
    assert walked("type:tool+") == Selection(None, frozenset(("tool:docs", "agent:orders")))
    assert walked("docs+", "tool:docs+") == Selection(None, frozenset(("tool:docs", "agent:orders")))
    assert len(built) == 6
    refused = walked("tag:core+")
    assert isinstance(refused, Diagnostic) and refused.code == "SST-PRT100"


def test_a_tool_another_team_owns_is_refused_bare_and_selects_its_callers_inside_the_operators() -> None:
    bare = _resolve("tool_ref:partner")
    assert isinstance(bare, Diagnostic) and "names a tool another team owns" in bare.message
    graph = LineageGraph(
        {
            "tool_ref:partner": LineageNode("tool_ref:partner", "tool_ref", "partner"),
            "agent:orders": LineageNode("agent:orders", "agent", "orders"),
        },
        {"agent:orders": ("tool_ref:partner",)},
        {"tool_ref:partner": ("agent:orders",)},
    )
    walked = resolve_selectors(("tool_ref:partner+",), artifact_types=TYPES, universe=UNIVERSE, graph=lambda: graph)
    assert walked == Selection(None, frozenset(("agent:orders",)))
