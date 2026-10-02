"""The golden suite over an in-memory `GoldenStore`: routes, comparisons, normalization, and refusals."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.project import CompileProject
from snowflake_semantic_tools.app.golden import CompareGoldens, golden_payloads
from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.eval import EvalCatalog
from snowflake_semantic_tools.domain.model.profile import DesktopProfile, ProfileCatalog
from snowflake_semantic_tools.domain.model.project import SemanticViewProject
from snowflake_semantic_tools.domain.model.skill import SkillCatalog
from snowflake_semantic_tools.domain.ports.golden import GoldenPath
from tests.helpers.compile_builders import CHANNELS, agent, plugin, skill, view
from tests.helpers.eval_builders import resolved_eval
from tests.helpers.golden_store import InMemoryGoldenStore
from tests.helpers.project_inputs import InMemoryProjectInputs


def project() -> CompileResult:
    profile = DesktopProfile(
        "analyst", "profiles/analyst", "P.", "Data", ("solo",), (), (), "Be brief.\n", Origin("p"), plugins=("kit",)
    )
    inputs = InMemoryProjectInputs(
        tree={"skills": CHANNELS},
        views=SemanticViewProject((view("SALES"),)),
        skills=SkillCatalog((skill("solo"),), (plugin("kit", "solo"),)),
        profiles=ProfileCatalog((profile,)),
        agent_models=(agent("sales_agent"),),
        evals=EvalCatalog((resolved_eval(),), ()),
    )
    return CompileProject(inputs).run()


def committed(result: CompileResult) -> InMemoryGoldenStore:
    """Every required golden, committed exactly as compiled."""
    store = InMemoryGoldenStore()
    for item in result.compiled:
        for payload in golden_payloads(item, store):
            store.goldens[payload.golden] = payload.content
    return store


def compare(store: InMemoryGoldenStore, result: CompileResult, sha: str = "WORKTREE") -> tuple[str, ...]:
    return CompareGoldens(store, lambda: sha).run(result).failures


def test_each_artifact_type_routes_to_its_own_golden() -> None:
    result = project()
    routes = {payload.golden for item in result.compiled for payload in golden_payloads(item, InMemoryGoldenStore())}

    assert routes == {
        GoldenPath(None, ("sales.sql",)),
        GoldenPath("skill", ("solo.bundle.json",)),
        GoldenPath("plugin", ("kit.bundle.json",)),
        GoldenPath("profile", ("analyst.profile.json",)),
        GoldenPath("agent", ("sales_agent.json",)),
        GoldenPath("eval", ("sales_agent_repeat.yaml",)),
        GoldenPath("eval", ("sales_source.sql",)),
    }
    assert sum(1 for item in result.compiled) == 6


def test_committed_goldens_pass_and_a_missing_one_is_named() -> None:
    result = project()
    store = committed(result)
    report = CompareGoldens(store, lambda: "WORKTREE").run(result)
    assert report.passed and report.failures == ()

    del store.goldens[GoldenPath("agent", ("sales_agent.json",))]

    assert compare(store, result) == ("missing golden golden/agent/sales_agent.json",)


def test_a_differing_golden_is_reported_as_a_diff_ddl_goldens_skipping_leading_comments() -> None:
    result = project()
    store = committed(result)
    ddl = GoldenPath(None, ("sales.sql",))
    store.goldens[ddl] = "-- generated\n\n" + store.goldens[ddl] + "\n\n"
    agent_golden = GoldenPath("agent", ("sales_agent.json",))
    store.goldens[agent_golden] = store.goldens[agent_golden].replace('"', "'", 1)

    [failure] = compare(store, result)

    assert failure.startswith("--- golden/agent/sales_agent.json\n+++ compiled/sales_agent.json")


def test_a_ddl_golden_without_a_statement_is_refused() -> None:
    result = project()
    store = committed(result)
    store.goldens[GoldenPath(None, ("sales.sql",))] = "-- only a comment\n\n"

    with pytest.raises(ValueError, match="golden golden/ddl/sales.sql contains no DDL"):
        compare(store, result)


def test_optional_goldens_are_compared_only_when_committed() -> None:
    result = project()
    store = committed(result)
    flattened = GoldenPath("skill", ("solo-flattened.md",))
    manifest = GoldenPath("plugin", ("kit.plugin.json",))
    prompt = GoldenPath("profile", ("analyst", "AGENTS.md"))
    store.goldens.update({flattened: "stale\n", manifest: "stale\n", prompt: "stale\n"})

    failures = compare(store, result)

    assert [failure.splitlines()[0] for failure in failures] == [
        "--- golden/skill/solo-flattened.md",
        "--- golden/plugin/kit.plugin.json",
        "--- golden/profile/analyst/AGENTS.md",
    ]


@dataclass(frozen=True)
class Rendered:
    content: str


@dataclass(frozen=True)
class Payload:
    """A compiled artifact of a routed type, reduced to what routing reads."""

    name: str
    artifact_type: str
    rendered_artifact: Rendered


def test_the_commit_is_normalized_and_asked_for_once_per_golden_that_exists() -> None:
    asked: list[str] = []

    def sha() -> str:
        asked.append("asked")
        return "abc1234"

    staged = Payload("helper", "agent", Rendered('{"stage": "@S/helper/GIT_abc1234"}'))
    absent = Payload("absent", "agent", Rendered("{}"))
    store = InMemoryGoldenStore({GoldenPath("agent", ("helper.json",)): '{"stage": "@S/helper/GIT_0000000"}\n'})

    failures = CompareGoldens(store, sha).run(CompileResult((staged, absent)))  # type: ignore[arg-type]

    assert failures.failures == ("missing golden golden/agent/absent.json",)
    assert asked == ["asked"]
    # Outside a git work tree the payload keeps its commit, so it differs.
    assert len(compare(store, CompileResult((staged,)))) == 1  # type: ignore[arg-type]


def test_a_tool_routes_beside_the_ddl_directory_and_compares_as_ddl() -> None:
    tool = Payload("menu_search", "tool", Rendered("CREATE CORTEX SEARCH SERVICE X"))
    [payload] = golden_payloads(tool, InMemoryGoldenStore())  # type: ignore[arg-type]
    assert (payload.golden, payload.compiled_path, payload.ddl) == (
        GoldenPath("tool", ("menu_search.sql",)),
        "compiled/menu_search.sql",
        True,
    )
    store = InMemoryGoldenStore({payload.golden: "-- golden\nCREATE CORTEX SEARCH SERVICE X\n"})
    assert compare(store, CompileResult((tool,))) == ()  # type: ignore[arg-type]


def test_an_artifact_type_without_a_route_is_refused() -> None:
    class Unknown:
        name = "x"
        artifact_type = "dashboard"
        rendered_artifact = None

    with pytest.raises(ValueError, match="no golden route for artifact type 'dashboard'"):
        golden_payloads(Unknown(), InMemoryGoldenStore())  # type: ignore[arg-type]
