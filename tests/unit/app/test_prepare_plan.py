"""The plan use case, driven by in-memory ports: what `select` refuses offline, and what `run` plans."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.manifest import manifest_for
from snowflake_semantic_tools.app.plan import PlanCandidates, PlanReady, PlanRefused, PlanScope, PreparePlan
from snowflake_semantic_tools.domain.diagnostics import D, Origin, Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import ShowRow
from snowflake_semantic_tools.domain.ports.project import ValidationDefaults
from snowflake_semantic_tools.domain.state import AppliedEntry, State
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.clocks import FixedClock
from tests.helpers.compile_builders import compiled, with_diagnostics
from tests.helpers.project_inputs import EMPTY_SOURCES, InMemoryProjectInputs, dev_target
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.sql_values import texts

EVERYTHING = PlanScope((), None, None, None, None, False)
STATE_TABLE = QualifiedName.parse("DB.SCH.SST_STATE")


class ScopeRecordingSnowflake(FakeSnowflake):
    """Records every schema a plan lists objects in."""

    def __init__(self) -> None:
        super().__init__()
        self.scopes: list[str] = []

    def show_objects(self, object_type: str, scope: SchemaScope) -> tuple[ShowRow, ...]:
        self.scopes.append(scope.sql)
        return super().show_objects(object_type, scope)


def select(
    result: CompileResult,
    *,
    scope: PlanScope = EVERYTHING,
    partial: bool = False,
    strict: bool | None = None,
    connected: bool | None = False,
    inputs: InMemoryProjectInputs | None = None,
    compiled_manifest: object = None,
) -> tuple[PlanCandidates | PlanRefused, PreparePlan, InMemoryProjectInputs]:
    project_inputs = inputs or InMemoryProjectInputs()
    use_case = PreparePlan(project_inputs, FixedClock())
    manifest = compiled_manifest or manifest_for(result, EMPTY_SOURCES)
    outcome = use_case.select(
        result,
        manifest,  # type: ignore[arg-type]
        scope,
        partial=partial,
        strict=strict,
        connected=connected,
        project="the-project",
    )
    return outcome, use_case, project_inputs


def test_selectors_that_match_nothing_refuse_the_plan_unless_it_prunes() -> None:
    result = compiled()
    scope = PlanScope(("nope",), None, frozenset(("semantic_view:nope",)), None, None, False)

    refused, _, inputs = select(result, scope=scope)

    assert refused == PlanRefused(reason="selectors ('nope',) matched no artifact in the-project")
    assert inputs.reads == []
    pruning, _, _ = select(result, scope=replace(scope, include_prune=True))
    assert isinstance(pruning, PlanCandidates) and pruning.selected.compiled == ()


def test_a_scope_covers_what_its_types_or_keys_select_less_what_it_excludes() -> None:
    result = compiled("MENU", "SALES")
    by_key = PlanScope(("menu",), None, frozenset(("semantic_view:menu",)), None, None, False)
    excluding = PlanScope((), None, None, None, frozenset(("semantic_view:menu",)), False)
    no_views = PlanScope((), frozenset(("tool",)), None, frozenset(("semantic_view",)), None, False)
    # A selector that names nothing, such as `path:` over no file, covers nothing, not everything.
    named_nothing = PlanScope(("path:none/*",), None, frozenset(), None, None, False)

    covered = [
        [item.artifact_key for item in result.compiled if scope.covers(item)]
        for scope in (by_key, excluding, no_views, named_nothing)
    ]

    assert covered == [["semantic_view:menu"], ["semantic_view:sales"], [], []]


def test_a_compile_error_refuses_the_plan_and_partial_says_why_nothing_can_split_off() -> None:
    result = compiled()
    broken = with_diagnostics(result, D("SST-REF001", model="missing", subject="semantic_view:sales"))
    unplaced = with_diagnostics(
        result,
        D("SST-CFG047", origin=Origin("sst_config.yml"), subject="config:project.hooks_dir", key="k", value="v"),
    )

    refused, _, _ = select(broken)
    without_split, _, _ = select(unplaced, partial=True)

    assert isinstance(refused, PlanRefused) and refused.reason is None
    assert [item.code for item in refused.diagnostics] == ["SST-REF001"]
    assert isinstance(without_split, PlanRefused)
    assert [item.code for item in without_split.diagnostics] == ["SST-CFG047", "SST-PLN033"]


def test_partial_splits_a_failed_compile_and_reads_settings_only_once_something_can_publish() -> None:
    failing = with_diagnostics(
        compiled("MENU", "SALES"), D("SST-REF001", model="missing", subject="semantic_view:menu")
    )
    healthy = replace(failing, compiled=tuple(item for item in failing.compiled if item.name == "SALES"))

    candidates, _, inputs = select(failing, partial=True, compiled_manifest=manifest_for(healthy, EMPTY_SOURCES))

    assert isinstance(candidates, PlanCandidates)
    assert candidates.split is not None and candidates.split.excluded == ("semantic_view:menu",)
    assert candidates.source is candidates.split.healthy
    assert [item.artifact_key for item in candidates.selected.compiled] == ["semantic_view:sales"]
    assert inputs.reads == ["validation_defaults", "manifest_sources"]


def test_a_stale_compile_refuses_the_plan() -> None:
    other = manifest_for(compiled("OTHER"), EMPTY_SOURCES)

    refused, _, _ = select(compiled(), compiled_manifest=other)

    assert refused == PlanRefused(reason="compiled SST manifest is stale; run sst compile before plan or apply")


def test_flags_override_the_validation_block() -> None:
    inputs = InMemoryProjectInputs(validation=ValidationDefaults(strict=True, snowflake_syntax_check=False))

    configured, _, _ = select(compiled(), connected=None, inputs=inputs)
    overridden, _, _ = select(compiled(), strict=False, connected=True, inputs=inputs)

    assert isinstance(configured, PlanCandidates) and (configured.strict, configured.connected) == (True, False)
    assert isinstance(overridden, PlanCandidates) and (overridden.strict, overridden.connected) == (False, True)


def test_run_plans_from_state_with_agents_staged_and_every_composite_type_handled() -> None:
    tree: dict[str, object] = {"apply": {"agent_spec_stage": {"database": "SPECS", "stage": "AGENT_STAGE"}}}
    inputs = InMemoryProjectInputs(tree=tree)
    result = compiled(tree=tree, agents=("helper",))
    candidates, use_case, _ = select(result, inputs=inputs)
    port = ScopeRecordingSnowflake()
    gone = AppliedEntry("f" * 64, "OTHER.SCHEMA.GONE", "now", "run", "applied", "f" * 64, "m" * 64)
    port.remote_state = MappingProxyType({"semantic_view:gone": gone})
    store = InMemoryStateStore(State.empty(dev_target()))
    assert isinstance(candidates, PlanCandidates)

    ready = use_case.run(candidates, port, store, target=dev_target(), state_table=STATE_TABLE)

    assert isinstance(ready, PlanReady)
    assert set(ready.lifecycle_handlers) == {"eval", "skill", "plugin", "profile"}
    assert ready.changeset.observation_at == "2026-01-01T00:00:01Z"
    assert ready.changeset.diagnostics[0].code == "SST-MAN027"
    assert "OTHER.SCHEMA" in port.scopes and "DB.SCH" in port.scopes
    agent_change = next(change for change in ready.changeset.changes if change.artifact_type == "agent")
    assert agent_change.rendered is not None
    assert "@SPECS.SCH.AGENT_STAGE/helper/abc1234" in "\n".join(texts(agent_change.rendered.statements))
    assert ready.state.applied == {"semantic_view:gone": gone}
    assert inputs.reads[-2:] == ["config", "git_sha"]


def test_run_refuses_what_validation_promotes_to_an_error_without_reading_state() -> None:
    warned = with_diagnostics(compiled(), D("SST-LOD003", file="warning.yml"))
    candidates, use_case, _ = select(warned, strict=True)
    port = FakeSnowflake()
    store = InMemoryStateStore()
    assert isinstance(candidates, PlanCandidates)

    refused = use_case.run(candidates, port, store, target=dev_target(), state_table=STATE_TABLE)

    assert isinstance(refused, PlanRefused) and refused.reason is None
    assert [(item.code, item.severity) for item in refused.diagnostics] == [
        ("SST-LOD003", Severity.ERROR),
        ("SST-VAL020", Severity.INFO),
        ("SST-VAL020", Severity.INFO),
        ("SST-VAL020", Severity.INFO),
        ("SST-VAL020", Severity.INFO),
    ]
    assert port.queries == [] and store.writes == []


def test_partial_run_leaves_out_what_validation_excludes_and_names_everything_left_out() -> None:
    result = with_diagnostics(
        compiled("ALPHA", "BRAVO", "CHARLIE"),
        D("SST-REF001", model="missing", subject="semantic_view:alpha"),
        D("SST-LOD003", file="warning.yml", subject="semantic_view:bravo"),
    )
    healthy = replace(result, compiled=tuple(item for item in result.compiled if item.name != "ALPHA"))
    candidates, use_case, _ = select(
        result, partial=True, strict=True, compiled_manifest=manifest_for(healthy, EMPTY_SOURCES)
    )
    assert isinstance(candidates, PlanCandidates)

    ready = use_case.run(
        candidates, FakeSnowflake(), InMemoryStateStore(), target=dev_target(), state_table=STATE_TABLE
    )

    assert isinstance(ready, PlanReady)
    assert [item.artifact_key for item in ready.result.compiled] == ["semantic_view:charlie"]
    notices = [str(item.subject) for item in ready.result.diagnostics if item.code == "SST-PLN032"]
    assert notices == ["semantic_view:alpha", "semantic_view:bravo"]
    assert [change.key for change in ready.changeset.changes] == ["semantic_view:charlie"]


def test_partial_run_refuses_a_validation_error_that_names_no_artifact() -> None:
    warned = with_diagnostics(compiled(), D("SST-LOD003", file="warning.yml"))
    candidates, use_case, _ = select(warned, partial=True, strict=True)
    assert isinstance(candidates, PlanCandidates)

    refused = use_case.run(
        candidates, FakeSnowflake(), InMemoryStateStore(), target=dev_target(), state_table=STATE_TABLE
    )

    assert isinstance(refused, PlanRefused)
    assert [item.code for item in refused.diagnostics] == [
        "SST-LOD003",
        "SST-VAL020",
        "SST-VAL020",
        "SST-VAL020",
        "SST-VAL020",
        "SST-PLN033",
    ]


def test_run_validates_against_snowflake_when_connected() -> None:
    candidates, use_case, _ = select(compiled(), connected=True)
    port = FakeSnowflake()
    assert isinstance(candidates, PlanCandidates)

    ready = use_case.run(candidates, port, InMemoryStateStore(), target=dev_target(), state_table=STATE_TABLE)

    assert isinstance(ready, PlanReady)
    assert any(sql.startswith("EXPLAIN ") for sql, _ in port.queries)
