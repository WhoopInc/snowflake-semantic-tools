"""The plan use case, driven by in-memory ports: what `select` refuses offline, and what `run` plans."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.manifest import manifest_for
from snowflake_semantic_tools.app.plan import PreparePlan
from snowflake_semantic_tools.app.plan_values import PlanCandidates, PlanReady, PlanRefused, PlanScope
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag, Origin, Severity
from snowflake_semantic_tools.domain.diagnostics.baseline import Baseline, baseline_entry
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import ShowRow
from snowflake_semantic_tools.domain.ports.project import ValidationDefaults
from snowflake_semantic_tools.domain.state import AppliedEntry, State
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.clocks import FixedClock
from tests.helpers.compile_builders import compiled, with_diagnostics
from tests.helpers.cowork_profiles import LISTED, compile_profiles, cowork_profile, described_agent
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

    assert refused == PlanRefused(
        DiagnosticBag((D("SST-DIS010", selector="nope"),)),
        reason="selectors ('nope',) matched no artifact in the-project",
    )
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

    stale = D("SST-MAN009", command="plan or apply")
    assert refused == PlanRefused(DiagnosticBag((stale,)), reason=stale.message)
    assert stale.message == "compiled SST manifest is stale; run sst compile before plan or apply"


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
    assert set(ready.lifecycle_handlers) == {"eval", "skill", "plugin", "coco_desktop_profile"}
    assert ready.changeset.observation_at == "2026-01-01T00:00:01Z"
    assert ready.changeset.diagnostics[0].code == "SST-MAN027"
    assert "OTHER.SCHEMA" in port.scopes and "DB.SCH" in port.scopes
    agent_change = next(change for change in ready.changeset.changes if change.artifact_type == "agent")
    assert agent_change.rendered is not None
    assert "@SPECS.SCH.AGENT_STAGE/helper/abc1234" in "\n".join(texts(agent_change.rendered.statements))
    assert ready.state.applied == {"semantic_view:gone": gone}
    # Clean, so no version is labeled dirty.
    assert "-dirty" not in "\n".join(texts(agent_change.rendered.statements))
    assert inputs.reads[-3:] == ["config", "git_dirty", "git_sha"]


def test_a_plan_from_uncommitted_files_labels_each_agent_version_dirty_and_warns() -> None:
    tree: dict[str, object] = {"apply": {"agent_spec_stage": {"database": "SPECS", "stage": "AGENT_STAGE"}}}
    inputs = InMemoryProjectInputs(tree=tree, uncommitted=("agents/helper/agent.yml",))
    candidates, use_case, _ = select(compiled(tree=tree, agents=("helper",)), inputs=inputs)
    store = InMemoryStateStore(State.empty(dev_target()))
    assert isinstance(candidates, PlanCandidates)

    ready = use_case.run(candidates, ScopeRecordingSnowflake(), store, target=dev_target(), state_table=STATE_TABLE)

    assert isinstance(ready, PlanReady)
    agent_change = next(change for change in ready.changeset.changes if change.artifact_type == "agent")
    assert agent_change.rendered is not None
    statements = "\n".join(texts(agent_change.rendered.statements))
    # The stage path keeps the sha; the version names it dirty.
    assert "@SPECS.SCH.AGENT_STAGE/helper/abc1234/" in statements
    assert "'git:abc1234-dirty sst-spec:" in statements
    [warning] = [item for item in ready.changeset.diagnostics if item.code == "SST-PLN042"]
    assert "(agents/helper/agent.yml), so the 1 agent version(s)" in warning.message


def test_a_cowork_profile_is_staged_under_its_own_folder_and_never_made_temporary() -> None:
    # CoWork profiles are a Snowflake private preview; a profile publishes as a staged AGENT.
    stage = {"agent_spec_stage": {"database": "SPECS", "stage": "AGENT_STAGE"}}
    tree: dict[str, object] = {**LISTED, "apply": stage}
    result = compile_profiles(cowork_profile(), agents=(described_agent(),), tree=tree)
    candidates, use_case, _ = select(result, inputs=InMemoryProjectInputs(tree=tree))
    assert isinstance(candidates, PlanCandidates)
    for temporary in (False, True):
        store = InMemoryStateStore(State.empty(dev_target()))
        ready = use_case.run(
            candidates,
            ScopeRecordingSnowflake(),
            store,
            target=dev_target(),
            state_table=STATE_TABLE,
            temporary=temporary,
        )
        assert isinstance(ready, PlanReady)
        changes = {change.artifact_type: change for change in ready.changeset.changes}
        rendered = changes["cowork_profile"].rendered
        assert rendered is not None and not rendered.temporary
        assert "FROM @SPECS.SCH.AGENT_STAGE/cowork-profiles/everyone/abc1234/" in "\n".join(texts(rendered.statements))
        assert rendered.upload_path == "@SPECS.SCH.AGENT_STAGE/cowork-profiles/everyone/abc1234/agent_spec.yaml"
        assert rendered.upload_stage == QualifiedName.parse("SPECS.SCH.AGENT_STAGE")
        agent = changes["agent"].rendered
        assert agent is not None and agent.temporary is temporary


def test_a_strict_plan_is_not_refused_over_a_local_state_cache_the_table_overrides() -> None:
    # Strict promotes every warning; a stale cache is rewritten from the table, so it is only info.
    candidates, use_case, _ = select(compiled(), strict=True)
    port = ScopeRecordingSnowflake()
    gone = AppliedEntry("f" * 64, "OTHER.SCHEMA.GONE", "now", "run", "applied", "f" * 64, "m" * 64)
    port.remote_state = MappingProxyType({"semantic_view:gone": gone})
    assert isinstance(candidates, PlanCandidates) and candidates.strict

    ready = use_case.run(
        candidates, port, InMemoryStateStore(State.empty(dev_target())), target=dev_target(), state_table=STATE_TABLE
    )

    assert isinstance(ready, PlanReady)
    [notice] = [item for item in ready.changeset.diagnostics if item.code == "SST-MAN027"]
    assert notice.severity is Severity.INFO and ready.promoted == 0


def _strict_plan(*warnings: object) -> PlanReady | PlanRefused:
    """Plan MENU alone, strictly, from a compile of MENU and SALES reporting `warnings`."""
    result = with_diagnostics(compiled("MENU", "SALES"), *warnings)
    menu_only = PlanScope(("menu",), None, frozenset(("semantic_view:menu",)), None, None, False)
    candidates, use_case, _ = select(result, scope=menu_only, strict=True)
    assert isinstance(candidates, PlanCandidates)
    return use_case.run(candidates, FakeSnowflake(), InMemoryStateStore(), target=dev_target(), state_table=STATE_TABLE)


def test_strict_leaves_a_warning_about_an_unselected_artifact_a_warning() -> None:
    ready = _strict_plan(D("SST-LOD003", file="sales.yml", subject="semantic_view:sales"))
    assert isinstance(ready, PlanReady)
    [warning] = [item for item in ready.result.diagnostics if item.code == "SST-LOD003"]
    assert warning.severity is Severity.WARNING
    assert [change.key for change in ready.changeset.changes] == ["semantic_view:menu"]


def test_strict_promotes_a_warning_about_a_selected_artifact_or_the_whole_project() -> None:
    for subject in ("semantic_view:menu", None, "config:validation.strict", "tool_ref:partner"):
        refused = _strict_plan(D("SST-LOD003", file="a.yml", subject=subject))
        assert isinstance(refused, PlanRefused), subject
        assert [(item.code, item.severity) for item in refused.diagnostics][0] == ("SST-LOD003", Severity.ERROR)


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
        ("SST-VAL020", Severity.INFO),
        ("SST-VAL020", Severity.INFO),
        ("SST-VAL020", Severity.INFO),
    ]
    assert port.queries == [] and store.writes == []


def test_run_plans_past_a_promoted_warning_the_baseline_holds_and_refuses_once_it_has_expired() -> None:
    warning = D("SST-LOD003", file="warning.yml")
    warned = with_diagnostics(compiled(), warning)
    held = Baseline(".sst/baseline.json", "2027-01-01", (baseline_entry(warning, "known"),))

    def run(baseline: Baseline) -> PlanReady | PlanRefused:
        use_case = PreparePlan(InMemoryProjectInputs(), FixedClock(), baseline=baseline)
        candidates = use_case.select(
            warned,
            manifest_for(warned, EMPTY_SOURCES),
            EVERYTHING,
            partial=False,
            strict=True,
            connected=False,
            project="p",
        )
        assert isinstance(candidates, PlanCandidates)
        return use_case.run(
            candidates, FakeSnowflake(), InMemoryStateStore(), target=dev_target(), state_table=STATE_TABLE
        )

    assert isinstance(run(held), PlanReady)
    assert isinstance(run(replace(held, expires_on="2025-01-01")), PlanRefused)


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


def test_run_reports_a_state_table_of_the_other_kind_than_configured() -> None:
    candidates, use_case, _ = select(compiled())
    port = FakeSnowflake()
    port.preflight.table_kinds = {STATE_TABLE.sql: "standard"}
    assert isinstance(candidates, PlanCandidates)

    def planned(kind: str) -> PlanReady | PlanRefused:
        store = InMemoryStateStore(State.empty(dev_target()))
        return use_case.run(
            candidates, port, store, target=dev_target(), state_table=STATE_TABLE, state_table_kind=kind, preflight=port
        )

    hybrid, standard = planned("hybrid"), planned("standard")
    assert isinstance(hybrid, PlanReady) and isinstance(standard, PlanReady)
    [refused] = [item for item in hybrid.changeset.diagnostics if item.code == "SST-APL031"]
    assert refused.message == "DB.SCH.SST_STATE is a standard table, and state.+table_kind says hybrid"
    assert "SST-APL031" not in [item.code for item in standard.changeset.diagnostics]
