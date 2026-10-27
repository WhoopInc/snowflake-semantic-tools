"""A skill's or plugin's default version is the version its project declares, after every apply.

Snowflake serves an extension's default version unless one is certified, and leaves a default
set by hand where it is when a version is added, so apply points the default at the version
holding the declared alias whenever no certified version outranks it.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.apply import ApplyArtifacts
from snowflake_semantic_tools.app.compile.skills import CompiledExtension, CompileSkills
from snowflake_semantic_tools.app.lifecycle.extensions import ExtensionLifecycleHandler
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.app.plan_artifacts import PlanArtifacts
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    ApplyOptions,
    ApplyResult,
    ChangeReason,
    ChangeSet,
    OutcomeStatus,
)
from snowflake_semantic_tools.domain.model.skill import SkillCatalog
from snowflake_semantic_tools.domain.state import State
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.artifact_builders import empty_state, target
from tests.helpers.clocks import FixedClock
from tests.helpers.skill_inputs import compile_extensions, plugin, skill, skill_catalog
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.sql_values import statement

SKILL = QualifiedName.parse("DB.S.MONTH_CLOSE")
KIT = QualifiedName.parse("DB.S.FINANCE_KIT")
FIRST = compile_extensions(skill_catalog(skill()))
SECOND = compile_extensions(skill_catalog(skill(files={"reference/steps.md": "Steps, revised.\n"})))
THIRD = compile_extensions(skill_catalog(skill(files={"reference/steps.md": "Steps, again.\n"})))


def _handlers(port: FakeSnowflake, compiled: dict[str, CompiledExtension]) -> dict[str, ExtensionLifecycleHandler]:
    releases = {key: item.release for key, item in compiled.items()}
    return {kind: ExtensionLifecycleHandler(port, releases, kind) for kind in ("skill", "plugin")}


def _plan(port: FakeSnowflake, compiled: dict[str, CompiledExtension], previous: State) -> ChangeSet:
    rendered = {key: item.rendered_artifact for key, item in compiled.items()}
    manifest = build_manifest(CompileSkills(SkillCatalog(), None).run_result())
    return PlanArtifacts(port, lifecycle_handlers=_handlers(port, compiled)).run(
        rendered, manifest, previous, target(), fetched_at="now"
    )


def _apply(
    port: FakeSnowflake, compiled: dict[str, CompiledExtension], changeset: ChangeSet, previous: State
) -> tuple[ApplyResult, State]:
    store = InMemoryStateStore(previous)
    result = ApplyArtifacts(
        port,
        store,
        FixedClock(),
        state_table=QualifiedName.parse("DB.S.SST_STATE"),
        lifecycle_handlers=_handlers(port, compiled),
    ).run(changeset, previous, ApplyOptions())
    return result, store.state or previous


def _publish(
    port: FakeSnowflake, compiled: dict[str, CompiledExtension], previous: State
) -> tuple[ChangeSet, ApplyResult, State]:
    changeset = _plan(port, compiled, previous)
    result, state = _apply(port, compiled, changeset, previous)
    assert result.success, [outcome.error for outcome in result.outcomes]
    return changeset, result, state


def _default(port: FakeSnowflake, name: QualifiedName = SKILL) -> list[str]:
    return [item.name for item in port.extension_versions(name) if item.is_default]


def _pin(port: FakeSnowflake, version: str, name: QualifiedName = SKILL) -> None:
    """Set the default by hand, as an operator would."""
    sql = statement(f"ALTER CORTEX EXTENSION {name.sql} SET DEFAULT_VERSION = {version}")
    assert port.execute_script((sql,)).ok


def _sent_since(port: FakeSnowflake, count: int) -> list[str]:
    return [item for script in port.scripts[count:] for item in script]


def _codes(changeset: ChangeSet) -> list[str]:
    return [item.code for item in changeset.diagnostics]


def test_a_default_set_by_hand_plans_as_an_update_that_points_it_back() -> None:
    port = FakeSnowflake(existing=())
    _, _, first = _publish(port, FIRST, empty_state())
    _, _, second = _publish(port, SECOND, first)
    _pin(port, "VERSION$1")
    changeset = _plan(port, SECOND, second)
    assert [(change.action, change.reason) for change in changeset.changes] == [
        (Action.UPDATE, ChangeReason.DEFAULT_VERSION_DIFFERS)
    ]
    assert _codes(changeset) == []
    sent = len(port.scripts)
    result, repaired = _apply(port, SECOND, changeset, second)
    assert result.success
    assert _sent_since(port, sent) == ["ALTER CORTEX EXTENSION DB.S.MONTH_CLOSE SET DEFAULT_VERSION = 'VERSION$2'"]
    assert _default(port) == ["VERSION$2"]
    assert [change.action for change in _plan(port, SECOND, repaired).changes] == [Action.NOOP]


def test_every_publish_after_a_pinned_default_becomes_the_default() -> None:
    port = FakeSnowflake(existing=())
    _, _, first = _publish(port, FIRST, empty_state())
    _pin(port, "'VERSION$1'")
    sent = len(port.scripts)
    _, _, second = _publish(port, SECOND, first)
    statements = _sent_since(port, sent)
    # ADD VERSION leaves a pinned default where it was, so apply moves it.
    assert statements[0].startswith("ALTER CORTEX EXTENSION DB.S.MONTH_CLOSE ADD VERSION ")
    assert statements[-1] == "ALTER CORTEX EXTENSION DB.S.MONTH_CLOSE SET DEFAULT_VERSION = 'VERSION$2'"
    observed = port.observe_extension(SKILL)
    assert _default(port) == ["VERSION$2"] and observed is not None and observed.effective_version == "VERSION$2"
    # The default SST set is pinned too, and the next publish moves it again.
    _, _, third = _publish(port, THIRD, second)
    assert _default(port) == ["VERSION$3"]
    assert [change.action for change in _plan(port, THIRD, third).changes] == [Action.NOOP]


def test_a_default_that_moved_since_the_plan_fails_the_change_unwritten() -> None:
    port = FakeSnowflake(existing=())
    _, _, first = _publish(port, FIRST, empty_state())
    _, _, second = _publish(port, SECOND, first)
    _, _, third = _publish(port, THIRD, second)
    changeset = _plan(port, FIRST, third)
    assert [change.reason for change in changeset.changes] == [ChangeReason.DEFAULT_VERSION_DIFFERS]
    _pin(port, '"VERSION$2"')
    sent = len(port.scripts)
    result, _ = _apply(port, FIRST, changeset, third)
    [outcome] = result.outcomes
    assert outcome.status is OutcomeStatus.FAILED and outcome.error is not None
    assert (outcome.error.code, outcome.error.message) == ("SST-APL012", "DB.S.MONTH_CLOSE changed since the plan")
    assert _sent_since(port, sent) == [] and _default(port) == ["VERSION$2"]


def test_a_refused_or_unread_default_fails_the_change() -> None:
    refusing = FakeSnowflake(existing=())
    _, _, first = _publish(refusing, FIRST, empty_state())
    _, _, second = _publish(refusing, SECOND, first)
    refusing.refused = ("SET DEFAULT_VERSION",)
    result, _ = _apply(refusing, FIRST, _plan(refusing, FIRST, second), second)
    [outcome] = result.outcomes
    assert outcome.error is not None and outcome.error.code == "SST-APL001"
    assert outcome.error.message.startswith("ALTER CORTEX EXTENSION DB.S.MONTH_CLOSE SET DEFAULT_VERSION")
    assert not outcome.write_succeeded

    ignoring = FakeSnowflake(existing=())
    _, _, first = _publish(ignoring, FIRST, empty_state())
    _, _, second = _publish(ignoring, SECOND, first)
    record = ignoring._take_effect

    def ignore_default(statement: str) -> None:
        if "DEFAULT_VERSION" not in statement:
            record(statement)

    ignoring._take_effect = ignore_default  # type: ignore[method-assign]
    result, _ = _apply(ignoring, FIRST, _plan(ignoring, FIRST, second), second)
    [outcome] = result.outcomes
    assert outcome.error is not None
    assert (outcome.error.code, outcome.error.message) == (
        "SST-APL016",
        "VERSION$1 of DB.S.MONTH_CLOSE is not the default after SET DEFAULT_VERSION",
    )
    assert outcome.write_succeeded


def test_a_plugin_published_through_a_live_version_becomes_the_default() -> None:
    kit = compile_extensions(skill_catalog(skill(), plugins=(plugin(),)))
    changed = compile_extensions(
        skill_catalog(skill(files={"reference/steps.md": "Steps, revised.\n"}), plugins=(plugin(),))
    )
    only = {key: item for key, item in kit.items() if key.startswith("plugin:")}
    revised = {key: item for key, item in changed.items() if key.startswith("plugin:")}
    port = FakeSnowflake(existing=())
    _, _, first = _publish(port, only, empty_state())
    _pin(port, "VERSION$1", KIT)
    port.refused = ("ADD VERSION SST_",)
    sent = len(port.scripts)
    _publish(port, revised, first)
    statements = _sent_since(port, sent)
    assert statements[-2:] == [
        "ALTER CORTEX EXTENSION DB.S.FINANCE_KIT COMMIT",
        "ALTER CORTEX EXTENSION DB.S.FINANCE_KIT SET DEFAULT_VERSION = 'VERSION$2'",
    ]
    assert _default(port, KIT) == ["VERSION$2"]


def test_the_fake_takes_a_default_as_snowflake_does() -> None:
    # MEASURED on a live account: a system name, bare, quoted or a literal, and LAST are
    # accepted; an alias is refused; UNSET makes the newest version the default again.
    port = FakeSnowflake(existing=())
    _, _, first = _publish(port, FIRST, empty_state())
    _publish(port, SECOND, first)
    alias = FIRST["skill:month-close"].release.alias
    refused = port.execute_script(
        (statement(f"ALTER CORTEX EXTENSION DB.S.MONTH_CLOSE SET DEFAULT_VERSION = '{alias}'"),)
    )
    assert not refused.ok and refused.error is not None
    assert refused.error.message == "099103 (22023): Invalid default"
    for named in ("VERSION$1", '"VERSION$1"', "'VERSION$1'"):
        _pin(port, named)
        assert _default(port) == ["VERSION$1"]
    _pin(port, "LAST")
    _pin(port, "VERSION$1")
    assert port.execute_script((statement("ALTER CORTEX EXTENSION DB.S.MONTH_CLOSE UNSET DEFAULT_VERSION"),)).ok
    assert _default(port) == ["VERSION$2"]
    dropped = port.execute_script((statement("DROP CORTEX EXTENSION DB.S.MONTH_CLOSE"),))
    assert dropped.ok and port.observe_extension(SKILL) is None
    again = port.execute_script((statement("DROP CORTEX EXTENSION DB.S.MONTH_CLOSE"),))
    assert not again.ok and again.error is not None and "does not exist" in again.error.message
